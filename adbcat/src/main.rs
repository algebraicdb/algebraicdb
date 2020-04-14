#![recursion_limit = "4096"]

mod tokenizer;

use crate::tokenizer::{TokenType, Tokenizer};
use futures::executor::block_on;
use futures::{select, FutureExt};
use std::error::Error;
use std::thread;
use std::{
    io::{stdin, stdout},
    path::PathBuf,
};
use structopt::StructOpt;
use termion::event::Key;
use termion::input::TermRead;
use termion::raw::IntoRawMode;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixStream};
use tokio::prelude::*;
use tokio::sync::mpsc;
use tui::backend::TermionBackend;
use tui::layout::{Constraint, Direction, Layout};
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders, Paragraph, Text, Widget};
use tui::Terminal;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
enum Opt {
    Tcp {
        /// The host address to connect to
        #[structopt(short, long, default_value = "localhost")]
        host: String,

        /// The port of the host to connect to
        #[structopt(short, long, default_value = "2345")]
        port: u16,
    },

    Uds {
        /// Unix domain socket
        #[structopt(short, long, default_value = "/tmp/adbsocket")]
        socket: PathBuf,
    },
}

fn input_handler(mut sender: mpsc::Sender<Key>) -> Result<(), Box<dyn Error>> {
    let stdin = stdin();
    let stdin = stdin.lock();

    for key in stdin.keys() {
        block_on(sender.send(key?))?;
    }

    Ok(())
}

fn highlight_syntax<'a>(input: &'a str) -> impl Iterator<Item = Text<'static>> + 'a {
    Tokenizer::from(input).map(|(word, tt)| {
        let style = match tt {
            TokenType::Keyword => Style::default().fg(Color::Blue),
            TokenType::Number => Style::default().fg(Color::Red),
            TokenType::Symbol => Style::default().fg(Color::Yellow),
            TokenType::String => Style::default().fg(Color::Red),
            TokenType::Word | TokenType::Whitespace => Style::default(),
        };
        Text::styled(word.to_owned(), style)
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Command-line arguments
    let opt = Opt::from_args();
    match &opt {
        Opt::Tcp { host, port } => {
            let stream = TcpStream::connect((host.as_str(), *port)).await?;
            run(stream, &opt).await
        }
        Opt::Uds { socket } => {
            let stream = UnixStream::connect(socket).await?;
            run(stream, &opt).await
        }
    }
}

async fn run<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    opt: &Opt,
) -> Result<(), Box<dyn Error>> {
    // Terminal output
    let stdout = stdout().into_raw_mode()?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut stream_buf = [0u8; 4096];

    // The current input line
    let mut line = String::new();

    // The history of previously entered commands
    let mut history: Vec<String> = vec![];

    // If the user has Up and is browsing the history, this will be Some(i) for history[i]
    let mut in_history: Option<usize> = None;

    // The stream of commands & db responses
    let mut console: Vec<Text> = vec![];

    // The number of actual entries in console; the number of commands enters + the number of lines
    // read from the network stream.
    let mut console_len = 0;

    terminal.hide_cursor()?;
    terminal.clear()?;

    let (sender, mut inputs) = mpsc::channel(255);

    thread::spawn(move || match input_handler(sender) {
        Err(e) => panic!("{}", e),
        Ok(()) => {}
    });

    loop {
        terminal.draw(|mut f| {
            let mut output: Vec<_> = vec![Text::raw("> ")];
            highlight_syntax(&line).for_each(|word| output.push(word));
            output.push(Text::styled(
                " ",
                Style::default().bg(Color::Rgb(255, 255, 255)),
            ));

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([Constraint::Min(0), Constraint::Length(3)].as_ref())
                .split(f.size());

            Paragraph::new(console.iter())
                .wrap(false) // FIXME: wrapping doesn't work with the scrollback logic
                .block(
                    Block::default()
                        .title(&format!(
                            "Connected to {}",
                            match opt {
                                Opt::Tcp { host, port } => format!("address: {}:{}", host, port),
                                Opt::Uds { socket } => format!("socket: {:?}", socket),
                            }
                        ))
                        .borders(Borders::ALL),
                )
                // When the console screen fills up, we scroll to the bottom so that the latest
                // entries are always visible.
                .scroll({
                    let maxh = chunks[0].height;
                    let h = console_len + 2;
                    if h > maxh {
                        h - maxh
                    } else {
                        0
                    }
                })
                .render(&mut f, chunks[0]);

            // FIXME: this buffer should expand if the input line gets large enough.
            Paragraph::new(output.iter())
                .wrap(true)
                .block(Block::default().borders(Borders::ALL & !Borders::TOP))
                .render(&mut f, chunks[1])
        })?;

        let mut stream_read = stream.read(&mut stream_buf).fuse();

        select! {
            key = inputs.recv().fuse() => {
                match key {
                    None |
                    Some(Key::Ctrl('c')) |
                    Some(Key::Ctrl('d')) => {
                        terminal.clear()?;
                        return Ok(());
                    }

                    Some(Key::Char('\n')) => {
                        stream.write_all(line.as_bytes()).await?;
                        console.push(Text::styled("> ", Style::default().fg(Color::Blue)));
                        for word in highlight_syntax(&line) {
                            console.push(word);
                        }
                        console.push(Text::raw("\n"));

                        match in_history {
                            Some(i) if line == history[i] => {},
                            _ => history.push(line.clone()),
                        }
                        in_history = None;

                        console_len += 1;
                        line.clear();
                    }
                    Some(Key::Char(c)) => {
                        line.push(c);
                    }
                    Some(Key::Down) => {
                        in_history = match in_history {
                            Some(i) if i + 1 < history.len() => Some(i + 1),
                            None | Some(_) => None,
                        };
                        match in_history {
                            Some(i) => line = history[i].clone(),
                            None => line.clear(),
                        }
                    }
                    Some(Key::Up) => {
                        in_history = match in_history {
                            Some(i) if i > 0 => Some(i - 1),
                            None if history.len() > 0 => Some(history.len() - 1),
                            ih => ih,
                        };
                        match in_history {
                            Some(i) => line = history[i].clone(),
                            None => line.clear(),
                        }
                    }
                    Some(Key::Ctrl('h')) | Some(Key::Backspace) => {
                        line.pop();
                    }
                    Some(unknown_key) => {/*
                        console.push(Text::styled(
                                format!("? {:?}\n", unknown_key),
                                Style::default().fg(Color::Red)
                        ));
                        console_len += 1;
                        */
                    }
                }
            },
            n = stream_read => {
                let n = n?;
                let s = std::str::from_utf8(&stream_buf[..n]).unwrap();
                for l in s.lines() {
                    console.push(Text::styled("< ", Style::default().fg(Color::Green)));
                    for word in highlight_syntax(l) {
                        console.push(word);
                    }
                    console.push(Text::raw("\n"));
                    console_len += 1;
                }
            },
        }
    }
}
