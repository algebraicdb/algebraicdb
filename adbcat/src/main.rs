#![recursion_limit = "1024"]

mod tokenizer;

use crate::tokenizer::{TokenType, Tokenizer};
use futures::executor::block_on;
use futures::{select, FutureExt};
use std::error::Error;
use std::io::{stdin, stdout};
use std::thread;
use structopt::StructOpt;
use termion::event::Key;
use termion::input::TermRead;
use termion::raw::IntoRawMode;
use tokio::prelude::*;
use tokio::sync::mpsc;
use tui::backend::TermionBackend;
use tui::layout::{Constraint, Direction, Layout};
use tui::style::{Color, Style};
use tui::widgets::{Block, Borders, Paragraph, Text, Widget};
use tui::Terminal;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// The host address to connect to
    #[structopt(short, long, default_value = "localhost")]
    host: String,

    /// The port of the host to connect to
    #[structopt(short, long, default_value = "2345")]
    port: u16,
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
    Tokenizer::from(input).map(|(word, tt)| match tt {
        TokenType::Keyword => Text::styled(word.to_owned(), Style::default().fg(Color::Blue)),
        TokenType::Number => Text::styled(word.to_owned(), Style::default().fg(Color::Red)),
        TokenType::Symbol => Text::styled(word.to_owned(), Style::default().fg(Color::Yellow)),
        TokenType::String => Text::styled(word.to_owned(), Style::default().fg(Color::Red)),
        TokenType::Word | TokenType::Whitespace => Text::raw(word.to_owned()),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let opt = Opt::from_args();

    let mut stream = tokio::net::TcpStream::connect((opt.host.as_str(), opt.port)).await?;

    let stdout = stdout().into_raw_mode()?;
    let backend = TermionBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut stream_buf = [0; 4096];

    let mut line = String::new();

    let default_style: Style = Default::default();
    let response_style = default_style.fg(Color::Green);

    let mut history: Vec<String> = vec![];
    let mut in_history: Option<usize> = None;

    let mut console: Vec<Text> = vec![];
    let mut console_len = 0;

    let line_breaks = |c| ['\n', '\r'].contains(&c);

    terminal.hide_cursor()?;
    terminal.clear()?;

    let (sender, mut inputs) = mpsc::channel(255);

    thread::spawn(move || match input_handler(sender) {
        Err(e) => eprintln!("{}", e),
        Ok(()) => {}
    });

    loop {
        terminal.draw(|mut f| {
            let line_count = line.split(&line_breaks).count();
            let mut output: Vec<_> = vec![Text::raw("> ")];
            highlight_syntax(&line).for_each(|word| output.push(word));
            output.push(Text::styled(
                " ",
                Style::default().bg(Color::Rgb(255, 255, 255)),
            ));

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints(
                    [
                        Constraint::Min(0),
                        Constraint::Length(line_count as u16 + 2),
                    ]
                    .as_ref(),
                )
                .split(f.size());

            Paragraph::new(console.iter())
                .wrap(true)
                .block(
                    Block::default()
                        .title(&format!("Connected to {}:{}", opt.host, opt.port))
                        .borders(Borders::ALL),
                )
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

            Paragraph::new(output.iter())
                .wrap(true)
                //.raw(true)
                .block(
                    Block::default()
                        //.title("Block 2")
                        .borders(Borders::ALL & !Borders::TOP),
                )
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
                    Some(Key::Backspace) => {
                        line.pop();
                    }
                    Some(unknown_key) => {
                        //console.push(Text::styled(format!("? {:?}\n", unknown_key), Style::default().fg(Color::Red)));
                        //console_len += 1;
                    }
                }
            },
            n = stream_read => {
                let n = n?;
                let s = std::str::from_utf8(&stream_buf[..n]).unwrap();
                for l in s.lines() {
                    console.push(Text::styled("< ", response_style));
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
