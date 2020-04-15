use crate::executor::wrapper::execute_query;
use crate::local;
use regex::Regex;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

pub type State = local::WrapperState;

pub async fn client<R, W>(mut reader: R, writer: W, state: State) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut writer = BufWriter::new(writer);
    let mut buf = vec![];

    // This regex matches the entire string from the start to the first non-quoted semi-colon.
    // It also properly handles escaped quotes
    // valid string: SELECT "this is a quote -> \", this is a semicolon -> ;.";
    let r = Regex::new(r#"^(("((\\.)|[^"])*")|[^";])*;"#).expect("Invalid regex");

    loop {
        let _n: usize = match reader.read_buf(&mut buf).await? {
            // No bytes read means EOF was reached
            0 => {
                return Ok(());
            }
            // Read n bytes
            n => n,
        };

        // Loop over every statement (every substring ending with a semicolon)
        // This leaves the remaining un-terminated string in the buf.
        //   stmt 1         stmt 2           stmt 3   rest
        // ┍╌╌╌┷╌╌╌┑┍╌╌╌╌╌╌╌╌╌┷╌╌╌╌╌╌╌╌╌╌┑┍╌╌╌╌┷╌╌╌╌┑┍╌┷╌┑
        // SELECT 1; SELECT "stuff: \" ;";  SELECT 3; SELE
        loop {
            // Validate bytes as utf-8 string
            let input = match std::str::from_utf8(&buf[..]) {
                Ok(input) => input,
                Err(e) => {
                    writer
                        .write_all(format!("Error: {}\n", e).as_bytes())
                        .await?;
                    writer.flush().await?;
                    return Err(e.into());
                }
            };

            // Match string against regex
            let (input, end) = match r.find(input) {
                Some(matches) => (matches.as_str(), matches.end()),
                None => break,
            };

            execute_query(input, &state, &mut writer).await?;
            writer.flush().await?;

            // Remove the string of the executed query from the buffer
            buf.drain(..end);
        }
    }
}
