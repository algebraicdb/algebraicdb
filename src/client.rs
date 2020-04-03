use crate::executor::execute_query;
use crate::local;
use regex::Regex;
use std::error::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter};

#[cfg(not(feature = "wrapper"))]
pub type State = local::DbmsState;

#[cfg(feature = "wrapper")]
pub type State = local::PgWrapperState;

pub(crate) async fn client<R, W>(
    mut reader: R,
    writer: W,
    mut state: State,
) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let mut writer = BufWriter::new(writer);
    let mut buf = vec![];

    // This regex tokenizes the input string, and lets
    // us find the first non-quoted non-commented semicolon
    let r = Regex::new(
        r#"(?mx)
          (?P<string>    "([^"]|(\\.))*"?)
        | (?P<comment>   --.*$)
        | (?P<semicolon> ;)
        | (?P<other>     .)
    "#,
    )
    .expect("Invalid regex");

    loop {
        let _n: usize = match reader.read_buf(&mut buf).await? {
            // No bytes read means EOF was reached
            0 => return Ok(()),
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

            // Find the first semicolon, and take the entire string up to that character.
            let end = if let Some(semicolon) = r
                .captures_iter(input)
                .flat_map(|c| c.name("semicolon"))
                .next()
            {
                semicolon.end()
            } else {
                break;
            };
            let input = &input[..end];

            // Exectue the (semicolon-terminated) string as a query
            execute_query(input, &mut state, &mut writer).await?;

            writer.flush().await?;

            // Remove the string of the executed query from the buffer
            buf.drain(..end);
        }
    }
}
