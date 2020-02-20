use futures::executor::block_on;
use std::error::Error;
use std::io::{self, BufWriter, Write};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

struct BlockingWriter<W: AsyncWrite + Unpin> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> BlockingWriter<W> {
    pub fn new(writer: W) -> Self {
        BlockingWriter { writer }
    }
}

impl<W: AsyncWrite + Unpin> Write for BlockingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        block_on(self.writer.write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        block_on(self.writer.flush())
    }
}

pub async fn tcp_api(
    func: fn(&str, &mut dyn Write) -> Result<(), Box<dyn Error>>,
    address: String,
) -> Result<!, Box<dyn Error>> {
    let mut listener = TcpListener::bind(address).await?;

    loop {
        match listener.accept().await {
            Ok((mut socket, _)) => {
                tokio::spawn(async move {
                    let (reader, writer) = socket.split();
                    let mut writer = BufWriter::new(BlockingWriter::new(writer));
                    let mut buf = vec![];
                    let mut rest = String::new();
                    let mut reader: BufReader<_> = BufReader::new(reader);

                    loop {
                        let n: usize = reader.read_until(b';', &mut buf).await.unwrap();

                        let input = std::str::from_utf8(&buf[..n]).expect("Not valid utf-8");

                        rest.push_str(input);
                        rest = conga(func, input, &mut writer);
                        writer.flush().expect("Flushing writer failed");

                        // TODO: fix for unicode
                        buf.drain(..n);
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

// CONGA FIX EVERYTHING
fn conga(
    func: fn(&str, &mut dyn Write) -> Result<(), Box<dyn Error>>,
    stmt: &str,
    w: &mut dyn Write,
) -> String {
    let mut in_string = false;
    let mut lasti = 0;
    let chars = stmt.chars().enumerate();

    for (i, ch) in chars {
        // TODO: Handle escape characters
        if ch == '"' {
            in_string = !in_string;
        }

        if ch == ';' && !in_string {
            let q = &stmt[lasti..=i];
            func(q, w).expect("Query errored");
            lasti = i + 1;
        }
    }

    if stmt.len() == 0 {
        String::new()
    } else if lasti != (stmt.len() - 1) {
        String::from(&stmt[lasti..stmt.len()])
    } else {
        String::new()
    }
}

#[cfg(test)]
pub mod tests {

    use super::conga;
    use std::error::Error;
    use std::io::Write;

    #[test]
    pub fn test_conga() {
        let s1 = "SELECT dsdasd FROM dadasd".to_string();
        let s2 = "SELECT dasdas FROM dasdasd; INSERT dadasd into sdadad;".to_string();

        let mut r1: Vec<u8> = vec![];
        let rest1 = conga(always_success, &s1, &mut r1);

        assert!(r1.is_empty());
        assert_eq!(rest1, s1);

        let mut r2: Vec<u8> = vec![];
        let rest2 = conga(always_success, &s2, &mut r2);

        assert!(!r2.is_empty());
        assert_eq!(rest2, "");
    }

    fn always_success(_: &str, w: &mut dyn Write) -> Result<(), Box<dyn Error>> {
        w.write_all("Success".as_bytes())?;
        Ok(())
    }
}
