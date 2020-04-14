use pin_utils::pin_mut;
use staticvec::StaticVec;
use std::future::Future;
use std::io::Write;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio::sync::mpsc;

const CHANNEL_CAP: usize = 255;
const BUFFER_CAP: usize = 255;

type Chunk = StaticVec<u8, BUFFER_CAP>;

pub struct Reader {
    buffer: Chunk,
    channel: mpsc::Receiver<Chunk>,
}

pub struct Writer {
    channel: mpsc::Sender<Chunk>,
}

/// Create a writer-reader pair
pub fn pair() -> (Writer, Reader) {
    let (tx, rx) = mpsc::channel(CHANNEL_CAP);

    let r = Reader {
        buffer: Chunk::new(),
        channel: rx,
    };

    let w = Writer { channel: tx };

    (w, r)
}

impl AsyncRead for Reader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        mut buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.buffer.len() > 0 {
            let n = buf.write(&self.buffer).unwrap();
            self.buffer.drain(..n);
            Poll::Ready(Ok(n))
        } else {
            self.buffer = {
                let read = self.channel.recv();
                pin_mut!(read);
                match read.poll(cx) {
                    Poll::Ready(r) => match r {
                        Some(chunk) => chunk,
                        None => return Poll::Ready(Ok(0)),
                    },
                    Poll::Pending => return Poll::Pending,
                }
            };
            let n = buf.write(&self.buffer).unwrap();
            self.buffer.drain(..n);
            Poll::Ready(Ok(n))
        }
    }
}

impl AsyncWrite for Writer {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut chunk = StaticVec::new();
        let n = chunk.write(buf).unwrap();

        let write = self.channel.send(chunk.clone());
        pin_mut!(write);
        match write.poll(cx) {
            Poll::Ready(r) => match r {
                Ok(()) => Poll::Ready(Ok(n)),
                Err(e) => Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, e))),
            },
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::pair;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_big_streams() {
        let (mut writer, mut reader) = pair();

        const MSG_COUNT: u64 = 1000000;

        let msg = |i: u8| [i, i.overflowing_add(2).0, i.overflowing_add(69).0];

        tokio::spawn(async move {
            for i in 0..MSG_COUNT {
                let i = i as u8;
                let buf = msg(i);
                writer.write_all(&buf).await.unwrap();
            }
        });

        for i in 0..MSG_COUNT {
            let i = i as u8;
            let expected = msg(i);
            let mut buf = [0u8; 3];
            reader.read_exact(&mut buf).await.unwrap();
            assert_eq!(&buf, &expected);
        }
    }

    #[tokio::test]
    async fn test_many_streams() {
        for i in 0..255u8 {
            let (mut writer, mut reader) = pair();
            tokio::spawn(async move {
                let buf = [i; 1000];
                writer.write_all(&buf).await.unwrap();
            });

            let mut buf = vec![];
            reader.read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf.len(), 1000);
            for i2 in buf {
                assert_eq!(i2, i);
            }
        }
    }

    #[tokio::test]
    async fn test_send_string() {
        let msg = b"Hello there good sir!\n";

        let (mut writer, mut reader) = pair();

        tokio::spawn(async move {
            writer.write_all(msg).await.unwrap();
        });

        let mut buf = vec![];
        reader.read_to_end(&mut buf).await.unwrap();

        assert_eq!(&buf, msg);
    }
}
