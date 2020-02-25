use crate::client::{client, State};
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};

/// Start an instance of the dbms accepting raw queries through AsyncRead and AsyncWrite.
pub async fn create_with_writers<W, R>(reader: R, writer: W) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let state = State::new();
    client(reader, writer, state).await
}
