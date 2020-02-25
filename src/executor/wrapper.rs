use crate::local::PgWrapperState;
use std::error::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub async fn execute_query(
    _input: &str,
    _state: &PgWrapperState,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
) -> Result<(), Box<dyn Error>> {
    writer.write_all(b"not implemented.\n").await?;
    Ok(())
}
