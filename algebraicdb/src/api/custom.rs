use crate::api::config::DbmsConfig;
use crate::{client::client, state::DbmsState};
use std::error::Error;
use tokio::io::{AsyncRead, AsyncWrite};

/// Start an instance of the dbms accepting raw queries through AsyncRead and AsyncWrite.
pub async fn create_with_writers<W, R>(
    reader: R,
    writer: W,
    dbms_config: DbmsConfig,
) -> Result<(), Box<dyn Error>>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    let state = DbmsState::new(dbms_config).await;
    client(reader, writer, state).await
}
