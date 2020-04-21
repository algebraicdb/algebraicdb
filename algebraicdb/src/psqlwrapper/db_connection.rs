use std::error::Error;
use std::sync::Arc;
use tokio_postgres::{config::SslMode, Client, Config, NoTls};

pub async fn connect_db() -> Result<Arc<Client>, Box<dyn Error>> {
    let refcli = {
        let mut config = Config::new();

        config
            .user("postgres")
            .password("example")
            .host("localhost")
            .port(5432)
            .dbname("postgres")
            .ssl_mode(SslMode::Disable);
        let (client, bit_coooonnect) = config.connect(NoTls).await?;
        let refclii = Arc::new(client);
        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = bit_coooonnect.await {
                eprintln!("connection error: {}", e);
            }
        });
        refclii
    };

    Ok(refcli)
}
