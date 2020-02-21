use crate::types::{TypeMap};
use crate::table::*;
use tokio_postgres::{Client,NoTls, Error};
use std::collections::HashMap;
pub struct Wrapper{
    client: Client,
    types: TypeMap,
    tables: TableMap,
}
pub type TableMap = HashMap<String, Table>;

pub async fn setup(types: TypeMap, tables: TableMap) -> Result<Wrapper, Error>{
    let (client, _) =
        tokio_postgres::connect("host=localhost user=postgres", NoTls).await?;

    
    Ok(Wrapper{client, types, tables})
}