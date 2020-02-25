mod dbms_state;
mod types;

#[cfg(test)]
mod tests;

pub use self::dbms_state::*;
pub use self::types::*;
use crate::table::Schema;
use async_trait::async_trait;

pub trait TTable {
    fn get_schema(&self) -> &Schema;
}

#[async_trait]
pub trait DbState<T>
where
    T: TTable,
{
    async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources<T>, String>;
    async fn create_table(&self, name: String, table: T) -> Result<(), ()>;
}

pub struct PgWrapperState {}