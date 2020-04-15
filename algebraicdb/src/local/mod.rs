mod dbms_state;
pub mod types;
mod wrapper_state;

#[cfg(test)]
mod tests;

pub use self::dbms_state::*;
pub use self::types::*;
pub use self::wrapper_state::*;
use crate::table::Schema;
use async_trait::async_trait;

pub trait TTable {
    fn get_schema(&self) -> &Schema;
}

impl TTable for Schema {
    fn get_schema(&self) -> &Schema {
        return &self;
    }
}
#[async_trait]
pub trait DbState<T>
where
    T: TTable,
{
    async fn acquire_resources(&self, acquire: Acquire) -> Result<Resources<T>, String>;
    async fn create_table(&self, name: String, table: T) -> Result<(), ()>;
}
