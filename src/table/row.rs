use crate::types::TypeMap;
pub use super::{Cell, Schema};

pub struct Row<'tb> {
    schema: &'tb Schema,
    pub data: &'tb [u8],
}

impl<'tb> Row<'tb> {
    pub fn new(schema: &'tb Schema, data: &'tb [u8]) -> Self {
        Row {
            schema,
            data,
        }
    }

    pub fn get_cell<'ts>(&'tb self, types: &'ts TypeMap, col: usize) -> Cell<'ts, 'tb> {
        let mut start = 0;
        for (_, t_id) in &self.schema.columns[..col] {
            let t = &types[t_id];
            let t_size = t.size_of(types);
            start += t_size;
        }

        let end = start + types[&self.schema.columns[col].1].size_of(types);

        Cell::new(
            self.schema.columns[col].1,
            &self.data[start..end],
            types,
        )
    }

    pub fn cell_count(&self) -> usize {
        self.schema.len()
    }
}
