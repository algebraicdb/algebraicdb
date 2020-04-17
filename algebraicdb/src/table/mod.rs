mod cell;
mod iter;
mod row;
mod schema;

pub use self::cell::Cell;
pub use self::iter::RowIter;
pub use self::row::Row;
pub use self::schema::Schema;

//use crate::state::TTable;
use crate::types::{TypeId, TypeMap, Value};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct Column {
    col_type: TypeId,
    name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableData {
    pub data: Vec<u8>,
    pub row_size: usize,
}


/// Constructed from existing schema and tabledata
pub struct Table<'a> {
    pub schema: &'a Schema,
    pub table_data: &'a TableData,
}

//impl TTable for Table {
//    fn get_schema(&self) -> &Schema {
//        &self.schema
//    }
//}

impl TableData {
    pub fn new(schema: &Schema, types: &TypeMap) -> Self {
        Self {
            data: vec![],
            row_size: schema
                .columns
                .iter()
                .map(|(_, t_id)| &types[t_id])
                .map(|t| t.size_of(types))
                .sum(),
            //schema,
        }
    }
}
impl TableData {

    pub fn row_count(&self) -> usize {
        assert_eq!(self.data.len() % self.row_size, 0);

        self.data.len() / self.row_size
    }

    fn row_start(&self, row: usize) -> usize {
        row * self.row_size
    }

    pub fn push_row_bytes(&mut self, row: &[u8]) {
        assert_eq!(row.len(), self.row_size);
        self.data.extend_from_slice(row);
    }

    pub fn push_row(&mut self, cells: &[Value], schema: &Schema, types: &TypeMap) {
        assert_eq!(self.data.len() % self.row_size, 0);
        for (t_id, value) in schema.columns.iter().map(|(_, t)| t).zip(cells.iter()) {
            value.to_bytes(&mut self.data, types, &types[t_id])
        }

        assert_eq!(self.data.len() % self.row_size, 0);
    }
    
}



impl<'a> Table<'a> {

    pub fn iter(&'a self) -> RowIter<'a> {
        RowIter::new(self)
    }
    pub fn get_row(&'a self, row: usize) -> Row<'a> {
        let start = self.table_data.row_start(row);
        let end = start + self.table_data.row_size;

        Row::new(&self.schema, &self.table_data.data[start..end])
    }
    pub fn new(schema: &'a Schema, table_data: &'a TableData) -> Self {
        Self {
            schema,
            table_data,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_row_value(&self, row: usize, types: &TypeMap) -> Vec<Value> {
        let mut output = vec![];
        let mut data = &self.table_data.data[self.table_data.row_start(row)..];
        for (_, t_id) in self.schema.columns.iter() {
            let t = &types[t_id];
            let t_size = t.size_of(types);
            output.push(t.from_bytes(&data[..t_size], types).unwrap());
            data = &data[t_size..];
        }

        output
    }

}


 /*
#[cfg(test)]
pub mod tests {





    use super::*;
    use crate::types::{BaseType, Type, TypeMap, Value};

    pub struct TestTypeIds {
        int_id: TypeId,
        bool_id: TypeId,
        double_id: TypeId,
        int_or_nil_id: TypeId,
        big_type_id: TypeId,
        bigger_type_id: TypeId,
    }

    pub fn create_type_map() -> (TestTypeIds, TypeMap) {
        let mut types = TypeMap::new();
        let int_id = types.get_base_id(BaseType::Integer);
        let bool_id = types.get_base_id(BaseType::Bool);
        let double_id = types.get_base_id(BaseType::Double);
        let int_or_nil_id = types.insert(
            "IntOrNil",
            Type::Sum(vec![("Nil".into(), vec![]), ("Int".into(), vec![int_id])]),
        );
        let big_type_id = types.insert(
            "BigType",
            Type::Sum(vec![
                ("MaybeInt".into(), vec![int_or_nil_id]),
                ("DoubleInt".into(), vec![int_id, int_id]),
            ]),
        );
        let bigger_type_id = types.insert(
            "BiggerType",
            Type::Sum(vec![
                ("OtherThing".into(), vec![big_type_id]),
                ("Boolean".into(), vec![bool_id]),
            ]),
        );
        let ids = TestTypeIds {
            int_id,
            bool_id,
            double_id,
            int_or_nil_id,
            big_type_id,
            bigger_type_id,
        };
        (ids, types)
    }

    #[test]
    fn test_table() {
        let (ids, types) = create_type_map();
        let schema = vec![
            ("i".into(), ids.int_id),
            ("b".into(), ids.bool_id),
            ("s".into(), ids.bigger_type_id),
        ];
        let mut table = Table::new(Schema::new(schema.clone()), &types);

        assert_eq!(table.row_count(), 0);

        const ROWS_COUNT: usize = 5000;
        let rows: Vec<Vec<Value>> = (0..ROWS_COUNT)
            .map(|_| {
                schema
                    .iter()
                    .map(|(_, t_id)| types[t_id].random_value(&types))
                    .collect()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            println!("inserting {}: {:?}", i, row);
            table.push_row(&row[..], &types);
            assert_eq!(table.row_count(), i + 1);
            assert_eq!(&table.get_row_value(i, &types), row);
        }

        for (i, row) in rows.iter().enumerate() {
            assert_eq!(&table.get_row_value(i, &types), row);
        }

        assert_eq!(table.iter().count(), ROWS_COUNT);

        for (i, row) in table.iter().enumerate() {
            for j in 0..row.cell_count() {
                let cell = row.get_cell(&types, j);
                let t = &types[&schema[j].1];
                let v = t.from_bytes(cell.data, &types).unwrap();
                assert_eq!(v, rows[i][j]);
            }
        }
    }

    #[test]
    fn test_ord_ints() {
        let (ids, types) = create_type_map();
        let schema = Schema::new(vec![("i".into(), ids.int_id)]);
        let mut table = Table::new(schema, &types);

        table.push_row(&[Value::Integer(1)], &types);
        table.push_row(&[Value::Integer(2)], &types);
        table.push_row(&[Value::Integer(3)], &types);
        table.push_row(&[Value::Integer(9001)], &types);

        for i in 0..table.row_count() {
            let row_i = table.get_row(i);
            let cell_i = row_i.get_cell(&types, 0);
            assert!(cell_i == cell_i);
            for j in i + 1..table.row_count() {
                let row_j = table.get_row(j);
                let cell_j = row_j.get_cell(&types, 0);
                assert!(cell_i < cell_j);
                assert!(cell_j > cell_i);
            }
        }
    }

    #[test]
    fn test_ord_variants() {
        let (ids, types) = create_type_map();
        let schema = Schema::new(vec![("s".into(), ids.bigger_type_id)]);
        let mut table = Table::new(schema, &types);

        table.push_row(
            &[Value::Sum(
                Some("BiggerType".into()),
                "OtherThing".into(),
                vec![Value::Sum(
                    Some("BigType".into()),
                    "MaybeInt".into(),
                    vec![Value::Sum(Some("IntOrNil".into()), "Nil".into(), vec![])],
                )],
            )],
            &types,
        );
        table.push_row(
            &[Value::Sum(
                Some("BiggerType".into()),
                "Boolean".into(),
                vec![Value::Bool(false)],
            )],
            &types,
        );
        table.push_row(
            &[Value::Sum(
                Some("BiggerType".into()),
                "Boolean".into(),
                vec![Value::Bool(true)],
            )],
            &types,
        );

        for i in 0..table.row_count() {
            let row_i = table.get_row(i);
            let cell_i = row_i.get_cell(&types, 0);
            assert!(cell_i == cell_i);
            for j in i + 1..table.row_count() {
                let row_j = table.get_row(j);
                let cell_j = row_j.get_cell(&types, 0);
                assert!(cell_i < cell_j);
                assert!(cell_j > cell_i);
            }
        }
    }
}
*/