mod cell;
mod iter;
mod pattern_iter;
mod row;
mod schema;

pub use self::cell::Cell;
pub use self::iter::RowIter;
pub use self::pattern_iter::{CellPatternIter, RowPatternIter};
pub use self::row::Row;
pub use self::schema::Schema;

use crate::local::TTable;
use crate::pattern::CompiledPattern;
use crate::types::{TypeId, TypeMap, Value};

#[derive(Debug, Clone)]
pub struct Column {
    col_type: TypeId,
    name: String,
}

#[derive(Debug)]
pub struct Table {
    schema: Schema,
    data: Vec<u8>,
    row_size: usize,
}

impl TTable for Table {
    fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

impl Table {
    pub fn new(schema: Schema, types: &TypeMap) -> Self {
        Self {
            data: vec![],
            row_size: schema
                .columns
                .iter()
                .map(|(_, t_id)| &types[t_id])
                .map(|t| t.size_of(types))
                .sum(),
            schema,
        }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn iter<'a>(&'a self) -> RowIter<'a> {
        RowIter::new(self)
    }

    pub fn pattern_iter<'p, 'ts, 'tb>(
        &'tb self,
        pattern: &'p CompiledPattern,
        types: &'ts TypeMap,
    ) -> RowPatternIter<'p, 'ts, 'tb> {
        RowPatternIter::new(pattern, self, types)
    }

    pub fn get_row<'a>(&'a self, row: usize) -> Row<'a> {
        let start = self.row_start(row);
        let end = start + self.row_size;

        Row::new(&self.schema, &self.data[start..end])
    }

    pub fn get_row_value(&self, row: usize, types: &TypeMap) -> Vec<Value> {
        let mut output = vec![];
        let mut data = &self.data[self.row_start(row)..];
        for (_, t_id) in self.schema.columns.iter() {
            let t = &types[t_id];
            let t_size = t.size_of(types);
            output.push(t.from_bytes(&data[..t_size], types).unwrap());
            data = &data[t_size..];
        }

        output
    }

    pub fn row_count(&self) -> usize {
        assert_eq!(self.data.len() % self.row_size, 0);

        self.data.len() / self.row_size
    }

    fn row_start(&self, row: usize) -> usize {
        row * self.row_size
    }

    pub fn push_row(&mut self, cells: &[Value], types: &TypeMap) {
        assert_eq!(self.data.len() % self.row_size, 0);

        for (t_id, value) in self.schema.columns.iter().map(|(_, t)| t).zip(cells.iter()) {
            value.to_bytes(&mut self.data, types, &types[t_id])
        }

        assert_eq!(self.data.len() % self.row_size, 0);
    }
}

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

    #[test]
    fn test_pattern_iter() {
        use crate::ast::*;
        use crate::grammar::StmtParser;

        let tname = Some(String::from("MaybeInt"));
        let (ids, types) = create_type_map();

        let schema = Schema::new(vec![
            ("x".into(), ids.int_or_nil_id),
            ("y".into(), ids.int_or_nil_id),
        ]);

        let mut table = Table::new(schema.clone(), &types);

        let int_val1 = Value::Sum(tname.clone(), "Int".into(), vec![Value::Integer(1)]);
        let int_val2 = Value::Sum(tname.clone(), "Int".into(), vec![Value::Integer(2)]);
        let int_val3 = Value::Sum(tname.clone(), "Int".into(), vec![Value::Integer(3)]);
        let int_none = Value::Sum(tname.clone(), "Nil".into(), vec![]);

        table.push_row(&[int_val1.clone(), int_val2.clone()], &types);
        table.push_row(&[int_val3.clone(), int_val2.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val3.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val3.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val2.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);
        table.push_row(&[int_val2.clone(), int_val2.clone()], &types);
        table.push_row(&[int_val1.clone(), int_none.clone()], &types);
        table.push_row(&[int_none.clone(), int_none.clone()], &types);

        // helper function for extracting a pattern match ast from sql input
        let parse_pattern = |input: &str| -> CompiledPattern {
            let stmt = StmtParser::new().parse(input).unwrap_or_else(|_| panic!("Parsing pattern \"{}\" failed.", input));
            match stmt {
                Stmt::Select(Select { where_clause, .. }) => {
                    if let Some(wc) = where_clause {
                        CompiledPattern::compile(&wc.items, &schema, &types)
                    } else {
                        CompiledPattern::compile(&[], &schema, &types)
                    }
                }
                _ => panic!("Not a select statement"),
            }
        };

        // function for parsing sql, extracting a pattern match, and trying to run it.
        let test_pattern = |input: &str, f: Box<dyn FnOnce(RowPatternIter<'_, '_, '_>)>| {
            let pattern = parse_pattern(input);
            println!("testing pattern \"{}\": {:#?}", input, pattern);
            let iter = table.pattern_iter(&pattern, &types);
            f(iter)
        };

        // iterate over all matching rows
        test_pattern("SELECT WHERE x: Int(1);", box |i| assert_eq!(i.count(), 2));
        test_pattern("SELECT WHERE x: Int(2);", box |i| assert_eq!(i.count(), 4));
        test_pattern("SELECT WHERE x: Int(3);", box |i| assert_eq!(i.count(), 1));
        test_pattern("SELECT WHERE x: Int(42);", box |i| assert_eq!(i.count(), 0));
        test_pattern("SELECT WHERE x: Nil();", box |i| assert_eq!(i.count(), 7));

        // iterate over all bound cells of all matching rows.
        // two rows should match, multiplied by three bindings x1, ý2, x3.
        test_pattern(
            "SELECT WHERE x: Int(2), x: x1, y: y2, x: x3, y: Int(2);",
            box |i| assert_eq!(i.flatten().count(), 6),
        );
    }
}
