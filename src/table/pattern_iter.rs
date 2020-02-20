use crate::pattern::CompiledPattern;
use crate::types::TypeMap;
use super::{Table, Cell};

pub struct RowPatternIter<'p, 'ts, 'tb> {
    pattern: &'p CompiledPattern,
    types: &'ts TypeMap,
    table: &'tb Table,
    row: usize,
}

pub struct CellPatternIter<'p, 'ts, 'tb> {
    pattern: &'p CompiledPattern,
    types: &'ts TypeMap,
    data: &'tb [u8],
    cursor: usize,
}

impl<'p, 'ts, 'tb> RowPatternIter<'p, 'ts, 'tb> {
    pub fn new(
        pattern: &'p CompiledPattern,
        table: &'tb Table,
        types: &'ts TypeMap,
    ) -> Self {
        RowPatternIter {
            pattern,
            table,
            row: 0,
            types,
        }
    }
}

impl<'p, 'ts, 'tb> Iterator for RowPatternIter<'p, 'ts, 'tb> {
    type Item = CellPatternIter<'p, 'ts, 'tb>;

    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            if self.row >= self.table.row_count() {
                return None;
            }

            let row = self.table.get_row(self.row);
            self.row += 1;
            for (i, value) in &self.pattern.matches {
                for j in 0..value.len() {
                    if row.data[i + j] != value[j] {
                        continue 'outer;
                    }
                }
            }

            return Some(CellPatternIter {
                cursor: 0,
                pattern: self.pattern,
                types: self.types,
                data: row.data,
            });
        }
    }
}

impl<'p, 'ts, 'tb> Iterator for CellPatternIter<'p, 'ts, 'tb> {
    type Item = (&'p str, Cell<'ts, 'tb>);

    fn next(&mut self) -> Option<Self::Item> {
        self.pattern
            .bindings
            .get(self.cursor)
            .map(|(index, type_id, ident)| {
                self.cursor += 1;

                let t = &self.types[type_id];
                let type_size = t.size_of(self.types);

                let cell = Cell::new(
                    *type_id,
                    &self.data[*index..*index + type_size],
                    self.types,
                );
                (ident.as_str(), cell)
            })
    }
}
