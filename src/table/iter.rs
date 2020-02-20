use super::{Row, Table};

pub struct RowIter<'tb> {
    table: &'tb Table,
    row: usize,
}

impl<'tb> RowIter<'tb> {
    pub fn new(table: &'tb Table) -> Self {
        RowIter { table, row: 0 }
    }
}

impl<'tb> Iterator for RowIter<'tb> {
    type Item = Row<'tb>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.table.row_count() {
            return None;
        }

        let row = self.table.get_row(self.row);
        self.row += 1;
        Some(row)
    }
}
