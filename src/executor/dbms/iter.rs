use crate::table::{Table, Schema, Cell};
use crate::types::{Type, TypeMap, EnumTag, TypeId};
use crate::pattern::Pattern;
use std::cmp::Ordering;
use bincode::serialize;
use std::sync::Arc;
use crate::ast::{WhereItem, Expr};

pub enum ModIter<'a> {
    Select(&'a [Expr]),
    Where(&'a [WhereItem]),
}

pub enum Rows<'a> {
    Scan(RowIter<'a>),
    Materialized {
        table: Table,
        mods: Vec<ModIter<'a>>,
    },
}

impl<'a> From<RowIter<'a>> for Rows<'a> {
    fn from(iter: RowIter<'a>) -> Self {
        Rows::Scan(iter)
    }
}

impl From<Table> for Rows<'static> {
    fn from(table: Table) -> Self {
        Rows::Materialized {
            table,
            mods: vec![],
        }
    }
}

impl<'a> Rows<'a> {
    pub fn schema(&self) -> Schema {
        // TODO: refactor this into something more efficient
        match self {
            Rows::Scan(iter) => Schema::new(iter.bindings.iter().map(|cr| (cr.name.to_owned(), cr.type_id)).collect()),
            Rows::Materialized { table, .. } => table.schema().clone(),
        }
    }

    pub fn iter<'b>(&'b self, type_map: &'b TypeMap) -> RowIter<'b> {
        match self {
            Rows::Scan(iter) => iter.clone(),
            Rows::Materialized {
                table,
                mods,
            } => {
                use super::full_table_scan;
                let mut scan = full_table_scan(&table, type_map);
                for m in mods {
                    match m {
                        ModIter::Select(selects) => {
                            scan.select(&selects);
                        }
                        ModIter::Where(clauses) => {
                            scan.apply_pattern(&clauses, type_map);
                        }
                    }
                }
                scan
            }
        }
    }

    pub fn select(&mut self, items: &'a [Expr]) {
        match self {
            Rows::Scan(iter) => iter.select(items),
            Rows::Materialized { mods, .. } => mods.push(ModIter::Select(items)),
        }
    }

    pub fn apply_pattern(&mut self, patterns: &'a [WhereItem], type_map: &TypeMap) {
        match self {
            Rows::Scan(iter) => iter.apply_pattern(patterns, type_map),
            Rows::Materialized { mods, .. } => mods.push(ModIter::Where(patterns)),
        }
    }
}

#[derive(Clone)]
pub struct RowIter<'a> {
    // FIXME: Avoid using Arc:s
    /// The actual data cells iterated over
    pub bindings: Arc<[CellRef<'a>]>,

    /// Filter rows
    pub matches: Arc<[CellFilter<'a>]>,

    pub type_map: &'a TypeMap,

    /// The current row
    pub row: Option<usize>,
}

#[derive(Clone)]
pub struct CellIter<'a> {
    pub type_map: &'a TypeMap,
    pub bindings: Arc<[CellRef<'a>]>,
    pub row: usize,
    pub cell: usize,
}

#[derive(Clone, Copy)]
pub struct CellRef<'a> {
    /// Source of the data (e.g. a slice of an entire table)
    pub source: &'a [u8],

    /// The variable name bound to this cell
    pub name: &'a str,

    /// The data type of this cell
    pub type_id: TypeId,

    /// The size of a row in the source
    pub row_size: usize,

    /// The byte count offset from the start of the row, e.g. index of the start of a column.
    pub offset: usize,

    /// The size of the cell in bytes
    pub size: usize,
}

#[derive(Clone)]
pub struct CellFilter<'a> {
    /// Source of the data (e.g. a slice of an entire table)
    source: &'a [u8],

    /// The size of a row in the source
    row_size: usize,

    /// The byte count offset from the start of the row, e.g. index of the start of a column.
    offset: usize,

    /// The value to equal the and:ed row
    value: Arc<[u8]>,
}

struct JoinIter {
    result: Table,
}

enum SelectIter<'a> {
    FromTable(RowIter<'a>),
    FromJoin(JoinIter),
}

impl<'a> Iterator for RowIter<'a> {
    type Item = CellIter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.row.as_mut() {
            'rows: loop {
                for source in self.bindings.iter() {
                    // Check if any "table" is out-of-bounds
                    if *row * source.row_size >= source.source.len() {
                        self.row = None;
                        return None;
                    }
                }

                // Check that all rows matches the filters
                for m in self.matches.iter() {
                    if m.check(*row) != Ordering::Equal {
                        *row += 1;
                        continue 'rows;
                    }
                }

                let cr = CellIter {
                    bindings: self.bindings.clone(),
                    type_map: self.type_map,
                    row: *row,
                    cell: 0,
                };

                *row += 1;

                return Some(cr);
            }
        } else {
            None
        }
    }
}

impl<'a> Iterator for CellIter<'a> {
    type Item = (&'a str, Cell<'a, 'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cell) = self.bindings.get(self.cell) {
            let start = self.row * cell.row_size + cell.offset;
            let end = start + cell.size;
            let data = &cell.source[start..end];

            self.cell += 1;

            Some((cell.name, Cell::new(
                cell.type_id,
                data,
                self.type_map,
            )))
        } else {
            None
        }
    }
}

impl<'a> RowIter<'a> {
    pub fn select(&mut self, items: &'a [Expr]) {
        let mut bindings = vec![];
        'outer: for item in items {
            match item {
                Expr::Ident(name) => {
                    for binding in self.bindings.iter() {
                        if binding.name == name {
                            bindings.push(*binding);
                            continue 'outer;
                        }
                    }
                    panic!("No matching bindings: \"{}\"", name);
                }
                _expr => unimplemented!("Selecting non-ident expressions"),
            }
        }

        self.bindings = bindings.into();
    }

    pub fn apply_pattern(&mut self, patterns: &'a [WhereItem], type_map: &TypeMap) {
        fn build_pattern<'a>(
            pattern: &'a Pattern,
            mut byte_index: usize,
            type_map: &TypeMap,
            type_id: TypeId,
            data: &'a [u8],
            row_size: usize,
            bindings: &mut Vec<CellRef<'a>>,
            matches: &mut Vec<CellFilter<'a>>,
        ) {
            match pattern {
                Pattern::Char(v) => {
                    matches.push(CellFilter {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap().into(),
                    });
                }
                Pattern::Int(v) => {
                    matches.push(CellFilter {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap().into(),
                    });
                }
                Pattern::Bool(v) => {
                    matches.push(CellFilter {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap().into(),
                    });
                }
                Pattern::Double(v) => {
                    matches.push(CellFilter {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap().into(),
                    });
                }
                Pattern::Ignore => {}
                Pattern::Binding(ident) => {
                    let t = type_map.get_by_id(type_id);
                    bindings.push(CellRef {
                        source: data,
                        name: ident,
                        type_id,
                        offset: byte_index,
                        size: t.size_of(type_map),
                        row_size,
                    });
                }
                Pattern::Variant {
                    namespace: _namespace,
                    name,
                    sub_patterns,
                } => {
                    if let Type::Sum(variants) = &type_map[&type_id] {
                        let (i, (_, sub_types)) = variants
                            .iter()
                            .enumerate()
                            .find(|(_, (variant, _))| variant == name)
                            .unwrap();

                        matches.push(CellFilter {
                            source: data,
                            row_size,
                            offset: byte_index,
                            value: serialize(&i).unwrap().into(),
                        });

                        byte_index += std::mem::size_of::<EnumTag>();
                        for (type_id, pattern) in sub_types.iter().zip(sub_patterns.iter()) {
                            let t = &type_map[type_id];
                            build_pattern(pattern, byte_index, type_map, *type_id, data, row_size, bindings, matches);
                            byte_index += t.size_of(type_map);
                        }
                    } else {
                        panic!("not a sum-type")
                    }
                }
            }
        }

        let mut bindings: Vec<CellRef> = vec![];
        let mut matches: Vec<CellFilter> = vec![];

        for select_item in patterns {
            match select_item {
                WhereItem::Expr(_) => {} // Ignore expressions for now
                WhereItem::Pattern(name, pattern) => {
                    for cell_ref in self.bindings.iter() {
                        if cell_ref.name == name {
                            let byte_index = cell_ref.offset;
                            let type_id = cell_ref.type_id;
                            let data = cell_ref.source;
                            let row_size = cell_ref.row_size;

                            build_pattern(pattern,
                                          byte_index,
                                          type_map,
                                          type_id,
                                          data,
                                          row_size,
                                          &mut bindings,
                                          &mut matches,
                                          );
                        }
                    }
                }
            }
        }

        if bindings.len() > 0 {
            bindings.extend(self.bindings.into_iter());
            self.bindings = bindings.into();
        }

        if matches.len() > 0 {
            matches.extend_from_slice(&self.matches);
            self.matches = matches.into();
        }
    }
}

impl CellFilter<'_> {
    pub fn check(&self, row: usize) -> Ordering {
        let CellFilter { source, row_size, offset, value } = self;

        let start = row * row_size + offset;
        let end = start + value.len();

        let row = &source[start..end];

        for (row_b, value_b) in row.iter().zip(value.iter()) {
            let cmp = row_b.cmp(value_b);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    }
}
