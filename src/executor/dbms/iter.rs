use crate::table::Cell;
use crate::types::{Type, TypeMap, EnumTag, TypeId};
use crate::pattern::Pattern;
use std::cmp::Ordering;
use bincode::serialize;
use std::sync::Arc;
use crate::ast::{WhereItem, Expr};

pub struct RowIter<'a> {
    /// The actual data cells iterated over
    // TODO: Avoid having an Arc here
    pub bindings: Arc<[CellRef<'a>]>,

    /// Filter rows
    pub matches: Vec<RowCmp<'a>>,

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
    pub source: &'a [u8],
    pub name: &'a str,
    pub type_id: TypeId,
    pub offset: usize,
    pub size: usize,
    pub row_size: usize,
}

pub struct RowCmp<'a> {
    /// Source of the data (e.g. a slice of an entire table)
    source: &'a [u8],

    /// The size of a row in the data
    row_size: usize,

    /// The byte count offset from the start of the row, e.g. index of the start of a column.
    offset: usize,

    /// The value to equal the and:ed row
    value: Vec<u8>,
}

impl<'a> Iterator for RowIter<'a> {
    type Item = CellIter<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(row) = self.row.as_mut() {
            'outer: loop {
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
                        continue 'outer;
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
    type Item = Cell<'a, 'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cell) = self.bindings.get(self.cell) {
            let start = self.row * cell.row_size + cell.offset;
            let end = start + cell.size;
            let data = &cell.source[start..end];

            self.cell += 1;

            Some(Cell::new(
                cell.type_id,
                data,
                self.type_map,
            ))
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
            matches: &mut Vec<RowCmp<'a>>,
        ) {
            match pattern {
                Pattern::Char(v) => {
                    matches.push(RowCmp {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap(),
                    });
                }
                Pattern::Int(v) => {
                    matches.push(RowCmp {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap(),
                    });
                }
                Pattern::Bool(v) => {
                    matches.push(RowCmp {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap(),
                    });
                }
                Pattern::Double(v) => {
                    matches.push(RowCmp {
                        source: data,
                        row_size,
                        offset: byte_index,
                        value: serialize(v).unwrap(),
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

                        matches.push(RowCmp {
                            source: data,
                            row_size,
                            offset: byte_index,
                            value: serialize(&i).unwrap(),
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

        let mut bindings = vec![];
        //let mut matches = vec![];

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
                                          &mut self.matches,
                                          );
                        }
                    }
                }
            }
        }

        bindings.extend(self.bindings.iter());
        self.bindings = bindings.into();
    }
}

impl RowCmp<'_> {
    pub fn check(&self, row: usize) -> Ordering {
        let RowCmp { source, row_size, offset, value } = self;

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
