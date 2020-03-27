use crate::types::TypeId;

#[derive(Clone, Debug)]
pub struct Schema {
    pub columns: Vec<(String, TypeId)>,
}

impl Schema {
    pub fn new(columns: Vec<(String, TypeId)>) -> Self {
        Schema { columns }
    }

    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn column(&self, name: &str) -> Option<TypeId> {
        self.columns
            .iter()
            .find(|(entry_name, _)| entry_name == name)
            .map(|(_, type_id)| *type_id)
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn union(&self, other: &Schema) -> Schema {
        let mut columns = self.columns.clone();
        columns.extend_from_slice(&other.columns);
        Schema::new(columns)
    }
}
