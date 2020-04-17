use crate::types::TypeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use crate::table::{Schema, TableData};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub enum RW {
    Read,
    Write,
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct TableRequest {
    pub table: String,
    pub rw: RW,
}

#[derive(Debug)]
pub struct Acquire {
    pub schema_reqs: Vec<TableRequest>,
    pub data_reqs: Vec<TableRequest>,
    pub type_map_perms: RW,
}

#[derive(Debug)]
pub enum CreateTableResponse {
    TableCreated,
    TableAlreadyExists,
}

pub struct PermLock<T> {
    pub perm: RW,
    pub lock: Arc<RwLock<T>>,
}

impl<T> PermLock<T> {
    pub fn new(perm: RW, lock: Arc<RwLock<T>>) -> Self {
        Self {
            perm,
            lock,
        }
    }
}

struct TakeOnce<T> {
    item: T,
    dirty: AtomicBool,
}

/// This struct lets you acquire read/write access to the requested resources.
///
/// In ensures that you can only take the locks ONCE, and that you take the locks in an ordered manner.
/// Any violation of these rules will result in a panic.
pub struct Resources {
    type_map: TakeOnce<PermLock<TypeMap>>,
    table_schemas: TakeOnce<Vec<(String, PermLock<Schema>)>>,
    table_datas: TakeOnce<Vec<(String, PermLock<TableData>)>>,
}

/*
pub struct ResourcesGuard<'a> {
    pub type_map: Resource<'a, TypeMap>,
    pub table_schemas: Vec<(&'a str, Resource<'a, Schema>)>,
    pub table_datas: Vec<(&'a str, Resource<'a, TableData>)>,
}
*/

pub enum Resource<'a, T> {
    Write(RwLockWriteGuard<'a, T>),
    Read(RwLockReadGuard<'a, T>),
}

impl Resources {
    pub fn new(
        type_map: PermLock<TypeMap>,
        table_schemas: Vec<(String, PermLock<Schema>)>,
        table_datas: Vec<(String, PermLock<TableData>)>,
    ) -> Self {
        Self {
            type_map: TakeOnce::new(type_map),
            table_schemas: TakeOnce::new(table_schemas),
            table_datas: TakeOnce::new(table_datas),
        }
    }

    pub async fn take_type_map<'a>(&'a self) -> Resource<'a, TypeMap> {
        self.type_map.take().lock().await
    }

    pub async fn take_schemas<'a>(&'a self) -> HashMap<&'a str, Resource<'a, Schema>> {
        self.type_map.set_dirty(); // the type map must be taken before the schemas

        let mut table_schemas = HashMap::new();
        for (name, lock) in self.table_schemas.take().iter() {
            table_schemas.insert(name.as_str(), lock.lock().await);
        }
        table_schemas
    }
    pub async fn take_data<'a>(&'a self) -> HashMap<&'a str, Resource<'a, TableData>> {
        self.type_map.set_dirty(); // the type map must be taken before the schemas
        self.table_schemas.set_dirty(); // the schemas must be taken before the data

        let mut table_datas = HashMap::new();
        for (name, lock) in self.table_datas.take().iter() {
            table_datas.insert(name.as_str(), lock.lock().await);
        }
        table_datas
    }
}

impl<T> PermLock<T> {
    pub async fn lock<'a>(&'a self) -> Resource<'a, T> {
        match self.perm {
            RW::Read => Resource::Read(self.lock.read().await),
            RW::Write => Resource::Write(self.lock.write().await),
        }
    }
}

impl<T> TakeOnce<T> {
    pub fn new(item: T) -> Self {
        Self {
            dirty: AtomicBool::from(false),
            item,
        }
    }

    pub fn set_dirty(&self) {
        self.dirty.store(true, Ordering::Release);
    }

    /// May only be called once
    pub fn take(&self) -> &T {
        assert!(!self.dirty.load(Ordering::Acquire));
        self.set_dirty();
        &self.item
    }
}

impl<'a, T> Deref for Resource<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Resource::Read(guard) => guard.deref(),
            Resource::Write(guard) => guard.deref(),
        }
    }
}

/// Panics if Resource is read-only
impl<'a, T> DerefMut for Resource<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Resource::Read(_) => panic!("Tried to get write access to a read-only resource"),
            Resource::Write(guard) => guard.deref_mut(),
        }
    }
}
/*
impl<'a, T> ResourcesGuard<'a, T> {
    // Get a read-only handle to a table.
    //
    // Panics if the read-handle wasn't requested.
    pub fn read_table(&self, name: &str) -> &T {
        self.tables
            .iter()
            .find(|(entry_name, _)| entry_name == &name)
            .map(|(_, resource)| resource.deref())
            .unwrap()
    }

    pub fn write_table(&mut self, name: &str) -> (&mut T, &Resource<'a, TypeMap>) {
        let table = self
            .tables
            .iter_mut()
            .find(|(entry_name, _)| entry_name == &name)
            .map(|(_, resource)| resource.deref_mut())
            .unwrap();
        (table, &self.type_map)
    }
}*/
