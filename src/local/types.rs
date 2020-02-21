use crate::types::TypeMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

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
pub enum Request<T> {
    Acquire(Acquire),
    CreateTable(String, T),
}

#[derive(Debug)]
pub struct Acquire {
    pub table_reqs: Vec<TableRequest>,
    pub type_map_perms: RW,
}

#[derive(Debug)]
pub enum CreateTableResponse {
    TableCreated,
    TableAlreadyExists,
}

pub enum Response<T> {
    AcquiredResources(Resources<T>),
    NoSuchTable(String),
    CreateTable(Result<(), ()>),
    // TODO add future table deleted???????
}

pub struct Resources<T> {
    dirty: bool,
    type_map_perms: RW,
    type_map: Arc<RwLock<TypeMap>>,
    tables: Vec<(RW, String, Arc<RwLock<T>>)>,
}

pub struct ResourcesGuard<'a, T> {
    pub type_map: Resource<'a, TypeMap>,
    pub tables: Vec<(&'a str, Resource<'a, T>)>,
}

pub enum Resource<'a, T> {
    Write(RwLockWriteGuard<'a, T>),
    Read(RwLockReadGuard<'a, T>),
}

impl<T> Resources<T> {
    pub(super) fn new(
        type_map: Arc<RwLock<TypeMap>>,
        type_map_perms: RW,
        tables: Vec<(RW, String, Arc<RwLock<T>>)>,
    ) -> Self {
        Self {
            dirty: false,
            type_map,
            type_map_perms,
            tables,
        }
    }

    /// Actually acquire read/write access to the requested resources.
    ///
    /// This function will take the locks of all requested resources.
    /// The guard that is returned will release the locks when dropped.
    ///
    /// You may only call this function once. This is to ensure atomicness. That is,
    /// to not drop the guard (and the locks) until you are done with the resources.
    pub async fn take<'a>(&'a mut self) -> ResourcesGuard<'a, T> {
        assert_eq!(self.dirty, false);
        self.dirty = true;

        let mut tables = Vec::with_capacity(self.tables.len());
        for (rw, name, lock) in self.tables.iter() {
            let resource = match rw {
                RW::Read => Resource::Read(lock.read().await),
                RW::Write => Resource::Write(lock.write().await),
            };

            tables.push((name.as_str(), resource));
        }

        ResourcesGuard {
            type_map: match self.type_map_perms {
                RW::Read => Resource::Read(self.type_map.read().await),
                RW::Write => Resource::Write(self.type_map.write().await),
            },
            tables,
        }
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
}
