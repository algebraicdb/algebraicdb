use crate::table::Table;
use crate::types::TypeMap;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq)]
pub enum RW {
    Read,
    Write,
}

#[derive(Ord, PartialOrd, PartialEq, Eq)]
pub struct TableRequest {
    pub table: String,
    pub rw: RW,
}

pub enum Request {
    AcquireResources(Vec<TableRequest>),
    CreateTable(String, Table),
}

pub enum Response {
    AcquiredResources(Resources),
    NoSuchTable(String),
    TableCreated,
    TableAlreadyExists,
}

pub struct Resources {
    dirty: bool,
    types: Arc<RwLock<TypeMap>>,
    tables: Vec<(RW, Arc<RwLock<Table>>)>,
}

pub struct ResourcesGuard<'a> {
    pub types: Resource<'a, TypeMap>,
    pub tables: Vec<Resource<'a, Table>>,
}

pub enum Resource<'a, T> {
    Write(RwLockWriteGuard<'a, T>),
    Read(RwLockReadGuard<'a, T>),
}

impl Resources {
    pub(super) fn new(types: Arc<RwLock<TypeMap>>, tables: Vec<(RW, Arc<RwLock<Table>>)>) -> Self {
        Self {
            dirty: false,
            types,
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
    pub fn take<'a>(&'a mut self) -> ResourcesGuard<'a> {
        assert_eq!(self.dirty, false);
        self.dirty = true;

        ResourcesGuard {
            types: Resource::Read(self.types.read().expect("Lock is poisoned")),
            tables: self
                .tables
                .iter()
                .map(|(rw, lock)| match rw {
                    RW::Read => Resource::Read(lock.read().expect("Lock is poisoned")),
                    RW::Write => Resource::Write(lock.write().expect("Lock is poisoned")),
                })
                .collect(),
        }
    }
}
