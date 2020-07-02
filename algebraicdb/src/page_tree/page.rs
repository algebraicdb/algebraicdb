use crate::types::Value;
use std::marker::PhantomData;
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use bincode::{serialize_into, deserialize};
use std::mem::size_of;



pub struct Page<V> {
    length: usize,
    data: [u8; 8192],
    _ph: PhantomData<V>,
}



#[derive(Debug, Deserialize, Serialize)]
pub struct PageKey {
    offset: u16,
    length: u16,
}

const PAGE_CAP: usize = 8192;

impl<V> Page<V> {
    pub fn new() -> Page<V> {
        Page {
            /// Number of items
            length: 0,
            data: [0; PAGE_CAP],
            _ph: PhantomData::default(),
        }
    }

    fn get_key(&self, index: usize) -> PageKey {
        let key_size = size_of::<u16>() * 2;
        let start = index * key_size;
        let end = start + key_size;
        deserialize(&self.data[start .. end]).unwrap()
    }

    pub fn get(&self, index: usize) -> &[u8] {
        let key = self.get_key(index);

        let offset = key.offset as usize;
        let length = key.length as usize;
        
        &self.data[offset..offset+length]
    }

    pub fn append(&mut self, data: &[u8]) -> Result<(), ()> {
        let key_size = size_of::<u16>() * 2;

        assert!(data.len() + key_size <= self.free_space());
        
        let length = data.len() as u16;

        let offset = if self.length == 0 {
            self.data.len() as u16 - length
        } else {
            let last_key = self.get_key(self.length - 1);
            last_key.offset - length
        };

        let key = PageKey {
            offset,
            length,
        };

        let key_space = &mut self.data[self.length as usize..self.length as usize + key_size];
        serialize_into(key_space, &key).unwrap();

        let entry_start = offset as usize;
        let entry_end = entry_start + data.len();
        (&mut self.data[entry_start..entry_end]).copy_from_slice(data);
        
        self.length += 1;

        Ok(())
    }
    
    pub fn used_space(&self) -> usize {
        let key_size = size_of::<u16>() * 2;
        (0..self.length).map(|i| {
                let start = i * key_size;
                let end = start + key_size;
                let key: PageKey = deserialize(&self.data[start .. end]).unwrap();
                key.length as usize + key_size
            })
            .sum()
    }
    
    pub fn free_space(&self) -> usize {
        self.data.len() - self.used_space()
    }
}






// data5 -> extract item from 4-5

// pg(off, l)
// adb(offsetend)

