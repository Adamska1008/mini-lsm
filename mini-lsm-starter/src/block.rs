#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut data = self.data.clone();
        let num_of_elements: u16 = self.offsets.len() as u16;
        for offset in &self.offsets {
            data.extend_from_slice(&offset.to_ne_bytes());
        }
        data.extend_from_slice(&num_of_elements.to_ne_bytes());
        Bytes::copy_from_slice(data.as_slice())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let last_two_bytes = &data[data.len() - 2..];
        let num_of_elements = u16::from_ne_bytes([last_two_bytes[0], last_two_bytes[1]]) as usize;
        let offsets_section = &data[(data.len() - 2 - num_of_elements * 2)..(data.len() - 2)];
        let mut offsets = vec![];
        for i in 0..num_of_elements {
            let this_two_bytes = &offsets_section[(2 * i)..(2 * i + 2)];
            let offset = u16::from_ne_bytes([this_two_bytes[0], this_two_bytes[1]]);
            offsets.push(offset);
        }
        Self {
            data: data[..data.len() - 2 - num_of_elements * 2].to_vec(),
            offsets,
        }
    }
}
