#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: Vec::with_capacity(block_size),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    pub fn current_size(&self) -> usize {
        self.data.len() + 2 * self.offsets.len() + 2
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let size_after_add = self.current_size() + key.len() + value.len() + 6; // 6 for key_len, value_len, and offset
        if size_after_add > self.block_size && !self.is_empty() {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16_ne(key.len() as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16_ne(value.len() as u16);
        self.data.put(value);
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty() || self.data.len() == 0 || self.offsets.len() == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
