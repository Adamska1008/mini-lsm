#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn extract_key(&self, idx: usize) -> KeyVec {
        let data = &self.block.data;
        let offset = self.block.offsets[idx] as usize;
        let key_len_bytes = &data[offset..(offset + 2)];
        let key_len = u16::from_ne_bytes([key_len_bytes[0], key_len_bytes[1]]) as usize;
        let key_bytes = &data[(offset + 2)..(offset + 2 + key_len)];
        KeyVec::from_vec(key_bytes.to_vec())
    }

    fn extract_value_range(&self, idx: usize, key: KeySlice) -> (usize, usize) {
        let data = &self.block.data;
        let offset = self.block.offsets[idx] as usize + 2 + key.len();
        let value_len_bytes = &data[offset..(offset + 2)];
        let value_len = u16::from_ne_bytes([value_len_bytes[0], value_len_bytes[1]]) as usize;
        (offset + 2, offset + 2 + value_len)
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key = self.extract_key(0);
        self.value_range = self.extract_value_range(0, self.key.as_key_slice());
        self.idx = 0;
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            return;
        }
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }
        self.key = self.extract_key(self.idx);
        self.value_range = self.extract_value_range(self.idx, self.key.as_key_slice());
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.key() < key {
            self.next();
            if !self.is_valid() {
                return;
            }
        }
    }
}
