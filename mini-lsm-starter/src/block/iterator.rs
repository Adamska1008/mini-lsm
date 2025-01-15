#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

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
    pub(crate) fn new(block: Arc<Block>) -> Self {
        // extract first_key
        let mut data = &block.data[..];
        let key_len = data.get_u16_ne() as usize;
        let key = data
            .get(..key_len)
            .expect("first key should be decoded smoothly")
            .to_vec();
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::from_vec(key),
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
        if idx == 0 {
            return self.first_key.clone();
        }
        let offset = self.block.offsets[idx] as usize;
        let mut data = &self.block.data[offset..];
        let key_overlap_len = data.get_u16_ne();
        let rest_key_len = data.get_u16_ne();
        let prefix = &self.first_key.raw_ref()[..key_overlap_len as usize];
        let rest_key = data
            .get(..rest_key_len as usize)
            .expect("key range should not out of bounds.");
        KeyVec::from_vec([prefix, rest_key].concat())
    }

    fn extract_value_range(&self, idx: usize) -> (usize, usize) {
        if idx == 0 {
            let mut data = &self.block.data[..];
            let key_len = data.get_u16_ne() as usize;
            let (_, mut data) = data.split_at(key_len);
            let value_len = data.get_u16_ne() as usize;
            return (2 + key_len + 2, 2 + key_len + 2 + value_len);
        }
        let offset = self.block.offsets[idx] as usize;
        let mut data = &self.block.data[offset..];
        let key_overlap_len = data.get_u16_ne() as usize;
        let rest_key_len = data.get_u16_ne() as usize;
        let (_, mut data) = data.split_at(rest_key_len);
        let value_len = data.get_u16_ne() as usize;
        (
            offset + 4 + rest_key_len + 2,
            offset + 4 + rest_key_len + 2 + value_len,
        )
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.key = self.extract_key(0);
        self.value_range = self.extract_value_range(0);
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
        self.value_range = self.extract_value_range(self.idx);
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
