#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ops::Bound, sync::Arc};

use anyhow::Result;
use bytes::Bytes;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
    end_bound: Bound<Bytes>,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block_cached(0)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            blk_iter,
            blk_idx: 0,
            end_bound: Bound::Unbounded,
        })
    }

    pub fn scan(table: Arc<SsTable>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<Self> {
        let mut slf = Self::create_and_seek_to_first(table)?;
        slf.end_bound = upper.map(Bytes::copy_from_slice);
        match lower {
            Bound::Included(key) => slf.seek_to_key(KeySlice::from_slice(key))?,
            Bound::Excluded(key) => {
                let key = KeySlice::from_slice(key);
                slf.seek_to_key(key)?;
                while slf.key() == key {
                    slf.next()?;
                }
            }
            Bound::Unbounded => {}
        }
        Ok(slf)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.load_block(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut slf = Self::create_and_seek_to_first(table)?;
        slf.seek_to_key(key)?;
        Ok(slf)
    }

    fn load_block(&mut self, blk_idx: usize) -> Result<()> {
        self.blk_idx = blk_idx;
        if blk_idx < self.table.num_of_blocks() {
            let block = self.table.read_block_cached(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let blk_idx = self.table.find_block_idx(key);
        self.load_block(blk_idx)?;
        self.blk_iter.seek_to_key(key);
        if !self.blk_iter.is_valid() {
            self.load_block(blk_idx + 1)?;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        if !(self.blk_idx < self.table.num_of_blocks() && self.blk_iter.is_valid()) {
            return false;
        }
        match &self.end_bound {
            Bound::Included(bound) => self.key().raw_ref() < bound.as_ref(),
            Bound::Excluded(bound) => self.key().raw_ref() <= bound.as_ref(),
            Bound::Unbounded => true,
        }
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.blk_iter.next();
            if !self.is_valid() && self.blk_idx < self.table.num_of_blocks() {
                // allow self.blk_idx to be equal to num_of_blocks
                self.load_block(self.blk_idx + 1)?;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
