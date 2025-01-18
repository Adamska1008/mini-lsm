#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ops::Bound, sync::Arc};

use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        // not using `map` because of error handling
        let current = if let Some(sst) = sstables.get(0) {
            Some(SsTableIterator::create_and_seek_to_first(sst.clone())?)
        } else {
            None
        };
        if current.is_none() {
            Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            })
        } else {
            Ok(Self {
                current,
                next_sst_idx: 1,
                sstables,
            })
        }
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);
        if idx >= sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }
        let mut slf = Self {
            current: Some(SsTableIterator::create_and_seek_to_key(
                sstables[idx].clone(),
                key,
            )?),
            next_sst_idx: idx + 1,
            sstables,
        };
        slf.skip_invalid()?;
        Ok(slf)
    }

    // loop: when current iter is invalid, move to next sst
    fn skip_invalid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_ref() {
            if iter.is_valid() {
                break;
            }
            if let Some(sst) = self.sstables.get(self.next_sst_idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(sst.clone())?);
                self.next_sst_idx += 1;
            } else {
                self.current = None;
                break;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|c| c.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = self.current.as_mut() {
            iter.next()?;
            self.skip_invalid()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
