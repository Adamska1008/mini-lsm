#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, item) in iters.into_iter().enumerate() {
            if item.is_valid() {
                heap.push(HeapWrapper(idx, item));
            }
        }
        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        match &self.current {
            Some(cur) => cur.1.key(),
            None => KeySlice::from_slice(&[]),
        }
    }

    fn value(&self) -> &[u8] {
        match &self.current {
            Some(cur) => cur.1.value(),
            None => &[],
        }
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        let current_key = self.current.as_ref().unwrap().1.key();
        while let Some(mut iter_wrapper) = self.iters.peek_mut() {
            if iter_wrapper.1.key() == current_key {
                if let e @ Err(_) = iter_wrapper.1.next() {
                    PeekMut::pop(iter_wrapper);
                    return e;
                }
                if !iter_wrapper.1.is_valid() {
                    PeekMut::pop(iter_wrapper);
                }
            } else {
                break;
            }
        }
        if let Some(mut current) = self.current.take() {
            current.1.next()?;
            if current.1.is_valid() {
                self.iters.push(current);
            }
        }
        self.current = self.iters.pop();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters.len() + 1
    }
}
