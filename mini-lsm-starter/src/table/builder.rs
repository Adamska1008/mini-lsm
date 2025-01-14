#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::bloom::Bloom;
use super::{BlockMeta, SsTable};
use crate::table::FileObject;
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    num_of_kvs: usize, // for debugging
    key_hashes: Vec<u32>,
}

fn vec_u8_to_keybytes(v: Vec<u8>) -> KeyBytes {
    let bytes = Bytes::from_iter(v.into_iter());
    KeyBytes::from_bytes(bytes)
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![],
            block_size,
            num_of_kvs: 0,
            key_hashes: vec![],
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.flush_current_block();
            assert!(
                self.builder.add(key, value),
                "empty blockbuilder should always accept a key"
            );
        }
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }
        self.last_key = key.to_key_vec().into_inner();
        self.num_of_kvs += 1;
        self.key_hashes.push(farmhash::hash32(key.raw_ref()));
    }

    // for debugging
    pub fn num_of_kvs(&self) -> usize {
        self.num_of_kvs
    }

    /// dump the block from blockbuilder into data
    fn flush_current_block(&mut self) {
        let old_first_key = std::mem::replace(&mut self.first_key, vec![]);
        let old_last_key = std::mem::replace(&mut self.last_key, vec![]);
        let old_metadata = BlockMeta {
            offset: self.data.len(),
            first_key: vec_u8_to_keybytes(old_first_key),
            last_key: vec_u8_to_keybytes(old_last_key),
        };
        self.meta.push(old_metadata);
        let old_builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let data = old_builder.build().encode();
        self.data.put(&data[..]);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.flush_current_block();
        }
        let mut buf: Vec<u8> = self.data;
        let meta_block_offset = buf.len() as u32;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32_le(meta_block_offset);
        // build and encode bloom filter
        let bloom_filter_offset = buf.len() as u32;
        let bloom_bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bloom_bits_per_key);
        bloom.encode(&mut buf);
        buf.put_u32_le(bloom_filter_offset);

        let file_object = FileObject::create(path.as_ref(), buf)?;
        let sst = SsTable {
            file: file_object,
            block_meta_offset: meta_block_offset as usize,
            id,
            block_cache,
            first_key: self
                .meta
                .first()
                .map_or(KeyBytes::default(), |m| m.first_key.clone()),
            last_key: self
                .meta
                .last()
                .map_or(KeyBytes::default(), |m| m.last_key.clone()),
            block_meta: self.meta,
            bloom: Some(bloom),
            max_ts: 0,
        };
        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
