#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction { sstables: Vec<usize> },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::Leveled(_) => todo!(),
            CompactionTask::Tiered(_) => todo!(),
            CompactionTask::Simple(_) => todo!(),
            CompactionTask::ForceFullCompaction { sstables } => {
                let sstables: Vec<_> = {
                    let ro_state = self.state.read();
                    sstables
                        .iter()
                        .map(|id| {
                            let sst = ro_state
                                .sstables
                                .get(id)
                                .expect("should not miss sst ids")
                                .clone();
                            let iter = SsTableIterator::create_and_seek_to_first(sst).unwrap();
                            Box::new(iter)
                        })
                        .collect()
                };
                let target_sst_size = self.options.target_sst_size;
                let mut merge_iterator = MergeIterator::create(sstables);
                let mut compacted_ssts = vec![];
                let mut mem_table = MemTable::create(self.next_sst_id());
                while merge_iterator.is_valid() {
                    let key = merge_iterator.key();
                    let value = merge_iterator.value();
                    if value.is_empty() {
                        merge_iterator.next()?;
                        continue;
                    }
                    mem_table.put(&key.raw_ref(), value)?;
                    if mem_table.approximate_size() >= target_sst_size {
                        let sst = self.flush_single_memtable(&mem_table)?;
                        compacted_ssts.push(sst);
                        mem_table = MemTable::create(self.next_sst_id());
                    }
                    merge_iterator.next()?;
                }
                if !mem_table.is_empty() {
                    compacted_ssts.push(self.flush_single_memtable(&mem_table)?);
                }
                Ok(compacted_ssts)
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let ssts_to_be_compacted = {
            let ro_state = self.state.read();
            let mut ids = ro_state.l0_sstables.clone();
            ids.extend(&ro_state.levels[0].1);
            ids
        };
        let compacted_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            sstables: ssts_to_be_compacted.clone(),
        })?;
        let new_sst_ids: Vec<_> = compacted_ssts.iter().map(|s| s.sst_id()).collect();
        {
            let _lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut new_state = state.as_ref().clone();
            new_state
                .l0_sstables
                .retain(|x| !ssts_to_be_compacted.contains(x));
            new_state.levels[0] = (1, new_sst_ids);
            for sst_id in ssts_to_be_compacted {
                new_state.sstables.remove(&sst_id);
            }
            new_state
                .sstables
                .extend(compacted_ssts.iter().map(|s| (s.sst_id(), s.clone())));
            *state = Arc::new(new_state);
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let mem_table_num = {
            let ro_state = self.state.read();
            ro_state.imm_memtables.len() + 1
        };
        if mem_table_num > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
