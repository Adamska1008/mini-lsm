use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        // check l0 cond
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }
        return None;
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut ns = snapshot.clone();
        let mut removed = vec![];
        fn filter_ssts(sst_ids: &[usize], to_remove: &[usize]) -> (Vec<usize>, Vec<usize>) {
            let to_remove_set: HashSet<_> = to_remove.iter().collect();
            let (retained, removed): (Vec<_>, Vec<_>) =
                sst_ids.iter().partition(|id| !to_remove_set.contains(id));
            (retained, removed)
        }
        if let Some(upper_level) = task.upper_level {
            todo!()
        } else {
            let (l0_retained, l0_removed) =
                filter_ssts(&snapshot.l0_sstables, &task.upper_level_sst_ids);
            ns.l0_sstables = l0_retained;
            removed.extend(l0_removed);
            let (l1_retained, l1_removed) =
                filter_ssts(&snapshot.levels[0].1, &task.lower_level_sst_ids);
            ns.levels[0].1 = l1_retained;
            removed.extend(l1_removed);
            ns.levels[0].1.extend(output);
        }
        (ns, removed)
    }
}
