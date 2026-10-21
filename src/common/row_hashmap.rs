use ahash::{HashMap, RandomState};
use datafusion::common::ScalarValue;
use datafusion::{arrow::array::RecordBatch, physical_plan::PhysicalExpr};
use std::sync::Arc;

pub struct GroupedRowList {
    pub row_list: Vec<(usize, u64)>,
}

pub type GroupedRowHashMap = HashMap<Vec<ScalarValue>, GroupedRowList>;

pub fn grouped_row_hashmap_add_batch(
    hashmap: &mut GroupedRowHashMap,
    batch: &Arc<RecordBatch>,
    batch_id: usize,
    exprs: &Vec<Arc<dyn PhysicalExpr>>,
    _random_state: &RandomState,
) {
    let arrays = exprs
        .iter()
        .map(|expr| {
            expr.evaluate(batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
        })
        .collect::<Vec<_>>();
    for row in 0..batch.num_rows() {
        let key = arrays
            .iter()
            .map(|a| ScalarValue::try_from_array(a, row).unwrap())
            .collect::<Vec<_>>();
        hashmap
            .entry(key)
            .or_insert_with(|| GroupedRowList { row_list: vec![] })
            .row_list
            .push((batch_id, row as u64));
    }
}
