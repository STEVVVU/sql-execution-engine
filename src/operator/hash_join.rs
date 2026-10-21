use ahash;
use std::cell::RefCell;
use std::ops::Deref;
use std::sync::Arc;
use std::vec;

use datafusion::arrow::array::{Array, RecordBatch, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::physical_plan::PhysicalExpr;

use crate::common::enums::operator_result_type::{OperatorResultType, SinkResultType};
use crate::common::enums::physical_operator_type::PhysicalOperatorType;
use crate::common::row_hashmap::{grouped_row_hashmap_add_batch, GroupedRowHashMap};
use crate::helper::Entry;
use crate::physical_operator::{IntermediateOperator, PhysicalOperator, Sink};
use ahash::RandomState;
use datafusion::arrow::compute;
use datafusion::common::ScalarValue;

pub struct JoinLeftData {
    pub hash_table: GroupedRowHashMap,
    pub random_state: RandomState,
    pub originals: Vec<Arc<RecordBatch>>,
}

pub struct HashJoinBuildOperator {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    pub schema: Arc<Schema>,
    pub hash_table: GroupedRowHashMap,
    pub originals: Vec<Arc<RecordBatch>>,
    batch_id: usize,
    random_state: RandomState,
}

impl HashJoinBuildOperator {
    pub fn new(expr: Vec<Arc<dyn PhysicalExpr>>, schema: Arc<Schema>) -> Self {
        HashJoinBuildOperator {
            expr,
            schema,
            hash_table: Default::default(),
            originals: vec![],
            batch_id: 0,
            random_state: RandomState::generate_with(1, 2, 7, 8),
        }
    }
}

impl PhysicalOperator for HashJoinBuildOperator {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashJoinBuild
    }
}

impl Sink for HashJoinBuildOperator {
    fn sink(&mut self, batch: &Arc<RecordBatch>) -> SinkResultType {
        grouped_row_hashmap_add_batch(
            &mut self.hash_table,
            batch,
            self.batch_id,
            &self.expr,
            &self.random_state,
        );
        //append data to the collector
        self.originals.push(Arc::clone(batch));
        //increase the row counter for next iteration
        self.batch_id += 1;

        SinkResultType::NeedMoreInput
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn finalize(&mut self) -> Entry {
        let join_left_data = JoinLeftData {
            hash_table: std::mem::take(&mut self.hash_table),
            random_state: self.random_state.clone(),
            originals: std::mem::take(&mut self.originals),
        };
        Entry::HashMap(join_left_data)
    }
}

pub struct HashJoinProbeOperator {
    expr: Vec<Arc<dyn PhysicalExpr>>,
    left_side_schema: Arc<Schema>,
    output_schema: Arc<Schema>,
    store: Arc<RefCell<ahash::HashMap<usize, Entry>>>,
    uuid: usize,
    join_left_data: Entry,
}

impl HashJoinProbeOperator {
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        schema: Arc<Schema>,
        left_side_schema: Arc<Schema>,
        store: Arc<RefCell<ahash::HashMap<usize, Entry>>>,
        uuid: usize,
    ) -> Self {
        let num_output_fields = left_side_schema.fields.len() + schema.fields().len();
        let mut fields: Vec<Field> = Vec::with_capacity(num_output_fields);
        for f in left_side_schema.fields() {
            fields.push(f.deref().clone());
        }
        for f in schema.fields() {
            fields.push(f.deref().clone());
        }
        let output_schema = Arc::new(Schema::new(fields));
        HashJoinProbeOperator {
            expr,
            left_side_schema,
            output_schema,
            store,
            uuid,
            join_left_data: Entry::Empty,
        }
    }
}

impl IntermediateOperator for HashJoinProbeOperator {
    fn execute(&mut self, input: &Arc<RecordBatch>) -> OperatorResultType {
        if matches!(&self.join_left_data, Entry::Empty) {
            self.join_left_data = self.store.borrow_mut().remove(&self.uuid).unwrap();
        }
        let join_left_data = match &self.join_left_data {
            Entry::HashMap(data) => data,
            _ => {
                panic!()
            }
        };

        let num_rows = input.num_rows();
        let keys_values = self
            .expr
            .iter()
            .map(|f| {
                f.evaluate(input)
                    .unwrap()
                    .into_array(input.num_rows())
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let combined = compute::concat_batches(
            &self.left_side_schema,
            join_left_data.originals.iter().map(|b| b.as_ref()),
        )
        .unwrap();
        let mut offsets = Vec::new();
        let mut offset = 0;
        for batch in &join_left_data.originals {
            offsets.push(offset);
            offset += batch.num_rows() as u64;
        }
        let mut left_indices_vec = Vec::new();
        let mut right_indices_vec = Vec::new();
        for right_row_idx in 0..num_rows {
            let key = keys_values
                .iter()
                .map(|a| ScalarValue::try_from_array(a, right_row_idx).unwrap())
                .collect::<Vec<_>>();
            if key.iter().any(ScalarValue::is_null) {
                continue;
            }
            if let Some(matches) = join_left_data.hash_table.get(&key) {
                for (batch_id, row) in &matches.row_list {
                    left_indices_vec.push(offsets[*batch_id] + row);
                    right_indices_vec.push(right_row_idx as u64);
                }
            }
        }
        if right_indices_vec.is_empty() {
            return OperatorResultType::Finished(Arc::new(RecordBatch::new_empty(
                self.output_schema.clone(),
            )));
        }

        let mut columns: Vec<Arc<dyn Array>> =
            Vec::with_capacity(self.output_schema.fields().len());

        let left_indices = UInt64Array::from(left_indices_vec);
        for column in combined.columns() {
            columns.push(compute::take(column, &left_indices, None).unwrap());
        }
        let right_indices = UInt64Array::from(right_indices_vec);
        for right_col in input.columns() {
            let new_col = compute::take(right_col, &right_indices, None).unwrap();
            columns.push(new_col);
        }

        let output = RecordBatch::try_new(self.output_schema.clone(), columns).unwrap();

        OperatorResultType::Finished(Arc::new(output))
    }
}

impl PhysicalOperator for HashJoinProbeOperator {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.output_schema)
    }

    fn get_type(&self) -> PhysicalOperatorType {
        PhysicalOperatorType::HashJoinProbe
    }
}
