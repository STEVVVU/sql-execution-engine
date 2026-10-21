use ahash::{HashMap, HashMapExt};
use datafusion::arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::common::ScalarValue;
use datafusion::physical_expr::expressions::Column;
use ee2::{
    common::enums::operator_result_type::OperatorResultType,
    operator::hash_join::{HashJoinBuildOperator, HashJoinProbeOperator},
    physical_operator::{IntermediateOperator, Sink},
};
use std::{cell::RefCell, sync::Arc};

#[test]
fn upstream_join_preserves_row_pairs_across_build_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
    let batch = |values| {
        Arc::new(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))]).unwrap(),
        )
    };
    let mut build =
        HashJoinBuildOperator::new(vec![Arc::new(Column::new("key", 0))], schema.clone());
    build.sink(&batch(vec![Some(1), None]));
    build.sink(&batch(vec![Some(2), Some(1)]));
    let store = Arc::new(RefCell::new(HashMap::new()));
    store.borrow_mut().insert(0, build.finalize());
    let mut probe = HashJoinProbeOperator::new(
        vec![Arc::new(Column::new("key", 0))],
        schema.clone(),
        schema.clone(),
        store,
        0,
    );
    let output = match probe.execute(&batch(vec![Some(2), Some(1), None])) {
        OperatorResultType::Finished(b) | OperatorResultType::HaveMoreOutput(b) => b,
    };
    assert_eq!(output.num_rows(), 3);
    for row in 0..output.num_rows() {
        assert_eq!(
            ScalarValue::try_from_array(output.column(0), row).unwrap(),
            ScalarValue::try_from_array(output.column(1), row).unwrap()
        );
    }
}
