use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::ScalarValue,
    physical_expr::expressions::Column,
    physical_plan::aggregates::{create_aggregate_expr, AggregateFunction},
};
use ee2::{
    helper::Entry, operator::hash_aggregate::HashAggregateOperator, physical_operator::Sink,
};
use std::sync::Arc;

#[test]
fn upstream_empty_aggregate_has_sql_empty_semantics() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    for grouped in [false, true] {
        let column = Arc::new(Column::new("id", 0));
        let count = create_aggregate_expr(
            &AggregateFunction::Count,
            false,
            &[column.clone()],
            &[],
            &schema,
            "n",
        )
        .unwrap();
        let group = if grouped {
            vec![(
                column as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                "id".into(),
            )]
        } else {
            vec![]
        };
        let mut op = HashAggregateOperator::new(schema.clone(), vec![count], group);
        let Entry::Batch(output) = op.finalize() else {
            panic!("expected batches")
        };
        if grouped {
            assert_eq!(output.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        } else {
            assert_eq!(output[0].num_rows(), 1);
            assert_eq!(
                ScalarValue::try_from_array(output[0].column(0), 0).unwrap(),
                ScalarValue::Int64(Some(0))
            );
        }
    }
}
