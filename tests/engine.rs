use datafusion::{
    arrow::{
        array::{Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    },
    common::ScalarValue,
    datasource::MemTable,
    prelude::{SessionConfig, SessionContext},
};
use ee2::engine::Engine;
use std::sync::Arc;

fn context(empty: bool) -> SessionContext {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(2)
            .set_bool("datafusion.execution.coalesce_batches", false),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("category", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));
    for name in ["l", "r"] {
        let batches = if empty {
            vec![RecordBatch::new_empty(schema.clone())]
        } else {
            vec![
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![Some(1), Some(2), None])),
                        Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
                        Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
                    ],
                )
                .unwrap(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![Some(2), Some(1), Some(1)])),
                        Arc::new(StringArray::from(vec![Some("y"), Some("x"), Some("other")])),
                        Arc::new(Int64Array::from(vec![Some(30), Some(40), Some(50)])),
                    ],
                )
                .unwrap(),
            ]
        };
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema.clone(), vec![batches]).unwrap()),
        )
        .unwrap();
    }
    ctx
}

fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(format!(
                "{:?}",
                batch
                    .columns()
                    .iter()
                    .map(|c| ScalarValue::try_from_array(c, row).unwrap())
                    .collect::<Vec<_>>()
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn differential_queries_and_parallel_workers() {
    for empty in [false, true] {
        let ctx = context(empty);
        for sql in [
            "select * from l",
            "select count(*) as n from l",
            "select count(*) as n from l where value>20",
            "select count(*) as n from l join r on l.id=r.id",
            "select 1 as n",
            "select value+1 as next from l where value>=20 order by next desc limit 2 offset 1",
            "select l.id,l.category,l.value,r.value as rv from l join r on l.id=r.id and l.category=r.category",
            "select l.id,l.value,r.value as rv from l join r on l.id=r.id",
            "select category,count(*) as n,sum(value) as total,min(value),max(value),avg(value) from l group by category",
            "select count(*),sum(value),avg(value) from l",
            "select id,category,count(*) from l group by id,category",
            "select category,count(*) from l where value>1000 group by category",
        ] {
            let plan=ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
            let expected=datafusion::physical_plan::collect(plan.clone(),ctx.task_ctx()).await.unwrap();
            for workers in [1,4] {
                let mut engine=Engine::new(workers).unwrap();
                let actual=engine.execute(&plan).unwrap_or_else(|e|panic!("{sql}: {e}"));
                assert_eq!(rows(&actual),rows(&expected),"empty={empty} workers={workers} sql={sql}");
                if format!("{plan:?}").contains("HashJoinExec") || format!("{plan:?}").contains("AggregateExec") {
                    assert!(engine.metrics.tasks>=workers);
                    assert!(engine.metrics.worker_ids.len()>=workers);
                }
            }
        }
    }
}

#[tokio::test]
async fn unsupported_plans_fail_explicitly() {
    let ctx = context(false);
    for sql in [
        "select * from l left join r on l.id=r.id",
        "select * from l cross join r",
        "select id from l union all select id from r",
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(Engine::new(2).unwrap().execute(&plan).is_err(), "{sql}");
    }
    assert!(Engine::new(0).is_err());
    assert!(Engine::new(33).is_err());
}

#[test]
fn exact_keys_survive_forced_hash_collisions() {
    use std::{
        collections::HashMap,
        hash::{BuildHasherDefault, Hasher},
    };
    #[derive(Default)]
    struct Collision;
    impl Hasher for Collision {
        fn finish(&self) -> u64 {
            0
        }
        fn write(&mut self, _: &[u8]) {}
    }
    let mut map: HashMap<ee2::engine::Key, usize, BuildHasherDefault<Collision>> =
        HashMap::default();
    map.insert(
        vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("x".into())),
        ],
        1,
    );
    map.insert(
        vec![
            ScalarValue::Int64(Some(2)),
            ScalarValue::Utf8(Some("x".into())),
        ],
        2,
    );
    assert_eq!(map.len(), 2);
    assert_eq!(
        map[&vec![
            ScalarValue::Int64(Some(1)),
            ScalarValue::Utf8(Some("x".into()))
        ]],
        1
    );
}

#[tokio::test]
async fn partial_and_final_aggregate_states_match_reference() {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(2)
            .set_bool("datafusion.optimizer.repartition_aggregations", false),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None])),
        ],
    )
    .unwrap();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(schema, vec![vec![batch.clone()], vec![batch]]).unwrap()),
    )
    .unwrap();
    for sql in [
        "select id,count(*),sum(value),avg(value) from t group by id",
        "select count(*),sum(value),avg(value) from t",
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let printed = format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        assert!(printed.contains("mode=Partial"), "{printed}");
        let expected = datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx())
            .await
            .unwrap();
        assert_eq!(
            rows(&Engine::new(4).unwrap().execute(&plan).unwrap()),
            rows(&expected),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn expression_errors_are_returned() {
    let ctx = context(false);
    let plan = ctx
        .sql("select value/(id-id) from l")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    assert!(Engine::new(2).unwrap().execute(&plan).is_err());
}

#[tokio::test]
async fn join_output_budget_returns_error_before_materializing_excess_rows() {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64; 1001]))],
    )
    .unwrap();
    for name in ["a", "b"] {
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![batch.clone()]]).unwrap()),
        )
        .unwrap();
    }
    let plan = ctx
        .sql("select a.id,b.id from a join b on a.id=b.id")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let error = Engine::new(4)
        .unwrap()
        .execute(&plan)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("join output exceeds 1000000 rows"),
        "{error}"
    );
}
