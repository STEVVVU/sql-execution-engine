use datafusion::arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::JoinType;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    aggregates::{AggregateExec, AggregateMode},
    joins::HashJoinExec,
    memory::MemoryExec,
    ExecutionPlan,
};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Barrier,
};
use std::thread;

pub type Key = Vec<ScalarValue>;
pub const MAX_ROWS: usize = 1_000_000;

fn unsupported(message: impl Into<String>) -> DataFusionError {
    DataFusionError::NotImplemented(message.into())
}

#[derive(Default, Debug)]
pub struct Metrics {
    pub tasks: usize,
    pub worker_ids: Vec<String>,
}

pub struct Engine {
    workers: usize,
    pub metrics: Metrics,
}

fn values(exprs: &[Arc<dyn PhysicalExpr>], batch: &RecordBatch) -> Result<Vec<ArrayRef>> {
    exprs
        .iter()
        .map(|e| e.evaluate(batch)?.into_array(batch.num_rows()))
        .collect()
}

fn key(arrays: &[ArrayRef], row: usize) -> Result<Key> {
    arrays
        .iter()
        .map(|a| ScalarValue::try_from_array(a, row))
        .collect()
}

fn partition(key: &Key, workers: usize) -> usize {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hash);
    hash.finish() as usize % workers
}

fn select(batch: &RecordBatch, rows: Vec<u64>) -> Result<RecordBatch> {
    let options =
        datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows.len()));
    let rows = UInt64Array::from(rows);
    Ok(RecordBatch::try_new_with_options(
        batch.schema(),
        batch
            .columns()
            .iter()
            .map(|c| take(c, &rows, None))
            .collect::<std::result::Result<_, _>>()?,
        &options,
    )?)
}

impl Engine {
    pub fn new(workers: usize) -> Result<Self> {
        if !(1..=32).contains(&workers) {
            return Err(DataFusionError::Plan(
                "workers must be between 1 and 32".into(),
            ));
        }
        Ok(Self {
            workers,
            metrics: Metrics::default(),
        })
    }

    pub fn execute(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<RecordBatch>> {
        use datafusion::physical_plan::{
            coalesce_batches::CoalesceBatchesExec,
            coalesce_partitions::CoalescePartitionsExec,
            filter::FilterExec,
            limit::{GlobalLimitExec, LocalLimitExec},
            projection::ProjectionExec,
            sorts::sort::SortExec,
        };

        let node = plan.as_any();
        if node.is::<datafusion::physical_plan::placeholder_row::PlaceholderRowExec>() {
            let schema = plan.schema();
            let columns = schema
                .fields()
                .iter()
                .map(|f| datafusion::arrow::array::new_null_array(f.data_type(), 1))
                .collect();
            let options =
                datafusion::arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1));
            return Ok(vec![RecordBatch::try_new_with_options(
                schema, columns, &options,
            )?]);
        }
        if node.is::<datafusion::physical_plan::empty::EmptyExec>() {
            return Ok(vec![RecordBatch::new_empty(plan.schema())]);
        }
        if let Some(memory) = node.downcast_ref::<MemoryExec>() {
            if memory
                .partitions()
                .iter()
                .flatten()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                > MAX_ROWS
            {
                return Err(DataFusionError::ResourcesExhausted(
                    "input exceeds 1000000 rows".into(),
                ));
            }
            return memory
                .partitions()
                .iter()
                .flatten()
                .map(|b| match memory.projection() {
                    Some(p) => Ok(b.project(p)?),
                    None => Ok(b.clone()),
                })
                .collect();
        }
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            if *join.join_type() != JoinType::Inner
                || join.filter().is_some()
                || join.null_equals_null()
            {
                return Err(unsupported(
                    "only inner equijoins without residual filters are supported",
                ));
            }
            let children = plan.children();
            let left = concat_batches(&children[0].schema(), &self.execute(&children[0])?)?;
            let right = concat_batches(&children[1].schema(), &self.execute(&children[1])?)?;
            return self.join(left, right, join);
        }
        let children = plan.children();
        if children.len() != 1 {
            return Err(unsupported(format!("unsupported plan {plan:?}")));
        }
        if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            let batches = self.execute(&children[0])?;
            let batch = concat_batches(&children[0].schema(), &batches)?;
            return self.aggregate(batch, aggregate);
        }
        if node.is::<CoalesceBatchesExec>() || node.is::<CoalescePartitionsExec>() {
            return self.execute(&children[0]);
        }
        let batches = self.execute(&children[0])?;
        if let Some(filter) = node.downcast_ref::<FilterExec>() {
            return batches
                .into_iter()
                .map(|batch| {
                    let predicate = filter
                        .predicate()
                        .evaluate(&batch)?
                        .into_array(batch.num_rows())?;
                    let predicate = predicate
                        .as_any()
                        .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("filter must be boolean".into())
                        })?;
                    Ok(datafusion::arrow::compute::filter_record_batch(
                        &batch, predicate,
                    )?)
                })
                .collect();
        }
        if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            return batches
                .into_iter()
                .map(|batch| {
                    let columns = projection
                        .expr()
                        .iter()
                        .map(|(e, _)| e.evaluate(&batch)?.into_array(batch.num_rows()))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(RecordBatch::try_new(projection.schema(), columns)?)
                })
                .collect();
        }
        if let Some(sort) = node.downcast_ref::<SortExec>() {
            let combined = concat_batches(&children[0].schema(), &batches)?;
            let columns = sort
                .expr()
                .iter()
                .map(|e| e.evaluate_to_sort_column(&combined))
                .collect::<Result<Vec<_>>>()?;
            let indices = datafusion::arrow::compute::lexsort_to_indices(&columns, sort.fetch())?;
            return Ok(vec![RecordBatch::try_new(
                plan.schema(),
                combined
                    .columns()
                    .iter()
                    .map(|c| take(c, &indices, None))
                    .collect::<std::result::Result<_, _>>()?,
            )?]);
        }
        if let Some(limit) = node.downcast_ref::<GlobalLimitExec>() {
            let batch = concat_batches(&plan.schema(), &batches)?;
            let skip = limit.skip().min(batch.num_rows());
            return Ok(vec![batch.slice(
                skip,
                limit
                    .fetch()
                    .unwrap_or(usize::MAX)
                    .min(batch.num_rows() - skip),
            )]);
        }
        if let Some(limit) = node.downcast_ref::<LocalLimitExec>() {
            let batch = concat_batches(&plan.schema(), &batches)?;
            return Ok(vec![batch.slice(0, limit.fetch().min(batch.num_rows()))]);
        }
        Err(unsupported(format!("unsupported plan {plan:?}")))
    }

    fn join(
        &mut self,
        left: RecordBatch,
        right: RecordBatch,
        join: &HashJoinExec,
    ) -> Result<Vec<RecordBatch>> {
        let left_keys = values(
            &join.on().iter().map(|(l, _)| l.clone()).collect::<Vec<_>>(),
            &left,
        )?;
        let right_keys = values(
            &join.on().iter().map(|(_, r)| r.clone()).collect::<Vec<_>>(),
            &right,
        )?;
        let mut builds = vec![Vec::new(); self.workers];
        let mut probes = vec![Vec::new(); self.workers];
        for (batch, arrays, partitions) in [
            (&left, &left_keys, &mut builds),
            (&right, &right_keys, &mut probes),
        ] {
            for row in 0..batch.num_rows() {
                let key = key(arrays, row)?;
                if key.iter().any(ScalarValue::is_null) {
                    continue;
                }
                partitions[partition(&key, self.workers)].push((key, row as u64));
            }
        }
        let barrier = Arc::new(Barrier::new(self.workers));
        let schema = join.schema();
        let output_rows = AtomicUsize::new(0);
        let results = thread::scope(|scope| {
            let handles: Vec<_> = builds
                .into_iter()
                .zip(probes)
                .map(|(build, probe)| {
                    let (left, right, schema, barrier, output_rows) =
                        (&left, &right, schema.clone(), barrier.clone(), &output_rows);
                    scope.spawn(move || -> Result<_> {
                        barrier.wait();
                        let mut map: HashMap<Key, Vec<u64>> = HashMap::new();
                        for (key, row) in build {
                            map.entry(key).or_default().push(row);
                        }
                        let (mut ls, mut rs) = (Vec::new(), Vec::new());
                        for (key, row) in probe {
                            if let Some(matches) = map.get(&key) {
                                for l in matches {
                                    if output_rows.fetch_add(1, Ordering::Relaxed) >= MAX_ROWS {
                                        return Err(DataFusionError::ResourcesExhausted(
                                            "join output exceeds 1000000 rows".into(),
                                        ));
                                    }
                                    ls.push(*l);
                                    rs.push(row);
                                }
                            }
                        }
                        let l = select(left, ls)?;
                        let r = select(right, rs)?;
                        let columns = l.columns().iter().chain(r.columns()).cloned().collect();
                        Ok((
                            RecordBatch::try_new(schema, columns)?,
                            format!("{:?}", thread::current().id()),
                        ))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join()
                        .map_err(|_| DataFusionError::Execution("join worker panicked".into()))?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        self.record(results)
    }

    fn record(&mut self, results: Vec<(RecordBatch, String)>) -> Result<Vec<RecordBatch>> {
        self.metrics.tasks += results.len();
        for (_, id) in &results {
            if !self.metrics.worker_ids.contains(id) {
                self.metrics.worker_ids.push(id.clone());
            }
        }
        Ok(results.into_iter().map(|(batch, _)| batch).collect())
    }

    fn aggregate(
        &mut self,
        batch: RecordBatch,
        aggregate: &AggregateExec,
    ) -> Result<Vec<RecordBatch>> {
        if aggregate
            .aggr_expr()
            .iter()
            .any(|e| e.order_bys().is_some())
            || aggregate.filter_expr().iter().any(Option::is_some)
            || !aggregate.group_by().null_expr().is_empty()
            || aggregate.group_by().groups().len() > 1
        {
            return Err(unsupported(
                "ordered aggregates, aggregate FILTER and grouping sets are unsupported",
            ));
        }
        let exprs: Vec<_> = aggregate
            .group_by()
            .expr()
            .iter()
            .map(|(e, _)| e.clone())
            .collect();
        let group_count = exprs.len();
        let arrays = values(&exprs, &batch)?;
        let mut partitions: Vec<HashMap<Key, Vec<u64>>> =
            (0..self.workers).map(|_| HashMap::new()).collect();
        for row in 0..batch.num_rows() {
            let key = key(&arrays, row)?;
            partitions[partition(&key, self.workers)]
                .entry(key)
                .or_default()
                .push(row as u64);
        }
        if exprs.is_empty() && batch.num_rows() == 0 {
            partitions[0].insert(vec![], vec![]);
        }
        let barrier = Arc::new(Barrier::new(self.workers));
        let results = thread::scope(|scope| {
            let handles: Vec<_> = partitions
                .into_iter()
                .map(|groups| {
                    let (batch, barrier) = (&batch, barrier.clone());
                    scope.spawn(move || -> Result<_> {
                        barrier.wait();
                        let mut rows = Vec::new();
                        for (key, indices) in groups {
                            let input = select(batch, indices)?;
                            let mut output = key;
                            let mut state_offset = group_count;
                            for expr in aggregate.aggr_expr() {
                                let mut acc = expr.create_accumulator()?;
                                let inputs = if matches!(
                                    aggregate.mode(),
                                    AggregateMode::Final | AggregateMode::FinalPartitioned
                                ) {
                                    let count = expr.state_fields()?.len();
                                    let arrays = input.columns()
                                        [state_offset..state_offset + count]
                                        .to_vec();
                                    state_offset += count;
                                    arrays
                                } else {
                                    values(&expr.expressions(), &input)?
                                };
                                match aggregate.mode() {
                                    AggregateMode::Final | AggregateMode::FinalPartitioned => {
                                        acc.merge_batch(&inputs)?
                                    }
                                    _ => acc.update_batch(&inputs)?,
                                }
                                if matches!(aggregate.mode(), AggregateMode::Partial) {
                                    output.extend(acc.state()?);
                                } else {
                                    output.push(acc.evaluate()?);
                                }
                            }
                            rows.push(output);
                        }
                        let output = rows_to_batch(aggregate.schema(), rows)?;
                        Ok((output, format!("{:?}", thread::current().id())))
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| {
                    h.join().map_err(|_| {
                        DataFusionError::Execution("aggregate worker panicked".into())
                    })?
                })
                .collect::<Result<Vec<_>>>()
        })?;
        self.record(results)
    }
}

fn rows_to_batch(schema: SchemaRef, rows: Vec<Vec<ScalarValue>>) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let columns = (0..schema.fields().len())
        .map(|col| ScalarValue::iter_to_array(rows.iter().map(|r| r[col].clone())))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema, columns)?)
}
