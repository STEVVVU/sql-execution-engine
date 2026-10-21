use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::MemTable;
use datafusion::prelude::{CsvReadOptions, ParquetReadOptions, SessionConfig, SessionContext};
use ee2::engine::Engine;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "--help") {
        println!("SQL execution engine\n--sql SQL [--workers 1..32] [--batch-size N] [--synthetic N]\n[--csv NAME=PATH] [--parquet NAME=PATH] [--reference]\nSynthetic input creates tables l and r with id, category, value. Output is CSV; metrics and physical plan are on stderr.");
        return Ok(());
    }
    let mut sql = None;
    let (mut workers, mut batch_size, mut synthetic) = (4, 1024, None);
    let (mut reference, mut tables) = (false, Vec::new());
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        if arg == "--reference" {
            reference = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| DataFusionError::Plan(format!("missing value for {arg}")))?;
        let number = || {
            value
                .parse::<usize>()
                .map_err(|_| DataFusionError::Plan(format!("invalid integer for {arg}")))
        };
        match arg.as_str() {
            "--sql" => sql = Some(value.clone()),
            "--workers" => workers = number()?,
            "--batch-size" => batch_size = number()?,
            "--synthetic" => synthetic = Some(number()?),
            "--csv" | "--parquet" => tables.push((arg.clone(), value.clone())),
            _ => return Err(DataFusionError::Plan(format!("unknown argument {arg}"))),
        }
        i += 2;
    }
    if !(1..=65536).contains(&batch_size) || synthetic.is_some_and(|n| n > 1_000_000) {
        return Err(DataFusionError::Plan(
            "batch size must be 1..65536; synthetic rows at most 1000000".into(),
        ));
    }
    let sql = sql.ok_or_else(|| DataFusionError::Plan("--sql is required".into()))?;
    if sql.len() > 65536 {
        return Err(DataFusionError::Plan("SQL exceeds 65536 bytes".into()));
    }
    use datafusion::sql::{
        parser::{DFParser, Statement},
        sqlparser::ast::Statement as SqlStatement,
    };
    let statements = DFParser::parse_sql(&sql).map_err(|e| DataFusionError::Plan(e.to_string()))?;
    if statements.len() != 1
        || !matches!(statements.front(), Some(Statement::Statement(statement)) if matches!(statement.as_ref(), SqlStatement::Query(_)))
    {
        return Err(DataFusionError::Plan(
            "exactly one SELECT query is required".into(),
        ));
    }
    let mut engine = Engine::new(workers)?;
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size)
        .set_bool("datafusion.optimizer.repartition_aggregations", false)
        .set_bool("datafusion.optimizer.repartition_joins", false)
        .set_bool("datafusion.execution.coalesce_batches", false);
    let ctx = SessionContext::new_with_config(config);
    if let Some(n) = synthetic {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        for name in ["l", "r"] {
            let mut batches = Vec::new();
            for start in (0..n).step_by(batch_size) {
                let end = (start + batch_size).min(n);
                batches.push(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(
                            (start..end)
                                .map(|i| {
                                    if i % 11 == 0 {
                                        None
                                    } else {
                                        Some((i % 17) as i64)
                                    }
                                })
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(StringArray::from(
                            (start..end)
                                .map(|i| if i % 2 == 0 { "a" } else { "b" })
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            (start..end).map(|i| i as i64).collect::<Vec<_>>(),
                        )),
                    ],
                )?);
            }
            if batches.is_empty() {
                batches.push(RecordBatch::new_empty(schema.clone()));
            }
            ctx.register_table(
                name,
                Arc::new(MemTable::try_new(schema.clone(), vec![batches])?),
            )?;
        }
    }
    for (kind, table) in tables {
        let (name, path) = table
            .split_once('=')
            .ok_or_else(|| DataFusionError::Plan("table must be NAME=PATH".into()))?;
        if name.is_empty() || path.is_empty() {
            return Err(DataFusionError::Plan(
                "table name and path must be nonempty".into(),
            ));
        }
        // Load through DataFusion's format readers, then register Arrow batches for custom execution.
        if std::fs::metadata(path)?.len() > 268_435_456 {
            return Err(DataFusionError::ResourcesExhausted(
                "input file exceeds 256 MiB".into(),
            ));
        }
        let df = if kind == "--csv" {
            ctx.read_csv(path, CsvReadOptions::new()).await?
        } else {
            ctx.read_parquet(path, ParquetReadOptions::default())
                .await?
        };
        let schema = Arc::new(Schema::from(df.schema()));
        let batches = df.collect().await?;
        ctx.register_table(name, Arc::new(MemTable::try_new(schema, vec![batches])?))?;
    }
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    eprintln!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    let output_schema = plan.schema();
    let start = std::time::Instant::now();
    let batches = if reference {
        datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?
    } else {
        engine.execute(&plan)?
    };
    let mut writer = csv::Writer::from_writer(std::io::stdout());
    writer
        .write_record(output_schema.fields().iter().map(|f| f.name()))
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let mut rows = 0;
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let fields = batch
                .columns()
                .iter()
                .map(|c| {
                    datafusion::common::ScalarValue::try_from_array(c, row).map(|s| {
                        if s.is_null() {
                            String::new()
                        } else {
                            s.to_string()
                        }
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            writer
                .write_record(fields)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            rows += 1;
        }
    }
    writer.flush()?;
    eprintln!(
        r#"{{"mode":"{}","rows":{},"tasks":{},"worker_ids":{:?},"elapsed_us":{}}}"#,
        if reference {
            "datafusion-reference"
        } else {
            "custom"
        },
        rows,
        engine.metrics.tasks,
        engine.metrics.worker_ids,
        start.elapsed().as_micros()
    );
    Ok(())
}
