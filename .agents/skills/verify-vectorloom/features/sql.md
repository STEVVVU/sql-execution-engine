# SQL CLI
## Sub-features
Synthetic, CSV, Parquet input; SQL filters, projections, joins, aggregates, sorting and limits.
## How to get to it (user POV)
Run cargo run -- --help, then provide --sql and tables.
## Driving it with PowerShell
Run scripts/verify.ps1 after building. Compare custom execution with --reference explicitly.
## Gotchas
Only supported physical plans execute. In-memory, bounded workloads; no spill. Driver proof covers empty and multibatch joins/aggregates against DataFusion, filtered and joined COUNT(*) with zero-column projections, CSV and Parquet, invalid workers and unsupported outer joins. Evidence is under evidence/.
