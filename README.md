# Vectorloom SQL

Vectorloom is a bounded, noninteractive SQL runner built on the complete MIT-licensed CMU 15-721 Spring 2024 Execution Engine #2 capstone. The original Arrow operators, tests, data, and Git history remain here. This adaptation adds a usable CLI, an explicit custom physical-plan executor, exact-key joins/grouping, parallel partition workers, and differential verification. It is a portfolio adaptation, not a claim of original authorship or course completion.

Upstream is [cmu-db/15721-s24-ee2](https://github.com/cmu-db/15721-s24-ee2), baseline `4272268d129455f0e3059d27fe731748674bd3cd`, listed as Execution Engine #2 on the [official project showcase](https://15721.courses.cs.cmu.edu/spring2024/project-showcase.html). Preserve [LICENSE](LICENSE) and [README.upstream.md](README.upstream.md).

## Run

Requires Rust with an MSVC linker on Windows, or a normal Rust host toolchain on Linux. Cargo.lock fixes the application dependency graph. Arrow 50 requires the pinned Chrono 0.4.34 to avoid its `quarter` method conflict with newer Chrono.

```powershell
cargo build --locked --jobs 2
cargo run --locked -- --synthetic 35 --batch-size 3 --workers 4 --sql "select l.id,l.value,r.value as rv from l join r on l.id=r.id and l.category=r.category order by l.value,rv"
cargo run --locked -- --parquet products=data/products.parquet --sql "select fruit,sum(price*quantity) as total from products group by fruit order by fruit"
cargo run --locked -- --csv items=items.csv --sql "select id,count(*) from items group by id"
```

`--help` lists arguments. `--synthetic N` registers `l` and `r`, each with nullable repeating `id`, string `category`, and integer `value`. `--batch-size` controls synthetic batches and planner batch size. CSV uses a header and inferred types. Parquet uses its embedded schema. Multiple `--csv` and `--parquet` registrations are supported. Each command executes one SQL query and exits. Errors use a nonzero exit status.

Stdout is CSV with headers, quoting via the csv crate, and empty fields for nulls. Empty strings and nulls share that CSV representation. Stderr contains the physical plan and `mode`, `rows`, `tasks`, `worker_ids`, and `elapsed_us`. The final stderr line is JSON metrics. Timing is diagnostic only, with no performance claim. Hash output order is unspecified without `ORDER BY`.

## Execution and limits

DataFusion 36 parses, optimizes, and plans SQL and supplies scalar/aggregate expressions. Its file readers decode input into Arrow memory tables. In normal mode, Vectorloom walks the physical plan and executes scan projection, vector filters and projections, sorting, limits, inner equijoins, and grouped/global aggregates itself. There is no silent fallback. `--reference` explicitly runs the DataFusion execution plan as a differential oracle.

The custom join hashes exact composite scalar keys into owned partitions. Each scoped worker builds its own hash table, probes its own partition, and gathers paired Arrow rows. Equality rechecks full keys even when hashes collide. Null join keys do not match. Aggregation partitions complete groups and runs independent Arrow accumulators per worker, supporting partial states and final merges. COUNT, SUM, MIN, MAX, and AVG are covered by differential tests. All workers rendezvous at a barrier; recorded thread IDs prove multiple worker tasks execute. This does not promise speedup or parallel execution of the entire query.

The retained upstream pipeline uses `Arc<RefCell>` and remains serial. Its shared grouping map now uses exact scalar keys, its join gathers both sides in matching order across build batches, and empty aggregate inputs are handled. `src/engine.rs` supplies the safe owned parallel path without making the serial pipeline share mutable state across threads.

This is an in-memory teaching engine. Each scanned table and join output is limited to 1,000,000 rows; CLI files are limited to 256 MiB, synthetic rows to 1,000,000, workers to 1..32, batch size to 1..65536, and SQL to 65536 bytes. Input decoding occurs before the table-row check. The explicit DataFusion reference mode bypasses custom engine row/output caps and is intended for small verification inputs. There is no spill, persistent catalog, transaction support, or cancellation. Heavy skew can leave most worker partitions empty.

Outer/semi/anti/cross joins, residual join filters, null-equal joins, windows, unions, grouping sets, aggregate FILTER/order clauses, and other unsupported physical nodes return explicit errors. Constant projections and optimizer-produced empty/placeholder inputs are supported. SQL DDL/DML are rejected before planning. Optimization can select an unsupported node even for SQL that appears similar to a supported query. The physical plan in stderr explains that boundary.

## Verify

```powershell
cargo fmt --all -- --check
cargo test --locked --jobs 2
pwsh -NoProfile -File scripts/verify.ps1
```

The maintained [verification skill](.agents/skills/verify-vectorloom/SKILL.md) maps the public library and CLI. `scripts/verify.ps1` preserves command output, test results, custom/reference CSV pairs, worker metrics, input-format results, and negative cases under ignored `evidence/`; it removes only its scratch CSV. The driver compares exact sorted query output for empty and multibatch inputs. Library differential tests compare row multisets, preserving duplicate multiplicity, with one and four workers. A regression reproduces the upstream multibatch row-pair bug, and a forced-hash-collision check validates exact-key equality.

CI uses a tokenless hosted Ubuntu runner for formatting, tests, and the CLI driver. The inherited workflow used a self-hosted runner and an embedded coverage-service credential. That workflow has been replaced. Historical commits still contain inherited content; no credential is required or used by these checks. Repository history was preserved, so upstream credential owners should treat the old credential as exposed.
