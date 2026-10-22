# Feature map

- library.md describes upstream Arrow operators and public consumer tests.
- sql.md describes SQL execution, table loading, exact joins, and grouped aggregation.
- The benchmark path runs `python3 scripts/benchmark.py --output evidence/benchmark` after a locked release build. It sweeps 1,000, 100,000 and 1,000,000 disk CSV rows with aggregate, selective filter, grouped aggregate, top-k and dimension join queries. Five alternating custom-one-worker, custom-four-worker and tuned DataFusion samples retain wall time, GNU-time peak RSS and exact ordered CSV digests. `--smoke --sizes 1000 --repeats 1` is correctness setup only. `--profile` is a separate syscall profile, excluded from reported measurements. No spill or disk-cache-cold claim.
