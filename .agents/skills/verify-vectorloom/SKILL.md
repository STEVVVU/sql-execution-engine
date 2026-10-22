---
name: verify-vectorloom
description: Build and drive Vectorloom SQL and its public library consumer tests; retain command, output, and exit-code evidence.
---

# Verify Vectorloom

## Launch
Use the isolated Rust toolchain configured in AGENTS.md. Run `cargo build --locked --jobs 2` from the repository root. The process is short lived and creates only ignored build outputs.

## Doctor
Run `target/debug/ee2.exe --help` on Windows or `target/debug/ee2 --help` on Unix; expect the Vectorloom SQL banner and exit 0. Also inspect `git status --short`. A successful build of this checkout is required before driving it.

## Drive
Run `pwsh -NoProfile -File scripts/verify.ps1`. The maintained driver invokes the public library integration tests and the noninteractive CLI. See features/README.md for the mapped surfaces.

## Evidence
For performance work, build `cargo build --release --locked --jobs 2`, then run `python3 scripts/benchmark.py --output evidence/benchmark` on an isolated Linux host. The workflow `.github/workflows/benchmark.yml` retains the full dataset/query/config manifest, raw samples, output digests, plans, wall time, and per-command GNU-time resources. Run `--smoke --sizes 1000 --repeats 1` only to prove setup. Run `--profile` into a different output directory for diagnostics. Inspect the real operator/source limiter before acting on an apparent difference. The regular CLI driver also checks the explicit `--reference-tuned` DataFusion default planner path.

For the demonstrated join regression, dispatch `.github/workflows/profile.yml`. It installs Callgrind only on the disposable runner, builds the same optimized implementation with line information, and drives all three sides with `scripts/profile.py`. It verifies exact frozen fixture and result hashes from campaign 39729583363. Do not report instrumented wall times as benchmark results. Read per-function/source instruction evidence before changing the executor. Each subprocess has a 240-second bound and the profile command checks the 10 GiB disk reserve.

For the profiled hashing candidate, dispatch `.github/workflows/compare.yml`. It runs the existing real-product verifier and builds frozen `1f73fca` plus the candidate on one runner/toolchain. `python3 scripts/compare.py --baseline-binary /path/to/baseline --candidate-binary /path/to/candidate --output evidence/compare` drives the unchanged full matrix with pinned Hyperfine 2.0.0. The only outer loop reverses six build/executor arms across five single-run native exports after warmup. `scripts/frozen-benchmark.json` fixes the original SQL, input hashes and all 15 result hashes. Inspect schema-2 raw measurements and GNU-time RSS; no custom timers/statistics or cross-runner speedup comparison. This path remains a draft until its first hosted execution passes.

The driver writes timestamped command transcripts and exit codes under ignored `evidence/`. Tests must assert resulting values. CLI comparisons use an explicit DataFusion reference execution and compare parsed rows, preserving duplicates. Logs must survive cleanup. Record unsupported features as limitations instead of silently running the oracle.

## Cleanup
No service is launched. Each invocation exits on completion. Remove only scratch files created by the driver, if any. Keep evidence and build outputs for inspection. Never stop unrelated cargo processes.
