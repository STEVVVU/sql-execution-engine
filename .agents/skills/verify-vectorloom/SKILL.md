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
The driver writes timestamped command transcripts and exit codes under ignored `evidence/`. Tests must assert resulting values. CLI comparisons use an explicit DataFusion reference execution and compare parsed rows, preserving duplicates. Logs must survive cleanup. Record unsupported features as limitations instead of silently running the oracle.

## Cleanup
No service is launched. Each invocation exits on completion. Remove only scratch files created by the driver, if any. Keep evidence and build outputs for inspection. Never stop unrelated cargo processes.
