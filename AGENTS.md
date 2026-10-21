# Vectorloom development

Use `cargo build --locked --jobs 2`, `cargo test --locked --jobs 2`, `cargo fmt --all -- --check`, and `pwsh -NoProfile -File scripts/verify.ps1`. The CLI is `cargo run --locked -- --help`. Development/test profiles disable debug symbols to bound disk use.

On this Windows workspace the isolated toolchain uses RUSTUP_HOME=C:/Users/steve/.codex/tools/coursework-rust/rustup and CARGO_HOME=C:/Users/steve/.codex/tools/coursework-rust/cargo. Prepend CARGO_HOME/bin to PATH. Do not install globally.

Run the project verification skill under `.agents/skills/verify-vectorloom/`. Preserve retained evidence separately from maintained tooling. Keep upstream tests, provenance, LICENSE, README.upstream.md and Git history. The original pipeline is serial; only the custom owned-partition executor is parallel. Never execute unsupported physical plans through an implicit DataFusion fallback. Changes to supported plans require differential result tests and updated feature-map entries. No benchmark claims from diagnostic elapsed time.
