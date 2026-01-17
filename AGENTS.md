# AGENTS Guide for Ruuster

This file provides build/test commands and coding conventions for agents working
in this repository.

## Workspace overview
- Rust workspace with crates: `queues`, `exchanges`, `protos`, `ruuster_grpc`,
  `utils`, `apps`, `internals`.
- gRPC APIs are defined in `protos/` and built via `tonic-build`.
- Main binaries live in `apps/` (`server`, `producer`, `consumer`, etc.).

## Build commands
- Build entire workspace: `cargo build`
- Build release: `cargo build --release`
- Build a single crate: `cargo build -p queues`
- Build a single binary: `cargo build -p apps --bin server`

## Run commands
- Run the server: `cargo run --bin server`
- Run producer: `cargo run --bin producer -- --server-addr "http://127.0.0.1:50051" ...`
- Run consumer: `cargo run --bin consumer -- --server-addr "http://127.0.0.1:50051" ...`
- Run scenario runner: `cargo run --bin run_scenario -- --server-addr "http://127.0.0.1:50051" ...`

## Prerequisites
- Install Rust stable toolchain (`rustup toolchain install stable`).
- Ensure `protoc` is available for gRPC codegen (`protos` uses `tonic-build`).
- If builds fail on TLS deps, install system OpenSSL development packages.
- Prefer running commands from the workspace root.

## Developer shortcuts
- Watch server: `cargo watch -x 'run --bin server' --ignore protos/`
- Watch tests: `cargo watch -x 'test' --ignore protos/`
- Enable logs: `RUST_LOG=info cargo run --bin server`
- gRPC descriptor output: `protos/defs/ruuster_descriptor.bin`

## Workspace notes
- Generated build artifacts land in `target/`.
- gRPC code is generated at build time; avoid editing generated files.
- Binaries in `apps/` are the primary runtime entrypoints.
- Keep `docker-compose.yml` focused on local observability tools.

## Lint and format
- Lint workspace: `cargo clippy --all-targets --all-features`
- Lint a crate: `cargo clippy -p exchanges`
- Format all Rust code: `cargo fmt --all`
- Check formatting only: `cargo fmt --all -- --check`

## Test commands
- Run all tests: `cargo test`
- Run tests for a crate: `cargo test -p queues`
- Run a single test by name: `cargo test my_test_name`
- Run a single test in a crate: `cargo test -p exchanges my_test_name`
- Run a specific module test: `cargo test -p queues module::tests::my_test`
- Run tests with logs: `RUST_LOG=info cargo test -- --nocapture`

## E2E scenario command (from CI)
- Build: `cargo build`
- Start server in background: `cargo run --bin server &`
- Run scenario: `RUST_LOG=info target/debug/run_scenario --server-addr "http://127.0.0.1:50051" \
  --config-file "apps/src/test_client/scenarios/all_consumers.json" \
  --builder-bin "target/debug/scenario_builder" \
  --consumer-bin "target/debug/consumer" \
  --producer-bin "target/debug/producer"`

## Docker/infra helpers
- Start Jaeger (traces UI): `docker compose up -d`
- Jaeger UI: `http://localhost:16686`

## Code style conventions

### Formatting
- Use `rustfmt` defaults; do not hand-format.
- Keep line length close to 100 characters when practical.
- Prefer trailing commas in multi-line structs, enums, and function calls.

### Imports
- Order imports: standard library, external crates, internal crates.
- Group imports with blank lines between groups.
- Prefer explicit imports over glob imports unless re-exporting prelude modules.
- Use `use crate::` or `use super::` for local modules when appropriate.

### Naming
- Modules/files: `snake_case`.
- Types/traits/enums: `PascalCase`.
- Functions/vars: `snake_case`.
- Consts/statics: `SCREAMING_SNAKE_CASE`.
- gRPC request/response structs follow proto naming conventions.

### Types and interfaces
- Prefer concrete types in public APIs; use traits when needed for extensibility.
- Use `Option<T>` for nullable data and `Result<T, E>` for fallible operations.
- Avoid `unwrap()` in production paths; use `?`, `expect()` with context, or
  return a structured error.
- Keep struct fields private and expose minimal accessors when possible.

### Error handling
- Use `thiserror` for custom error enums where needed.
- Return `Result` from fallible functions and propagate with `?`.
- Add context to errors with `expect()` or custom error variants.
- Log errors with `tracing` rather than `println!`.

### Logging and tracing
- Use `tracing` macros (`info!`, `warn!`, `error!`, `debug!`).
- Prefer structured fields: `info!(queue=%name, "message")`.
- Avoid noisy logs in hot loops; gate with appropriate levels.

### Async
- Prefer `tokio::main` for binaries.
- Avoid blocking calls in async contexts; use async filesystem/network APIs.
- Use `tokio::time::sleep` instead of `std::thread::sleep` in async code.

### gRPC/proto updates
- Proto changes live in `protos/defs`.
- Run `cargo build` to regenerate Rust code from proto definitions.
- Keep request/response naming consistent with proto definitions.

### CLI conventions
- CLI binaries use `clap` derive (`#[derive(Parser)]`).
- Use `--server-addr` and explicit flags rather than positional args.
- Document new flags in `README.md` if they change usage.

### Testing
- Unit tests live next to modules under `mod tests`.
- Prefer deterministic tests; avoid time-based flakes.
- When adding tests, keep setup minimal and reuse helpers.

### Documentation
- Update `README.md` when changing developer workflows.
- Keep comments concise and avoid redundant explanations.

### Configuration and CLI details
- Keep CLI flags consistent with existing names and casing.
- Prefer `String` fields for CLI args to simplify ownership.
- Use explicit defaults in `clap` when they improve UX.
- Surface configuration via flags before adding env vars.

### Performance and safety
- Avoid blocking calls inside async tasks.
- Prefer `Arc`/`Bytes` for shared payloads over excessive cloning.
- Keep locks scoped tightly and avoid holding them across awaits.
- Validate user input early and return structured errors.

### Git and workflow notes
- CI runs `cargo clippy` and `cargo test` on PRs.
- Keep changes focused and avoid refactors unrelated to the task.

### Repository hygiene
- Keep changes focused to the issue at hand.
- Avoid reformatting unrelated files or modules.

### Rules from editor integrations
- No `.cursor` rules or GitHub Copilot instructions were found in this repo.
