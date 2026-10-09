# List workspace and crate recipes
help:
    @just --list --list-submodules --unsorted

# Format the workspace
fmt:
    cargo +nightly fmt --all

# Check workspace formatting without changing files
fmt-check:
    cargo +nightly fmt --all --check

# Run Clippy on all workspace targets
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Run cargo check on the workspace members
check:
    cargo check --workspace --all-targets

# Check all workspace targets with profiling enabled
check-profile:
    cargo check --workspace --all-targets --features hotpath

# Run cargo build on the workspace members
build version commit_sha:
    @test -n {{quote(version)}} && test -n {{quote(commit_sha)}} || { echo 'version and commit_sha must not be empty' >&2; exit 1; }
    PGTEST_VERSION={{quote(version)}} PGTEST_COMMIT_SHA={{quote(commit_sha)}} cargo build --workspace --all-targets

# Build the local release server binary
build-release:
    cargo build --locked --release -p server --bin server

# Build the local release server binary with Hotpath
build-release-profile:
    cargo build --locked --release -p server --bin server --features hotpath

# Build the release Docker image (pgtest-server:<tag>)
docker-build tag="latest":
    docker build --build-arg CARGO_FEATURES= -t {{quote("pgtest-server:" + tag)}} .

# Build the Docker image with Hotpath (pgtest-server-hotpath:<tag>)
docker-build-profile tag="latest":
    docker build --build-arg CARGO_FEATURES=hotpath -t {{quote("pgtest-server-hotpath:" + tag)}} .

# Run cargo clean on the workspace members
clean:
    cargo clean

# Run workspace tests, including deterministic simulations (requires Docker)
test:
    cargo nextest run --locked -p pgtest-core --no-default-features --features test-support --no-fail-fast
    cargo nextest run --locked --workspace --features pgtest-core/runtime-tests --no-fail-fast
    cargo test --locked --workspace --doc --no-fail-fast

mod core "crates/pgtest-core"
mod server "apps/server"
mod wire "crates/pgtest-wire"
mod database "crates/pgtest-database-operations"
mod utils "crates/pgtest-utils"
mod cli "apps/cli"
