# syntax=docker/dockerfile:1
FROM rust:trixie AS chef

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential cmake mold musl-tools pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY rust-toolchain.toml ./
RUN rustup set profile minimal && rustup show active-toolchain
RUN cargo install cargo-chef --locked --version 0.1.78
COPY .cargo/ .cargo/

# uname reports x86_64 or aarch64 for the image's platform.
# Linker settings live in .cargo/config.toml; select musl explicitly below.
RUN rustup target add "$(uname -m)-unknown-linux-musl"

FROM chef AS planner
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS build
COPY --chmod=0755 scripts/docker-build.sh /usr/local/bin/pgtest-docker-build
COPY --from=planner /app/recipe.json recipe.json
ARG CARGO_FEATURES=""
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    pgtest-docker-build cook

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY apps/ apps/
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    pgtest-docker-build build

FROM scratch
COPY --from=build /out/ /
USER 65532:65532
WORKDIR /tmp
ENV PGTEST_LISTEN_ADDR=0.0.0.0
ENV PGTEST_LISTEN_PORT=6432
EXPOSE 6432
STOPSIGNAL SIGINT
ENTRYPOINT ["/usr/local/bin/pgtest-server"]
