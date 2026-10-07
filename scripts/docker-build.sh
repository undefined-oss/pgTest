#!/bin/sh
set -eu

phase="${1:-}"
case "$phase" in
    cook|build) ;;
    *)
        echo "Usage: $0 {cook|build}" >&2
        exit 2
        ;;
esac

target="$(uname -m)-unknown-linux-musl"
set -- --locked --release --target "$target" -p server --bin server
if [ -n "${CARGO_FEATURES:-}" ]; then
    set -- "$@" --features "$CARGO_FEATURES"
fi

if [ "$phase" = cook ]; then
    exec cargo chef cook --recipe-path recipe.json "$@"
fi

cargo build "$@"

binary="target/$target/release/server"
if readelf -l "$binary" | grep -q INTERP || readelf -d "$binary" | grep -q NEEDED; then
    readelf -l -d "$binary" >&2
    echo "The scratch runtime requires a fully static server binary" >&2
    exit 1
fi

output_dir="${PGTEST_OUTPUT_DIR:-/out}"
install -D -m 0755 "$binary" "$output_dir/usr/local/bin/pgtest-server"
# Preserve a writable /tmp for the non-root scratch runtime.
install -d -m 1777 "$output_dir/tmp"
