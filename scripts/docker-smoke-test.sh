#!/bin/sh
set -eu

image="${1:?Usage: docker-smoke-test.sh IMAGE}"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

# Fail during configuration, before starting workers or connecting to PostgreSQL.
# This exercises the executable and dynamic-loader requirements inside scratch.
status=0
docker run --rm -e PGTEST_LISTEN_ADDR=invalid "$image" >"$log" 2>&1 || status=$?
cat "$log"
test "$status" -eq 1
grep -q 'invalid server configuration' "$log"
