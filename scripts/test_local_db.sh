#!/usr/bin/env bash
# Run the whole test suite, database tests included, against a THROWAWAY
# local Postgres. Never against Neon or the VPS database.
#
#   scripts/test_local_db.sh                 # all tests
#   scripts/test_local_db.sh tests/test_scada.py -k mw_updates   # pytest args
#   KEEP_DB=1 scripts/test_local_db.sh       # leave the database running after
#
# What it does:
#   1. initdb a new cluster in a temp folder (trust auth, 127.0.0.1 only,
#      port $TEST_PG_PORT, default 55432);
#   2. loads schema/01_schema.sql and schema/02_seed.sql (2026 spine);
#   3. sets BOTH SPP_DSN and SPP_TEST_DSN to that database. Variables set in
#      the environment win over .env, so the DSNs in .env are not used;
#   4. runs pytest, then stops the server and deletes the folder.
#
# Needs the Postgres server binaries (initdb, pg_ctl), e.g.
#   brew install postgresql@16      # or any version >= 14
set -euo pipefail

cd "$(dirname "$0")/.."
PORT="${TEST_PG_PORT:-55432}"
PY="${PYTHON:-.venv/bin/python}"
[[ -x "$PY" ]] || PY=python3

for b in initdb pg_ctl psql; do
    command -v "$b" >/dev/null || { echo "ERROR: $b not found (install Postgres server binaries)" >&2; exit 1; }
done
if lsof -nP -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "ERROR: port $PORT is in use. Set TEST_PG_PORT to a free port." >&2
    exit 1
fi

PGDATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/spp-test-pg.XXXXXX")"   # TCP only: no socket path length limit
# shellcheck disable=SC2329  # run by the EXIT trap
stop() {
    if [[ "${KEEP_DB:-0}" == 1 ]]; then
        echo "KEEP_DB=1: database left running."
        echo "  SPP_TEST_DSN=$DSN"
        echo "  stop: pg_ctl -D $PGDATA_DIR stop && rm -rf $PGDATA_DIR"
        return
    fi
    pg_ctl -D "$PGDATA_DIR" -m fast stop >/dev/null 2>&1 || true
    rm -rf "$PGDATA_DIR"
}
trap stop EXIT

DSN="postgresql://spp@127.0.0.1:$PORT/spp_test"

initdb -D "$PGDATA_DIR" -U spp --auth=trust -E UTF8 --locale=C >/dev/null
pg_ctl -D "$PGDATA_DIR" -l "$PGDATA_DIR/server.log" -w \
    -o "-p $PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories='' -c fsync=off" \
    start >/dev/null || { echo "ERROR: Postgres did not start:" >&2; cat "$PGDATA_DIR/server.log" >&2; exit 1; }
createdb -h 127.0.0.1 -p "$PORT" -U spp spp_test

echo "loading schema into $DSN"
psql "$DSN" -q -v ON_ERROR_STOP=1 -f schema/01_schema.sql >/dev/null
psql "$DSN" -q -v ON_ERROR_STOP=1 -v seed_from=2026-01-01 -v seed_to=2026-12-31 \
     -f schema/02_seed.sql >/dev/null

# Both DSNs point at the throwaway database. The tests fall back to SPP_DSN
# for writes, and .env never overrides a variable that is already set.
export SPP_DSN="$DSN" SPP_TEST_DSN="$DSN"
case "$SPP_DSN$SPP_TEST_DSN" in
    *neon*|*@host.docker.internal*) echo "ERROR: refusing a non-local DSN" >&2; exit 1 ;;
esac

set +e
"$PY" -m pytest -q -rs "$@"
rc=$?
set -e
exit "$rc"
