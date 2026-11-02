#!/usr/bin/env bash
# Load SPP feeds for a calendar range, one month window at a time.
#
#   scripts/backfill.sh FEEDS START END [extra spp.run options]
#
#   FEEDS  one feed, a comma list, or "all"  (rt_lmp,rt_mcp,da_lmp,da_mcp,hourly_load)
#   START  first OPERATING day (date of the prints), YYYY-MM-DD
#   END    last operating day, included,             YYYY-MM-DD
#
# Examples:
#   scripts/backfill.sh da_lmp,da_mcp,hourly_load 2025-01-01 2025-12-31
#   scripts/backfill.sh rt_lmp 2026-10-01 2026-10-31 --groups nppd
#   scripts/backfill.sh all 2026-10-01 2026-10-31 --fill     # forward fill
#
# Each window is a separate `python -m spp.run ... --no-strict` call with its
# own log file in logs/backfill/. A failed window does not stop the script;
# the summary at the end lists it. Every write is an upsert, so you can run
# the same range again.
#
# PYTHON selects the interpreter (default: .venv/bin/python, else python3).
set -uo pipefail

if [[ $# -lt 3 ]]; then
    sed -n '2,21p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
fi

FEEDS_ARG=$1 START=$2 END=$3
shift 3
EXTRA=("$@")

cd "$(dirname "$0")/.." || exit 1
if [[ -z "${PYTHON:-}" ]]; then
    if [[ -x .venv/bin/python ]]; then PYTHON=.venv/bin/python; else PYTHON=python3; fi
fi

if [[ $FEEDS_ARG == all ]]; then
    FEEDS=(da_lmp da_mcp hourly_load rt_lmp rt_mcp)
else
    IFS=, read -r -a FEEDS <<< "$FEEDS_ARG"
fi

# Month windows [w_start, w_end] that cover [START, END].
windows() {
    "$PYTHON" - "$START" "$END" <<'EOF'
import sys
from datetime import date, timedelta
start, end = (date.fromisoformat(a) for a in sys.argv[1:3])
if end < start:
    sys.exit("END is before START")
d = start
while d <= end:
    nxt = (d.replace(day=1) + timedelta(days=32)).replace(day=1)
    print(d.isoformat(), min(end, nxt - timedelta(days=1)).isoformat())
    d = nxt
EOF
}

WINDOWS=$(windows) || exit 2
mkdir -p logs/backfill
failed=()

for feed in "${FEEDS[@]}"; do
    while read -r w_start w_end; do
        logfile="logs/backfill/${feed}_${w_start}_${w_end}.log"
        echo "$(date '+%F %T')  ${feed}  ${w_start} .. ${w_end}  -> ${logfile}"
        if ! "$PYTHON" -m spp.run --feed "$feed" --start "$w_start" --end "$w_end" \
                --no-strict "${EXTRA[@]}" > "$logfile" 2>&1; then
            failed+=("${feed} ${w_start}..${w_end}")
            echo "    FAILED (see ${logfile})"
        else
            tail -n 1 "$logfile" | sed 's/^/    /'
        fi
    done <<< "$WINDOWS"
done

if [[ ${#failed[@]} -gt 0 ]]; then
    echo "Failed windows:"
    printf '  %s\n' "${failed[@]}"
    exit 1
fi
echo "All windows complete."
