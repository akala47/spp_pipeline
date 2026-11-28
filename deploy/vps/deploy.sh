#!/usr/bin/env bash
# Nightly deploy of the prod branch on the VPS. See DEPLOY_VPS.md, step 15.
#
# Started by the systemd user timer deploy/systemd/spp-deploy.timer (00:05
# Central). Can also be run by hand:
#
#   deploy/vps/deploy.sh --dry-run    # check everything, change nothing
#   deploy/vps/deploy.sh --no-wait    # deploy now, do not wait for the quiet point
#   deploy/vps/deploy.sh              # what the timer runs
#
# Steps:
#   1. git fetch origin prod. Nothing new -> stop.
#   2. Gates (any "no" -> skip, no restart, Airflow keeps running):
#      - the new commit is a fast-forward of the deployed one;
#      - it did not fail a deploy before (failed_shas);
#      - GitHub Actions workflow ci.yml succeeded for this exact commit;
#      - every schema patch the commit adds is listed in applied_patches
#        (the script never runs SQL; you apply patches yourself);
#      - pre-flight: the Docker image builds, and every DAG file imports in it.
#   3. Wait for the quiet point (default: 14 min 45 s into a quarter hour, when
#      the Sherburne cycle has stopped reading). Then git merge --ff-only and
#      astro dev restart.
#   4. Health check: scheduler container up, scheduler job alive, no DAG
#      import errors, every DAG id from the pre-flight is in `airflow dags list`.
#   5. Health check fails -> roll back to the previous commit, restart, check
#      again. The failed commit goes into failed_shas, so it is not tried again.
#
# State and logs: ${XDG_STATE_HOME:-~/.local/state}/spp-deploy/
#   deploy.log      full log of each run (also in journalctl --user -u spp-deploy)
#   history.tsv     one line per run: time, result, from, to, message
#   failed_shas     commits that failed a deploy (delete a line to try it again)
#   applied_patches schema/patches/*.sql file names you have applied
#
# Exit status: 0 = deployed, nothing new, or skipped by a gate;
#              1 = deploy failed and the rollback worked;
#              2 = rollback failed too (Airflow may be down: act now);
#              3 = cannot run (configuration, lock, dirty tree).
set -euo pipefail

# ----------------------------------------------------------------- settings
CONFIG_FILE="${SPP_DEPLOY_CONFIG:-$HOME/.config/spp-deploy/deploy.env}"
# KEY=VALUE lines only (not run as shell code). A variable that is already
# set in the environment wins, so `DEPLOY_QUIET_OFFSET=none deploy.sh` works.
if [[ -f "$CONFIG_FILE" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        [[ "$line" =~ ^[[:space:]]*([A-Z_][A-Z0-9_]*)=(.*)$ ]] || continue
        key="${BASH_REMATCH[1]}"; val="${BASH_REMATCH[2]}"
        val="${val%"${val##*[![:space:]]}"}"                 # trailing spaces
        if [[ "$val" =~ ^\"(.*)\"$ || "$val" =~ ^\'(.*)\'$ ]]; then val="${BASH_REMATCH[1]}"; fi
        [[ -n "${!key+x}" ]] || export "$key=$val"
    done < "$CONFIG_FILE"
    unset line key val
fi

PROJECT_DIR="${PROJECT_DIR:-$HOME/spp_data_pipeline}"
DEPLOY_BRANCH="${DEPLOY_BRANCH:-prod}"
DEPLOY_REMOTE="${DEPLOY_REMOTE:-origin}"
DEPLOY_REPO="${DEPLOY_REPO:-akala47/spp_pipeline}"           # owner/name on GitHub
DEPLOY_CI_WORKFLOW="${DEPLOY_CI_WORKFLOW:-ci.yml}"
DEPLOY_GITHUB_API="${DEPLOY_GITHUB_API:-https://api.github.com}"
GITHUB_TOKEN="${GITHUB_TOKEN:-}"                              # fine-grained, read-only
# Seconds into each quarter hour at which the restart starts. 885 = mm:14:45,
# mm:29:45, mm:44:45, mm:59:45. "none" = restart at once.
DEPLOY_QUIET_OFFSET="${DEPLOY_QUIET_OFFSET:-885}"
DEPLOY_HEALTH_TIMEOUT="${DEPLOY_HEALTH_TIMEOUT:-600}"         # seconds
DEPLOY_RESTART_TIMEOUT="${DEPLOY_RESTART_TIMEOUT:-1800}"      # seconds (includes image build)
DEPLOY_ALLOW_NON_FF="${DEPLOY_ALLOW_NON_FF:-0}"
STATE_DIR="${SPP_DEPLOY_STATE_DIR:-${XDG_STATE_HOME:-$HOME/.local/state}/spp-deploy}"

DRY_RUN=0
NO_WAIT=0
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN=1 ;;
        --no-wait) NO_WAIT=1 ;;
        -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 3 ;;
    esac
done

mkdir -p "$STATE_DIR"
LOG_FILE="$STATE_DIR/deploy.log"
HISTORY_FILE="$STATE_DIR/history.tsv"
FAILED_FILE="$STATE_DIR/failed_shas"
APPLIED_FILE="$STATE_DIR/applied_patches"
touch "$FAILED_FILE" "$APPLIED_FILE"

RUN_ID="$(date '+%Y%m%dT%H%M%S')-$$"
CURRENT=""
TARGET=""
PREFLIGHT_IMAGE=""
TMP_DIR=""

# ----------------------------------------------------------------- helpers
log() {
    local line
    line="$(date '+%Y-%m-%d %H:%M:%S %Z') [$RUN_ID] $*"
    echo "$line"
    echo "$line" >> "$LOG_FILE"
}

short() { echo "${1:0:10}"; }

history() {   # result message
    printf '%s\t%s\t%s\t%s\t%s\n' "$(date '+%Y-%m-%d %H:%M:%S %Z')" "$1" \
        "$(short "$CURRENT")" "$(short "$TARGET")" "$2" >> "$HISTORY_FILE"
}

finish() {    # exit_code result message
    log "$2: $3"
    if [[ "$DRY_RUN" == 1 ]]; then
        history "dry-run:$2" "$3"
    else
        history "$2" "$3"
    fi
    exit "$1"
}

# shellcheck disable=SC2329  # run by the EXIT trap
cleanup() {
    [[ -n "$TMP_DIR" && -d "$TMP_DIR" ]] && rm -rf "$TMP_DIR"
    if [[ -n "$PREFLIGHT_IMAGE" ]]; then
        docker image rm "$PREFLIGHT_IMAGE" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

run_with_timeout() {   # seconds cmd...
    local secs="$1"; shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "$secs" "$@"
    else
        "$@"
    fi
}

# ---------------------------------------------------------------- the lock
LOCK_DIR="$STATE_DIR/lock"
if command -v flock >/dev/null 2>&1; then
    exec 9>"$STATE_DIR/lock.flock"
    flock -n 9 || { echo "another deploy is running" >&2; exit 3; }
else
    mkdir "$LOCK_DIR" 2>/dev/null || { echo "another deploy is running ($LOCK_DIR)" >&2; exit 3; }
    trap 'rmdir "$LOCK_DIR" 2>/dev/null; cleanup' EXIT
fi

# ------------------------------------------------------- Airflow inspection
astro_project() {
    sed -n 's/^ *name: *//p' "$PROJECT_DIR/.astro/config.yaml" 2>/dev/null | head -n 1
}

scheduler_container() {
    local project
    project="$(astro_project)"
    docker ps --format '{{.Names}}' | grep -E "^${project:-.}.*scheduler" | head -n 1
}

airflow_exec() {   # args for the airflow CLI in the scheduler container
    local c
    c="$(scheduler_container)"
    [[ -n "$c" ]] || return 1
    docker exec "$c" airflow "$@"
}

# Prints the DAG ids in the running Airflow, one per line (empty if down).
running_dag_ids() {
    airflow_exec dags list -o json 2>/dev/null | python3 -c '
import json, sys
try:
    rows = json.load(sys.stdin)
except ValueError:
    rows = []
for r in rows:
    print(r.get("dag_id", ""))
' || true
}

# One health probe. Prints why it fails. $1 = space-separated DAG ids required.
health_probe() {
    local required="$1" c errs ids missing=""
    c="$(scheduler_container)"
    if [[ -z "$c" ]]; then echo "no running scheduler container"; return 1; fi
    if ! docker exec "$c" airflow jobs check --job-type SchedulerJob --local >/dev/null 2>&1; then
        echo "scheduler job is not alive yet"; return 1
    fi
    errs="$(docker exec "$c" airflow dags list-import-errors -o json 2>/dev/null || echo "?")"
    if ! printf '%s' "$errs" | python3 -c '
import json, sys
raw = sys.stdin.read().strip()
if raw in ("", "No data found"):
    sys.exit(0)
try:
    sys.exit(1 if json.loads(raw) else 0)
except ValueError:
    sys.exit(1)
'; then
        echo "DAG import errors: $(printf '%s' "$errs" | head -c 2000)"; return 1
    fi
    ids=" $(running_dag_ids | tr '\n' ' ') "
    for d in $required; do
        [[ "$ids" == *" $d "* ]] || missing="$missing $d"
    done
    if [[ -n "$missing" ]]; then echo "DAGs not loaded yet:$missing"; return 1; fi
    return 0
}

health_check() {   # required_dag_ids
    local required="$1" deadline why
    deadline=$(( $(date +%s) + DEPLOY_HEALTH_TIMEOUT ))
    while :; do
        if why="$(health_probe "$required")"; then
            log "health: OK (scheduler alive, no import errors, required DAGs:${required:+ $required})"
            return 0
        fi
        if (( $(date +%s) >= deadline )); then
            log "health: FAILED after ${DEPLOY_HEALTH_TIMEOUT}s: $why"
            return 1
        fi
        log "health: waiting: $why"
        sleep "${DEPLOY_HEALTH_POLL:-15}"
    done
}

restart_airflow() {
    log "astro dev restart (Airflow stops; running tasks are cut short)"
    if run_with_timeout "$DEPLOY_RESTART_TIMEOUT" astro dev restart --wait 5m >>"$LOG_FILE" 2>&1; then
        log "astro dev restart: done"
        return 0
    fi
    log "astro dev restart: FAILED (output in $LOG_FILE)"
    return 1
}

wait_for_quiet_point() {
    if [[ "$NO_WAIT" == 1 || "$DEPLOY_QUIET_OFFSET" == none ]]; then
        return 0
    fi
    local m s offset wait
    m=$((10#$(date +%M))); s=$((10#$(date +%S)))
    offset=$(( (m % 15) * 60 + s ))
    wait=$(( (DEPLOY_QUIET_OFFSET - offset + 900) % 900 ))
    if [[ "$DRY_RUN" == 1 ]]; then
        log "dry-run: would wait ${wait}s for the quiet point, then restart"
        return 0
    fi
    log "waiting ${wait}s for the quiet point (${DEPLOY_QUIET_OFFSET}s into the quarter hour)"
    sleep "$wait"
}

# ---------------------------------------------------------------- the gates
ci_passed() {   # sha -> 0 when a ci.yml run succeeded for it
    local sha="$1" url body
    if [[ -z "$GITHUB_TOKEN" ]]; then
        log "gate CI: GITHUB_TOKEN is empty (set it in $CONFIG_FILE)"
        return 1
    fi
    url="$DEPLOY_GITHUB_API/repos/$DEPLOY_REPO/actions/workflows/$DEPLOY_CI_WORKFLOW/runs?head_sha=$sha&per_page=20"
    # The token goes to curl on stdin, not on the command line (ps shows that).
    if ! body="$(printf 'header = "Authorization: Bearer %s"\n' "$GITHUB_TOKEN" |
            curl -sS --fail-with-body --max-time 30 -K - \
                 -H 'Accept: application/vnd.github+json' \
                 -H 'X-GitHub-Api-Version: 2022-11-28' "$url" 2>&1)"; then
        log "gate CI: GitHub API call failed: $(printf '%s' "$body" | head -c 300)"
        return 1
    fi
    local verdict
    verdict="$(printf '%s' "$body" | python3 -c '
import json, sys
runs = json.load(sys.stdin).get("workflow_runs", [])
if any(r.get("conclusion") == "success" for r in runs):
    print("success")
elif not runs:
    print("no CI run for this commit")
else:
    r = runs[0]
    print("latest run %s/%s %s" % (r.get("status"), r.get("conclusion"), r.get("html_url")))
' 2>&1 || echo "cannot read the GitHub answer")"
    if [[ "$verdict" == success ]]; then
        log "gate CI: OK (a $DEPLOY_CI_WORKFLOW run succeeded for $(short "$sha"))"
        return 0
    fi
    log "gate CI: not green: $verdict"
    return 1
}

unapplied_patches() {   # from to -> prints patch files added and not in applied_patches
    local f
    git diff --name-only --diff-filter=A "$1" "$2" -- schema/patches/ | while read -r f; do
        grep -qxF "$(basename "$f")" "$APPLIED_FILE" || basename "$f"
    done
}

PREFLIGHT_DAGS=""
preflight() {   # sha -> builds the image, imports the DAGs; sets PREFLIGHT_DAGS
    local sha="$1" out
    TMP_DIR="$(mktemp -d)"
    git archive "$sha" | tar -x -C "$TMP_DIR"
    if [[ ! -f "$TMP_DIR/deploy/check_dag_imports.py" ]]; then
        log "pre-flight: deploy/check_dag_imports.py is missing in $(short "$sha")"
        return 1
    fi
    PREFLIGHT_IMAGE="spp-deploy-preflight:$(short "$sha")"
    log "pre-flight: docker build (also warms the cache for astro dev restart)"
    if ! run_with_timeout "$DEPLOY_RESTART_TIMEOUT" docker build -q -t "$PREFLIGHT_IMAGE" "$TMP_DIR" >>"$LOG_FILE" 2>&1; then
        log "pre-flight: docker build FAILED (output in $LOG_FILE)"
        return 1
    fi
    if ! out="$(docker run --rm -v "$TMP_DIR/dags:/usr/local/airflow/dags:ro" "$PREFLIGHT_IMAGE" \
            python /usr/local/airflow/deploy/check_dag_imports.py 2>&1)"; then
        log "pre-flight: DAG import check FAILED:"
        printf '%s\n' "$out" | tee -a "$LOG_FILE"
        return 1
    fi
    PREFLIGHT_DAGS="$(printf '%s\n' "$out" | sed -n 's/^dags found: [0-9]*: //p' | tail -n 1)"
    log "pre-flight: OK, DAGs: $PREFLIGHT_DAGS"
    return 0
}

mark_failed() {
    grep -qxF "$1" "$FAILED_FILE" || echo "$1" >> "$FAILED_FILE"
}

# ------------------------------------------------------------------- main
log "---- deploy start (branch $DEPLOY_BRANCH, dry-run=$DRY_RUN, project $PROJECT_DIR)"
cd "$PROJECT_DIR" || { log "cannot cd to $PROJECT_DIR"; exit 3; }

branch="$(git symbolic-ref --short -q HEAD || echo "(detached)")"
if [[ "$branch" != "$DEPLOY_BRANCH" ]]; then
    finish 3 error "checkout is on '$branch', not '$DEPLOY_BRANCH' (git checkout $DEPLOY_BRANCH)"
fi
if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
    finish 3 error "tracked files were changed on the VPS; see git status (do not edit tracked files here)"
fi

CURRENT="$(git rev-parse HEAD)"
if ! git fetch --quiet "$DEPLOY_REMOTE" "$DEPLOY_BRANCH" >>"$LOG_FILE" 2>&1; then
    finish 3 error "git fetch $DEPLOY_REMOTE $DEPLOY_BRANCH failed (deploy key? network?)"
fi
TARGET="$(git rev-parse "$DEPLOY_REMOTE/$DEPLOY_BRANCH")"
log "deployed $(short "$CURRENT"), $DEPLOY_REMOTE/$DEPLOY_BRANCH is $(short "$TARGET")"

if [[ "$CURRENT" == "$TARGET" ]]; then
    finish 0 noop "already at $DEPLOY_REMOTE/$DEPLOY_BRANCH"
fi
if git merge-base --is-ancestor "$TARGET" "$CURRENT"; then
    finish 0 noop "$DEPLOY_REMOTE/$DEPLOY_BRANCH is behind the deployed commit (after a rollback?)"
fi
if ! git merge-base --is-ancestor "$CURRENT" "$TARGET" && [[ "$DEPLOY_ALLOW_NON_FF" != 1 ]]; then
    finish 0 skipped "$DEPLOY_BRANCH was rewritten (not a fast-forward); set DEPLOY_ALLOW_NON_FF=1 for one run to take it"
fi
if grep -qxF "$TARGET" "$FAILED_FILE"; then
    finish 0 skipped "$(short "$TARGET") failed a deploy before; push a new commit (or delete it from $FAILED_FILE)"
fi

log "changes: $(git log --oneline "$CURRENT..$TARGET" | wc -l | tr -d ' ') commit(s)"
git log --oneline "$CURRENT..$TARGET" | sed 's/^/    /' | tee -a "$LOG_FILE"

if ! ci_passed "$TARGET"; then
    finish 0 skipped "CI is not green for $(short "$TARGET")"
fi

missing_patches="$(unapplied_patches "$CURRENT" "$TARGET" | tr '\n' ' ')"
if [[ -n "${missing_patches// /}" ]]; then
    finish 0 skipped "new schema patch(es) not applied: $missing_patches- apply them, then add each file name to $APPLIED_FILE"
fi

if ! preflight "$TARGET"; then
    [[ "$DRY_RUN" == 1 ]] || mark_failed "$TARGET"
    finish 0 skipped "pre-flight failed for $(short "$TARGET"); Airflow was not restarted"
fi

if [[ "$DRY_RUN" == 1 ]]; then
    wait_for_quiet_point
    finish 0 ok "would deploy $(short "$CURRENT") -> $(short "$TARGET") (git merge --ff-only, astro dev restart, health check)"
fi

# DAG ids running now: the rollback must bring them back.
BEFORE_DAGS="$(running_dag_ids | tr '\n' ' ')"
log "DAGs running before the deploy: ${BEFORE_DAGS:-(none: Airflow is down?)}"

wait_for_quiet_point
echo "$CURRENT" > "$STATE_DIR/previous_sha"

if [[ "$DEPLOY_ALLOW_NON_FF" == 1 ]]; then
    git reset --quiet --hard "$TARGET"
else
    git merge --quiet --ff-only "$TARGET"
fi
log "checked out $(short "$TARGET")"

if restart_airflow && health_check "$PREFLIGHT_DAGS"; then
    if [[ -x .venv/bin/pip ]]; then
        if .venv/bin/pip install -q -e . >>"$LOG_FILE" 2>&1; then
            log "host .venv updated"
        else
            log "WARNING: .venv/bin/pip install -e . failed (Airflow is fine; fix the venv by hand)"
        fi
    fi
    echo "$TARGET" > "$STATE_DIR/last_good_sha"
    finish 0 deployed "$(short "$CURRENT") -> $(short "$TARGET")"
fi

# ---------------------------------------------------------------- rollback
log "ROLLBACK to $(short "$CURRENT")"
mark_failed "$TARGET"
git reset --quiet --hard "$CURRENT"
if restart_airflow && health_check "$BEFORE_DAGS"; then
    finish 1 rolled-back "$(short "$TARGET") failed the health check; back on $(short "$CURRENT")"
fi
finish 2 rollback-failed "$(short "$TARGET") failed and the rollback to $(short "$CURRENT") is not healthy: Airflow needs a person NOW"
