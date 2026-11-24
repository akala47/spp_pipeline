#!/usr/bin/env bash
# Test of deploy/vps/deploy.sh with a fake origin and stub astro, docker and
# curl. Nothing here touches Docker, GitHub, the VPS or any database.
#
#   deploy/vps/test_deploy.sh            # from the repo root; CI runs it too
#
# It copies the working tree (without .env, .venv, logs, local) into a temp
# folder, makes a fake origin with a prod branch, and runs deploy.sh through
# each path: noop, dry-run, CI gate, deploy, rollback, patches, pre-flight,
# failed rollback, dirty tree, wrong branch, force push, quiet point, lock.
set -uo pipefail
REPO="${1:-$(git rev-parse --show-toplevel)}"
T="$(mktemp -d)"; trap 'rm -rf "$T"' EXIT
mkdir -p "$T/stubs" "$T/home"
SCRIPT="$REPO/deploy/vps/deploy.sh"

# ---- stubs
cat > "$T/stubs/curl" <<'EOF'
#!/usr/bin/env bash
echo "ARGS: $*" >> "$T/curl.args"
cat > "$T/curl.stdin"                # the -K - config with the token
case "$(cat "$T/ci_state")" in
  success) echo '{"workflow_runs":[{"status":"completed","conclusion":"success","html_url":"u"}]}' ;;
  failure) echo '{"workflow_runs":[{"status":"completed","conclusion":"failure","html_url":"https://x/run/1"}]}' ;;
  none)    echo '{"workflow_runs":[]}' ;;
  http500) echo '{"message":"boom"}'; exit 22 ;;
esac
EOF
cat > "$T/stubs/docker" <<'EOF'
#!/usr/bin/env bash
echo "docker $*" >> "$T/calls"
case "$1" in
  build) exit "$(cat "$T/build_rc" 2>/dev/null || echo 0)" ;;
  run)   if [[ -f "$T/preflight_fail" ]]; then echo "IMPORT ERROR dags/x.py: boom" >&2; exit 1; fi
         echo "dags found: 2: scada_beatrice spp_rt_lmp"; exit 0 ;;
  image) exit 0 ;;
  ps)    [[ -f "$T/airflow_down" ]] || echo "spp-data-pipeline_abc123-scheduler-1"; exit 0 ;;
  exec)  shift 2
         case "$*" in
           "airflow jobs check"*) exit 0 ;;
           "airflow dags list-import-errors"*)
               if [[ -f "$T/project/BREAK_HEALTH" ]]; then echo '[{"filename":"/usr/local/airflow/dags/x.py"}]'; else echo '[]'; fi ;;
           "airflow dags list"*) echo '[{"dag_id":"scada_beatrice"},{"dag_id":"spp_rt_lmp"}]' ;;
         esac ;;
esac
EOF
cat > "$T/stubs/astro" <<'EOF'
#!/usr/bin/env bash
echo "astro $* @ $(git -C "$T/project" rev-parse --short HEAD)" >> "$T/calls"
exit "$(cat "$T/restart_rc" 2>/dev/null || echo 0)"
EOF
chmod +x "$T/stubs/"*

# ---- fake origin with prod, and the "VPS" checkout
git clone -q --no-hardlinks "$REPO" "$T/seed"
rsync -a --exclude .git --exclude .env --exclude .venv --exclude logs --exclude local \
      --exclude __pycache__ --exclude .pytest_cache "$REPO/" "$T/seed/"   # include uncommitted CI files
git -C "$T/seed" add -A >/dev/null && git -C "$T/seed" -c user.email=t@t -c user.name=t commit -qm "ci/cd files"
git -C "$T/seed" branch -q -f prod HEAD
git clone -q --bare "$T/seed" "$T/origin.git"
git clone -q -b prod "$T/origin.git" "$T/project"
git -C "$T/project" config pull.ff only

push_commit() {   # message [file to add]
    local w="$T/seed"
    git -C "$w" checkout -q prod
    if [[ -n "${2:-}" ]]; then mkdir -p "$w/$(dirname "$2")"; echo x > "$w/$2"; git -C "$w" add "$2"; fi
    git -C "$w" -c user.email=t@t -c user.name=t commit -q --allow-empty -m "$1"
    git -C "$w" push -q "$T/origin.git" prod
}
remove_file() {
    git -C "$T/seed" rm -q "$1"; git -C "$T/seed" -c user.email=t@t -c user.name=t commit -qm "rm $1"
    git -C "$T/seed" push -q "$T/origin.git" prod
}

export T PATH="$T/stubs:$PATH" HOME="$T/home"
export PROJECT_DIR="$T/project" SPP_DEPLOY_STATE_DIR="$T/state" SPP_DEPLOY_CONFIG="$T/deploy.env"
export DEPLOY_HEALTH_TIMEOUT=2 DEPLOY_HEALTH_POLL=1
printf 'GITHUB_TOKEN="ghp_FAKE_SECRET_123"\n# comment\nDEPLOY_QUIET_OFFSET=none  \n' > "$T/deploy.env"

PASS=0; FAIL=0
pass() { PASS=$((PASS+1)); echo "PASS  $1"; }
fail() { FAIL=$((FAIL+1)); echo "FAIL  $1"; }
expect() {   # name command... -> PASS when the command succeeds
    local name="$1"; shift
    if "$@"; then pass "$name"; else fail "$name"; fi
}
check() {   # name expected_rc expected_head pattern_in_last_history rc
    local name="$1" exp_rc="$2" exp_head="$3" pat="$4" rc="$5" head last
    head="$(git -C "$T/project" rev-parse HEAD)"
    last="$(tail -n 1 "$T/state/history.tsv")"
    if [[ "$rc" == "$exp_rc" && "$head" == "$exp_head" && "$last" == *"$pat"* ]]; then
        pass "$name   [$last]"
    else
        fail "$name   rc=$rc (want $exp_rc) head=${head:0:10} (want ${exp_head:0:10}) last=[$last] want *$pat*"
    fi
}
run() { : > "$T/calls"; bash "$SCRIPT" "$@" > "$T/out" 2>&1; echo $?; }
origin() { git -C "$T/origin.git" rev-parse prod; }
no_astro_call() { ! grep -q astro "$T/calls"; }
token_hidden() { ! grep -q FAKE_SECRET "$T/curl.args" "$T/out" "$T/state/deploy.log"; }

echo success > "$T/ci_state"
H0="$(git -C "$T/project" rev-parse HEAD)"

rc=$(run); check "nothing new -> noop" 0 "$H0" "noop" "$rc"

push_commit "feature A"; A=$(origin)
rc=$(run --dry-run); check "dry-run: would deploy, HEAD unchanged" 0 "$H0" "dry-run:ok" "$rc"
expect "dry-run did not restart Airflow" no_astro_call

echo failure > "$T/ci_state"
rc=$(run); check "CI red -> skipped" 0 "$H0" "CI is not green" "$rc"
expect "CI red: log names the run" grep -q "https://x/run/1" "$T/out"
echo none > "$T/ci_state"
rc=$(run); check "no CI run -> skipped" 0 "$H0" "CI is not green" "$rc"
echo http500 > "$T/ci_state"
rc=$(run); check "GitHub API error -> skipped" 0 "$H0" "CI is not green" "$rc"
expect "token not in curl args or logs" token_hidden
expect "token sent on curl stdin" grep -q FAKE_SECRET "$T/curl.stdin"

echo success > "$T/ci_state"
rc=$(run); check "CI green -> deployed" 0 "$A" "deployed" "$rc"
expect "restart ran on the new commit" grep -q "astro dev restart --wait 5m @ ${A:0:7}" "$T/calls"
expect "last_good_sha written" grep -qxF "$A" "$T/state/last_good_sha"

push_commit "breaks health" BREAK_HEALTH; B=$(origin)
rc=$(run); check "unhealthy -> rolled back" 1 "$A" "rolled-back" "$rc"
expect "bad commit in failed_shas" grep -qxF "$B" "$T/state/failed_shas"
expect "restarted twice (deploy + rollback)" test "$(grep -c 'astro dev restart' "$T/calls")" = 2
rc=$(run); check "same bad commit -> skipped next night" 0 "$A" "failed a deploy before" "$rc"

remove_file BREAK_HEALTH; C=$(origin)
rc=$(run); check "fix pushed -> deployed" 0 "$C" "deployed" "$rc"

push_commit "needs patch" schema/patches/2026-12-01_new.sql; D=$(origin)
rc=$(run); check "unapplied schema patch -> skipped" 0 "$C" "schema patch" "$rc"
echo 2026-12-01_new.sql >> "$T/state/applied_patches"
rc=$(run); check "patch marked applied -> deployed" 0 "$D" "deployed" "$rc"

push_commit "bad dag"; touch "$T/preflight_fail"
rc=$(run); check "pre-flight import error -> skipped" 0 "$D" "pre-flight failed" "$rc"
expect "no restart after pre-flight failure" no_astro_call
rm "$T/preflight_fail"

push_commit "build breaks"; echo 1 > "$T/build_rc"
rc=$(run); check "image build fails -> skipped" 0 "$D" "pre-flight failed" "$rc"
rm "$T/build_rc"

push_commit "restart breaks"; echo 1 > "$T/restart_rc"
rc=$(run); check "restart fails, rollback restart fails -> rc 2" 2 "$D" "rollback-failed" "$rc"
rm "$T/restart_rc"

echo "x" >> "$T/project/README.md"
push_commit "after dirty"
rc=$(run); check "edited tracked file on VPS -> refuse" 3 "$D" "tracked files were changed" "$rc"
git -C "$T/project" restore README.md

git -C "$T/project" checkout -q -b other
rc=$(run); check "wrong branch -> refuse" 3 "$D" "not 'prod'" "$rc"
git -C "$T/project" checkout -q prod

# rewritten prod (force push) is not taken
git -C "$T/seed" reset -q --hard "$C"
git -C "$T/seed" -c user.email=t@t -c user.name=t commit -q --allow-empty -m rewritten
git -C "$T/seed" push -q -f "$T/origin.git" prod
rc=$(run); check "force-pushed prod -> skipped" 0 "$D" "not a fast-forward" "$rc"
R=$(origin)
rc=$(DEPLOY_ALLOW_NON_FF=1 run); check "DEPLOY_ALLOW_NON_FF=1 -> deployed" 0 "$R" "deployed" "$rc"

# quiet point: 3 s from now
push_commit "quiet"; Q=$(origin)
now=$(( (10#$(date +%M) % 15) * 60 + 10#$(date +%S) ))
start=$(date +%s)
rc=$(DEPLOY_QUIET_OFFSET=$(( (now + 3) % 900 )) run)
waited=$(( $(date +%s) - start ))
check "quiet point wait, then deploy" 0 "$Q" "deployed" "$rc"
expect "waited ${waited}s for the quiet point" test "$waited" -ge 2

# lock: a second run while one holds the lock
if command -v flock >/dev/null; then
    exec 8>"$T/state/lock.flock"; flock -n 8
    rc=$(run); exec 8>&-
else
    mkdir "$T/state/lock"; rc=$(run); rmdir "$T/state/lock"
fi
expect "lock held -> rc 3 (rc=$rc)" test "$rc" = 3

echo; echo "PASS=$PASS FAIL=$FAIL"
[[ "$FAIL" == 0 ]]
