#!/usr/bin/env bash
# Bounded offered-load profiles. A passing run is not maximum-capacity certification.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
stage=${OWLCRON_LOAD_STAGE:-all}
case "$stage" in
    all|regression|cluster|capacity-race|capacity-plain|daemon-soak|cluster-soak) ;;
    *) echo "Unknown load acceptance stage: $stage" >&2; exit 1 ;;
esac
selected() { [[ "$stage" == all || "$stage" == "$1" ]]; }
command -v docker >/dev/null
command -v python3 >/dev/null
run_dir=$(mktemp -d "${TMPDIR:-/tmp}/owlcron-linux-load.XXXXXXXX")
run_dir=$(cd "$run_dir" && pwd -P)
results=${OWLCRON_LOAD_RESULTS:-"$run_dir/results"}
build_dir=${OWLCRON_LINUX_BUILD_DIR:-"$run_dir/build"}
module_dir=${OWLCRON_LINUX_MODULE_CACHE:-"$run_dir/modules"}
mkdir -p "$results" "$build_dir/cache" "$build_dir/work" "$module_dir"
results=$(cd "$results" && pwd -P)
# Never merge this run with previous success/failure evidence.
python3 - "$results" <<'PY_CHECK_EMPTY'
import os, sys
if any(os.scandir(sys.argv[1])):
    raise SystemExit("OWLCRON_LOAD_RESULTS must be empty; previous evidence is preserved")
PY_CHECK_EMPTY
started_epoch=$(python3 -c 'import time; print(time.time())')
build_dir=$(cd "$build_dir" && pwd -P)
module_dir=$(cd "$module_dir" && pwd -P)
runner="owlcron-load-$(date +%s)-$$"
image=${OWLCRON_LINUX_GO_IMAGE:-golang:1.27.3-trixie}
repeat=${OWLCRON_LOAD_REPEAT:-3}
[[ "$repeat" =~ ^[1-9]$ ]] || { echo 'OWLCRON_LOAD_REPEAT must be 1..9' >&2; exit 1; }
cleanup() {
    status=$?
    # Bash 3 may run EXIT traps in a failed pipeline subshell. Only the
    # owning shell may export evidence and remove this runner.
    if (( BASH_SUBSHELL > 0 )); then exit "$status"; fi
    trap - EXIT INT TERM
    # docker cp gives exported files the invoking host user's ownership, also on
    # rootful Docker. No socket or other host control is mounted in this runner.
    docker cp "$runner:/results/." "$results/" >/dev/null 2>&1 || true
    docker logs --tail 100 "$runner" >"$results/runner.log" 2>&1 || true
    docker rm -f "$runner" >/dev/null 2>&1 || true
    echo "Load acceptance exit: $status; evidence: $results"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker run -d --name "$runner" --label io.owlcron.acceptance=true \
    --cpus="${OWLCRON_LOAD_CPUS:-2}" --memory="${OWLCRON_LOAD_MEMORY:-2g}" --pids-limit=512 \
    -v "$root:/workspace:ro" -v "$module_dir:/go/pkg/mod" -v "$build_dir:/build" \
    -w /workspace -e GOCACHE=/build/cache -e GOTMPDIR=/build/work -e GOFLAGS=-p=2 \
    -e "GOMAXPROCS=${OWLCRON_LOAD_GOMAXPROCS:-2}" \
    -e "OWLCRON_LOAD_API_P99_MS=${OWLCRON_LOAD_API_P99_MS:-4096}" \
    -e "OWLCRON_LOAD_JOB_P99_MS=${OWLCRON_LOAD_JOB_P99_MS:-16384}" \
    -e "OWLCRON_LOAD_RESTART_P99_MS=${OWLCRON_LOAD_RESTART_P99_MS:-16384}" \
    -e "OWLCRON_LOAD_RSS_MIB=${OWLCRON_LOAD_RSS_MIB:-512}" \
    -e "GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}" "$image" sleep infinity >/dev/null
docker exec "$runner" mkdir -p /results
{
    docker exec "$runner" sh -c 'test "$(uname -s)" = Linux; uname -srmo; go version; cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/memory.max; df -T /build /tmp'
    docker image inspect "$image" --format '{{.Id}} {{.Architecture}}'
} | tee "$results/environment.log"
if selected regression; then
    docker exec -e OWLCRON_ACCEPTANCE_RESULTS=/results/regression "$runner" \
    go test -race ./... -count=1 -timeout=360s 2>&1 | tee "$results/regression.log"
fi
if selected cluster; then
    docker exec -e OWLCRON_ACCEPTANCE_RESULTS=/results/cluster "$runner" \
    go test -race . -run '^TestProcessClusterLoadedConvergence$' -count="$repeat" -timeout=900s -v \
    2>&1 | tee "$results/cluster-repeat.log"
fi
for instrumentation in race plain; do
    selected "capacity-$instrumentation" || continue
    # Keep the array nonempty for Bash 3 with nounset.
    flags=(-count=1)
    if [[ "$instrumentation" == race ]]; then flags=(-race -count=1); fi
    docker exec -e "OWLCRON_ACCEPTANCE_RESULTS=/results/capacity-$instrumentation" \
        -e "OWLCRON_CAPACITY_EVENTS=${OWLCRON_CAPACITY_EVENTS:-1000}" \
        -e "OWLCRON_CAPACITY_JOBS=${OWLCRON_CAPACITY_JOBS:-96}" \
        -e "OWLCRON_CAPACITY_CONCURRENCY=${OWLCRON_CAPACITY_CONCURRENCY:-8}" \
        "$runner" go test "${flags[@]}" . -run '^TestProcessCapacityProfile$' -timeout=600s -v \
        2>&1 | tee "$results/capacity-$instrumentation.log"
done
if selected daemon-soak; then
    docker exec -e OWLCRON_ACCEPTANCE_RESULTS=/results/daemon-soak \
    -e "OWLCRON_SOAK_DURATION=${OWLCRON_SOAK_DURATION:-3m}" "$runner" \
    go test -race . -run '^TestProcessSoakProfile$' -count=1 \
    -timeout="${OWLCRON_LOAD_SOAK_TIMEOUT:-10m}" -v 2>&1 | tee "$results/daemon-soak.log"
fi
if selected cluster-soak; then
    docker exec -e OWLCRON_ACCEPTANCE_RESULTS=/results/cluster-soak \
    -e "OWLCRON_PROCESS_SOAK_DURATION=${OWLCRON_PROCESS_SOAK_DURATION:-3m}" "$runner" \
    go test -race . -run '^TestProcessClusterSoak$' -count=1 \
    -timeout="${OWLCRON_LOAD_SOAK_TIMEOUT:-10m}" -v 2>&1 | tee "$results/cluster-soak.log"
fi
if selected regression; then
    docker exec "$runner" go vet ./... 2>&1 | tee "$results/vet.log"
fi
python3 -m unittest discover -s "$root/tests" -p 'test_load_evidence.py' 2>&1 | tee "$results/evidence-tests.log"
docker cp "$runner:/results/." "$results/" >/dev/null
# Also supports a runner started before this script gained the start marker.
# environment.log is opened once before any test stage in that run.
if [[ -z "${started_epoch:-}" ]]; then
    started_epoch=$(python3 -c 'import os,sys; print(os.stat(sys.argv[1]).st_mtime)' "$results/environment.log")
fi
check_args=(--matrix-repeat "$repeat" --not-before "$started_epoch")
if [[ "$stage" != all ]]; then check_args+=(--stages "$stage"); fi
python3 "$root/tests/check_load_evidence.py" "$results" "${check_args[@]}" | tee "$results/evidence-check.log"
