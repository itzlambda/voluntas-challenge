#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
run_dir="${FX_RUN_DIR:-fake-exchange/runs/smoke-$(date +%s)}"
mkdir -p "$(dirname "$run_dir")"
mkdir "$run_dir"
ws_port="${FX_WS_PORT:-19001}"
gamma_port="${FX_GAMMA_PORT:-19002}"
seconds="${FX_SMOKE_SECONDS:-60}"
if ! [[ "$seconds" =~ ^[1-9][0-9]*$ ]]; then
    printf 'FX_SMOKE_SECONDS must be a positive integer.\n' >&2
    exit 1
fi
server_pid=''
collector_pid=''
cleanup() {
    for pid in "$collector_pid" "$server_pid"; do
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill -INT "$pid" 2>/dev/null || true
            for _ in {1..100}; do
                kill -0 "$pid" 2>/dev/null || break
                sleep 0.1
            done
            kill -KILL "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT
cargo build --release -j 2 --manifest-path fake-exchange/Cargo.toml
cargo build --release -j 2 --bin engineering-challenge
bin_dir="$(cargo metadata --offline --no-deps --format-version 1 --manifest-path fake-exchange/Cargo.toml | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"]+"/release")')"
collector_dir="$(cargo metadata --offline --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"]+"/release")')"
for attempt in {1..19}; do
    uptime
    if python3 - <<'PYLOAD'
import os,sys
sys.exit(0 if os.getloadavg()[0] <= 12 else 1)
PYLOAD
    then break; fi
    if [[ "$attempt" == 19 ]]; then
        printf 'Host load remains above 12; smoke run postponed.\n' >&2
        exit 1
    fi
    sleep 10
done
"$bin_dir/fake-exchange" --markets 200 --cores 2 --rate 1000 \
    --new-markets-per-second 0.2 --resolutions-per-second 0.1 \
    --ws-port "$ws_port" --gamma-port "$gamma_port" --history "$run_dir/history" \
    2>"$run_dir/server.jsonl" &
server_pid=$!
ready=0
for _ in {1..200}; do
    kill -0 "$server_pid"
    if python3 - "$gamma_port" <<'PY'
import sys,urllib.request
try:
    urllib.request.urlopen('http://127.0.0.1:'+sys.argv[1]+'/markets/keyset?closed=false',timeout=.2).close()
except Exception:
    sys.exit(1)
PY
    then
        kill -0 "$server_pid"
        ready=1
        break
    fi
    kill -0 "$server_pid"
    sleep 0.1
done
if (( ready == 0 )); then
    printf 'Exchange readiness timed out; inspect %s/server.jsonl.\n' "$run_dir" >&2
    exit 1
fi
"$collector_dir/engineering-challenge" --metrics-bind "${FX_METRICS_BIND:-127.0.0.1:0}" --workers 2 --markets-per-connection 50 \
    --scan-interval-secs 10 --gamma-requests-per-second 20 \
    --ws-url "ws://127.0.0.1:$ws_port/ws/market" \
    --gamma-url "http://127.0.0.1:$gamma_port" \
    --decode-error-log "$run_dir/decode-errors.jsonl" \
    >"$run_dir/books.sbe" 2>"$run_dir/collector.jsonl" &
collector_pid=$!
sleep "$seconds"
kill -INT "$collector_pid"
wait "$collector_pid"
collector_pid=''
kill -INT "$server_pid"
wait "$server_pid"
server_pid=''
"$bin_dir/fx-check" "$run_dir/books.sbe" "$run_dir/history" | tee "$run_dir/check.json"
printf 'Run artifacts: %s\n' "$run_dir"
