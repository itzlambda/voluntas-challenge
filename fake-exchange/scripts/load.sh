#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
run_dir="${FX_RUN_DIR:-fake-exchange/runs/load-$(date +%s)}"
mkdir -p "$(dirname "$run_dir")"
mkdir "$run_dir"
seconds="${FX_LOAD_SECONDS:-20}"
if ! [[ "$seconds" =~ ^[0-9]+$ ]] || (( seconds < 1 || seconds > 30 )); then
    printf 'FX_LOAD_SECONDS must be 1 through 30.\n' >&2
    exit 1
fi
server_pid=''
perf_pid=''
cleanup() {
    if [[ -n "$perf_pid" ]]; then
        kill -INT "$perf_pid" 2>/dev/null || true
        wait "$perf_pid" 2>/dev/null || true
        perf_pid=''
    fi
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill -INT "$server_pid" 2>/dev/null || true
        for _ in {1..100}; do
            kill -0 "$server_pid" 2>/dev/null || break
            sleep 0.1
        done
        kill -KILL "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT
bin_dir="${FX_BIN_DIR:-$(cargo metadata --offline --no-deps --format-version 1 --manifest-path fake-exchange/Cargo.toml | python3 -c 'import json,sys;print(json.load(sys.stdin)["target_directory"]+"/release")')}"
for attempt in {1..19}; do
    uptime | tee -a "$run_dir/uptime.txt"
    if python3 - <<'PY'
import os,sys
sys.exit(0 if os.getloadavg()[0] <= 12 else 1)
PY
    then break; fi
    if [[ "$attempt" == 19 ]]; then
        printf 'Host load remains above 12; load run postponed.\n' >&2
        exit 1
    fi
    sleep 10
done
taskset -c "${FX_SERVER_CORE:-0}" "${FX_SERVER_BIN:-$bin_dir/fake-exchange}" \
    --cores 1 --markets 5000 --rate "${FX_RATE:-100000}" --repeat-book-rate 0 --noise-rate 0 \
    --outbound-capacity "${FX_OUTBOUND_CAPACITY:-16384}" \
    --ws-port 19101 --gamma-port 19102 2>"$run_dir/server.jsonl" &
server_pid=$!
ready=0
for _ in {1..200}; do
    kill -0 "$server_pid"
    if python3 - <<'PY'
import sys,urllib.request
try:
    urllib.request.urlopen('http://127.0.0.1:19102/markets/keyset?closed=false',timeout=.2).close()
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
if [[ "${FX_PROFILE:-0}" == 1 ]]; then
    perf record -e cpu-clock:u -F 199 --call-graph dwarf,8192 \
        -p "$server_pid" -o "$run_dir/perf.data" 2>"$run_dir/perf.log" &
    perf_pid=$!
fi
python3 - "$server_pid" "$run_dir/cpu-start.json" <<'PY'
import glob,json,os,sys,time
waiting=sum(int(open(p).read().split()[1]) for p in glob.glob('/proc/'+sys.argv[1]+'/task/*/schedstat'))/1e9
s=open('/proc/'+sys.argv[1]+'/stat').read().split()
json.dump(dict(cpu=(int(s[13])+int(s[14]))/os.sysconf('SC_CLK_TCK'),wall=time.monotonic(),runqueue_seconds=waiting,user_seconds=int(s[13])/os.sysconf('SC_CLK_TCK'),system_seconds=int(s[14])/os.sysconf('SC_CLK_TCK')),open(sys.argv[2],'w'))
PY
status=0
client_command=(taskset -c "${FX_CLIENT_CORE:-1}" "${FX_CLIENT_BIN:-$bin_dir/fx-load}"
    --cores 1 --connections 50 --assets-per-connection 200 --seconds "$seconds"
    --ws-url ws://127.0.0.1:19101/ws/market --gamma-url http://127.0.0.1:19102)
if [[ "${FX_PROFILE_CLIENT:-0}" == 1 ]]; then
    client_command=(perf record -e cpu-clock:u -F 199 --call-graph dwarf,8192
        -o "$run_dir/client-perf.data" -- "${client_command[@]}")
fi
/usr/bin/time -q -f '{"elapsed_seconds":%e,"user_seconds":%U,"system_seconds":%S}' \
    -o "$run_dir/client-cpu.json" "${client_command[@]}" \
    >"$run_dir/load.json" 2>"$run_dir/client.log" || status=$?

python3 - "$server_pid" "$run_dir/cpu-start.json" <<'PY' >"$run_dir/server-cpu.json"
import glob,json,os,sys,time
waiting=sum(int(open(p).read().split()[1]) for p in glob.glob('/proc/'+sys.argv[1]+'/task/*/schedstat'))/1e9
start=json.load(open(sys.argv[2]));s=open('/proc/'+sys.argv[1]+'/stat').read().split()
cpu=(int(s[13])+int(s[14]))/os.sysconf('SC_CLK_TCK')-start['cpu'];elapsed=time.monotonic()-start['wall']
print(json.dumps(dict(elapsed_seconds=elapsed,cpu_seconds=cpu,cpu_cores=cpu/elapsed,runqueue_seconds=waiting-start['runqueue_seconds'],user_seconds=int(s[13])/os.sysconf('SC_CLK_TCK')-start['user_seconds'],system_seconds=int(s[14])/os.sysconf('SC_CLK_TCK')-start['system_seconds'])))
PY
if [[ -n "$perf_pid" ]]; then
    kill -INT "$perf_pid" 2>/dev/null || true
    wait "$perf_pid" || true
    perf_pid=''
fi
kill -INT "$server_pid"
wait "$server_pid"
server_pid=''
cat "$run_dir/load.json"
cat "$run_dir/server-cpu.json"
cat "$run_dir/client-cpu.json"
printf 'Run artifacts: %s\n' "$run_dir"
exit "$status"
