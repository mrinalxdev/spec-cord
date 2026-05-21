#!/usr/bin/env bash
set -euo pipefail

COORDINATOR_URL="${COORDINATOR_URL:-http://coordinator:8080}"
THREADS="${THREADS:-8}"
DURATION="${DURATION:-60}"       # seconds
REQUESTS_PER_THREAD="${RPT:-500}"

echo "==> Sysbench-style cross-shard transfer benchmark"
echo "    Coordinator: $COORDINATOR_URL"
echo "    Threads:     $THREADS"
echo "    Duration:    ${DURATION}s"
echo ""

curl -sf "${COORDINATOR_URL}/healthz" > /dev/null || {
    echo "ERROR: coordinator not reachable at ${COORDINATOR_URL}"
    exit 1
}

curl -sf "${COORDINATOR_URL}/admin/shards" | python3 -m json.tool

echo ""
echo "==> Starting load..."

run_worker() {
    local tid=$1
    local count=0
    local errors=0
    local latencies=()

    for i in $(seq 1 $REQUESTS_PER_THREAD); do
        FROM_ID=$((RANDOM % 10000 + 1))         # shard-a range: 1–10000
        TO_ID=$((RANDOM % 10000 + 10001))        # shard-b range: 10001–20000
        AMOUNT=$(echo "scale=2; ($RANDOM % 100 + 1) / 10" | bc)

        START_NS=$(date +%s%N)

        HTTP_CODE=$(curl -s -o /tmp/resp_${tid}.json -w "%{http_code}" \
            -X POST "${COORDINATOR_URL}/transfer" \
            -H "Content-Type: application/json" \
            -d "{\"ops\":[
                    {\"shard_id\":\"shard-a\",\"account_id\":${FROM_ID},\"delta\":-${AMOUNT}},
                    {\"shard_id\":\"shard-b\",\"account_id\":${TO_ID},\"delta\":${AMOUNT}}
                ]}")

        END_NS=$(date +%s%N)
        LATENCY_MS=$(( (END_NS - START_NS) / 1000000 ))

        if [ "$HTTP_CODE" = "200" ]; then
            count=$((count + 1))
            latencies+=($LATENCY_MS)
        else
            errors=$((errors + 1))
        fi
    done
    printf "Thread %02d: %d ok / %d errors\n" $tid $count $errors
    printf "%s\n" "${latencies[@]}" | sort -n > /tmp/latencies_${tid}.txt
}

for t in $(seq 1 $THREADS); do
    run_worker $t &
done
wait

echo ""
echo "==> Aggregating latency results..."


cat /tmp/latencies_*.txt | sort -n > /tmp/all_latencies.txt
TOTAL=$(wc -l < /tmp/all_latencies.txt)

p50_idx=$((TOTAL * 50 / 100))
p95_idx=$((TOTAL * 95 / 100))
p99_idx=$((TOTAL * 99 / 100))

P50=$(sed -n "${p50_idx}p" /tmp/all_latencies.txt)
P95=$(sed -n "${p95_idx}p" /tmp/all_latencies.txt)
P99=$(sed -n "${p99_idx}p" /tmp/all_latencies.txt)


: '
claude will be formating the scripts a little bit
:p
'

echo "────────────────────────────────────"
echo "  Baseline 2PC Latency Results"
echo "────────────────────────────────────"
echo "  Total requests : $TOTAL"
echo "  p50 latency    : ${P50}ms"
echo "  p95 latency    : ${P95}ms"
echo "  p99 latency    : ${P99}ms   <── record this for Milestone 2 comparison"
echo "────────────────────────────────────"
echo ""
echo "==> These numbers are also in Grafana: http://localhost:3000"
