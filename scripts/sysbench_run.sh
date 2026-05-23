#!/usr/bin/env bash
set -euo pipefail

COORDINATOR_URL="${COORDINATOR_URL:-http://coordinator:8080}"
THREADS="${THREADS:-8}"
REQUESTS_PER_THREAD="${RPT:-500}"

echo "==> Sysbench-style cross-shard transfer benchmark"
echo "    Coordinator: $COORDINATOR_URL"
echo "    Threads:     $THREADS"
echo ""

curl -sf "${COORDINATOR_URL}/healthz" > /dev/null || {
    echo "ERROR: coordinator not reachable"
    exit 1
}

echo "==> Shard health:"
curl -sf "${COORDINATOR_URL}/admin/shards"
echo -e "\n"

echo "==> Starting load..."

run_worker() {
    local tid=$1
    local count=0
    local errors=0
    > /tmp/latencies_${tid}.txt

    for i in $(seq 1 $REQUESTS_PER_THREAD); do
        FROM_ID=$((RANDOM % 10000 + 1))
        TO_ID=$((RANDOM % 10000 + 10001))
        AMOUNT=$(echo "scale=2; ($RANDOM % 100 + 1) / 10" | bc)
        RESULT=$(curl -s \
            -o /tmp/resp_${tid}.json \
            -w "%{http_code} %{time_total}" \
            -X POST "${COORDINATOR_URL}/transfer" \
            -H "Content-Type: application/json" \
            -d "{\"ops\":[
                    {\"shard_id\":\"shard-a\",\"account_id\":${FROM_ID},\"delta\":-${AMOUNT}},
                    {\"shard_id\":\"shard-b\",\"account_id\":${TO_ID},\"delta\":${AMOUNT}}
                ]}")

        HTTP_CODE=$(echo "$RESULT" | awk '{print $1}')
        TIME_S=$(echo "$RESULT" | awk '{print $2}')
        TIME_MS=$(echo "$TIME_S * 1000 / 1" | bc)

        if [ "$HTTP_CODE" = "200" ]; then
            count=$((count + 1))
            echo "$TIME_MS" >> /tmp/latencies_${tid}.txt
        else
            errors=$((errors + 1))
        fi
    done

    printf "Thread %02d: %d ok / %d errors\n" $tid $count $errors
}

for t in $(seq 1 $THREADS); do
    run_worker $t &
done
wait

echo ""
echo "==> Aggregating latency results..."

cat /tmp/latencies_*.txt 2>/dev/null | sort -n > /tmp/all_latencies.txt
TOTAL=$(wc -l < /tmp/all_latencies.txt | tr -d ' ')

if [ "$TOTAL" -eq 0 ]; then
    echo "ERROR: no successful requests recorded"
    exit 1
fi

p50_idx=$(( TOTAL * 50 / 100 ))
p95_idx=$(( TOTAL * 95 / 100 ))
p99_idx=$(( TOTAL * 99 / 100 ))

[ "$p50_idx" -lt 1 ] && p50_idx=1
[ "$p95_idx" -lt 1 ] && p95_idx=1
[ "$p99_idx" -lt 1 ] && p99_idx=1

P50=$(sed -n "${p50_idx}p" /tmp/all_latencies.txt)
P95=$(sed -n "${p95_idx}p" /tmp/all_latencies.txt)
P99=$(sed -n "${p99_idx}p" /tmp/all_latencies.txt)

echo "────────────────────────────────────"
echo "  Baseline 2PC Latency Results"
echo "────────────────────────────────────"
echo "  Total requests : $TOTAL"
echo "  p50 latency    : ${P50}ms"
echo "  p95 latency    : ${P95}ms"
echo "  p99 latency    : ${P99}ms   <── record this for Milestone 2 comparison"
echo "────────────────────────────────────"
echo ""
echo "==> Metrics: http://localhost:9090"
echo "==> Grafana:  http://localhost:3000"