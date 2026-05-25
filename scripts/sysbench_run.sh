#!/usr/bin/env bash
set -euo pipefail

COORDINATOR_URL="${COORDINATOR_URL:-http://coordinator:8080}"
THREADS="${THREADS:-8}"
REQUESTS_PER_THREAD="${RPT:-500}"
MODE="${MODE:-baseline}"  # baseline | speculative | compare

echo "==> Sysbench-style cross-shard transfer benchmark"
echo "    Coordinator: $COORDINATOR_URL"
echo "    Threads:     $THREADS"
echo "    Mode:        $MODE"
echo ""

# Health check
curl -sf "${COORDINATOR_URL}/healthz" > /dev/null || {
    echo "ERROR: coordinator not reachable"
    exit 1
}

echo "==> Shard health:"
curl -sf "${COORDINATOR_URL}/admin/shards"
echo -e "\n"

# Run benchmark in specified mode
run_benchmark() {
    local label=$1
    local spec_flag=$2
    
    echo "==> Starting load [$label]..."
    
    run_worker() {
        local tid=$1
        local count=0
        local errors=0
        > /tmp/latencies_${label}_${tid}.txt

        for i in $(seq 1 $REQUESTS_PER_THREAD); do
            FROM_ID=$((RANDOM % 10000 + 1))
            TO_ID=$((RANDOM % 10000 + 10001))
            AMOUNT=$(printf "%.2f" $(echo "scale=2; ($RANDOM % 100 + 1) / 10" | bc))
            
            RESULT=$(curl -s \
                -o /tmp/resp_${label}_${tid}.json \
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
                echo "$TIME_MS" >> /tmp/latencies_${label}_${tid}.txt
            else
                errors=$((errors + 1))
                if [ "$HTTP_CODE" != "200" ]; then
                    echo "ERROR [$label] Response: $(cat /tmp/resp_${label}_${tid}.json)" >&2
                fi
            fi
        done
        printf "Thread %02d [%s]: %d ok / %d errors\n" $tid "$label" $count $errors
    }

    for t in $(seq 1 $THREADS); do
        run_worker $t &
    done
    wait
}

case "$MODE" in
    baseline)
        # Ensure speculation is disabled
        export SPECULATION_ENABLED=false
        run_benchmark "baseline" ""
        ;;
    speculative)
        # Ensure speculation is enabled
        export SPECULATION_ENABLED=true
        run_benchmark "speculative" ""
        ;;
    compare)
        # Run baseline first
        export SPECULATION_ENABLED=false
        run_benchmark "baseline" ""
        
        # Small pause to let metrics settle
        sleep 2
        
        # Run speculative
        export SPECULATION_ENABLED=true
        run_benchmark "speculative" ""
        ;;
    *)
        echo "ERROR: unknown MODE: $MODE (use baseline|speculative|compare)"
        exit 1
        ;;
esac

echo ""
echo "==> Aggregating latency results..."

# Aggregate based on mode
if [ "$MODE" = "compare" ]; then
    for label in baseline speculative; do
        cat /tmp/latencies_${label}_*.txt 2>/dev/null | sort -n > /tmp/all_latencies_${label}.txt
        TOTAL=$(wc -l < /tmp/all_latencies_${label}.txt | tr -d ' ')
        
        if [ "$TOTAL" -eq 0 ]; then
            echo "ERROR [$label]: no successful requests recorded"
            continue
        fi
        
        p50_idx=$(( TOTAL * 50 / 100 ))
        p95_idx=$(( TOTAL * 95 / 100 ))
        p99_idx=$(( TOTAL * 99 / 100 ))
        [ "$p50_idx" -lt 1 ] && p50_idx=1
        [ "$p95_idx" -lt 1 ] && p95_idx=1
        [ "$p99_idx" -lt 1 ] && p99_idx=1
        
        P50=$(sed -n "${p50_idx}p" /tmp/all_latencies_${label}.txt)
        P95=$(sed -n "${p95_idx}p" /tmp/all_latencies_${label}.txt)
        P99=$(sed -n "${p99_idx}p" /tmp/all_latencies_${label}.txt)
        
        echo "────────────────────────────────────"
        echo "  [$label] 2PC Latency Results"
        echo "────────────────────────────────────"
        echo "  Total requests : $TOTAL"
        echo "  p50 latency    : ${P50}ms"
        echo "  p95 latency    : ${P95}ms"
        echo "  p99 latency    : ${P99}ms"
        echo "────────────────────────────────────"
    done
    
    # Calculate improvement
    BASE_P99=$(sed -n "${p99_idx}p" /tmp/all_latencies_baseline.txt)
    SPEC_P99=$(sed -n "${p99_idx}p" /tmp/all_latencies_speculative.txt)
    if [ -n "$BASE_P99" ] && [ -n "$SPEC_P99" ]; then
        IMPROVEMENT=$(echo "scale=1; (($BASE_P99 - $SPEC_P99) * 100) / $BASE_P99" | bc)
        echo ""
        echo "==> Speculation Improvement: ${IMPROVEMENT}% p99 reduction"
        echo "    Baseline p99: ${BASE_P99}ms → Speculative p99: ${SPEC_P99}ms"
    fi
else
    # Single mode: aggregate all threads
    cat /tmp/latencies_${MODE}_*.txt 2>/dev/null | sort -n > /tmp/all_latencies.txt
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
    echo "  [$MODE] 2PC Latency Results"
    echo "────────────────────────────────────"
    echo "  Total requests : $TOTAL"
    echo "  p50 latency    : ${P50}ms"
    echo "  p95 latency    : ${P95}ms"
    echo "  p99 latency    : ${P99}ms   <── RECORD FOR MILESTONE 2"
    echo "────────────────────────────────────"
fi

echo ""
echo "==> Metrics: http://localhost:9090"
echo "==> Grafana:  http://localhost:3000"