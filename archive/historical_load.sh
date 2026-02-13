#!/bin/bash
# Historical data load script for Toast orders
# Loads data month by month to avoid timeouts

FUNCTION_URL="https://toast-purpose-bulk-120665665070.us-west1.run.app"

echo "================================"
echo "Toast Orders Historical Data Load"
echo "================================"
echo ""
echo "This script will load orders from 2025-01-01 to 2026-02-09"
echo "Loading month by month to avoid timeouts"
echo ""

# Define date ranges (monthly chunks)
declare -a DATE_RANGES=(
    "2025-01-01:2025-01-31"
    "2025-02-01:2025-02-28"
    "2025-03-01:2025-03-31"
    "2025-04-01:2025-04-30"
    "2025-05-01:2025-05-31"
    "2025-06-01:2025-06-30"
    "2025-07-01:2025-07-31"
    "2025-08-01:2025-08-31"
    "2025-09-01:2025-09-30"
    "2025-10-01:2025-10-31"
    "2025-11-01:2025-11-30"
    "2025-12-01:2025-12-31"
    "2026-01-01:2026-01-31"
    "2026-02-01:2026-09"
)

TOTAL=${#DATE_RANGES[@]}
CURRENT=0
FAILED=0

echo "Total periods to load: $TOTAL"
echo ""

# Create log file
LOG_FILE="historical_load_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to: $LOG_FILE"
echo ""

# Function to load data for a date range
load_date_range() {
    local start_date=$1
    local end_date=$2
    local period="$start_date to $end_date"

    echo "[$CURRENT/$TOTAL] Loading: $period"

    RESPONSE=$(curl -s -X POST "$FUNCTION_URL" \
        -H "Content-Type: application/json" \
        -d "{\"start_date\": \"$start_date\", \"end_date\": \"$end_date\"}" \
        --max-time 300)

    echo "$RESPONSE" | tee -a "$LOG_FILE"

    # Check for success
    if echo "$RESPONSE" | grep -q '"status": "success"'; then
        ORDERS=$(echo "$RESPONSE" | grep -o '"orders_loaded": [0-9]*' | grep -o '[0-9]*')
        echo "✅ Success: $ORDERS orders loaded"
        echo ""
    else
        echo "❌ Failed to load period: $period"
        echo "Response: $RESPONSE"
        echo ""
        ((FAILED++))
    fi

    # Small delay to avoid rate limiting
    sleep 2
}

# Load data for each period
for range in "${DATE_RANGES[@]}"; do
    ((CURRENT++))

    IFS=':' read -r start_date end_date <<< "$range"
    load_date_range "$start_date" "$end_date"
done

echo "================================"
echo "Historical Load Complete"
echo "================================"
echo ""
echo "Periods loaded: $((TOTAL - FAILED))/$TOTAL"
echo "Failed periods: $FAILED"
echo ""
echo "Check log file for details: $LOG_FILE"
echo ""

if [ $FAILED -gt 0 ]; then
    echo "⚠️  Some periods failed. Review the log and retry failed periods."
    exit 1
else
    echo "✅ All periods loaded successfully!"
    echo ""
    echo "Next steps:"
    echo "1. Verify data in BigQuery"
    echo "2. Set up Cloud Scheduler for daily runs"
    echo "3. Create transformation queries for dimensional tables"
    exit 0
fi
