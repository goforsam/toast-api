#!/bin/bash
# Historical Load Script - Load Toast orders from 2025-01-01 to 2026-02-09
# Loads data month by month to avoid timeout issues

FUNCTION_URL="https://us-west1-possible-coast-439421-q5.cloudfunctions.net/toast-purpose-bulk"

echo "Starting historical load: 2025-01-01 to 2026-02-09"
echo "This will take approximately 17-20 hours due to rate limiting"
echo ""

# 2025 months
months=(
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
    "2026-02-01:2026-02-09"
)

total=${#months[@]}
current=0

for range in "${months[@]}"; do
    ((current++))
    IFS=':' read -r start_date end_date <<< "$range"

    echo "[$current/$total] Loading $start_date to $end_date..."

    response=$(curl -s -X POST "$FUNCTION_URL" \
        -H "Content-Type: application/json" \
        -d "{\"start_date\": \"$start_date\", \"end_date\": \"$end_date\"}")

    # Parse response
    status=$(echo "$response" | python -m json.tool 2>/dev/null | grep '"status"' | cut -d'"' -f4)
    orders_loaded=$(echo "$response" | python -m json.tool 2>/dev/null | grep '"orders_loaded"' | cut -d':' -f2 | tr -d ' ,')

    if [ "$status" = "success" ]; then
        echo "  ✓ Success: $orders_loaded orders loaded"
    else
        echo "  ✗ Failed: $response"
    fi

    echo ""

    # Small delay between months
    sleep 5
done

echo "Historical load complete!"
echo "Check BigQuery table: possible-coast-439421-q5.purpose.toast_orders_raw"
