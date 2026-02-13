#!/bin/bash
# Load all 2025 data + 2026 YTD

FUNCTION_URL="https://toast-purpose-bulk-vtpo3hu6ba-uw.a.run.app"

echo "Loading historical Toast data: 2025 to present"
echo "This will take 15-20 minutes for a full year..."
echo ""

# Load by month to avoid timeouts
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
  "2026-02-01:2026-02-10"
)

for range in "${months[@]}"; do
  IFS=':' read -r start end <<< "$range"
  echo "=================================================="
  echo "Loading: $start to $end"
  echo "=================================================="

  curl -X POST "$FUNCTION_URL" \
    -H "Content-Type: application/json" \
    -d "{\"start_date\": \"$start\", \"end_date\": \"$end\"}" \
    -s -w "\nStatus: %{http_code}\n" \
    --max-time 600

  echo ""
  echo "Waiting 5 seconds before next month..."
  sleep 5
done

echo ""
echo "=================================================="
echo "Historical load complete!"
echo "=================================================="
echo ""
echo "Check BigQuery to verify data:"
echo "  bq query \"SELECT COUNT(*) as total_orders, MIN(businessDate) as earliest, MAX(businessDate) as latest FROM \\\`possible-coast-439421-q5.purpose.toast_orders_raw\\\`\""
