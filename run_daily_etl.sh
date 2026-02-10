#!/bin/bash
# Master ETL Orchestration Script
# Runs the complete Toast data pipeline: API → Staging → Dimensions → Facts
# Project: possible-coast-439421-q5

set -e  # Exit on error

PROJECT_ID="possible-coast-439421-q5"
REGION="us-west1"
FUNCTION_URL="https://toast-purpose-bulk-120665665070.${REGION}.run.app"
DATASET="purpose"

echo "============================================"
echo "Toast Data Pipeline - Daily ETL"
echo "============================================"
echo "Project: $PROJECT_ID"
echo "Date: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# ============================================
# STEP 1: Extract - Load from Toast API to Staging
# ============================================
echo "STEP 1: Extracting orders from Toast API..."
echo "--------------------------------------------"

# Call Cloud Function to fetch yesterday's orders
RESPONSE=$(curl -s -X GET "$FUNCTION_URL" --max-time 300)

echo "Response: $RESPONSE"

# Check if successful
if echo "$RESPONSE" | grep -q "Success\|success"; then
    echo "✅ Orders extracted successfully"
else
    echo "❌ Failed to extract orders"
    echo "$RESPONSE"
    exit 1
fi

echo ""

# ============================================
# STEP 2: Transform - Load Dimensions
# ============================================
echo "STEP 2: Loading dimension tables..."
echo "--------------------------------------------"

bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  --max_rows=0 \
  < etl_load_dimensions.sql

if [ $? -eq 0 ]; then
    echo "✅ Dimensions loaded successfully"
else
    echo "❌ Failed to load dimensions"
    exit 1
fi

echo ""

# ============================================
# STEP 3: Transform - Load Facts
# ============================================
echo "STEP 3: Loading fact tables..."
echo "--------------------------------------------"

bq query \
  --project_id=$PROJECT_ID \
  --use_legacy_sql=false \
  --max_rows=0 \
  < etl_load_facts.sql

if [ $? -eq 0 ]; then
    echo "✅ Facts loaded successfully"
else
    echo "❌ Failed to load facts"
    exit 1
fi

echo ""

# ============================================
# STEP 4: Validate Data Quality
# ============================================
echo "STEP 4: Validating data quality..."
echo "--------------------------------------------"

# Check for yesterday's data
YESTERDAY=$(date -d "yesterday" '+%Y-%m-%d' 2>/dev/null || date -v-1d '+%Y-%m-%d')

ORDER_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.FactOrders\` WHERE BusinessDate = '$YESTERDAY'" | tail -n 1)

CHECK_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.FactChecks\` WHERE BusinessDate = '$YESTERDAY'" | tail -n 1)

PAYMENT_COUNT=$(bq query --project_id=$PROJECT_ID --use_legacy_sql=false --format=csv \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET}.FactPayments\` WHERE BusinessDate = '$YESTERDAY'" | tail -n 1)

echo "Date: $YESTERDAY"
echo "Orders loaded: $ORDER_COUNT"
echo "Checks loaded: $CHECK_COUNT"
echo "Payments loaded: $PAYMENT_COUNT"

if [ "$ORDER_COUNT" -gt 0 ]; then
    echo "✅ Data quality check passed"
else
    echo "⚠️  No orders found for $YESTERDAY (might be normal if restaurant was closed)"
fi

echo ""

# ============================================
# STEP 5: Refresh Power BI Views (if needed)
# ============================================
echo "STEP 5: Refreshing analytical views..."
echo "--------------------------------------------"

# Views are automatically refreshed on query in BigQuery
# But we can verify they exist
bq ls --project_id=$PROJECT_ID ${DATASET} | grep "vw_" > /dev/null

if [ $? -eq 0 ]; then
    echo "✅ Analytical views ready"
else
    echo "⚠️  Some analytical views may be missing"
fi

echo ""

# ============================================
# Summary
# ============================================
echo "============================================"
echo "ETL Pipeline Complete!"
echo "============================================"
echo "Next data refresh: Tomorrow at scheduled time"
echo ""
echo "To view data in BigQuery:"
echo "  https://console.cloud.google.com/bigquery?project=${PROJECT_ID}"
echo ""
echo "To check logs:"
echo "  ./view_etl_logs.sh"
echo ""
