#!/bin/bash
# Deploy Toast ETL Cloud Functions
# Usage: bash deploy_all.sh [orders|cash|labor|config|all]

set -e

PROJECT_ID="possible-coast-439421-q5"
REGION="us-west1"
RUNTIME="python312"

# Function definitions: name -> entry_point, memory
declare -A FUNCTIONS
FUNCTIONS=(
    ["toast-orders-etl"]="orders_daily:1GB"
    ["toast-cash-etl"]="cash_daily:512MB"
    ["toast-labor-etl"]="labor_daily:512MB"
    ["toast-config-etl"]="config_weekly:512MB"
)

deploy_function() {
    local func_name=$1
    local config=${FUNCTIONS[$func_name]}
    local entry_point="${config%%:*}"
    local memory="${config##*:}"

    echo ""
    echo "=== Deploying $func_name ==="
    echo "  Entry point: $entry_point"
    echo "  Memory: $memory"
    echo ""

    gcloud functions deploy "$func_name" \
        --gen2 \
        --runtime="$RUNTIME" \
        --region="$REGION" \
        --source=. \
        --entry-point="$entry_point" \
        --trigger-http \
        --allow-unauthenticated \
        --timeout=540s \
        --memory="$memory" \
        --max-instances=10 \
        --set-env-vars BQ_DATASET_ID=purpose

    echo ""
    echo "Deployed $func_name:"
    gcloud functions describe "$func_name" \
        --gen2 \
        --region="$REGION" \
        --format="value(serviceConfig.uri)"
}

# Check gcloud
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI not found."
    exit 1
fi

# Set project
gcloud config set project "$PROJECT_ID" --quiet

TARGET=${1:-orders}

case $TARGET in
    orders)
        deploy_function "toast-orders-etl"
        ;;
    cash)
        deploy_function "toast-cash-etl"
        ;;
    labor)
        deploy_function "toast-labor-etl"
        ;;
    config)
        deploy_function "toast-config-etl"
        ;;
    all)
        for func in "${!FUNCTIONS[@]}"; do
            deploy_function "$func"
        done
        ;;
    *)
        echo "Usage: bash deploy_all.sh [orders|cash|labor|config|all]"
        exit 1
        ;;
esac

echo ""
echo "Done."
