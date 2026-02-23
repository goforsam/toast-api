#!/bin/bash
# Deploy Toast ETL Cloud Functions
# Usage: bash deploy_all.sh [orders|cash|labor|config|all] [--dataset purpose|rodrigos]

set -e

PROJECT_ID="possible-coast-439421-q5"
REGION="us-west1"
RUNTIME="python312"

# Parse --dataset flag (default: purpose)
DATASET="purpose"
for arg in "$@"; do
    case $arg in
        --dataset=*) DATASET="${arg#*=}" ;;
    esac
done

# PREFIX is the Cloud Function name prefix (e.g. "toast", "rodrigos", "slim").
# All other config (dataset, secret suffix, GUIDs) is resolved in Python via CLIENT_NAME -> shared/clients.py.
# To add a new client: add it to shared/clients.py, then add one line here.
case $DATASET in
    purpose)    PREFIX="toast" ;;
    rodrigos)   PREFIX="rodrigos" ;;
    slim_husky) PREFIX="slim" ;;
    *)
        echo "ERROR: Unknown dataset '$DATASET'. Add it to shared/clients.py first."
        exit 1
        ;;
esac

# Entry points and memory (same for all clients)
declare -A ENTRY_POINTS
ENTRY_POINTS=(
    ["orders"]="orders_daily:1GB"
    ["cash"]="cash_daily:512MB"
    ["labor"]="labor_daily:512MB"
    ["config"]="config_weekly:512MB"
)

deploy_function() {
    local etl_type=$1
    local config=${ENTRY_POINTS[$etl_type]}
    local entry_point="${config%%:*}"
    local memory="${config##*:}"
    local func_name="${PREFIX}-${etl_type}-etl"

    echo ""
    echo "=== Deploying $func_name (dataset=$DATASET) ==="
    echo "  Entry point: $entry_point"
    echo "  Memory: $memory"
    echo ""

    # CLIENT_NAME is the only env var needed; all other config resolved in shared/clients.py
    local env_vars="CLIENT_NAME=${DATASET}"

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
        --set-env-vars "$env_vars"

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
gcloud config set project "$PROJECT_ID" --quiet || true

# Get target (first non-flag arg, default: orders)
TARGET="orders"
for arg in "$@"; do
    case $arg in
        --*) ;; # skip flags
        *) TARGET="$arg"; break ;;
    esac
done

echo "Dataset: $DATASET | Prefix: $PREFIX | Target: $TARGET"

case $TARGET in
    orders)  deploy_function "orders" ;;
    cash)    deploy_function "cash" ;;
    labor)   deploy_function "labor" ;;
    config)  deploy_function "config" ;;
    all)
        for etl_type in orders cash labor config; do
            deploy_function "$etl_type"
        done
        ;;
    *)
        echo "Usage: bash deploy_all.sh [orders|cash|labor|config|all] [--dataset=purpose|rodrigos]"
        exit 1
        ;;
esac

echo ""
echo "Done."
