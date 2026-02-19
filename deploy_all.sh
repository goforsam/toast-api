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

# Client config per dataset
case $DATASET in
    purpose)
        PREFIX="toast"
        SECRET_SUFFIX=""
        GUIDS_CSV=""
        ;;
    rodrigos)
        PREFIX="rodrigos"
        SECRET_SUFFIX="_RODRIGOS"
        GUIDS_CSV="ab3c4f80-5529-4b5f-bba1-cc9abaf33431,3383074f-b565-4501-ae86-41f21c866cba,8cb95c1f-2f82-4f20-9dce-446a956fd4bb,bef05e5c-3b38-49f3-9b8d-ca379130f718,8c37412b-a13b-4edd-bbd8-b26222fcbe68,dedecf4f-ee34-41ab-a740-f3b461eed4eb,eea6e77a-46b2-4631-907e-10d85a845bb8,e2fbc555-2cc4-49ee-bbdc-1e4c652ec6f4,d0bbc362-63d4-4277-af85-2bf2c808bdc7,1903fd30-c0ff-4682-b9af-b184c77d9653"
        ;;
    slim_husky)
        PREFIX="slim"
        SECRET_SUFFIX="_SLIM"
        GUIDS_CSV="9ee73d8b-7d6d-4227-b005-9a3e6e749dbe,cd8c8f17-7868-4281-97a1-589c0b0799e4,89674e99-65bb-4855-998c-c6eee25fe032,c50c9ccc-7cb9-42e9-8359-04414258eb6a,dfcda609-9262-4181-9b26-a9db7a87c2ea,2fe1af2a-1021-4b80-b060-4b70fad83e9b,b00be8e0-a7d9-4a90-a4e2-3d8191a86796,6371f5c4-a26b-49ba-943a-c27178a21dad"
        ;;
    *)
        echo "ERROR: Unknown dataset '$DATASET'. Use: purpose, rodrigos, slim_husky"
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

    # Use ^;;^ delimiter so commas in GUIDS_CSV don't break gcloud parsing
    local env_vars="^;;^BQ_DATASET_ID=${DATASET};;SECRET_SUFFIX=${SECRET_SUFFIX}"
    if [ -n "$GUIDS_CSV" ]; then
        env_vars="${env_vars};;RESTAURANT_GUIDS_CSV=${GUIDS_CSV}"
    fi

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
