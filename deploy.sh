#!/bin/bash
# Deployment script for Toast API Cloud Function

set -e  # Exit on error

PROJECT_ID="possible-coast-439421-q5"
FUNCTION_NAME="toast-purpose-bulk"
REGION="us-west1"
RUNTIME="python312"
ENTRY_POINT="orders_daily"

echo "================================"
echo "Deploying Toast API Cloud Function"
echo "================================"
echo ""
echo "Project: $PROJECT_ID"
echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo ""

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

# Check if logged in
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
    echo "ERROR: Not logged into gcloud. Run: gcloud auth login"
    exit 1
fi

# Set project
echo "Setting project to $PROJECT_ID..."
gcloud config set project $PROJECT_ID

# Backup current function
echo ""
echo "Backing up current function code..."
mkdir -p backups
BACKUP_DIR="backups/backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Download current function source
gcloud functions describe $FUNCTION_NAME \
    --gen2 \
    --region=$REGION \
    --format="value(sourceArchiveUrl)" > /dev/null 2>&1 || echo "No existing function to backup"

# Copy current local files to backup
cp main.py "$BACKUP_DIR/main.py.old" 2>/dev/null || echo "No main.py to backup"

# Use improved version
echo ""
echo "Preparing improved version..."
cp main_improved.py main.py

# Deploy function
echo ""
echo "Deploying Cloud Function..."
echo "This may take 2-3 minutes..."
echo ""

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=$RUNTIME \
    --region=$REGION \
    --source=. \
    --entry-point=$ENTRY_POINT \
    --trigger-http \
    --allow-unauthenticated \
    --timeout=540s \
    --memory=512MB \
    --max-instances=10 \
    --set-env-vars BQ_DATASET_ID=toast

echo ""
echo "================================"
echo "Deployment Complete!"
echo "================================"
echo ""
echo "Function URL:"
gcloud functions describe $FUNCTION_NAME \
    --gen2 \
    --region=$REGION \
    --format="value(serviceConfig.uri)"

echo ""
echo "To test:"
echo "  curl https://${FUNCTION_NAME}-120665665070.${REGION}.run.app"
echo ""
echo "To test with custom date:"
echo "  curl -X POST https://${FUNCTION_NAME}-120665665070.${REGION}.run.app \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"start_date\": \"2025-01-01\", \"end_date\": \"2025-01-31\"}'"
echo ""
