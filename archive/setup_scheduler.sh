#!/bin/bash
# Set up Cloud Scheduler for daily Toast orders sync

PROJECT_ID="possible-coast-439421-q5"
REGION="us-west1"
FUNCTION_URL="https://toast-purpose-bulk-120665665070.${REGION}.run.app"
JOB_NAME="toast-orders-daily"
SCHEDULE="0 2 * * *"  # 2 AM daily (after midnight when orders close)
TIMEZONE="America/Los_Angeles"  # Adjust to your restaurant timezone

echo "================================"
echo "Setting up Cloud Scheduler"
echo "================================"
echo ""
echo "Job Name: $JOB_NAME"
echo "Schedule: $SCHEDULE ($TIMEZONE)"
echo "Function: $FUNCTION_URL"
echo ""

# Check if job already exists
if gcloud scheduler jobs describe $JOB_NAME --location=$REGION &> /dev/null; then
    echo "Job already exists. Updating..."

    gcloud scheduler jobs update http $JOB_NAME \
        --location=$REGION \
        --schedule="$SCHEDULE" \
        --uri="$FUNCTION_URL" \
        --http-method=GET \
        --time-zone="$TIMEZONE" \
        --attempt-deadline=540s
else
    echo "Creating new scheduler job..."

    gcloud scheduler jobs create http $JOB_NAME \
        --location=$REGION \
        --schedule="$SCHEDULE" \
        --uri="$FUNCTION_URL" \
        --http-method=GET \
        --time-zone="$TIMEZONE" \
        --attempt-deadline=540s
fi

echo ""
echo "================================"
echo "Scheduler Setup Complete!"
echo "================================"
echo ""
echo "The function will run daily at 2 AM to fetch previous day's orders."
echo ""
echo "To manually trigger:"
echo "  gcloud scheduler jobs run $JOB_NAME --location=$REGION"
echo ""
echo "To view logs:"
echo "  gcloud scheduler jobs describe $JOB_NAME --location=$REGION"
echo ""
echo "To pause:"
echo "  gcloud scheduler jobs pause $JOB_NAME --location=$REGION"
echo ""
echo "To resume:"
echo "  gcloud scheduler jobs resume $JOB_NAME --location=$REGION"
echo ""
