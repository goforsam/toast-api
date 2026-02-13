#!/bin/bash
# Setup Google Secret Manager for Toast API credentials
# Run this once before deploying production code

PROJECT_ID="possible-coast-439421-q5"
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com"

echo "========================================="
echo "Setting up Secret Manager for Toast API"
echo "========================================="
echo ""

# Check if gcloud is configured
if ! gcloud config get-value project &>/dev/null; then
    echo "ERROR: gcloud not configured. Run: gcloud auth login"
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

echo "Step 1: Creating secrets..."
echo ""

# Note: Replace these with your actual credentials
# Or run interactively to paste them

echo "Enter your Toast Client ID:"
read -r TOAST_CLIENT_ID

echo "Enter your Toast Client Secret:"
read -rs TOAST_CLIENT_SECRET

# Create secrets
echo ""
echo "Creating TOAST_CLIENT_ID secret..."
echo -n "$TOAST_CLIENT_ID" | gcloud secrets create TOAST_CLIENT_ID \
    --data-file=- \
    --replication-policy="automatic" || echo "Secret already exists, updating..."

if gcloud secrets versions list TOAST_CLIENT_ID &>/dev/null; then
    echo -n "$TOAST_CLIENT_ID" | gcloud secrets versions add TOAST_CLIENT_ID --data-file=-
fi

echo "Creating TOAST_CLIENT_SECRET secret..."
echo -n "$TOAST_CLIENT_SECRET" | gcloud secrets create TOAST_CLIENT_SECRET \
    --data-file=- \
    --replication-policy="automatic" || echo "Secret already exists, updating..."

if gcloud secrets versions list TOAST_CLIENT_SECRET &>/dev/null; then
    echo -n "$TOAST_CLIENT_SECRET" | gcloud secrets versions add TOAST_CLIENT_SECRET --data-file=-
fi

echo ""
echo "Step 2: Granting Cloud Function access to secrets..."
echo ""

# Grant access to Cloud Function service account
gcloud secrets add-iam-policy-binding TOAST_CLIENT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor"

gcloud secrets add-iam-policy-binding TOAST_CLIENT_SECRET \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --role="roles/secretmanager.secretAccessor"

echo ""
echo "========================================="
echo "Setup Complete!"
echo "========================================="
echo ""
echo "Secrets created:"
echo "  - TOAST_CLIENT_ID"
echo "  - TOAST_CLIENT_SECRET"
echo ""
echo "Access granted to:"
echo "  - $SERVICE_ACCOUNT"
echo ""
echo "Next steps:"
echo "  1. cp main_production.py main.py"
echo "  2. cp requirements_production.txt requirements.txt"
echo "  3. Deploy function via Console or gcloud"
echo ""
