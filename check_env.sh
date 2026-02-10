#!/bin/bash
# Check current Cloud Function environment variables

gcloud functions describe toast-purpose-bulk \
  --gen2 \
  --region=us-west1 \
  --format="json" | grep -A 20 "environmentVariables"
