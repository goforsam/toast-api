# Toast API to BigQuery - Deployment Guide

## 📋 Pre-Deployment Checklist

- [x] Toast API credentials configured in Cloud Function environment variables
- [x] BigQuery dataset `toast` exists in project `possible-coast-439421-q5`
- [x] 13 restaurant GUIDs configured
- [x] Function deployed to `us-west1`
- [ ] Updated code deployed with date range support
- [ ] Historical data loaded (2025-01-01 to 2026-02-09)
- [ ] Cloud Scheduler configured for daily runs

## 🔍 Audit Results

### Current Status: ✅ FUNCTIONAL with limitations

**What's Working:**
- ✅ Cloud Function deployed and accessible
- ✅ Toast API authentication successful
- ✅ Data fetching from all 13 restaurants
- ✅ BigQuery loading mechanism functional

**Issues Found:**
- ⚠️ Hardcoded to yesterday's data only (no date range support)
- ⚠️ Dataset name mismatch (code defaults to 'purpose', should be 'toast')
- ⚠️ No deduplication logic
- ⚠️ Limited error handling
- ⚠️ No test mode for validation

## 🚀 Deployment Steps

### Step 1: Deploy Updated Function

The improved function adds:
- Date range parameters for historical loads
- Better error handling
- Test mode
- Enhanced logging
- Proper dataset name

**Option A: Using deployment script (Recommended)**
```bash
# Make script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

**Option B: Manual deployment via gcloud**
```bash
# Backup current version
cp main.py main.py.backup

# Use improved version
cp main_improved.py main.py

# Deploy
gcloud functions deploy toast-purpose-bulk \
  --gen2 \
  --runtime=python312 \
  --region=us-west1 \
  --source=. \
  --entry-point=orders_daily \
  --trigger-http \
  --allow-unauthenticated \
  --timeout=540s \
  --memory=512MB \
  --set-env-vars BQ_DATASET_ID=toast
```

**Option C: Via GCP Console**
1. Go to [Cloud Functions Console](https://console.cloud.google.com/functions)
2. Select `toast-purpose-bulk`
3. Click "Edit"
4. Replace `main.py` content with `main_improved.py`
5. Update environment variable: `BQ_DATASET_ID=toast`
6. Click "Deploy"

### Step 2: Test the Updated Function

**Test 1: Validate deployment**
```bash
# Test mode - no data loaded to BigQuery
curl "https://toast-purpose-bulk-120665665070.us-west1.run.app?mode=test"
```

Expected response:
```json
{
  "status": "test",
  "message": "Test mode - data not loaded to BigQuery",
  "orders_fetched": 5,
  "sample_order": {...}
}
```

**Test 2: Load a single day**
```bash
curl -X POST https://toast-purpose-bulk-120665665070.us-west1.run.app \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-01-15", "end_date": "2025-01-15"}'
```

Expected response:
```json
{
  "status": "success",
  "message": "Successfully loaded orders for 2025-01-15 to 2025-01-15",
  "orders_loaded": 156,
  "table": "possible-coast-439421-q5.toast.toast_orders_raw"
}
```

### Step 3: Load Historical Data

**Automated approach (Recommended):**
```bash
# Make script executable
chmod +x historical_load.sh

# Run historical load
./historical_load.sh
```

This will load data month by month from 2025-01-01 to 2026-02-09.

**Manual approach:**
```bash
# Load specific date range
curl -X POST https://toast-purpose-bulk-120665665070.us-west1.run.app \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'

# Repeat for each month...
```

**Note:** For large date ranges, break into monthly chunks to avoid timeouts.

### Step 4: Verify Data in BigQuery

Run these queries in [BigQuery Console](https://console.cloud.google.com/bigquery):

```sql
-- Check total orders loaded
SELECT
  COUNT(*) as total_orders,
  COUNT(DISTINCT _restaurant_guid) as unique_restaurants,
  MIN(DATE(_loaded_at)) as first_load_date,
  MAX(DATE(_loaded_at)) as last_load_date
FROM `possible-coast-439421-q5.toast.toast_orders_raw`;

-- Check orders by date
SELECT
  DATE(businessDate) as order_date,
  COUNT(*) as order_count,
  COUNT(DISTINCT _restaurant_guid) as restaurants_with_orders
FROM `possible-coast-439421-q5.toast.toast_orders_raw`
GROUP BY 1
ORDER BY 1 DESC
LIMIT 30;

-- Check for potential duplicates
SELECT
  guid as order_guid,
  COUNT(*) as duplicate_count
FROM `possible-coast-439421-q5.toast.toast_orders_raw`
GROUP BY 1
HAVING COUNT(*) > 1
ORDER BY 2 DESC;
```

### Step 5: Set Up Cloud Scheduler (Daily Automation)

**Automated approach:**
```bash
# Make script executable
chmod +x setup_scheduler.sh

# Run scheduler setup
./setup_scheduler.sh
```

**Manual approach:**
```bash
gcloud scheduler jobs create http toast-orders-daily \
  --location=us-west1 \
  --schedule="0 2 * * *" \
  --uri="https://toast-purpose-bulk-120665665070.us-west1.run.app" \
  --http-method=GET \
  --time-zone="America/Los_Angeles" \
  --attempt-deadline=540s
```

**Trigger manually to test:**
```bash
gcloud scheduler jobs run toast-orders-daily --location=us-west1
```

## 📊 BigQuery Schema

The function loads raw JSON into `toast_orders_raw` table with auto-detected schema plus metadata:

**Metadata columns (added by function):**
- `_loaded_at`: Timestamp when data was ingested
- `_restaurant_guid`: Restaurant identifier for this order
- `_data_source`: Always 'toast_api'

**Toast API fields** (auto-detected from JSON):
- `guid`: Unique order ID
- `businessDate`: Order date
- `checks`: Array of check details
- `selections`: Array of menu items ordered
- `payments`: Array of payment transactions
- And many more fields from Toast API...

## 🔄 Daily Operations

### View Function Logs
```bash
gcloud functions logs read toast-purpose-bulk \
  --gen2 \
  --region=us-west1 \
  --limit=50
```

### View Scheduler Logs
```bash
gcloud scheduler jobs describe toast-orders-daily \
  --location=us-west1
```

### Manually Trigger Daily Run
```bash
# Via scheduler
gcloud scheduler jobs run toast-orders-daily --location=us-west1

# Direct function call
curl https://toast-purpose-bulk-120665665070.us-west1.run.app
```

### Load Specific Date Range
```bash
curl -X POST https://toast-purpose-bulk-120665665070.us-west1.run.app \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2026-01-15", "end_date": "2026-01-20"}'
```

## 🚨 Troubleshooting

### Issue: "Failed to get Toast token"
**Solution:** Check environment variables in Cloud Function:
```bash
gcloud functions describe toast-purpose-bulk \
  --gen2 \
  --region=us-west1 \
  --format="value(serviceConfig.environmentVariables)"
```

### Issue: "No orders found"
**Possible causes:**
1. No orders on that date (normal if restaurant was closed)
2. Wrong date format (must be YYYY-MM-DD)
3. API rate limiting (wait and retry)

### Issue: Function timeout
**Solution:** Break large date ranges into smaller chunks (monthly recommended)

### Issue: Duplicate orders in BigQuery
**Solution:** Run deduplication query:
```sql
-- Create deduplicated table
CREATE OR REPLACE TABLE `possible-coast-439421-q5.toast.toast_orders_deduped` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY guid ORDER BY _loaded_at DESC) as row_num
  FROM `possible-coast-439421-q5.toast.toast_orders_raw`
)
WHERE row_num = 1;
```

## 📈 Next Steps

1. **Data Transformation**: Create views/tables for dimensional model
   - FactOrders
   - FactChecks
   - FactPayments
   - DimMenuItem
   - etc.

2. **Monitoring**: Set up alerts for:
   - Function failures
   - No data loaded
   - Unexpected data volumes

3. **Deduplication**: Implement MERGE strategy instead of APPEND

4. **Authentication**: Add authentication to Cloud Function endpoint

5. **Optimization**:
   - Batch API calls more efficiently
   - Add exponential backoff for retries
   - Cache tokens to reduce API calls

## 📞 Support

- Toast API Documentation: https://doc.toasttab.com/
- Google Cloud Functions: https://cloud.google.com/functions/docs
- BigQuery: https://cloud.google.com/bigquery/docs

## 📝 Change Log

### 2026-02-09: Improved Version
- Added date range parameters
- Fixed dataset name mismatch
- Added test mode
- Enhanced error handling
- Added metadata columns
- Improved logging and responses
