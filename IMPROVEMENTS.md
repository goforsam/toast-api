# Toast API to BigQuery - Code Improvements

## Summary of Changes

### ✅ Fixed Issues

1. **Dataset Name Mismatch** (CRITICAL)
   - **Old**: Default was `'purpose'`
   - **New**: Default is `'toast'` (matches your BigQuery dataset)
   - **Impact**: Function was trying to write to wrong dataset

2. **Added Date Range Flexibility** (CRITICAL for historical load)
   - **Old**: Hardcoded to yesterday only
   - **New**: Accepts `start_date` and `end_date` parameters via HTTP request
   - **Usage**:
     ```bash
     # Historical load for 2025
     curl -X POST https://your-function-url \
       -H "Content-Type: application/json" \
       -d '{"start_date": "2025-01-01", "end_date": "2025-12-31"}'

     # Daily load (default: yesterday)
     curl https://your-function-url
     ```

3. **Better Error Handling**
   - **Old**: One restaurant failure stops entire process
   - **New**: Continues processing all restaurants, reports errors separately
   - **Impact**: More resilient to partial failures

4. **Test Mode**
   - **New**: Add `?mode=test` to test without loading to BigQuery
   - **Usage**: Validate API connection and see sample data structure
   ```bash
   curl "https://your-function-url?mode=test"
   ```

5. **Enhanced Metadata**
   - Added to each order record:
     - `_loaded_at`: Timestamp when data was loaded
     - `_restaurant_guid`: Which restaurant this order came from
     - `_data_source`: Always 'toast_api' for tracking
   - **Impact**: Better data lineage and debugging

6. **Better Response Format**
   - **Old**: Plain text response
   - **New**: JSON response with detailed status
   ```json
   {
     "status": "success",
     "message": "Successfully loaded orders for 2025-01-01 to 2025-01-31",
     "orders_loaded": 1234,
     "date_range": "2025-01-01 to 2025-01-31",
     "table": "possible-coast-439421-q5.toast.toast_orders_raw",
     "errors": []
   }
   ```

7. **Improved Logging**
   - More detailed console logs for debugging
   - Tracks progress per restaurant
   - Shows exact error messages

8. **Empty GUID Handling**
   - **Old**: Could cause issues with empty strings from RESTAURANT_GUIDS split
   - **New**: Filters out empty strings before processing

### 🔄 Maintained Features

- OAuth authentication with Toast API
- Pagination for large result sets
- Timeout settings
- Multi-restaurant support
- BigQuery autodetect schema (with field addition allowed)

### ⚠️ Still To Address

1. **Deduplication**:
   - Running the function twice for the same date will create duplicates
   - **Solution Options**:
     - A) Add `order_id` as primary key in staging table
     - B) Use MERGE instead of APPEND in BigQuery
     - C) Create a deduplication query as separate step

2. **BigQuery Schema**:
   - Still using autodetect which can cause issues
   - **Recommendation**: Define explicit schema based on Toast API structure

3. **Rate Limiting**:
   - No exponential backoff for API failures
   - Could hit Toast API rate limits with many restaurants

4. **Authentication**:
   - Cloud Function endpoint is unauthenticated
   - **Recommendation**: Add Cloud Function authentication

## Testing Plan

### 1. Test Current Deployed Function
```bash
# Test with test mode (no BigQuery write)
curl "https://toast-purpose-bulk-120665665070.us-west1.run.app?mode=test"
```

### 2. Test Historical Load
```bash
# Load January 2025
curl -X POST https://toast-purpose-bulk-120665665070.us-west1.run.app \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-01-01", "end_date": "2025-01-31"}'
```

### 3. Verify Data in BigQuery
```sql
SELECT
  DATE(_loaded_at) as load_date,
  _restaurant_guid,
  COUNT(*) as order_count
FROM `possible-coast-439421-q5.toast.toast_orders_raw`
GROUP BY 1, 2
ORDER BY 1 DESC, 2
```

## Deployment

### Option 1: Deploy via gcloud CLI
```bash
gcloud functions deploy toast-purpose-bulk \
  --gen2 \
  --runtime=python312 \
  --region=us-west1 \
  --source=. \
  --entry-point=orders_daily \
  --trigger-http \
  --allow-unauthenticated \
  --set-env-vars TOAST_CLIENT_ID=your-client-id,TOAST_CLIENT_SECRET=your-secret,BQ_PROJECT_ID=possible-coast-439421-q5,BQ_DATASET_ID=toast,RESTAURANT_GUIDS="guid1,guid2,guid3"
```

### Option 2: Deploy via Console
1. Go to Cloud Functions in GCP Console
2. Select `toast-purpose-bulk`
3. Click "Edit"
4. Replace `main.py` content with `main_improved.py`
5. Update environment variable `BQ_DATASET_ID=toast`
6. Deploy

## Next Steps

1. ✅ Test current deployed function
2. ✅ Backup current code
3. Deploy improved version
4. Run test mode to validate
5. Load historical data (2025-01-01 to 2026-02-09)
6. Set up Cloud Scheduler for daily runs
7. Implement deduplication strategy
8. Define explicit BigQuery schema
