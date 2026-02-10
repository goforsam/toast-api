# Toast POS to BigQuery Data Warehouse

**Enterprise-grade ETL pipeline for Toast Restaurant POS data with Power BI analytics**

## Quick Start

```bash
# 1. Deploy improved Cloud Function
./deploy.sh

# 2. Load historical data (2025-2026)
./historical_load.sh

# 3. Transform to dimensional model
bq query --use_legacy_sql=false < etl_load_dimensions.sql
bq query --use_legacy_sql=false < etl_load_facts.sql

# 4. Setup daily automation
./setup_scheduler.sh

# 5. Connect Power BI to BigQuery dataset 'purpose'
```

## Project Structure

```
toast-api/
├── main.py                      # Current (original) Cloud Function
├── main_improved.py             # ✨ Enhanced version with date ranges
├── requirements.txt             # Python dependencies
│
├── 📊 ETL & Transformation
│   ├── create_staging_table.sql      # Create toast.orders staging
│   ├── transform_to_dimensions.sql   # Create dimensional tables
│   ├── etl_load_dimensions.sql       # Load dimensions (SCD Type 2)
│   ├── etl_load_facts.sql            # Load fact tables (MERGE)
│   └── powerbi_queries.sql           # 8 analytical views for dashboards
│
├── 🚀 Deployment & Automation
│   ├── deploy.sh                     # Deploy Cloud Function
│   ├── historical_load.sh            # Backfill 2025-2026 data
│   ├── setup_scheduler.sh            # Daily automation (Cloud Scheduler)
│   └── run_daily_etl.sh              # Master ETL orchestration
│
├── 🧪 Testing & Validation
│   ├── test_toast_api.py             # Local API connection test
│   ├── bigquery_check.sql            # Data validation queries
│   └── check_env.sh                  # Verify environment variables
│
└── 📖 Documentation
    ├── README.md                     # This file
    ├── ETL_DOCUMENTATION.md          # Complete technical docs
    ├── DEPLOYMENT_GUIDE.md           # Step-by-step deployment
    ├── IMPROVEMENTS.md               # What was improved
    ├── AUDIT_SUMMARY.md              # Code audit results
    └── MEMORY.md                     # Learnings (auto memory)
```

## Architecture

### Star Schema Design
```
Purpose Dataset (Dimensional Model)
│
├─ 📏 Dimensions (SCD Type 2)
│   ├─ DimLocation        13 restaurants
│   ├─ DimEmployee        Servers & staff (with history)
│   ├─ DimJob             Job roles
│   └─ DimMenuItem        Menu items (price history)
│
├─ 📊 Facts (Partitioned by date)
│   ├─ FactOrders         1 row per order
│   ├─ FactChecks         1 row per guest check
│   ├─ FactPayments       1 row per payment
│   ├─ FactMenuSelection  1 row per item ordered
│   ├─ FactCashEntries    Cash drawer transactions
│   ├─ FactDeposits       Bank deposits
│   └─ FactTimeEntries    Employee time clock
│
└─ 📈 Analytical Views (Power BI-ready)
    ├─ vw_DailySalesByLocation
    ├─ vw_ServerPerformance
    ├─ vw_MenuItemPerformance
    ├─ vw_PaymentTypeAnalysis
    ├─ vw_HourlySalesTrend
    ├─ vw_LaborProductivity
    ├─ vw_PeriodComparison
    └─ vw_TopServersByShift
```

## What This Gives You

### 🎯 Business Analytics
- **Revenue Analysis:** Daily sales by location, daypart, server
- **Menu Performance:** Best-sellers, revenue contribution, pricing analysis
- **Labor Optimization:** Sales per hour, labor cost %, productivity rankings
- **Server Performance:** Sales rankings, tip %, table turn time
- **Trend Analysis:** Day-over-day, week-over-week, rolling averages

### 🏗️ Technical Features
- ✅ **Incremental Loads:** Only process new data
- ✅ **Deduplication:** MERGE prevents duplicate records
- ✅ **History Tracking:** SCD Type 2 for employee/menu changes
- ✅ **Partitioned Tables:** Optimized for date-range queries
- ✅ **Clustered Keys:** Fast joins and aggregations
- ✅ **Metadata Tracking:** Data lineage, load timestamps
- ✅ **Error Handling:** Continue processing on partial failures

## Current vs Improved

| Feature | Original | Improved |
|---------|----------|----------|
| Date Range | Yesterday only | Any date range |
| Error Handling | Stop on first error | Continue all restaurants |
| Test Mode | None | `?mode=test` |
| Response Format | Plain text | JSON |
| Metadata | None | Load timestamp, source tracking |
| Dataset Name | Hardcoded 'purpose' | Correct 'purpose' |
| Deduplication | None | MERGE statements |
| Historical Load | Not possible | ✅ Full backfill |

## Environment Variables

Set in Cloud Function:
```bash
TOAST_CLIENT_ID=your-client-id
TOAST_CLIENT_SECRET=your-secret
BQ_PROJECT_ID=possible-coast-439421-q5
BQ_DATASET_ID=purpose
RESTAURANT_GUIDS=6d035dad-924f-47b4-ba93-fd86575e73a3,53ae28f1-87c7-4a07-9a43-b619c009b7b0,...
```

## Daily Operations

### Automated (via Cloud Scheduler)
- **2:00 AM:** Cloud Function extracts yesterday's orders
- **2:15 AM:** Load dimensions (SCD updates)
- **2:30 AM:** Load facts (incremental)
- **3:00 AM:** Power BI refreshes (if scheduled)

### Manual Triggers
```bash
# Fetch specific date range
curl -X POST https://toast-purpose-bulk-120665665070.us-west1.run.app \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2025-12-01", "end_date": "2025-12-31"}'

# Test without loading data
curl "https://toast-purpose-bulk-120665665070.us-west1.run.app?mode=test"

# Run full ETL pipeline
./run_daily_etl.sh

# View logs
gcloud functions logs read toast-purpose-bulk --gen2 --region=us-west1
```

## Power BI Setup

1. **Connect to BigQuery:**
   - Project: `possible-coast-439421-q5`
   - Dataset: `purpose`
   - Mode: DirectQuery (real-time) or Import (faster)

2. **Import These Tables:**
   - All `FactXXX` tables (star schema center)
   - All `DimXXX` tables (dimensions)
   - All `vw_XXX` views (pre-aggregated)

3. **Create Relationships:**
   ```
   FactOrders[LocationKey] → DimLocation[LocationKey]
   FactChecks[ServerKey] → DimEmployee[EmployeeKey]
   FactMenuSelection[MenuItemKey] → DimMenuItem[MenuItemKey]
   ```

4. **Sample Dashboard Pages:**
   - Executive Summary (revenue, trends, top locations)
   - Server Performance (rankings, tips, productivity)
   - Menu Analysis (best-sellers, pricing, categories)
   - Labor Management (hours, cost %, efficiency)
   - Payment Analysis (cash vs card, trends)

## Monitoring & Alerts

### Health Checks
```sql
-- Check latest data load
SELECT MAX(BusinessDate) as latest_date
FROM `possible-coast-439421-q5.purpose.FactOrders`;

-- Verify row counts
SELECT
  'Orders' as table_name, COUNT(*) as row_count
FROM `possible-coast-439421-q5.purpose.FactOrders`
UNION ALL
SELECT 'Checks', COUNT(*) FROM `possible-coast-439421-q5.purpose.FactChecks`;
```

### Recommended Alerts
- No data for current day by 3 AM
- Order count drops >50% day-over-day
- ETL job failures
- BigQuery cost spikes

## Costs

**Estimated Monthly:**
- Cloud Function: $5-10
- BigQuery Storage (100GB): $10-20
- BigQuery Queries: $20-50
- **Total: ~$35-80/month**

**Optimization:**
- Use partition pruning (always filter by date)
- Materialize expensive views
- Set data retention (90 days for staging)

## Troubleshooting

### No data loading?
```bash
# Check Cloud Function logs
gcloud functions logs read toast-purpose-bulk --gen2 --region=us-west1 --limit=50

# Check staging table
bq query "SELECT COUNT(*), MAX(business_date) FROM \`possible-coast-439421-q5.toast.orders\`"
```

### Duplicates in fact tables?
```sql
-- Run deduplication
CREATE OR REPLACE TABLE `possible-coast-439421-q5.purpose.FactOrders_clean` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY OrderGuid ORDER BY _LoadedAt DESC) as row_num
  FROM `possible-coast-439421-q5.purpose.FactOrders`
)
WHERE row_num = 1;
```

### Performance issues?
- Ensure queries filter on partition column (`BusinessDate`)
- Use clustered columns in WHERE/JOIN
- Check query execution plan in BigQuery console

## Documentation

- **[ETL_DOCUMENTATION.md](ETL_DOCUMENTATION.md)** - Complete technical reference
- **[DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)** - Step-by-step setup
- **[AUDIT_SUMMARY.md](AUDIT_SUMMARY.md)** - Code audit results
- **[IMPROVEMENTS.md](IMPROVEMENTS.md)** - What was fixed/enhanced

## Support

- Toast API: https://doc.toasttab.com/
- BigQuery: https://cloud.google.com/bigquery/docs
- Power BI: https://docs.microsoft.com/power-bi/

## Status

- ✅ **Code Audit:** Complete - System is functional
- ✅ **Improvements:** Complete - Date range support added
- ✅ **ETL Design:** Complete - Star schema with SCD Type 2
- ✅ **Documentation:** Complete - Full technical docs
- ⏳ **Deployment:** Ready to deploy
- ⏳ **Historical Load:** Ready to execute
- ⏳ **Power BI:** Ready to connect

## Next Steps

1. **Review** the improved code ([main_improved.py](main_improved.py))
2. **Deploy** the updated Cloud Function ([deploy.sh](deploy.sh))
3. **Test** with a single day ([DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md))
4. **Load** historical data ([historical_load.sh](historical_load.sh))
5. **Transform** to dimensional model (run SQL scripts)
6. **Connect** Power BI to BigQuery
7. **Automate** with Cloud Scheduler ([setup_scheduler.sh](setup_scheduler.sh))

---

**Version:** 1.0
**Last Updated:** 2026-02-09
**Status:** Production-Ready ✅
