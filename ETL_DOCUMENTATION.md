## Toast POS to BigQuery - Complete Data Warehouse Solution

**Project:** possible-coast-439421-q5
**Dataset:** purpose (dimensional model)
**Staging:** toast.orders (raw JSON)

---

## Architecture Overview

```
┌─────────────┐
│  Toast API  │
│  (13 Locs)  │
└──────┬──────┘
       │ 1. Extract (Cloud Function)
       ↓
┌──────────────────────┐
│  toast.orders        │
│  (Raw Staging)       │
│  - JSON storage      │
│  - Partitioned daily │
└──────┬───────────────┘
       │ 2. Transform (SQL)
       ↓
┌──────────────────────────────────────────┐
│  purpose Dataset (Star Schema)           │
├──────────────────────────────────────────┤
│  Dimensions:                             │
│  ├─ DimLocation      (SCD Type 2)        │
│  ├─ DimEmployee      (SCD Type 2)        │
│  ├─ DimJob                               │
│  └─ DimMenuItem      (SCD Type 2)        │
│                                           │
│  Facts:                                  │
│  ├─ FactOrders       (1 row per order)   │
│  ├─ FactChecks       (1 row per check)   │
│  ├─ FactPayments     (1 row per payment) │
│  ├─ FactMenuSelection (1 row per item)   │
│  ├─ FactCashEntries                      │
│  ├─ FactDeposits                         │
│  └─ FactTimeEntries                      │
└──────┬───────────────────────────────────┘
       │ 3. Consume
       ↓
┌──────────────────────┐
│    Power BI          │
│  (Dashboards)        │
└──────────────────────┘
```

---

## Data Flow

### Phase 1: Extract (Cloud Function)
**File:** `main_improved.py`
**Trigger:** HTTP (manual) or Cloud Scheduler (daily at 2 AM)

**Process:**
1. Authenticates with Toast API
2. Fetches orders for all 13 restaurants
3. Handles pagination (100 orders per page)
4. Loads raw JSON to `toast.orders` staging table
5. Adds metadata: `_loaded_at`, `_restaurant_guid`, `_data_source`

**Output:** Raw JSON in `toast.orders` partitioned by `business_date`

### Phase 2: Transform - Dimensions (SQL)
**File:** `etl_load_dimensions.sql`
**Run After:** Phase 1 completes

**Process:**
1. **DimLocation** - Extract unique restaurant GUIDs (SCD Type 1)
2. **DimEmployee** - Extract servers from checks (SCD Type 2 for history)
3. **DimMenuItem** - Extract menu items with price history (SCD Type 2)
4. **DimJob** - Extract job roles from employee data

**SCD Type 2 Logic:**
- When employee/menu item changes detected, close old record (`IsCurrent = FALSE`)
- Insert new record with current values (`IsCurrent = TRUE`)
- Maintains full history for reporting

### Phase 3: Transform - Facts (SQL)
**File:** `etl_load_facts.sql`
**Run After:** Phase 2 completes

**Process:**
1. **FactOrders** - One row per order (header level)
2. **FactChecks** - One row per check (guest check/tab)
3. **FactPayments** - One row per payment transaction
4. **FactMenuSelection** - One row per menu item ordered (most granular)
5. **FactCashEntries** - Cash drawer transactions
6. **FactDeposits** - Bank deposits
7. **FactTimeEntries** - Employee time clock data

**Deduplication:** MERGE statements prevent duplicates (WHEN NOT MATCHED)

### Phase 4: Consume (Power BI)
**File:** `powerbi_queries.sql`
**Views Created:**

1. **vw_DailySalesByLocation** - Revenue by restaurant/day
2. **vw_ServerPerformance** - Server sales, tips, rankings
3. **vw_MenuItemPerformance** - Best-selling items, revenue contribution
4. **vw_PaymentTypeAnalysis** - Cash vs Card trends
5. **vw_HourlySalesTrend** - Daypart analysis (breakfast, lunch, dinner)
6. **vw_LaborProductivity** - Labor cost vs sales
7. **vw_PeriodComparison** - Day-over-day, week-over-week trends
8. **vw_TopServersByShift** - Top performers by shift

---

## Table Schemas

### Staging Table: toast.orders

```sql
CREATE TABLE `possible-coast-439421-q5.toast.orders` (
  raw_json STRING,
  order_guid STRING,
  restaurant_guid STRING,
  business_date DATE,
  opened_date TIMESTAMP,
  closed_date TIMESTAMP,
  _loaded_at TIMESTAMP,
  _load_id STRING,
  _data_source STRING
)
PARTITION BY business_date
CLUSTER BY restaurant_guid, order_guid;
```

### Dimension: DimEmployee

```sql
CREATE TABLE `possible-coast-439421-q5.purpose.DimEmployee` (
  EmployeeKey INT64,        -- Surrogate key
  EmployeeGuid STRING,      -- Natural key from Toast
  FirstName STRING,
  LastName STRING,
  FullName STRING,
  Email STRING,
  JobGuid STRING,
  JobTitle STRING,
  LocationGuid STRING,
  IsActive BOOL,
  HireDate DATE,
  TerminationDate DATE,
  EffectiveDate DATE,       -- SCD Type 2
  ExpirationDate DATE,      -- SCD Type 2
  IsCurrent BOOL,           -- SCD Type 2
  CreatedAt TIMESTAMP,
  UpdatedAt TIMESTAMP
)
CLUSTER BY EmployeeGuid;
```

### Fact: FactOrders

```sql
CREATE TABLE `possible-coast-439421-q5.purpose.FactOrders` (
  OrderKey INT64,
  OrderGuid STRING,
  LocationKey INT64,        -- FK to DimLocation
  BusinessDateKey INT64,    -- YYYYMMDD format
  OrderNumber STRING,
  ExternalId STRING,
  Source STRING,
  BusinessDate DATE,
  OpenedDate TIMESTAMP,
  ClosedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  PaidDate TIMESTAMP,
  NumberOfGuests INT64,
  NumberOfChecks INT64,
  DurationMinutes INT64,
  IsVoided BOOL,
  VoidDate TIMESTAMP,
  ApprovalStatus STRING,
  _LoadedAt TIMESTAMP,
  _LoadId STRING,
  _DataSource STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, OrderGuid;
```

---

## Running the ETL

### One-Time Setup

1. **Deploy Cloud Function:**
   ```bash
   ./deploy.sh
   ```

2. **Create Staging Table:**
   ```bash
   bq query --project_id=possible-coast-439421-q5 --use_legacy_sql=false < create_staging_table.sql
   ```

3. **Create Dimensional Tables:**
   ```bash
   bq query --project_id=possible-coast-439421-q5 --use_legacy_sql=false < transform_to_dimensions.sql
   ```

4. **Create Analytical Views:**
   ```bash
   bq query --project_id=possible-coast-439421-q5 --use_legacy_sql=false < powerbi_queries.sql
   ```

5. **Setup Cloud Scheduler:**
   ```bash
   ./setup_scheduler.sh
   ```

### Daily Execution (Automated)

Cloud Scheduler triggers at 2 AM daily:
1. Calls Cloud Function → loads `toast.orders`
2. Scheduled Query runs `etl_load_dimensions.sql`
3. Scheduled Query runs `etl_load_facts.sql`

**Manual trigger:**
```bash
./run_daily_etl.sh
```

### Historical Data Load

Load 2025-2026 data:
```bash
./historical_load.sh
```

Then run dimension/fact loads:
```bash
bq query --project_id=possible-coast-439421-q5 --use_legacy_sql=false < etl_load_dimensions.sql
bq query --project_id=possible-coast-439421-q5 --use_legacy_sql=false < etl_load_facts.sql
```

---

## Performance Optimization

### Partitioning Strategy
- **Staging:** Partitioned by `business_date`
- **Facts:** Partitioned by `BusinessDate`
- **Benefits:** Query pruning, faster scans, lower costs

### Clustering Strategy
- **Staging:** Clustered by `restaurant_guid, order_guid`
- **Facts:** Clustered by high-cardinality keys (LocationKey, ServerKey, etc.)
- **Benefits:** Faster joins, reduced shuffle

### Query Optimization Tips
1. Always filter on partition column (`business_date` / `BusinessDate`)
2. Use clustered columns in WHERE/JOIN clauses
3. Pre-aggregate with materialized views for common queries
4. Use `require_partition_filter=true` to enforce partition usage

---

## Data Quality Checks

### Validation Queries

**Check for duplicates in FactOrders:**
```sql
SELECT
  OrderGuid,
  COUNT(*) as duplicate_count
FROM `possible-coast-439421-q5.purpose.FactOrders`
GROUP BY 1
HAVING COUNT(*) > 1;
```

**Verify daily load completeness:**
```sql
SELECT
  BusinessDate,
  COUNT(DISTINCT OrderGuid) as orders,
  COUNT(DISTINCT CheckGuid) as checks,
  SUM(TotalAmount) as revenue
FROM `possible-coast-439421-q5.purpose.FactChecks`
WHERE BusinessDate >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY 1
ORDER BY 1 DESC;
```

**Check SCD Type 2 integrity:**
```sql
SELECT
  EmployeeGuid,
  COUNT(*) as version_count,
  SUM(CAST(IsCurrent AS INT64)) as current_count
FROM `possible-coast-439421-q5.purpose.DimEmployee`
GROUP BY 1
HAVING current_count != 1;  -- Should be empty (each employee has exactly 1 current record)
```

---

## Power BI Integration

### Connection Setup
1. Open Power BI Desktop
2. Get Data → More → Google BigQuery
3. Project: `possible-coast-439421-q5`
4. Dataset: `purpose`
5. Mode: DirectQuery (for large datasets) or Import (for small)

### Recommended Tables for Power BI Model
```
Fact Tables (Import or DirectQuery):
- FactOrders
- FactChecks
- FactPayments
- FactMenuSelection

Dimensions (Import):
- DimLocation
- DimEmployee
- DimMenuItem
- DimJob

Pre-Aggregated Views (Import):
- vw_DailySalesByLocation
- vw_ServerPerformance
- vw_MenuItemPerformance
```

### Sample DAX Measures

**Total Revenue:**
```dax
Total Revenue = SUM(FactChecks[TotalAmount])
```

**Average Check Size:**
```dax
Avg Check Size = AVERAGE(FactChecks[TotalAmount])
```

**Tip Percentage:**
```dax
Tip % = DIVIDE(SUM(FactChecks[TipAmount]), SUM(FactChecks[SubtotalAmount]))
```

**YTD Revenue:**
```dax
YTD Revenue = TOTALYTD(SUM(FactChecks[TotalAmount]), FactOrders[BusinessDate])
```

---

## Monitoring & Alerts

### Cloud Function Monitoring
```bash
# View logs
gcloud functions logs read toast-purpose-bulk --gen2 --region=us-west1 --limit=50

# Check errors
gcloud functions logs read toast-purpose-bulk --gen2 --region=us-west1 --limit=100 | grep ERROR
```

### BigQuery Job Monitoring
```bash
# List recent jobs
bq ls --jobs --max_results=10

# Check specific job
bq show -j <job_id>
```

### Recommended Alerts (via Google Cloud Monitoring)
1. Cloud Function failures > 2 in 1 hour
2. No data loaded for current day by 3 AM
3. BigQuery query costs spike > 10x normal
4. Row count anomalies (e.g., 50% drop in daily orders)

---

## Troubleshooting

### Issue: No data in FactOrders after ETL
**Diagnosis:**
```sql
SELECT COUNT(*) FROM `possible-coast-439421-q5.toast.orders` WHERE business_date = CURRENT_DATE() - 1;
```
**Solution:** Check if staging has data. If not, Cloud Function may have failed.

### Issue: Duplicate records in Facts
**Diagnosis:** Run duplicate check query (see Data Quality section)
**Solution:** MERGE statements should prevent this. If duplicates exist:
```sql
-- Create deduplicated table
CREATE OR REPLACE TABLE `possible-coast-439421-q5.purpose.FactOrders_deduped` AS
SELECT * EXCEPT(row_num)
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY OrderGuid ORDER BY _LoadedAt DESC) as row_num
  FROM `possible-coast-439421-q5.purpose.FactOrders`
)
WHERE row_num = 1;
```

### Issue: SCD Type 2 not closing old records
**Solution:** Ensure `IsCurrent = TRUE` filter is in MERGE condition:
```sql
ON target.EmployeeGuid = source.EmployeeGuid AND target.IsCurrent = TRUE
```

---

## Costs & Performance

### Estimated Costs (per month)
- **Cloud Function:** ~$5-10 (2-3 min runtime daily)
- **BigQuery Storage:** ~$10-20 (100GB historical data)
- **BigQuery Queries:** ~$20-50 (depends on Power BI usage)
- **Total:** ~$35-80/month

### Optimization Tips
1. Use views for simple aggregations (no storage cost)
2. Materialize complex aggregations as tables
3. Set expiration on staging tables (90 days retention)
4. Use partitioned tables with partition pruning

---

## Future Enhancements

1. **Real-time Updates:** Stream orders via Pub/Sub instead of batch
2. **Advanced Analytics:** Predictive models for demand forecasting
3. **Data Governance:** Add data lineage tracking, PII masking
4. **More Dimensions:** Customer demographics, weather data, promotions
5. **Deduplication:** Implement change data capture (CDC) logic
6. **Testing:** Add dbt for transformation testing and documentation

---

## Support & References

- **Toast API Docs:** https://doc.toasttab.com/
- **BigQuery Docs:** https://cloud.google.com/bigquery/docs
- **Star Schema Design:** Kimball methodology
- **Power BI Docs:** https://docs.microsoft.com/power-bi/

---

**Last Updated:** 2026-02-09
**Version:** 1.0
**Author:** Claude Code
