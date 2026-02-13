"""
Check current status of loaded data in BigQuery
"""
from google.cloud import bigquery

BQ_PROJECT_ID = 'possible-coast-439421-q5'
BQ_DATASET_ID = 'purpose'

client = bigquery.Client(project=BQ_PROJECT_ID)

# Check toast_orders_raw
query = """
SELECT
  COUNT(*) as total_orders,
  COUNT(DISTINCT _restaurant_guid) as num_restaurants,
  MIN(businessDate) as earliest_date,
  MAX(businessDate) as latest_date,
  MIN(_loaded_at) as first_load,
  MAX(_loaded_at) as last_load
FROM `possible-coast-439421-q5.purpose.toast_orders_raw`
"""

print("Checking toast_orders_raw...")
result = client.query(query).result()

for row in result:
    print(f"\nCurrent Data Status:")
    print(f"  Total Orders: {row.total_orders:,}")
    print(f"  Restaurants: {row.num_restaurants}")
    print(f"  Date Range: {row.earliest_date} to {row.latest_date}")
    print(f"  Load Time: {row.first_load} to {row.last_load}")

# Check by month
query_monthly = """
SELECT
  EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', CAST(businessDate AS STRING))) as year,
  EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', CAST(businessDate AS STRING))) as month,
  COUNT(*) as order_count
FROM `possible-coast-439421-q5.purpose.toast_orders_raw`
GROUP BY year, month
ORDER BY year, month
"""

print("\n\nOrders by Month:")
result = client.query(query_monthly).result()

for row in result:
    print(f"  {int(row.year)}-{int(row.month):02d}: {row.order_count:,} orders")
