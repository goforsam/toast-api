"""Check BigQuery data status"""
from google.cloud import bigquery

client = bigquery.Client(project='possible-coast-439421-q5')

print("="*70)
print("BigQuery Data Status Check")
print("="*70)

# Check toast_orders_raw table
query = """
SELECT
    COUNT(*) as total_orders,
    MIN(businessDate) as earliest_date,
    MAX(businessDate) as latest_date,
    COUNT(DISTINCT restaurantGuid) as num_restaurants
FROM `possible-coast-439421-q5.purpose.toast_orders_raw`
"""

print("\n1. Raw Orders Table (toast_orders_raw):")
print("-"*70)
try:
    results = client.query(query).result()
    for row in results:
        print(f"  Total Orders: {row.total_orders:,}")
        print(f"  Date Range: {row.earliest_date} to {row.latest_date}")
        print(f"  Restaurants: {row.num_restaurants}")
except Exception as e:
    print(f"  Table not found or error: {e}")

# Check if dimensional tables exist
print("\n2. Checking Dimensional Tables:")
print("-"*70)

dim_tables = [
    'DimLocation', 'DimEmployee', 'DimMenuItem', 'DimJob',
    'FactOrders', 'FactChecks', 'FactPayments', 'FactMenuSelection'
]

for table_name in dim_tables:
    try:
        query = f"SELECT COUNT(*) as cnt FROM `possible-coast-439421-q5.purpose.{table_name}` LIMIT 1"
        result = client.query(query).result()
        count = next(result).cnt
        print(f"  ✓ {table_name}: {count:,} rows")
    except Exception:
        print(f"  ✗ {table_name}: Not created yet")

print("\n" + "="*70)
print("Status Summary:")
print("="*70)

# Get raw orders by restaurant
query = """
SELECT
    restaurantGuid,
    COUNT(*) as order_count,
    MIN(businessDate) as first_date,
    MAX(businessDate) as last_date
FROM `possible-coast-439421-q5.purpose.toast_orders_raw`
GROUP BY restaurantGuid
ORDER BY order_count DESC
"""

print("\n3. Orders by Restaurant:")
print("-"*70)
try:
    results = client.query(query).result()
    for i, row in enumerate(results, 1):
        guid_short = row.restaurantGuid[:8] + "..."
        print(f"  {i}. {guid_short}: {row.order_count:,} orders ({row.first_date} to {row.last_date})")
except Exception as e:
    print(f"  Error: {e}")

print("\n" + "="*70)
