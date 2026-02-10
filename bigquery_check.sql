-- Check existing data in toast_orders_raw table

-- 1. Check if table exists and row count
SELECT
  COUNT(*) as total_orders,
  COUNT(DISTINCT DATE(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.businessDate') AS TIMESTAMP))) as distinct_dates,
  MIN(DATE(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.businessDate') AS TIMESTAMP))) as earliest_date,
  MAX(DATE(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.businessDate') AS TIMESTAMP))) as latest_date
FROM `possible-coast-439421-q5.toast.toast_orders_raw` t;

-- 2. Check orders by restaurant (if data exists)
SELECT
  JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.restaurantGuid') as restaurant_guid,
  COUNT(*) as order_count,
  MIN(DATE(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.businessDate') AS TIMESTAMP))) as earliest_order,
  MAX(DATE(CAST(JSON_EXTRACT_SCALAR(TO_JSON_STRING(t), '$.businessDate') AS TIMESTAMP))) as latest_order
FROM `possible-coast-439421-q5.toast.toast_orders_raw` t
GROUP BY 1
ORDER BY 2 DESC;

-- 3. Sample data structure (if data exists)
SELECT *
FROM `possible-coast-439421-q5.toast.toast_orders_raw`
LIMIT 1;
