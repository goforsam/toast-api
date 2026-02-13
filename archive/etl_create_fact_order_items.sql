-- ============================================================================
-- Create fact_order_items from toast_orders_raw
-- Extracts menu item selections from nested checks array
-- ============================================================================

-- Step 1: Create the fact table with partitioning and clustering
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.fact_order_items` (
  -- Primary Keys
  selection_guid STRING NOT NULL,
  order_guid STRING NOT NULL,
  check_guid STRING NOT NULL,

  -- Foreign Keys
  restaurant_guid STRING NOT NULL,

  -- Date/Time
  business_date DATE NOT NULL,
  opened_datetime TIMESTAMP,
  paid_datetime TIMESTAMP,

  -- Menu Item Info
  menu_item_guid STRING,
  menu_item_name STRING,
  sales_category_name STRING,
  item_tags ARRAY<STRING>,

  -- Quantities and Prices
  item_quantity FLOAT64,
  item_price FLOAT64,
  applied_discount_amount FLOAT64,

  -- Payment Info
  payment_amount FLOAT64,
  tip_amount FLOAT64,
  refund_information STRING,
  payment_type STRING,
  card_entry_mode STRING,
  payment_status STRING,

  -- Metadata
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY business_date
CLUSTER BY restaurant_guid, business_date
OPTIONS(
  description='Fact table: 1 row per menu item ordered'
);

-- Step 2: Load data from toast_orders_raw
-- This extracts from the nested checks[] -> selections[] structure
INSERT INTO `possible-coast-439421-q5.purpose.fact_order_items`
SELECT
  -- Primary Keys
  selection.guid AS selection_guid,
  orders.guid AS order_guid,
  check.guid AS check_guid,

  -- Foreign Keys
  orders._restaurant_guid AS restaurant_guid,

  -- Date/Time - Convert integer businessDate (20260201) to DATE
  PARSE_DATE('%Y%m%d', CAST(orders.businessDate AS STRING)) AS business_date,
  orders.openedDate AS opened_datetime,
  orders.paidDate AS paid_datetime,

  -- Menu Item Info
  selection.itemGuid AS menu_item_guid,
  selection.itemName AS menu_item_name,
  selection.salesCategory.name AS sales_category_name,
  selection.itemTags AS item_tags,

  -- Quantities and Prices
  selection.quantity AS item_quantity,
  selection.price AS item_price,
  (
    SELECT SUM(CAST(discount.discountAmount AS FLOAT64))
    FROM UNNEST(selection.appliedDiscounts) AS discount
  ) AS applied_discount_amount,

  -- Payment Info (from check level)
  check.totalAmount AS payment_amount,
  check.tipAmount AS tip_amount,
  NULL AS refund_information,  -- TODO: Add from payments
  NULL AS payment_type,  -- TODO: Add from payments array
  NULL AS card_entry_mode,  -- TODO: Add from payments array
  NULL AS payment_status,  -- TODO: Add from check/payment status

  -- Metadata
  CURRENT_TIMESTAMP() AS _loaded_at

FROM `possible-coast-439421-q5.purpose.toast_orders_raw` AS orders,
UNNEST(orders.checks) AS check,
UNNEST(check.selections) AS selection

WHERE
  -- Only non-voided, non-deleted items
  COALESCE(selection.voided, FALSE) = FALSE
  AND COALESCE(selection.deferred, FALSE) = FALSE

  -- Dedup: only load new data
  AND NOT EXISTS (
    SELECT 1
    FROM `possible-coast-439421-q5.purpose.fact_order_items` existing
    WHERE existing.selection_guid = selection.guid
      AND existing.order_guid = orders.guid
  );

-- Verify loaded data
SELECT
  COUNT(*) as total_items,
  COUNT(DISTINCT restaurant_guid) as num_restaurants,
  MIN(business_date) as earliest_date,
  MAX(business_date) as latest_date
FROM `possible-coast-439421-q5.purpose.fact_order_items`;
