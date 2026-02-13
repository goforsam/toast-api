-- ============================================================================
-- COMPLETE ETL: Toast Orders → Dimensional Model
-- Extracts from toast_orders_raw (nested structure) into fact/dim tables
-- ============================================================================

-- ============================================================================
-- STEP 1: CREATE DIMENSION TABLES
-- ============================================================================

-- dim_menu_items: Menu items with price history (SCD Type 2)
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.dim_menu_items` (
  menu_item_key INT64,  -- Surrogate key (auto-generated)
  menu_item_guid STRING NOT NULL,
  menu_item_name STRING,
  sales_category_name STRING,
  base_price FLOAT64,

  -- SCD Type 2 fields
  effective_date DATE,
  expiration_date DATE,
  is_current BOOL DEFAULT TRUE,

  -- Metadata
  _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY effective_date
CLUSTER BY menu_item_guid;

-- dim_sales_categories: Sales category master
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.dim_sales_categories` (
  sales_category_guid STRING NOT NULL,
  sales_category_name STRING,
  description STRING,

  -- Metadata
  _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY sales_category_guid;

-- dim_employees: Server/employee master (SCD Type 2)
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.dim_employees` (
  employee_key INT64,  -- Surrogate key
  employee_guid STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  full_name STRING,
  email STRING,

  -- SCD Type 2 fields
  effective_date DATE,
  expiration_date DATE,
  is_current BOOL DEFAULT TRUE,

  -- Metadata
  _created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY effective_date
CLUSTER BY employee_guid;

-- ============================================================================
-- STEP 2: CREATE FACT TABLE
-- ============================================================================

CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.fact_order_items` (
  -- Primary Keys
  selection_guid STRING NOT NULL,
  order_guid STRING NOT NULL,
  check_guid STRING NOT NULL,

  -- Foreign Keys
  restaurant_guid STRING NOT NULL,
  menu_item_guid STRING,
  sales_category_guid STRING,
  server_guid STRING,

  -- Date/Time Dimensions
  business_date DATE NOT NULL,
  opened_datetime TIMESTAMP,
  closed_datetime TIMESTAMP,
  paid_datetime TIMESTAMP,

  -- Menu Item Details
  menu_item_name STRING,
  sales_category_name STRING,

  -- Quantities and Amounts
  item_quantity FLOAT64,
  item_price FLOAT64,
  pre_discount_price FLOAT64,
  applied_discount_amount FLOAT64,
  tax_amount FLOAT64,

  -- Check-level amounts (for aggregation)
  check_total_amount FLOAT64,
  check_tax_amount FLOAT64,
  check_tip_amount FLOAT64,

  -- Payment Info (from first payment)
  payment_amount FLOAT64,
  payment_type STRING,
  card_type STRING,
  tip_amount FLOAT64,

  -- Flags
  is_voided BOOL DEFAULT FALSE,
  is_deleted BOOL DEFAULT FALSE,

  -- Metadata
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY business_date
CLUSTER BY restaurant_guid, business_date
OPTIONS(
  description='Fact table: 1 row per menu item ordered'
);

-- ============================================================================
-- STEP 3: LOAD DIMENSIONS
-- ============================================================================

-- Load dim_menu_items (INSERT only - SCD Type 2 logic comes later)
INSERT INTO `possible-coast-439421-q5.purpose.dim_menu_items`
(menu_item_key, menu_item_guid, menu_item_name, sales_category_name, base_price, effective_date, expiration_date, is_current)
SELECT
  ROW_NUMBER() OVER (ORDER BY item.itemGuid) + COALESCE((SELECT MAX(menu_item_key) FROM `possible-coast-439421-q5.purpose.dim_menu_items`), 0) AS menu_item_key,
  item.itemGuid AS menu_item_guid,
  item.itemName AS menu_item_name,
  item.salesCategory.name AS sales_category_name,
  item.price AS base_price,
  CURRENT_DATE() AS effective_date,
  DATE('9999-12-31') AS expiration_date,
  TRUE AS is_current
FROM `possible-coast-439421-q5.purpose.toast_orders_raw` AS orders,
UNNEST(orders.checks) AS check,
UNNEST(check.selections) AS item
WHERE item.itemGuid IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM `possible-coast-439421-q5.purpose.dim_menu_items` existing
    WHERE existing.menu_item_guid = item.itemGuid
  )
GROUP BY item.itemGuid, item.itemName, item.salesCategory.name, item.price;

-- Load dim_sales_categories
INSERT INTO `possible-coast-439421-q5.purpose.dim_sales_categories`
(sales_category_guid, sales_category_name, description)
SELECT DISTINCT
  item.salesCategory.guid AS sales_category_guid,
  item.salesCategory.name AS sales_category_name,
  NULL AS description
FROM `possible-coast-439421-q5.purpose.toast_orders_raw` AS orders,
UNNEST(orders.checks) AS check,
UNNEST(check.selections) AS item
WHERE item.salesCategory.guid IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM `possible-coast-439421-q5.purpose.dim_sales_categories` existing
    WHERE existing.sales_category_guid = item.salesCategory.guid
  );

-- Load dim_employees (servers from checks)
INSERT INTO `possible-coast-439421-q5.purpose.dim_employees`
(employee_key, employee_guid, first_name, last_name, full_name, email, effective_date, expiration_date, is_current)
SELECT
  ROW_NUMBER() OVER (ORDER BY check.server.guid) + COALESCE((SELECT MAX(employee_key) FROM `possible-coast-439421-q5.purpose.dim_employees`), 0) AS employee_key,
  check.server.guid AS employee_guid,
  check.server.firstName AS first_name,
  check.server.lastName AS last_name,
  CONCAT(COALESCE(check.server.firstName, ''), ' ', COALESCE(check.server.lastName, '')) AS full_name,
  check.server.email AS email,
  CURRENT_DATE() AS effective_date,
  DATE('9999-12-31') AS expiration_date,
  TRUE AS is_current
FROM `possible-coast-439421-q5.purpose.toast_orders_raw` AS orders,
UNNEST(orders.checks) AS check
WHERE check.server.guid IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM `possible-coast-439421-q5.purpose.dim_employees` existing
    WHERE existing.employee_guid = check.server.guid
  )
GROUP BY check.server.guid, check.server.firstName, check.server.lastName, check.server.email;

-- ============================================================================
-- STEP 4: LOAD FACT TABLE
-- ============================================================================

INSERT INTO `possible-coast-439421-q5.purpose.fact_order_items`
SELECT
  -- Primary Keys
  selection.guid AS selection_guid,
  orders.guid AS order_guid,
  check.guid AS check_guid,

  -- Foreign Keys
  orders._restaurant_guid AS restaurant_guid,
  selection.itemGuid AS menu_item_guid,
  selection.salesCategory.guid AS sales_category_guid,
  check.server.guid AS server_guid,

  -- Date/Time - Convert integer businessDate (20260201) to DATE
  PARSE_DATE('%Y%m%d', CAST(orders.businessDate AS STRING)) AS business_date,
  orders.openedDate AS opened_datetime,
  check.closedDate AS closed_datetime,
  check.paidDate AS paid_datetime,

  -- Menu Item Details
  selection.itemName AS menu_item_name,
  selection.salesCategory.name AS sales_category_name,

  -- Quantities and Amounts
  selection.quantity AS item_quantity,
  selection.price AS item_price,
  selection.preDiscountPrice AS pre_discount_price,
  (
    SELECT SUM(CAST(discount.discountAmount AS FLOAT64))
    FROM UNNEST(selection.appliedDiscounts) AS discount
  ) AS applied_discount_amount,
  selection.tax AS tax_amount,

  -- Check-level amounts
  check.totalAmount AS check_total_amount,
  check.taxAmount AS check_tax_amount,
  check.tipAmount AS check_tip_amount,

  -- Payment Info (get first payment if available)
  (SELECT amount FROM UNNEST(check.payments) LIMIT 1) AS payment_amount,
  (SELECT type FROM UNNEST(check.payments) LIMIT 1) AS payment_type,
  (SELECT cardType FROM UNNEST(check.payments) LIMIT 1) AS card_type,
  (SELECT tipAmount FROM UNNEST(check.payments) LIMIT 1) AS tip_amount,

  -- Flags
  COALESCE(selection.voided, FALSE) AS is_voided,
  COALESCE(check.deleted, FALSE) AS is_deleted,

  -- Metadata
  CURRENT_TIMESTAMP() AS _loaded_at

FROM `possible-coast-439421-q5.purpose.toast_orders_raw` AS orders,
UNNEST(orders.checks) AS check,
UNNEST(check.selections) AS selection

WHERE
  -- Only non-voided, non-deleted items
  COALESCE(selection.voided, FALSE) = FALSE
  AND COALESCE(check.deleted, FALSE) = FALSE

  -- Dedup: only load new data
  AND NOT EXISTS (
    SELECT 1
    FROM `possible-coast-439421-q5.purpose.fact_order_items` existing
    WHERE existing.selection_guid = selection.guid
      AND existing.order_guid = orders.guid
      AND existing.check_guid = check.guid
  );

-- ============================================================================
-- STEP 5: VERIFICATION QUERIES
-- ============================================================================

-- Verify dimension counts
SELECT 'dim_menu_items' AS table_name, COUNT(*) AS row_count FROM `possible-coast-439421-q5.purpose.dim_menu_items`
UNION ALL
SELECT 'dim_sales_categories', COUNT(*) FROM `possible-coast-439421-q5.purpose.dim_sales_categories`
UNION ALL
SELECT 'dim_employees', COUNT(*) FROM `possible-coast-439421-q5.purpose.dim_employees`
UNION ALL
SELECT 'fact_order_items', COUNT(*) FROM `possible-coast-439421-q5.purpose.fact_order_items`;

-- Verify fact table date range
SELECT
  COUNT(*) AS total_items,
  COUNT(DISTINCT restaurant_guid) AS num_restaurants,
  MIN(business_date) AS earliest_date,
  MAX(business_date) AS latest_date,
  SUM(item_quantity * item_price) AS total_sales
FROM `possible-coast-439421-q5.purpose.fact_order_items`;

-- Sample query: Top 10 menu items by sales
SELECT
  menu_item_name,
  sales_category_name,
  COUNT(*) AS times_ordered,
  SUM(item_quantity) AS total_quantity,
  SUM(item_quantity * item_price) AS total_sales,
  AVG(item_price) AS avg_price
FROM `possible-coast-439421-q5.purpose.fact_order_items`
GROUP BY menu_item_name, sales_category_name
ORDER BY total_sales DESC
LIMIT 10;
