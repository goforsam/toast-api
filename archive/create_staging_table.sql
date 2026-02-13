-- Create staging table for raw Toast orders JSON
-- Project: possible-coast-439421-q5
-- Dataset: toast (staging area)

CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.toast.orders` (
  -- Raw JSON from Toast API
  raw_json STRING,

  -- Extracted key fields for partitioning/filtering
  order_guid STRING,
  restaurant_guid STRING,
  business_date DATE,
  opened_date TIMESTAMP,
  closed_date TIMESTAMP,

  -- Metadata
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _load_id STRING,
  _data_source STRING DEFAULT 'toast_api'
)
PARTITION BY business_date
CLUSTER BY restaurant_guid, order_guid
OPTIONS(
  description='Raw staging table for Toast orders - JSON from API',
  require_partition_filter=true
);

-- Alternative: If you want to store structured data instead of JSON string
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.toast.orders_structured` (
  -- All Toast API fields as structured columns
  guid STRING,
  entityType STRING,
  externalId STRING,
  restaurantGuid STRING,
  restaurantService STRUCT<
    guid STRING,
    entityType STRING
  >,

  -- Timestamps
  businessDate DATE,
  openedDate TIMESTAMP,
  closedDate TIMESTAMP,
  modifiedDate TIMESTAMP,
  promisedDate TIMESTAMP,

  -- Order details
  source STRING,
  duration INT64,
  numberOfGuests INT64,
  voided BOOL,
  voidDate TIMESTAMP,
  paidDate TIMESTAMP,

  -- Amounts
  estimatedFulfillmentDate TIMESTAMP,
  approvalStatus STRING,

  -- Arrays
  checks ARRAY<STRUCT<
    guid STRING,
    entityType STRING,
    externalId STRING,
    displayNumber STRING,
    openedDate TIMESTAMP,
    closedDate TIMESTAMP,
    deletedDate TIMESTAMP,
    deleted BOOL,

    -- Check totals
    totalAmount FLOAT64,
    taxAmount FLOAT64,
    tipAmount FLOAT64,

    -- Employee info
    server STRUCT<
      guid STRING,
      entityType STRING,
      firstName STRING,
      lastName STRING,
      email STRING
    >,

    -- Payments
    payments ARRAY<STRUCT<
      guid STRING,
      paidDate TIMESTAMP,
      amount FLOAT64,
      tipAmount FLOAT64,
      type STRING,
      cardType STRING,
      last4Digits STRING
    >>,

    -- Selections (menu items)
    selections ARRAY<STRUCT<
      guid STRING,
      itemGuid STRING,
      itemName STRING,
      preDiscountPrice FLOAT64,
      price FLOAT64,
      tax FLOAT64,
      quantity FLOAT64,
      modifiers ARRAY<STRUCT<
        modifierGuid STRING,
        modifierName STRING,
        price FLOAT64
      >>
    >>
  >>,

  -- Metadata
  _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  _restaurant_guid STRING,
  _data_source STRING DEFAULT 'toast_api'
)
PARTITION BY businessDate
CLUSTER BY restaurantGuid, guid
OPTIONS(
  description='Structured staging table for Toast orders - parsed from JSON',
  require_partition_filter=true
);

-- Create a view for easy JSON parsing if using JSON storage approach
CREATE OR REPLACE VIEW `possible-coast-439421-q5.toast.orders_parsed` AS
SELECT
  order_guid,
  restaurant_guid,
  business_date,
  opened_date,
  closed_date,
  JSON_EXTRACT_SCALAR(raw_json, '$.guid') as guid,
  JSON_EXTRACT_SCALAR(raw_json, '$.entityType') as entity_type,
  JSON_EXTRACT_SCALAR(raw_json, '$.source') as source,
  CAST(JSON_EXTRACT_SCALAR(raw_json, '$.numberOfGuests') AS INT64) as number_of_guests,
  CAST(JSON_EXTRACT_SCALAR(raw_json, '$.voided') AS BOOL) as voided,
  JSON_EXTRACT_ARRAY(raw_json, '$.checks') as checks,
  _loaded_at,
  _load_id,
  _data_source
FROM `possible-coast-439421-q5.toast.orders`;
