-- ============================================================================
-- ETL: Load Fact Tables from Toast Staging
-- Run this daily after dimensions are loaded
-- Project: possible-coast-439421-q5
-- ============================================================================

DECLARE load_id STRING DEFAULT GENERATE_UUID();
DECLARE load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
DECLARE process_date DATE DEFAULT CURRENT_DATE() - 1;  -- Yesterday's data

-- ============================================================================
-- STEP 1: Load FactOrders
-- ============================================================================

MERGE `possible-coast-439421-q5.purpose.FactOrders` AS target
USING (
  SELECT
    JSON_EXTRACT_SCALAR(raw_json, '$.guid') AS OrderGuid,
    JSON_EXTRACT_SCALAR(raw_json, '$.externalId') AS ExternalId,
    JSON_EXTRACT_SCALAR(raw_json, '$.entityType') AS OrderNumber,
    JSON_EXTRACT_SCALAR(raw_json, '$.source') AS Source,

    -- Dimension keys
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid')) AS LocationKey,
    CAST(FORMAT_DATE('%Y%m%d', PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate'))) AS INT64) AS BusinessDateKey,

    -- Dates and times
    PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate')) AS BusinessDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(raw_json, '$.openedDate')) AS OpenedDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(raw_json, '$.closedDate')) AS ClosedDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(raw_json, '$.modifiedDate')) AS ModifiedDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(raw_json, '$.paidDate')) AS PaidDate,

    -- Order details
    CAST(JSON_EXTRACT_SCALAR(raw_json, '$.numberOfGuests') AS INT64) AS NumberOfGuests,
    ARRAY_LENGTH(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS NumberOfChecks,
    CAST(JSON_EXTRACT_SCALAR(raw_json, '$.duration') AS INT64) / 60 AS DurationMinutes,

    -- Flags
    CAST(JSON_EXTRACT_SCALAR(raw_json, '$.voided') AS BOOL) AS IsVoided,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(raw_json, '$.voidDate')) AS VoidDate,
    JSON_EXTRACT_SCALAR(raw_json, '$.approvalStatus') AS ApprovalStatus,

    -- Metadata
    _loaded_at AS _LoadedAt,
    load_id AS _LoadId,
    'toast_api' AS _DataSource

  FROM `possible-coast-439421-q5.toast.orders`
  WHERE business_date >= process_date
    AND JSON_EXTRACT_SCALAR(raw_json, '$.guid') IS NOT NULL
) AS source
ON target.OrderGuid = source.OrderGuid
WHEN NOT MATCHED THEN
  INSERT (
    OrderKey,
    OrderGuid,
    LocationKey,
    BusinessDateKey,
    OrderNumber,
    ExternalId,
    Source,
    BusinessDate,
    OpenedDate,
    ClosedDate,
    ModifiedDate,
    PaidDate,
    NumberOfGuests,
    NumberOfChecks,
    DurationMinutes,
    IsVoided,
    VoidDate,
    ApprovalStatus,
    _LoadedAt,
    _LoadId,
    _DataSource
  )
  VALUES (
    FARM_FINGERPRINT(source.OrderGuid),
    source.OrderGuid,
    source.LocationKey,
    source.BusinessDateKey,
    source.OrderNumber,
    source.ExternalId,
    source.Source,
    source.BusinessDate,
    source.OpenedDate,
    source.ClosedDate,
    source.ModifiedDate,
    source.PaidDate,
    source.NumberOfGuests,
    source.NumberOfChecks,
    source.DurationMinutes,
    source.IsVoided,
    source.VoidDate,
    source.ApprovalStatus,
    source._LoadedAt,
    source._LoadId,
    source._DataSource
  );

-- ============================================================================
-- STEP 2: Load FactChecks
-- ============================================================================

MERGE `possible-coast-439421-q5.purpose.FactChecks` AS target
USING (
  SELECT
    JSON_EXTRACT_SCALAR(check, '$.guid') AS CheckGuid,
    JSON_EXTRACT_SCALAR(raw_json, '$.guid') AS OrderGuid,
    JSON_EXTRACT_SCALAR(check, '$.displayNumber') AS DisplayNumber,
    JSON_EXTRACT_SCALAR(check, '$.externalId') AS CheckNumber,

    -- Foreign keys
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.guid')) AS OrderKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid')) AS LocationKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(check, '$.server.guid')) AS ServerKey,
    CAST(FORMAT_DATE('%Y%m%d', PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate'))) AS INT64) AS BusinessDateKey,

    -- Dates
    PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate')) AS BusinessDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(check, '$.openedDate')) AS OpenedDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(check, '$.closedDate')) AS ClosedDate,

    -- Amounts (calculate from selections and payments)
    CAST(JSON_EXTRACT_SCALAR(check, '$.amount') AS FLOAT64) AS SubtotalAmount,
    CAST(JSON_EXTRACT_SCALAR(check, '$.taxAmount') AS FLOAT64) AS TaxAmount,
    CAST(JSON_EXTRACT_SCALAR(check, '$.tipAmount') AS FLOAT64) AS TipAmount,
    CAST(JSON_EXTRACT_SCALAR(check, '$.discountAmount') AS FLOAT64) AS DiscountAmount,
    CAST(JSON_EXTRACT_SCALAR(check, '$.totalAmount') AS FLOAT64) AS TotalAmount,

    -- Flags
    CAST(JSON_EXTRACT_SCALAR(check, '$.deleted') AS BOOL) AS IsDeleted,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(check, '$.deletedDate')) AS DeletedDate,

    -- Metadata
    _loaded_at AS _LoadedAt,
    load_id AS _LoadId

  FROM `possible-coast-439421-q5.toast.orders`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS check
  WHERE business_date >= process_date
    AND JSON_EXTRACT_SCALAR(check, '$.guid') IS NOT NULL
) AS source
ON target.CheckGuid = source.CheckGuid
WHEN NOT MATCHED THEN
  INSERT (
    CheckKey,
    CheckGuid,
    OrderGuid,
    OrderKey,
    LocationKey,
    ServerKey,
    BusinessDateKey,
    CheckNumber,
    DisplayNumber,
    BusinessDate,
    OpenedDate,
    ClosedDate,
    SubtotalAmount,
    TaxAmount,
    TipAmount,
    DiscountAmount,
    TotalAmount,
    IsDeleted,
    DeletedDate,
    _LoadedAt,
    _LoadId
  )
  VALUES (
    FARM_FINGERPRINT(source.CheckGuid),
    source.CheckGuid,
    source.OrderGuid,
    source.OrderKey,
    source.LocationKey,
    source.ServerKey,
    source.BusinessDateKey,
    source.CheckNumber,
    source.DisplayNumber,
    source.BusinessDate,
    source.OpenedDate,
    source.ClosedDate,
    source.SubtotalAmount,
    source.TaxAmount,
    source.TipAmount,
    source.DiscountAmount,
    source.TotalAmount,
    source.IsDeleted,
    source.DeletedDate,
    source._LoadedAt,
    source._LoadId
  );

-- ============================================================================
-- STEP 3: Load FactPayments
-- ============================================================================

MERGE `possible-coast-439421-q5.purpose.FactPayments` AS target
USING (
  SELECT
    JSON_EXTRACT_SCALAR(payment, '$.guid') AS PaymentGuid,
    JSON_EXTRACT_SCALAR(check, '$.guid') AS CheckGuid,
    JSON_EXTRACT_SCALAR(raw_json, '$.guid') AS OrderGuid,

    -- Foreign keys
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(check, '$.guid')) AS CheckKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.guid')) AS OrderKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid')) AS LocationKey,
    CAST(FORMAT_DATE('%Y%m%d', PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate'))) AS INT64) AS BusinessDateKey,

    -- Payment details
    PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate')) AS BusinessDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(payment, '$.paidDate')) AS PaidDate,
    JSON_EXTRACT_SCALAR(payment, '$.type') AS PaymentType,
    JSON_EXTRACT_SCALAR(payment, '$.cardType') AS CardType,
    JSON_EXTRACT_SCALAR(payment, '$.last4Digits') AS Last4Digits,

    -- Amounts
    CAST(JSON_EXTRACT_SCALAR(payment, '$.amount') AS FLOAT64) AS Amount,
    CAST(JSON_EXTRACT_SCALAR(payment, '$.tipAmount') AS FLOAT64) AS TipAmount,
    CAST(JSON_EXTRACT_SCALAR(payment, '$.cashbackAmount') AS FLOAT64) AS CashbackAmount,

    -- Metadata
    _loaded_at AS _LoadedAt,
    load_id AS _LoadId

  FROM `possible-coast-439421-q5.toast.orders`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS check,
    UNNEST(JSON_EXTRACT_ARRAY(check, '$.payments')) AS payment
  WHERE business_date >= process_date
    AND JSON_EXTRACT_SCALAR(payment, '$.guid') IS NOT NULL
) AS source
ON target.PaymentGuid = source.PaymentGuid
WHEN NOT MATCHED THEN
  INSERT (
    PaymentKey,
    PaymentGuid,
    CheckGuid,
    OrderGuid,
    CheckKey,
    OrderKey,
    LocationKey,
    BusinessDateKey,
    BusinessDate,
    PaidDate,
    PaymentType,
    CardType,
    Last4Digits,
    Amount,
    TipAmount,
    CashbackAmount,
    _LoadedAt,
    _LoadId
  )
  VALUES (
    FARM_FINGERPRINT(source.PaymentGuid),
    source.PaymentGuid,
    source.CheckGuid,
    source.OrderGuid,
    source.CheckKey,
    source.OrderKey,
    source.LocationKey,
    source.BusinessDateKey,
    source.BusinessDate,
    source.PaidDate,
    source.PaymentType,
    source.CardType,
    source.Last4Digits,
    source.Amount,
    source.TipAmount,
    source.CashbackAmount,
    source._LoadedAt,
    source._LoadId
  );

-- ============================================================================
-- STEP 4: Load FactMenuSelection
-- ============================================================================

MERGE `possible-coast-439421-q5.purpose.FactMenuSelection` AS target
USING (
  SELECT
    JSON_EXTRACT_SCALAR(selection, '$.guid') AS SelectionGuid,
    JSON_EXTRACT_SCALAR(check, '$.guid') AS CheckGuid,
    JSON_EXTRACT_SCALAR(raw_json, '$.guid') AS OrderGuid,

    -- Foreign keys
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(selection, '$.itemGuid')) AS MenuItemKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(check, '$.guid')) AS CheckKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.guid')) AS OrderKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid')) AS LocationKey,
    FARM_FINGERPRINT(JSON_EXTRACT_SCALAR(check, '$.server.guid')) AS ServerKey,
    CAST(FORMAT_DATE('%Y%m%d', PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate'))) AS INT64) AS BusinessDateKey,

    -- Selection details
    PARSE_DATE('%Y-%m-%d', JSON_EXTRACT_SCALAR(raw_json, '$.businessDate')) AS BusinessDate,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', JSON_EXTRACT_SCALAR(check, '$.openedDate')) AS OrderedDate,
    JSON_EXTRACT_SCALAR(selection, '$.name') AS MenuItemName,

    -- Pricing
    CAST(JSON_EXTRACT_SCALAR(selection, '$.quantity') AS FLOAT64) AS Quantity,
    CAST(JSON_EXTRACT_SCALAR(selection, '$.price') AS FLOAT64) AS UnitPrice,
    CAST(JSON_EXTRACT_SCALAR(selection, '$.preDiscountPrice') AS FLOAT64) AS PreDiscountPrice,
    CAST(JSON_EXTRACT_SCALAR(selection, '$.preDiscountPrice') AS FLOAT64) -
      CAST(JSON_EXTRACT_SCALAR(selection, '$.price') AS FLOAT64) AS DiscountAmount,
    CAST(JSON_EXTRACT_SCALAR(selection, '$.tax') AS FLOAT64) AS TaxAmount,
    CAST(JSON_EXTRACT_SCALAR(selection, '$.price') AS FLOAT64) *
      CAST(JSON_EXTRACT_SCALAR(selection, '$.quantity') AS FLOAT64) AS TotalPrice,

    -- Modifiers (stored as JSON)
    JSON_EXTRACT(selection, '$.modifiers') AS Modifiers,

    -- Flags
    CAST(JSON_EXTRACT_SCALAR(selection, '$.voided') AS BOOL) AS IsVoided,

    -- Metadata
    _loaded_at AS _LoadedAt,
    load_id AS _LoadId

  FROM `possible-coast-439421-q5.toast.orders`,
    UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS check,
    UNNEST(JSON_EXTRACT_ARRAY(check, '$.selections')) AS selection
  WHERE business_date >= process_date
    AND JSON_EXTRACT_SCALAR(selection, '$.guid') IS NOT NULL
) AS source
ON target.SelectionGuid = source.SelectionGuid
WHEN NOT MATCHED THEN
  INSERT (
    SelectionKey,
    SelectionGuid,
    CheckGuid,
    OrderGuid,
    MenuItemKey,
    CheckKey,
    OrderKey,
    LocationKey,
    ServerKey,
    BusinessDateKey,
    BusinessDate,
    OrderedDate,
    MenuItemName,
    Quantity,
    UnitPrice,
    PreDiscountPrice,
    DiscountAmount,
    TaxAmount,
    TotalPrice,
    Modifiers,
    IsVoided,
    _LoadedAt,
    _LoadId
  )
  VALUES (
    FARM_FINGERPRINT(source.SelectionGuid),
    source.SelectionGuid,
    source.CheckGuid,
    source.OrderGuid,
    source.MenuItemKey,
    source.CheckKey,
    source.OrderKey,
    source.LocationKey,
    source.ServerKey,
    source.BusinessDateKey,
    source.BusinessDate,
    source.OrderedDate,
    source.MenuItemName,
    source.Quantity,
    source.UnitPrice,
    source.PreDiscountPrice,
    source.DiscountAmount,
    source.TaxAmount,
    source.TotalPrice,
    source.Modifiers,
    source.IsVoided,
    source._LoadedAt,
    source._LoadId
  );

-- ============================================================================
-- Log completion
-- ============================================================================
SELECT
  load_id AS LoadId,
  load_timestamp AS LoadTimestamp,
  process_date AS ProcessDate,
  (SELECT COUNT(*) FROM `possible-coast-439421-q5.purpose.FactOrders` WHERE BusinessDate = process_date) AS OrdersLoaded,
  (SELECT COUNT(*) FROM `possible-coast-439421-q5.purpose.FactChecks` WHERE BusinessDate = process_date) AS ChecksLoaded,
  (SELECT COUNT(*) FROM `possible-coast-439421-q5.purpose.FactPayments` WHERE BusinessDate = process_date) AS PaymentsLoaded,
  (SELECT COUNT(*) FROM `possible-coast-439421-q5.purpose.FactMenuSelection` WHERE BusinessDate = process_date) AS SelectionsLoaded,
  'Facts loaded successfully' AS Status;
