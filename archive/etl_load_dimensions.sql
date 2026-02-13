-- ============================================================================
-- ETL: Load Dimensions from Toast Staging to Star Schema
-- Run this daily after new orders are loaded to toast.orders
-- Project: possible-coast-439421-q5
-- ============================================================================

DECLARE load_id STRING DEFAULT GENERATE_UUID();
DECLARE load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

-- ============================================================================
-- STEP 1: Load DimLocation (SCD Type 1 - overwrite changes)
-- ============================================================================

MERGE `possible-coast-439421-q5.purpose.DimLocation` AS target
USING (
  SELECT DISTINCT
    JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid') AS LocationGuid,
    -- Note: Location details may need to come from a separate Toast Location API call
    -- For now, using GUID as placeholder
    JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid') AS LocationName,
    NULL AS Address,
    NULL AS City,
    NULL AS State,
    NULL AS ZipCode,
    'America/Los_Angeles' AS TimeZone,  -- Default, should come from API
    CURRENT_DATE() AS EffectiveDate,
    DATE('9999-12-31') AS ExpirationDate,
    TRUE AS IsCurrent
  FROM `possible-coast-439421-q5.toast.orders`
  WHERE _loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)  -- Incremental
    AND JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid') IS NOT NULL
) AS source
ON target.LocationGuid = source.LocationGuid AND target.IsCurrent = TRUE
WHEN MATCHED THEN
  UPDATE SET
    UpdatedAt = load_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    LocationKey,
    LocationGuid,
    LocationName,
    Address,
    City,
    State,
    ZipCode,
    TimeZone,
    EffectiveDate,
    ExpirationDate,
    IsCurrent,
    CreatedAt,
    UpdatedAt
  )
  VALUES (
    FARM_FINGERPRINT(source.LocationGuid),  -- Generate surrogate key
    source.LocationGuid,
    source.LocationName,
    source.Address,
    source.City,
    source.State,
    source.ZipCode,
    source.TimeZone,
    source.EffectiveDate,
    source.ExpirationDate,
    source.IsCurrent,
    load_timestamp,
    load_timestamp
  );

-- ============================================================================
-- STEP 2: Load DimEmployee (SCD Type 2 - maintain history)
-- ============================================================================

-- Extract unique employees from checks
CREATE TEMP TABLE temp_employees AS
SELECT DISTINCT
  JSON_EXTRACT_SCALAR(check, '$.server.guid') AS EmployeeGuid,
  JSON_EXTRACT_SCALAR(check, '$.server.firstName') AS FirstName,
  JSON_EXTRACT_SCALAR(check, '$.server.lastName') AS LastName,
  CONCAT(
    JSON_EXTRACT_SCALAR(check, '$.server.firstName'),
    ' ',
    JSON_EXTRACT_SCALAR(check, '$.server.lastName')
  ) AS FullName,
  JSON_EXTRACT_SCALAR(check, '$.server.email') AS Email,
  JSON_EXTRACT_SCALAR(raw_json, '$.restaurantGuid') AS LocationGuid,
  TRUE AS IsActive,
  NULL AS HireDate,
  NULL AS TerminationDate
FROM `possible-coast-439421-q5.toast.orders`,
  UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS check
WHERE _loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND JSON_EXTRACT_SCALAR(check, '$.server.guid') IS NOT NULL;

-- Close out old records (SCD Type 2)
UPDATE `possible-coast-439421-q5.purpose.DimEmployee` AS target
SET
  ExpirationDate = CURRENT_DATE(),
  IsCurrent = FALSE,
  UpdatedAt = load_timestamp
WHERE IsCurrent = TRUE
  AND EmployeeGuid IN (
    SELECT EmployeeGuid
    FROM temp_employees
    WHERE EmployeeGuid IS NOT NULL
  )
  AND (
    -- Detect changes that warrant new version
    FullName != (SELECT FullName FROM temp_employees t WHERE t.EmployeeGuid = target.EmployeeGuid LIMIT 1)
    OR Email != (SELECT Email FROM temp_employees t WHERE t.EmployeeGuid = target.EmployeeGuid LIMIT 1)
  );

-- Insert new/updated records
MERGE `possible-coast-439421-q5.purpose.DimEmployee` AS target
USING temp_employees AS source
ON target.EmployeeGuid = source.EmployeeGuid AND target.IsCurrent = TRUE
WHEN NOT MATCHED THEN
  INSERT (
    EmployeeKey,
    EmployeeGuid,
    FirstName,
    LastName,
    FullName,
    Email,
    LocationGuid,
    IsActive,
    HireDate,
    TerminationDate,
    EffectiveDate,
    ExpirationDate,
    IsCurrent,
    CreatedAt,
    UpdatedAt
  )
  VALUES (
    FARM_FINGERPRINT(CONCAT(source.EmployeeGuid, CAST(CURRENT_DATE() AS STRING))),
    source.EmployeeGuid,
    source.FirstName,
    source.LastName,
    source.FullName,
    source.Email,
    source.LocationGuid,
    source.IsActive,
    source.HireDate,
    source.TerminationDate,
    CURRENT_DATE(),
    DATE('9999-12-31'),
    TRUE,
    load_timestamp,
    load_timestamp
  );

-- ============================================================================
-- STEP 3: Load DimMenuItem
-- ============================================================================

CREATE TEMP TABLE temp_menu_items AS
SELECT DISTINCT
  JSON_EXTRACT_SCALAR(selection, '$.guid') AS MenuItemGuid,
  JSON_EXTRACT_SCALAR(selection, '$.name') AS MenuItemName,
  CAST(JSON_EXTRACT_SCALAR(selection, '$.preDiscountPrice') AS FLOAT64) AS BasePrice,
  TRUE AS IsActive,
  NULL AS Category,
  NULL AS SubCategory
FROM `possible-coast-439421-q5.toast.orders`,
  UNNEST(JSON_EXTRACT_ARRAY(raw_json, '$.checks')) AS check,
  UNNEST(JSON_EXTRACT_ARRAY(check, '$.selections')) AS selection
WHERE _loaded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
  AND JSON_EXTRACT_SCALAR(selection, '$.guid') IS NOT NULL;

-- SCD Type 2 for menu items (track price changes)
UPDATE `possible-coast-439421-q5.purpose.DimMenuItem` AS target
SET
  ExpirationDate = CURRENT_DATE(),
  IsCurrent = FALSE,
  UpdatedAt = load_timestamp
WHERE IsCurrent = TRUE
  AND MenuItemGuid IN (SELECT MenuItemGuid FROM temp_menu_items)
  AND BasePrice != (
    SELECT BasePrice
    FROM temp_menu_items t
    WHERE t.MenuItemGuid = target.MenuItemGuid
    LIMIT 1
  );

MERGE `possible-coast-439421-q5.purpose.DimMenuItem` AS target
USING temp_menu_items AS source
ON target.MenuItemGuid = source.MenuItemGuid AND target.IsCurrent = TRUE
WHEN NOT MATCHED THEN
  INSERT (
    MenuItemKey,
    MenuItemGuid,
    MenuItemName,
    Category,
    SubCategory,
    BasePrice,
    IsActive,
    EffectiveDate,
    ExpirationDate,
    IsCurrent,
    CreatedAt,
    UpdatedAt
  )
  VALUES (
    FARM_FINGERPRINT(CONCAT(source.MenuItemGuid, CAST(CURRENT_DATE() AS STRING))),
    source.MenuItemGuid,
    source.MenuItemName,
    source.Category,
    source.SubCategory,
    source.BasePrice,
    source.IsActive,
    CURRENT_DATE(),
    DATE('9999-12-31'),
    TRUE,
    load_timestamp,
    load_timestamp
  );

-- ============================================================================
-- Cleanup temp tables
-- ============================================================================
DROP TABLE temp_employees;
DROP TABLE temp_menu_items;

-- ============================================================================
-- Log completion
-- ============================================================================
SELECT
  load_id AS LoadId,
  load_timestamp AS LoadTimestamp,
  'Dimensions loaded successfully' AS Status;
