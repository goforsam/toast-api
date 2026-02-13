-- ============================================================================
-- Transform Toast raw orders to Star Schema Dimensional Model
-- Project: possible-coast-439421-q5
-- Source: toast.orders (raw staging)
-- Target: purpose.* (dimensional tables)
-- ============================================================================

-- ============================================================================
-- PART 1: DIMENSION TABLES (SCD Type 1 with effective dating for Type 2)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DimLocation - Restaurant locations
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.DimLocation` (
  LocationKey INT64,  -- Surrogate key
  LocationGuid STRING,  -- Natural key from Toast
  LocationName STRING,
  Address STRING,
  City STRING,
  State STRING,
  ZipCode STRING,
  TimeZone STRING,

  -- SCD Type 2 fields
  EffectiveDate DATE,
  ExpirationDate DATE,
  IsCurrent BOOL,

  -- Metadata
  CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY LocationGuid;

-- ----------------------------------------------------------------------------
-- DimEmployee - Servers and staff (SCD Type 2 for history)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.DimEmployee` (
  EmployeeKey INT64,  -- Surrogate key
  EmployeeGuid STRING,  -- Natural key from Toast
  FirstName STRING,
  LastName STRING,
  FullName STRING,
  Email STRING,
  JobGuid STRING,
  JobTitle STRING,
  LocationGuid STRING,

  -- Employment status
  IsActive BOOL,
  HireDate DATE,
  TerminationDate DATE,

  -- SCD Type 2 fields
  EffectiveDate DATE,
  ExpirationDate DATE,
  IsCurrent BOOL,

  -- Metadata
  CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY EmployeeGuid;

-- ----------------------------------------------------------------------------
-- DimJob - Job titles/roles
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.DimJob` (
  JobKey INT64,
  JobGuid STRING,
  JobTitle STRING,
  JobCode STRING,
  Department STRING,
  IsHourly BOOL,

  -- Metadata
  CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY JobGuid;

-- ----------------------------------------------------------------------------
-- DimMenuItem - Menu items
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.DimMenuItem` (
  MenuItemKey INT64,
  MenuItemGuid STRING,
  MenuItemName STRING,
  Category STRING,
  SubCategory STRING,
  BasePrice FLOAT64,
  IsActive BOOL,

  -- SCD Type 2 for price history
  EffectiveDate DATE,
  ExpirationDate DATE,
  IsCurrent BOOL,

  -- Metadata
  CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  UpdatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY MenuItemGuid;

-- ============================================================================
-- PART 2: FACT TABLES (Granular transaction data)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- FactOrders - One row per order
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactOrders` (
  OrderKey INT64,  -- Surrogate key
  OrderGuid STRING,  -- Natural key from Toast

  -- Foreign keys to dimensions
  LocationKey INT64,
  BusinessDateKey INT64,  -- YYYYMMDD format for date dimension

  -- Order attributes
  OrderNumber STRING,
  ExternalId STRING,
  Source STRING,  -- (POS, Online, Mobile, etc.)

  -- Timestamps
  BusinessDate DATE,
  OpenedDate TIMESTAMP,
  ClosedDate TIMESTAMP,
  ModifiedDate TIMESTAMP,
  PaidDate TIMESTAMP,

  -- Order details
  NumberOfGuests INT64,
  NumberOfChecks INT64,
  DurationMinutes INT64,

  -- Flags
  IsVoided BOOL,
  VoidDate TIMESTAMP,
  ApprovalStatus STRING,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING,
  _DataSource STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, OrderGuid
OPTIONS(
  description='Fact table for Toast orders - one row per order',
  require_partition_filter=true
);

-- ----------------------------------------------------------------------------
-- FactChecks - One row per check (guest check/tab)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactChecks` (
  CheckKey INT64,
  CheckGuid STRING,
  OrderGuid STRING,

  -- Foreign keys
  OrderKey INT64,
  LocationKey INT64,
  ServerKey INT64,  -- Links to DimEmployee
  BusinessDateKey INT64,

  -- Check attributes
  CheckNumber STRING,
  DisplayNumber STRING,

  -- Timestamps
  BusinessDate DATE,
  OpenedDate TIMESTAMP,
  ClosedDate TIMESTAMP,

  -- Amounts
  SubtotalAmount FLOAT64,
  TaxAmount FLOAT64,
  TipAmount FLOAT64,
  DiscountAmount FLOAT64,
  TotalAmount FLOAT64,

  -- Flags
  IsDeleted BOOL,
  DeletedDate TIMESTAMP,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, ServerKey, OrderGuid
OPTIONS(
  description='Fact table for checks - one row per guest check',
  require_partition_filter=true
);

-- ----------------------------------------------------------------------------
-- FactPayments - One row per payment
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactPayments` (
  PaymentKey INT64,
  PaymentGuid STRING,
  CheckGuid STRING,
  OrderGuid STRING,

  -- Foreign keys
  CheckKey INT64,
  OrderKey INT64,
  LocationKey INT64,
  BusinessDateKey INT64,

  -- Payment details
  BusinessDate DATE,
  PaidDate TIMESTAMP,
  PaymentType STRING,  -- (Cash, Credit, Debit, Gift Card, etc.)
  CardType STRING,
  Last4Digits STRING,

  -- Amounts
  Amount FLOAT64,
  TipAmount FLOAT64,
  CashbackAmount FLOAT64,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, PaymentType
OPTIONS(
  description='Fact table for payments - one row per payment transaction',
  require_partition_filter=true
);

-- ----------------------------------------------------------------------------
-- FactMenuSelection - One row per menu item ordered
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactMenuSelection` (
  SelectionKey INT64,
  SelectionGuid STRING,
  CheckGuid STRING,
  OrderGuid STRING,

  -- Foreign keys
  MenuItemKey INT64,
  CheckKey INT64,
  OrderKey INT64,
  LocationKey INT64,
  ServerKey INT64,
  BusinessDateKey INT64,

  -- Item details
  BusinessDate DATE,
  OrderedDate TIMESTAMP,
  MenuItemName STRING,

  -- Pricing
  Quantity FLOAT64,
  UnitPrice FLOAT64,
  PreDiscountPrice FLOAT64,
  DiscountAmount FLOAT64,
  TaxAmount FLOAT64,
  TotalPrice FLOAT64,

  -- Modifiers (stored as JSON array or separate table)
  Modifiers STRING,  -- JSON array of modifiers

  -- Flags
  IsVoided BOOL,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, MenuItemKey, ServerKey
OPTIONS(
  description='Fact table for menu selections - one row per item ordered',
  require_partition_filter=true
);

-- ----------------------------------------------------------------------------
-- FactCashEntries - Cash drawer entries
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactCashEntries` (
  CashEntryKey INT64,
  CashEntryGuid STRING,

  -- Foreign keys
  EmployeeKey INT64,
  LocationKey INT64,
  BusinessDateKey INT64,

  -- Entry details
  BusinessDate DATE,
  EntryDate TIMESTAMP,
  EntryType STRING,  -- (Cash In, Cash Out, Tip Out, Starting Cash, etc.)
  Amount FLOAT64,
  Reason STRING,
  Notes STRING,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, EmployeeKey
OPTIONS(
  description='Fact table for cash drawer entries',
  require_partition_filter=true
);

-- ----------------------------------------------------------------------------
-- FactDeposits - Bank deposits
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactDeposits` (
  DepositKey INT64,
  DepositGuid STRING,

  -- Foreign keys
  LocationKey INT64,
  BusinessDateKey INT64,

  -- Deposit details
  BusinessDate DATE,
  DepositDate TIMESTAMP,
  DepositAmount FLOAT64,
  CashAmount FLOAT64,
  CheckAmount FLOAT64,
  CreditCardAmount FLOAT64,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey;

-- ----------------------------------------------------------------------------
-- FactTimeEntries - Employee time clock entries
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `possible-coast-439421-q5.purpose.FactTimeEntries` (
  TimeEntryKey INT64,
  TimeEntryGuid STRING,

  -- Foreign keys
  EmployeeKey INT64,
  LocationKey INT64,
  JobKey INT64,
  BusinessDateKey INT64,

  -- Time details
  BusinessDate DATE,
  ClockInTime TIMESTAMP,
  ClockOutTime TIMESTAMP,
  HoursWorked FLOAT64,
  RegularHours FLOAT64,
  OvertimeHours FLOAT64,

  -- Pay details
  HourlyRate FLOAT64,
  RegularPay FLOAT64,
  OvertimePay FLOAT64,
  Tips FLOAT64,
  TotalPay FLOAT64,

  -- Metadata
  _LoadedAt TIMESTAMP,
  _LoadId STRING
)
PARTITION BY BusinessDate
CLUSTER BY LocationKey, EmployeeKey
OPTIONS(
  description='Fact table for employee time entries',
  require_partition_filter=true
);

-- ============================================================================
-- Create indexes and optimize
-- ============================================================================

-- Note: BigQuery doesn't have traditional indexes, but uses partitioning and clustering
-- All tables above are already optimized with PARTITION BY and CLUSTER BY
