-- ============================================================================
-- Power BI Analytical Queries
-- Optimized views for Power BI dashboards
-- Project: possible-coast-439421-q5
-- ============================================================================

-- ============================================================================
-- VIEW 1: Daily Sales Summary by Location
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_DailySalesByLocation` AS
SELECT
  o.BusinessDate,
  l.LocationName,
  l.LocationGuid,

  -- Order metrics
  COUNT(DISTINCT o.OrderGuid) AS TotalOrders,
  SUM(o.NumberOfGuests) AS TotalGuests,
  AVG(o.NumberOfGuests) AS AvgGuestsPerOrder,

  -- Revenue metrics
  SUM(c.TotalAmount) AS TotalRevenue,
  SUM(c.SubtotalAmount) AS TotalSubtotal,
  SUM(c.TaxAmount) AS TotalTax,
  SUM(c.TipAmount) AS TotalTips,
  SUM(c.DiscountAmount) AS TotalDiscounts,

  -- Averages
  AVG(c.TotalAmount) AS AvgCheckAmount,
  AVG(c.TipAmount) AS AvgTipAmount,
  SAFE_DIVIDE(SUM(c.TipAmount), SUM(c.SubtotalAmount)) * 100 AS TipPercentage,

  -- Counts
  COUNT(DISTINCT c.CheckGuid) AS TotalChecks,
  COUNT(DISTINCT c.ServerKey) AS UniqueServers

FROM `possible-coast-439421-q5.purpose.FactOrders` o
JOIN `possible-coast-439421-q5.purpose.FactChecks` c ON o.OrderKey = c.OrderKey
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON o.LocationKey = l.LocationKey
WHERE o.IsVoided = FALSE
  AND c.IsDeleted = FALSE
GROUP BY 1, 2, 3;

-- ============================================================================
-- VIEW 2: Server Performance (Employee Analysis)
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_ServerPerformance` AS
SELECT
  c.BusinessDate,
  e.FullName AS ServerName,
  e.EmployeeGuid,
  l.LocationName,

  -- Shift metrics
  COUNT(DISTINCT c.CheckGuid) AS ChecksServed,
  SUM(c.TotalAmount) AS TotalSales,
  SUM(c.TipAmount) AS TotalTips,
  SUM(c.SubtotalAmount) AS TotalSubtotal,

  -- Averages
  AVG(c.TotalAmount) AS AvgCheckSize,
  AVG(c.TipAmount) AS AvgTipAmount,
  SAFE_DIVIDE(SUM(c.TipAmount), SUM(c.SubtotalAmount)) * 100 AS TipPercentage,

  -- Guest metrics
  SUM(o.NumberOfGuests) AS TotalGuestsServed,
  AVG(o.NumberOfGuests) AS AvgPartySize,

  -- Efficiency
  AVG(o.DurationMinutes) AS AvgTableTurnTime,

  -- Rankings (for Power BI)
  RANK() OVER (PARTITION BY c.BusinessDate, l.LocationName ORDER BY SUM(c.TotalAmount) DESC) AS SalesRank,
  RANK() OVER (PARTITION BY c.BusinessDate, l.LocationName ORDER BY SUM(c.TipAmount) DESC) AS TipsRank

FROM `possible-coast-439421-q5.purpose.FactChecks` c
JOIN `possible-coast-439421-q5.purpose.FactOrders` o ON c.OrderKey = o.OrderKey
JOIN `possible-coast-439421-q5.purpose.DimEmployee` e ON c.ServerKey = e.EmployeeKey AND e.IsCurrent = TRUE
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON c.LocationKey = l.LocationKey
WHERE c.IsDeleted = FALSE
  AND o.IsVoided = FALSE
GROUP BY 1, 2, 3, 4;

-- ============================================================================
-- VIEW 3: Menu Item Performance
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_MenuItemPerformance` AS
SELECT
  s.BusinessDate,
  l.LocationName,
  m.MenuItemName,
  m.Category,
  m.SubCategory,

  -- Sales metrics
  COUNT(*) AS ItemsSold,
  SUM(s.Quantity) AS TotalQuantity,
  SUM(s.TotalPrice) AS TotalRevenue,
  SUM(s.TaxAmount) AS TotalTax,
  SUM(s.DiscountAmount) AS TotalDiscounts,

  -- Averages
  AVG(s.UnitPrice) AS AvgPrice,
  AVG(s.Quantity) AS AvgQuantityPerOrder,

  -- Percentage of total sales
  SAFE_DIVIDE(
    SUM(s.TotalPrice),
    SUM(SUM(s.TotalPrice)) OVER (PARTITION BY s.BusinessDate, l.LocationName)
  ) * 100 AS PercentOfLocationSales,

  -- Rankings
  RANK() OVER (
    PARTITION BY s.BusinessDate, l.LocationName
    ORDER BY SUM(s.TotalPrice) DESC
  ) AS RevenueRank,
  RANK() OVER (
    PARTITION BY s.BusinessDate, l.LocationName
    ORDER BY COUNT(*) DESC
  ) AS PopularityRank

FROM `possible-coast-439421-q5.purpose.FactMenuSelection` s
JOIN `possible-coast-439421-q5.purpose.DimMenuItem` m ON s.MenuItemKey = m.MenuItemKey AND m.IsCurrent = TRUE
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON s.LocationKey = l.LocationKey
WHERE s.IsVoided = FALSE
GROUP BY 1, 2, 3, 4, 5;

-- ============================================================================
-- VIEW 4: Payment Type Analysis
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_PaymentTypeAnalysis` AS
SELECT
  p.BusinessDate,
  l.LocationName,
  p.PaymentType,
  p.CardType,

  -- Payment metrics
  COUNT(*) AS TransactionCount,
  SUM(p.Amount) AS TotalAmount,
  SUM(p.TipAmount) AS TotalTips,
  AVG(p.Amount) AS AvgTransactionAmount,
  AVG(p.TipAmount) AS AvgTipAmount,

  -- Percentage of total payments
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (PARTITION BY p.BusinessDate, l.LocationName)
  ) * 100 AS PercentOfTransactions,
  SAFE_DIVIDE(
    SUM(p.Amount),
    SUM(SUM(p.Amount)) OVER (PARTITION BY p.BusinessDate, l.LocationName)
  ) * 100 AS PercentOfRevenue

FROM `possible-coast-439421-q5.purpose.FactPayments` p
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON p.LocationKey = l.LocationKey
GROUP BY 1, 2, 3, 4;

-- ============================================================================
-- VIEW 5: Hourly Sales Trend (Daypart Analysis)
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_HourlySalesTrend` AS
SELECT
  c.BusinessDate,
  l.LocationName,
  EXTRACT(HOUR FROM c.OpenedDate) AS Hour,
  EXTRACT(DAYOFWEEK FROM c.BusinessDate) AS DayOfWeek,
  FORMAT_DATE('%A', c.BusinessDate) AS DayName,

  -- Categorize into dayparts
  CASE
    WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 6 AND 10 THEN 'Breakfast'
    WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 11 AND 14 THEN 'Lunch'
    WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 15 AND 16 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 17 AND 21 THEN 'Dinner'
    WHEN EXTRACT(HOUR FROM c.OpenedDate) >= 22 OR EXTRACT(HOUR FROM c.OpenedDate) < 6 THEN 'Late Night'
    ELSE 'Other'
  END AS Daypart,

  -- Metrics
  COUNT(DISTINCT c.CheckGuid) AS CheckCount,
  SUM(c.TotalAmount) AS TotalRevenue,
  AVG(c.TotalAmount) AS AvgCheckAmount,
  SUM(o.NumberOfGuests) AS TotalGuests

FROM `possible-coast-439421-q5.purpose.FactChecks` c
JOIN `possible-coast-439421-q5.purpose.FactOrders` o ON c.OrderKey = o.OrderKey
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON c.LocationKey = l.LocationKey
WHERE c.IsDeleted = FALSE
  AND o.IsVoided = FALSE
GROUP BY 1, 2, 3, 4, 5, 6;

-- ============================================================================
-- VIEW 6: Labor & Sales (Server Productivity)
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_LaborProductivity` AS
SELECT
  t.BusinessDate,
  l.LocationName,
  e.FullName AS EmployeeName,
  j.JobTitle,

  -- Labor metrics
  SUM(t.HoursWorked) AS HoursWorked,
  SUM(t.RegularHours) AS RegularHours,
  SUM(t.OvertimeHours) AS OvertimeHours,
  SUM(t.TotalPay) AS LaborCost,
  SUM(t.Tips) AS TipsEarned,

  -- Sales metrics (from checks served)
  COALESCE(SUM(c.TotalAmount), 0) AS SalesGenerated,
  COALESCE(COUNT(DISTINCT c.CheckGuid), 0) AS ChecksServed,

  -- Productivity ratios
  SAFE_DIVIDE(COALESCE(SUM(c.TotalAmount), 0), SUM(t.HoursWorked)) AS SalesPerHour,
  SAFE_DIVIDE(SUM(t.TotalPay), COALESCE(SUM(c.TotalAmount), 1)) * 100 AS LaborCostPercentage

FROM `possible-coast-439421-q5.purpose.FactTimeEntries` t
JOIN `possible-coast-439421-q5.purpose.DimEmployee` e ON t.EmployeeKey = e.EmployeeKey AND e.IsCurrent = TRUE
JOIN `possible-coast-439421-q5.purpose.DimJob` j ON t.JobKey = j.JobKey
JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON t.LocationKey = l.LocationKey
LEFT JOIN `possible-coast-439421-q5.purpose.FactChecks` c
  ON c.ServerKey = t.EmployeeKey
  AND c.BusinessDate = t.BusinessDate
  AND c.IsDeleted = FALSE
GROUP BY 1, 2, 3, 4;

-- ============================================================================
-- VIEW 7: Period-over-Period Comparison
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_PeriodComparison` AS
WITH daily_metrics AS (
  SELECT
    c.BusinessDate,
    l.LocationName,
    SUM(c.TotalAmount) AS DailyRevenue,
    COUNT(DISTINCT c.CheckGuid) AS DailyChecks,
    AVG(c.TotalAmount) AS AvgCheckAmount
  FROM `possible-coast-439421-q5.purpose.FactChecks` c
  JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON c.LocationKey = l.LocationKey
  WHERE c.IsDeleted = FALSE
  GROUP BY 1, 2
)
SELECT
  BusinessDate,
  LocationName,
  DailyRevenue,
  DailyChecks,
  AvgCheckAmount,

  -- Prior day comparison
  LAG(DailyRevenue, 1) OVER (PARTITION BY LocationName ORDER BY BusinessDate) AS PriorDayRevenue,
  DailyRevenue - LAG(DailyRevenue, 1) OVER (PARTITION BY LocationName ORDER BY BusinessDate) AS DayOverDayChange,
  SAFE_DIVIDE(
    DailyRevenue - LAG(DailyRevenue, 1) OVER (PARTITION BY LocationName ORDER BY BusinessDate),
    LAG(DailyRevenue, 1) OVER (PARTITION BY LocationName ORDER BY BusinessDate)
  ) * 100 AS DayOverDayPct,

  -- Prior week comparison (7 days ago)
  LAG(DailyRevenue, 7) OVER (PARTITION BY LocationName ORDER BY BusinessDate) AS PriorWeekRevenue,
  DailyRevenue - LAG(DailyRevenue, 7) OVER (PARTITION BY LocationName ORDER BY BusinessDate) AS WeekOverWeekChange,
  SAFE_DIVIDE(
    DailyRevenue - LAG(DailyRevenue, 7) OVER (PARTITION BY LocationName ORDER BY BusinessDate),
    LAG(DailyRevenue, 7) OVER (PARTITION BY LocationName ORDER BY BusinessDate)
  ) * 100 AS WeekOverWeekPct,

  -- Rolling averages
  AVG(DailyRevenue) OVER (
    PARTITION BY LocationName
    ORDER BY BusinessDate
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS Rolling7DayAvg,
  AVG(DailyRevenue) OVER (
    PARTITION BY LocationName
    ORDER BY BusinessDate
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS Rolling30DayAvg

FROM daily_metrics;

-- ============================================================================
-- Sample Power BI DAX-style query (in SQL)
-- "What's my best performing server by location and shift?"
-- ============================================================================
CREATE OR REPLACE VIEW `possible-coast-439421-q5.purpose.vw_TopServersByShift` AS
WITH server_shift_performance AS (
  SELECT
    c.BusinessDate,
    l.LocationName,
    e.FullName AS ServerName,
    CASE
      WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 6 AND 14 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 15 AND 21 THEN 'Evening'
      ELSE 'Night'
    END AS Shift,
    SUM(c.TotalAmount) AS TotalSales,
    SUM(c.TipAmount) AS TotalTips,
    COUNT(DISTINCT c.CheckGuid) AS ChecksServed,
    RANK() OVER (
      PARTITION BY c.BusinessDate, l.LocationName,
      CASE
        WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 6 AND 14 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM c.OpenedDate) BETWEEN 15 AND 21 THEN 'Evening'
        ELSE 'Night'
      END
      ORDER BY SUM(c.TotalAmount) DESC
    ) AS PerformanceRank
  FROM `possible-coast-439421-q5.purpose.FactChecks` c
  JOIN `possible-coast-439421-q5.purpose.DimEmployee` e ON c.ServerKey = e.EmployeeKey AND e.IsCurrent = TRUE
  JOIN `possible-coast-439421-q5.purpose.DimLocation` l ON c.LocationKey = l.LocationKey
  WHERE c.IsDeleted = FALSE
  GROUP BY 1, 2, 3, 4
)
SELECT *
FROM server_shift_performance
WHERE PerformanceRank <= 3  -- Top 3 servers per shift
ORDER BY BusinessDate DESC, LocationName, Shift, PerformanceRank;
