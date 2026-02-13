"""Configuration constants and BigQuery schemas"""

import os
from google.cloud import bigquery

# Environment configuration
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'purpose')

# API Configuration
MAX_PAGES = 100
REQUEST_TIMEOUT = 90  # seconds for bulk orders
TOKEN_REFRESH_THRESHOLD = 300  # Refresh token if expires in 5 minutes

# Rate limiting (seconds per request per restaurant)
RATE_LIMITS = {
    'orders': 12,   # 5 req/min per location
    'cash': 3,      # 20 req/sec (conservative)
    'labor': 3,     # 20 req/sec
    'menus': 60,    # 1 req/sec STRICT
    'config': 3     # 20 req/sec
}

# Restaurant GUIDs (13 locations)
RESTAURANT_GUIDS = [
    '6d035dad-924f-47b4-ba93-fd86575e73a3',
    '53ae28f1-87c7-4a07-9a43-b619c009b7b0',
    'def5e222-f458-41d0-bff9-48abaf20666a',
    '42f246b1-82f1-4048-93c1-d63554c7d9ef',
    'a405942e-179b-4f3f-a75b-a0d18882bd7f',
    'd587bfe9-9faa-48a8-9938-1a23ad36bc9e',
    'da6f0893-d17c-4f93-b7ee-0c708d2611a9',
    'a6a87c64-734e-4f39-90dc-598b5e743105',
    'e629b6e6-85f5-466f-9427-cfbb4f2a6bfe',
    '290ca643-8ee4-4d8f-9c70-3793e15ae8a6',
    'eaa7b168-db38-45be-82e8-bd25e6647fd1',
    'a4b4a7a2-0309-4451-8b62-ca0c98858a84',
    'd44d5122-3412-459a-946d-f91a5da03ea3'
]

# --- BigQuery Schemas ---

# fact_order_items: 1 row per menu item sold (flattened from orders.checks.selections)
SCHEMA_FACT_ORDER_ITEMS = [
    # Composite dedup key
    bigquery.SchemaField("selection_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("order_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("check_guid", "STRING", mode="REQUIRED"),

    # Dimension keys
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("menu_item_guid", "STRING"),
    bigquery.SchemaField("server_guid", "STRING"),

    # Denormalized attributes (for BI speed)
    bigquery.SchemaField("menu_item_name", "STRING"),
    bigquery.SchemaField("sales_category_name", "STRING"),

    # Item measures
    bigquery.SchemaField("item_quantity", "FLOAT64"),
    bigquery.SchemaField("item_price", "FLOAT64"),
    bigquery.SchemaField("pre_discount_price", "FLOAT64"),
    bigquery.SchemaField("discount_amount", "FLOAT64"),
    bigquery.SchemaField("tax_amount", "FLOAT64"),

    # Check-level measures (denormalized)
    bigquery.SchemaField("check_total", "FLOAT64"),
    bigquery.SchemaField("check_tax", "FLOAT64"),
    bigquery.SchemaField("check_tip", "FLOAT64"),
    bigquery.SchemaField("payment_type", "STRING"),

    # Flags
    bigquery.SchemaField("is_voided", "BOOLEAN"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# fact_cash_entries: 1 row per cash drawer entry
SCHEMA_FACT_CASH_ENTRIES = [
    # Dedup key
    bigquery.SchemaField("cash_entry_guid", "STRING", mode="REQUIRED"),

    # Dimension keys
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("employee_guid", "STRING"),

    # Entry details
    bigquery.SchemaField("entry_type", "STRING"),
    bigquery.SchemaField("amount", "FLOAT64"),
    bigquery.SchemaField("reason", "STRING"),
    bigquery.SchemaField("notes", "STRING"),
    bigquery.SchemaField("cash_drawer_guid", "STRING"),
    bigquery.SchemaField("entry_date", "TIMESTAMP"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# fact_cash_deposits: 1 row per bank deposit
SCHEMA_FACT_CASH_DEPOSITS = [
    # Dedup key
    bigquery.SchemaField("deposit_guid", "STRING", mode="REQUIRED"),

    # Dimension keys
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE", mode="REQUIRED"),

    # Deposit details
    bigquery.SchemaField("deposit_date", "TIMESTAMP"),
    bigquery.SchemaField("deposit_amount", "FLOAT64"),
    bigquery.SchemaField("cash_amount", "FLOAT64"),
    bigquery.SchemaField("check_amount", "FLOAT64"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# fact_labor_shifts: 1 row per employee time entry (clock in/out)
SCHEMA_FACT_LABOR_SHIFTS = [
    # Dedup key
    bigquery.SchemaField("time_entry_guid", "STRING", mode="REQUIRED"),

    # Dimension keys
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("employee_guid", "STRING"),
    bigquery.SchemaField("job_guid", "STRING"),

    # Denormalized attributes
    bigquery.SchemaField("job_title", "STRING"),

    # Shift details
    bigquery.SchemaField("in_date", "TIMESTAMP"),
    bigquery.SchemaField("out_date", "TIMESTAMP"),
    bigquery.SchemaField("regular_hours", "FLOAT64"),
    bigquery.SchemaField("overtime_hours", "FLOAT64"),
    bigquery.SchemaField("hourly_wage", "FLOAT64"),

    # Pay measures
    bigquery.SchemaField("regular_pay", "FLOAT64"),
    bigquery.SchemaField("overtime_pay", "FLOAT64"),
    bigquery.SchemaField("total_pay", "FLOAT64"),
    bigquery.SchemaField("declared_tips", "FLOAT64"),

    # Flags
    bigquery.SchemaField("is_deleted", "BOOLEAN"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# --- Dimension Schemas (full refresh weekly) ---

# dim_restaurants: 1 row per restaurant location
SCHEMA_DIM_RESTAURANTS = [
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_name", "STRING"),
    bigquery.SchemaField("location_name", "STRING"),
    bigquery.SchemaField("address_line1", "STRING"),
    bigquery.SchemaField("address_line2", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("state", "STRING"),
    bigquery.SchemaField("zip_code", "STRING"),
    bigquery.SchemaField("timezone", "STRING"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# dim_employees: 1 row per employee across all restaurants
SCHEMA_DIM_EMPLOYEES = [
    bigquery.SchemaField("employee_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("first_name", "STRING"),
    bigquery.SchemaField("last_name", "STRING"),
    bigquery.SchemaField("email", "STRING"),
    bigquery.SchemaField("external_id", "STRING"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# dim_jobs: 1 row per job role across all restaurants
SCHEMA_DIM_JOBS = [
    bigquery.SchemaField("job_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("job_title", "STRING"),
    bigquery.SchemaField("default_wage", "FLOAT64"),
    bigquery.SchemaField("tipped", "BOOLEAN"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

# dim_menu_items: 1 row per menu item across all restaurants
SCHEMA_DIM_MENU_ITEMS = [
    bigquery.SchemaField("menu_item_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("menu_name", "STRING"),
    bigquery.SchemaField("menu_group_name", "STRING"),
    bigquery.SchemaField("item_name", "STRING"),
    bigquery.SchemaField("price", "FLOAT64"),
    bigquery.SchemaField("sales_category_name", "STRING"),
    bigquery.SchemaField("visibility", "STRING"),
    bigquery.SchemaField("is_deleted", "BOOLEAN"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]
