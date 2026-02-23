"""Configuration constants and BigQuery schemas"""

import os
from google.cloud import bigquery
from .clients import CLIENTS

# Active client is selected via CLIENT_NAME env var; all other config derived from shared/clients.py
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
_CLIENT_NAME = os.environ.get('CLIENT_NAME', 'purpose')
_client = CLIENTS[_CLIENT_NAME]
BQ_DATASET_ID = _client['bq_dataset_id']
SECRET_SUFFIX = _client['secret_suffix']

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

# Restaurant GUIDs sourced from shared/clients.py (single source of truth)
RESTAURANT_GUIDS = _client['restaurant_guids']

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
