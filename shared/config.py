"""Configuration constants and BigQuery schemas"""

import os
from google.cloud import bigquery

# Environment configuration
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'purpose')

# API Configuration
MAX_PAGES = 100  # Reasonable limit for pagination
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

# Hardcoded restaurant GUIDs
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

# BigQuery Schema for fact_order_items (denormalized for BI performance)
SCHEMA_FACT_ORDER_ITEMS = [
    # Composite key
    bigquery.SchemaField("selection_guid", "STRING", mode="REQUIRED"),  # Primary dedup key
    bigquery.SchemaField("order_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("check_guid", "STRING", mode="REQUIRED"),

    # Dimension keys
    bigquery.SchemaField("restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("business_date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("menu_item_guid", "STRING"),
    bigquery.SchemaField("sales_category_guid", "STRING"),

    # Denormalized dimension attributes (for BI speed)
    bigquery.SchemaField("menu_item_name", "STRING"),
    bigquery.SchemaField("sales_category_name", "STRING"),

    # Measures
    bigquery.SchemaField("quantity", "FLOAT64"),
    bigquery.SchemaField("unit_price", "FLOAT64"),
    bigquery.SchemaField("total_price", "FLOAT64"),
    bigquery.SchemaField("tax_amount", "FLOAT64"),
    bigquery.SchemaField("discount_amount", "FLOAT64"),

    # Attributes
    bigquery.SchemaField("is_voided", "BOOLEAN"),
    bigquery.SchemaField("voided_date", "TIMESTAMP"),
    bigquery.SchemaField("modifiers", "JSON"),  # Array of modifier details

    # Timestamps
    bigquery.SchemaField("created_date", "TIMESTAMP"),
    bigquery.SchemaField("modified_date", "TIMESTAMP"),

    # Metadata (standard across all fact tables)
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_data_source", "STRING", mode="REQUIRED"),
]

# Legacy schema for orders (kept for backward compatibility during migration)
BQ_SCHEMA = [
    # Required fields
    bigquery.SchemaField("guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurantGuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("businessDate", "DATE", mode="REQUIRED"),

    # Basic fields
    bigquery.SchemaField("entityType", "STRING"),
    bigquery.SchemaField("externalId", "STRING"),
    bigquery.SchemaField("displayNumber", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("approvalStatus", "STRING"),
    bigquery.SchemaField("numberOfGuests", "INTEGER"),
    bigquery.SchemaField("duration", "INTEGER"),

    # Boolean fields
    bigquery.SchemaField("voided", "BOOLEAN"),
    bigquery.SchemaField("deleted", "BOOLEAN"),
    bigquery.SchemaField("excessFood", "BOOLEAN"),
    bigquery.SchemaField("createdInTestMode", "BOOLEAN"),

    # Timestamp fields
    bigquery.SchemaField("openedDate", "TIMESTAMP"),
    bigquery.SchemaField("closedDate", "TIMESTAMP"),
    bigquery.SchemaField("modifiedDate", "TIMESTAMP"),
    bigquery.SchemaField("paidDate", "TIMESTAMP"),
    bigquery.SchemaField("voidDate", "TIMESTAMP"),
    bigquery.SchemaField("deletedDate", "TIMESTAMP"),
    bigquery.SchemaField("createdDate", "TIMESTAMP"),
    bigquery.SchemaField("promisedDate", "TIMESTAMP"),
    bigquery.SchemaField("estimatedFulfillmentDate", "TIMESTAMP"),
    bigquery.SchemaField("voidBusinessDate", "DATE"),

    # String fields for simple values
    bigquery.SchemaField("requiredPrepTime", "STRING"),
    bigquery.SchemaField("channelGuid", "STRING"),

    # Complex objects stored as JSON strings
    bigquery.SchemaField("checks", "JSON"),
    bigquery.SchemaField("revenueCenter", "JSON"),
    bigquery.SchemaField("restaurantService", "JSON"),
    bigquery.SchemaField("createdDevice", "JSON"),
    bigquery.SchemaField("lastModifiedDevice", "JSON"),
    bigquery.SchemaField("pricingFeatures", "JSON"),
    bigquery.SchemaField("server", "JSON"),
    bigquery.SchemaField("table", "JSON"),
    bigquery.SchemaField("deliveryInfo", "JSON"),
    bigquery.SchemaField("serviceArea", "JSON"),
    bigquery.SchemaField("curbsidePickupInfo", "JSON"),
    bigquery.SchemaField("appliedPackagingInfo", "JSON"),
    bigquery.SchemaField("diningOption", "JSON"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_data_source", "STRING", mode="REQUIRED"),
]
