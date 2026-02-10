#!/usr/bin/env python3
"""
Historical Load Script - Load Toast orders from 2025-01-01 to 2026-02-09
Loads data month by month to avoid timeout issues
"""

import requests
import json
import time
from datetime import datetime

FUNCTION_URL = "https://us-west1-possible-coast-439421-q5.cloudfunctions.net/toast-purpose-bulk"

# Monthly date ranges to load
months = [
    ("2025-01-01", "2025-01-31"),
    ("2025-02-01", "2025-02-28"),
    ("2025-03-01", "2025-03-31"),
    ("2025-04-01", "2025-04-30"),
    ("2025-05-01", "2025-05-31"),
    ("2025-06-01", "2025-06-30"),
    ("2025-07-01", "2025-07-31"),
    ("2025-08-01", "2025-08-31"),
    ("2025-09-01", "2025-09-30"),
    ("2025-10-01", "2025-10-31"),
    ("2025-11-01", "2025-11-30"),
    ("2025-12-01", "2025-12-31"),
    ("2026-01-01", "2026-01-31"),
    ("2026-02-01", "2026-02-09"),
]

print("Starting historical load: 2025-01-01 to 2026-02-09")
print("This will take approximately 17-20 hours due to rate limiting")
print(f"Loading {len(months)} months in batches\n")

total_months = len(months)
total_orders = 0
total_errors = 0

for i, (start_date, end_date) in enumerate(months, 1):
    print(f"[{i}/{total_months}] Loading {start_date} to {end_date}...", end=" ", flush=True)

    try:
        response = requests.post(
            FUNCTION_URL,
            headers={"Content-Type": "application/json"},
            json={"start_date": start_date, "end_date": end_date},
            timeout=600  # 10 minute timeout
        )

        result = response.json()

        if result.get("status") == "success":
            orders_loaded = result.get("orders_loaded", 0)
            duplicates = result.get("duplicates_skipped", 0)
            total_orders += orders_loaded

            print(f"✓ {orders_loaded} new orders, {duplicates} duplicates")

            if result.get("errors"):
                print(f"  ⚠ Errors: {result['errors']}")
                total_errors += len(result['errors'])
        else:
            print(f"✗ Failed: {result.get('message', 'Unknown error')}")
            total_errors += 1

    except Exception as e:
        print(f"✗ Exception: {str(e)}")
        total_errors += 1

    # Small delay between months
    time.sleep(5)

print(f"\n{'='*60}")
print(f"Historical load complete!")
print(f"Total orders loaded: {total_orders:,}")
print(f"Total errors: {total_errors}")
print(f"BigQuery table: possible-coast-439421-q5.purpose.toast_orders_raw")
print(f"{'='*60}")
