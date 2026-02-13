#!/usr/bin/env python3
"""
Historical Load by Restaurant - Load all data for a single restaurant
Run this script 6 times in parallel (one per restaurant) for fastest load
"""

import requests
import json
import sys
import time
from datetime import datetime

FUNCTION_URL = "https://us-west1-possible-coast-439421-q5.cloudfunctions.net/toast-purpose-bulk"

# Restaurant GUIDs (all 13 locations)
RESTAURANTS = {
    '1': {'guid': '6d035dad-924f-47b4-ba93-fd86575e73a3', 'name': 'Location 1'},
    '2': {'guid': '53ae28f1-87c7-4a07-9a43-b619c009b7b0', 'name': 'Location 2'},
    '3': {'guid': 'def5e222-f458-41d0-bff9-48abaf20666a', 'name': 'Location 3'},
    '4': {'guid': '42f246b1-82f1-4048-93c1-d63554c7d9ef', 'name': 'Location 4'},
    '5': {'guid': 'a405942e-179b-4f3f-a75b-a0d18882bd7f', 'name': 'Location 5'},
    '6': {'guid': 'd587bfe9-9faa-48a8-9938-1a23ad36bc9e', 'name': 'Location 6'},
    '7': {'guid': 'da6f0893-d17c-4f93-b7ee-0c708d2611a9', 'name': 'Location 7'},
    '8': {'guid': 'a6a87c64-734e-4f39-90dc-598b5e743105', 'name': 'Location 8'},
    '9': {'guid': 'e629b6e6-85f5-466f-9427-cfbb4f2a6bfe', 'name': 'Location 9'},
    '10': {'guid': '290ca643-8ee4-4d8f-9c70-3793e15ae8a6', 'name': 'Location 10'},
    '11': {'guid': 'eaa7b168-db38-45be-82e8-bd25e6647fd1', 'name': 'Location 11'},
    '12': {'guid': 'a4b4a7a2-0309-4451-8b62-ca0c98858a84', 'name': 'Location 12'},
    '13': {'guid': 'd44d5122-3412-459a-946d-f91a5da03ea3', 'name': 'Location 13'},
}

if len(sys.argv) != 2 or sys.argv[1] not in RESTAURANTS:
    print(f"Usage: python load_by_restaurant.py <restaurant_number>")
    print(f"Restaurant numbers: 1-13")
    print(f"\nAvailable restaurants:")
    for num, info in RESTAURANTS.items():
        print(f"  {num}: {info['name']} - {info['guid']}")
    print(f"\nExample: python load_by_restaurant.py 1")
    sys.exit(1)

restaurant_num = sys.argv[1]
restaurant = RESTAURANTS[restaurant_num]
restaurant_guid = restaurant['guid']
restaurant_name = restaurant['name']

# Load in monthly chunks to balance speed vs timeout risk
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

print(f"="*70)
print(f"Loading historical data for: {restaurant_name}")
print(f"Restaurant GUID: {restaurant_guid}")
print(f"Date range: 2025-01-01 to 2026-02-09")
print(f"Loading {len(months)} months")
print(f"="*70)
print()

total_orders = 0
total_duplicates = 0
total_errors = 0
failed_months = []

for i, (start_date, end_date) in enumerate(months, 1):
    print(f"[{i}/{len(months)}] {start_date} to {end_date}...", end=" ", flush=True)

    try:
        response = requests.post(
            FUNCTION_URL,
            headers={"Content-Type": "application/json"},
            json={
                "start_date": start_date,
                "end_date": end_date,
                "restaurant_guids": [restaurant_guid]  # Single restaurant
            },
            timeout=600
        )

        result = response.json()

        if result.get("status") == "success":
            orders = result.get("orders_loaded", 0)
            dupes = result.get("duplicates_skipped", 0)
            total_orders += orders
            total_duplicates += dupes

            print(f"OK - {orders} new, {dupes} dupes")

            if result.get("errors"):
                print(f"  WARNING: {result['errors']}")
                total_errors += len(result['errors'])
        else:
            msg = result.get('message', 'Unknown error')
            print(f"FAILED - {msg}")
            failed_months.append((start_date, end_date))
            total_errors += 1

    except requests.exceptions.Timeout:
        print(f"TIMEOUT")
        failed_months.append((start_date, end_date))
        total_errors += 1
    except Exception as e:
        print(f"ERROR - {str(e)[:100]}")
        failed_months.append((start_date, end_date))
        total_errors += 1

    time.sleep(2)

print()
print(f"="*70)
print(f"COMPLETE: {restaurant_name}")
print(f"Total new orders: {total_orders:,}")
print(f"Total duplicates: {total_duplicates:,}")
print(f"Total errors: {total_errors}")

if failed_months:
    print(f"\nFailed months ({len(failed_months)}):")
    for start, end in failed_months:
        print(f"  - {start} to {end}")

print(f"="*70)
