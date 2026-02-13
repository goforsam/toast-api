"""
Test Toast API with specific date range to find data
"""
import os
import sys
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(__file__))

from shared.toast_client import ToastAPIClient
from shared.config import RESTAURANT_GUIDS

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def test_date_range(client_id, client_secret, start_date, end_date, restaurant_idx=0):
    """Test a specific date range"""

    print(f"\nTesting: {start_date} to {end_date}")
    print(f"Restaurant: {RESTAURANT_GUIDS[restaurant_idx]}")

    toast_client = ToastAPIClient(client_id, client_secret)

    orders, errors = toast_client.fetch_orders(
        RESTAURANT_GUIDS[restaurant_idx],
        start_date,
        end_date
    )

    if errors:
        print(f"Errors: {errors}")

    if orders:
        print(f"SUCCESS: Found {len(orders)} orders!")
        sample = orders[0]
        print(f"\nSample order:")
        print(f"  GUID: {sample.get('guid')}")
        print(f"  Business Date: {sample.get('businessDate')}")
        print(f"  Opened: {sample.get('openedDate')}")
        print(f"  Restaurant: {sample.get('restaurantGuid')}")
        return True, len(orders)
    else:
        print(f"No orders found")
        return False, 0

def main():
    client_id = os.environ.get('TOAST_CLIENT_ID', '')
    client_secret = os.environ.get('TOAST_CLIENT_SECRET', '')

    if not client_id or not client_secret:
        print("ERROR: Credentials not set")
        return

    print("=" * 70)
    print("Testing different date ranges to find data...")
    print("=" * 70)

    # Test different date ranges
    test_ranges = [
        ("2026-02-01", "2026-02-09"),  # This month
        ("2026-01-15", "2026-01-31"),  # Late January
        ("2026-01-01", "2026-01-14"),  # Early January
        ("2025-12-15", "2025-12-31"),  # December 2025
    ]

    total_orders = 0
    for start, end in test_ranges:
        success, count = test_date_range(client_id, client_secret, start, end)
        total_orders += count
        if success:
            print(f"\n*** FOUND DATA: {count} orders from {start} to {end} ***")
            break
        print("")

    if total_orders == 0:
        print("\n" + "=" * 70)
        print("No orders found in any date range.")
        print("Trying different restaurants...")
        print("=" * 70)

        # Try first 3 restaurants with recent date
        for i in range(min(3, len(RESTAURANT_GUIDS))):
            print(f"\n--- Restaurant {i+1}/{len(RESTAURANT_GUIDS)}: {RESTAURANT_GUIDS[i]} ---")
            success, count = test_date_range(
                client_id, client_secret,
                "2026-01-01", "2026-02-09",
                restaurant_idx=i
            )
            total_orders += count
            if success:
                print(f"\n*** FOUND DATA at restaurant {i+1} ***")
                break

    print("\n" + "=" * 70)
    if total_orders > 0:
        print(f"TEST PASSED: API working, found {total_orders} orders")
    else:
        print("TEST INCONCLUSIVE: API works but no orders found")
        print("\nPossible reasons:")
        print("  - Restaurants may not have recent order data")
        print("  - Need to check with Toast support about data availability")
        print("  - Production deployment may have different behavior")
    print("=" * 70)

if __name__ == '__main__':
    main()
