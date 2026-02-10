"""
Historical backfill script for Toast orders ETL

Loads historical order data in weekly chunks to avoid Cloud Function timeouts.
Date range: 2025-01-01 to 2026-02-09 (60+ weeks)
"""

import requests
import json
from datetime import datetime, timedelta
import time

# Cloud Function URL
FUNCTION_URL = "https://toast-orders-etl-vtpo3hu6ba-uw.a.run.app"

# Date range
START_DATE = "2025-01-01"
END_DATE = "2026-02-09"

def load_week(start_date: str, end_date: str, week_num: int):
    """
    Load one week of historical orders

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        week_num: Week number for tracking

    Returns:
        dict with results or None if error
    """
    print(f"\n[Week {week_num}] {start_date} to {end_date}")

    payload = {
        "start_date": start_date,
        "end_date": end_date
    }

    try:
        # Call Cloud Function (timeout: 10 minutes for safety)
        response = requests.post(
            FUNCTION_URL,
            json=payload,
            timeout=600
        )

        if response.status_code == 200:
            result = response.json()

            # Print summary
            print(f"  ✓ Orders: {result.get('orders_fetched', 0)}, "
                  f"Items loaded: {result.get('items_loaded', 0)}, "
                  f"Duplicates: {result.get('duplicates_skipped', 0)}, "
                  f"Errors: {len(result.get('errors', []))}")

            if result.get('errors'):
                print(f"  ⚠ Errors: {result['errors']}")

            return result
        else:
            print(f"  ✗ HTTP {response.status_code}: {response.text}")
            return None

    except requests.exceptions.Timeout:
        print(f"  ✗ TIMEOUT (>10 minutes)")
        return None
    except Exception as e:
        print(f"  ✗ ERROR: {str(e)}")
        return None


def run_backfill():
    """
    Run historical backfill in weekly chunks
    """
    print("=" * 70)
    print("Toast Orders Historical Backfill")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print("Chunk size: 7 days (weekly)")
    print("=" * 70)

    current = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")

    week_num = 1
    total_orders = 0
    total_items = 0
    total_duplicates = 0
    total_errors = 0
    failed_weeks = []

    start_time = time.time()

    while current <= end:
        # Calculate week end (7-day window)
        chunk_end = min(current + timedelta(days=6), end)

        # Load this week
        result = load_week(
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d"),
            week_num
        )

        if result:
            total_orders += result.get('orders_fetched', 0)
            total_items += result.get('items_loaded', 0)
            total_duplicates += result.get('duplicates_skipped', 0)
            total_errors += len(result.get('errors', []))
        else:
            failed_weeks.append(f"{current.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")

        # Move to next week
        current = chunk_end + timedelta(days=1)
        week_num += 1

        # Rate limiting: small delay between weeks
        time.sleep(2)

    # Final summary
    elapsed_time = time.time() - start_time
    elapsed_minutes = elapsed_time / 60

    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE")
    print("=" * 70)
    print(f"Total weeks processed: {week_num - 1}")
    print(f"Total orders fetched: {total_orders:,}")
    print(f"Total items loaded: {total_items:,}")
    print(f"Total duplicates: {total_duplicates:,}")
    print(f"Total errors: {total_errors}")
    print(f"Elapsed time: {elapsed_minutes:.1f} minutes")

    if failed_weeks:
        print(f"\n⚠ Failed weeks ({len(failed_weeks)}):")
        for week in failed_weeks:
            print(f"  - {week}")
    else:
        print("\n✓ All weeks completed successfully!")

    print("=" * 70)


if __name__ == "__main__":
    run_backfill()
