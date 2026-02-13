"""
Load historical Toast data using daily increments with retry logic
Loads from 2025-01-01 through present
"""
import requests
import time
from datetime import datetime, timedelta
import json

FUNCTION_URL = "https://toast-purpose-bulk-vtpo3hu6ba-uw.a.run.app"
MAX_RETRIES = 3
RETRY_DELAY = 30  # seconds

def load_date_range(start_date, end_date, retry_count=0):
    """Load data for a specific date range with retry logic"""
    try:
        print(f"  Loading {start_date} to {end_date}... ", end='', flush=True)

        response = requests.post(
            FUNCTION_URL,
            json={"start_date": start_date, "end_date": end_date},
            timeout=600  # 10 minutes
        )

        if response.status_code == 200:
            result = response.json()
            orders_loaded = result.get('orders_loaded', 0)
            print(f"Success - {orders_loaded} orders")
            return True
        elif response.status_code == 503 and retry_count < MAX_RETRIES:
            print(f"503 Error - Retry {retry_count + 1}/{MAX_RETRIES}")
            time.sleep(RETRY_DELAY)
            return load_date_range(start_date, end_date, retry_count + 1)
        else:
            print(f"Failed - Status {response.status_code}")
            return False

    except requests.Timeout:
        print("Timeout")
        if retry_count < MAX_RETRIES:
            print(f"  Retrying... ({retry_count + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY)
            return load_date_range(start_date, end_date, retry_count + 1)
        return False
    except Exception as e:
        print(f"Error: {str(e)}")
        return False

def main():
    print("Loading historical Toast data: 2025 to present")
    print("Using daily increments with retry logic\n")

    # Start from Jan 1, 2025
    start_date = datetime(2025, 1, 1)
    # End at yesterday (don't load today since it's incomplete)
    end_date = datetime.now() - timedelta(days=1)

    current_date = start_date
    total_days = (end_date - start_date).days + 1

    success_count = 0
    failure_count = 0

    print(f"Loading {total_days} days of data from {start_date.date()} to {end_date.date()}\n")

    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')

        # Show progress every 7 days
        if current_date.day == 1 or current_date == start_date:
            print(f"\n=== {current_date.strftime('%B %Y')} ===")

        if load_date_range(date_str, date_str):
            success_count += 1
        else:
            failure_count += 1

        current_date += timedelta(days=1)

        # Small delay between requests to avoid rate limiting
        time.sleep(2)

    print("\n" + "="*50)
    print("Historical load complete!")
    print(f"  Success: {success_count} days")
    print(f"  Failed: {failure_count} days")
    print("="*50)

    print("\nVerifying data in BigQuery...")
    print("  Run: python check_data_status.py")

if __name__ == '__main__':
    main()
