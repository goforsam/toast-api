"""
Simple test script for single day Toast API extraction
"""
import os
import json
import sys
import logging
from datetime import datetime, timedelta

# Add shared module to path
sys.path.insert(0, os.path.dirname(__file__))

from shared.toast_client import ToastAPIClient
from shared.config import BQ_PROJECT_ID, BQ_DATASET_ID, RESTAURANT_GUIDS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def test_single_day():
    """Test fetching orders for a single day (yesterday)"""

    print("=" * 60)
    print("Toast API Single Day Test")
    print("=" * 60)

    # Get credentials from environment
    client_id = os.environ.get('TOAST_CLIENT_ID', '')
    client_secret = os.environ.get('TOAST_CLIENT_SECRET', '')

    if not client_id or not client_secret:
        print("\n[ERROR] Missing credentials!")
        print("Set environment variables:")
        print("  - TOAST_CLIENT_ID")
        print("  - TOAST_CLIENT_SECRET")
        return False

    print(f"\nProject ID: {BQ_PROJECT_ID}")
    print(f"Dataset ID: {BQ_DATASET_ID}")
    print(f"Number of restaurants: {len(RESTAURANT_GUIDS)}")
    print(f"Client ID: {client_id[:10]}...")

    # Get yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    test_date = yesterday.strftime('%Y-%m-%d')

    print(f"\nTest Date: {test_date}")
    print(f"Test Restaurant: {RESTAURANT_GUIDS[0]}")

    # Initialize Toast client
    try:
        toast_client = ToastAPIClient(client_id, client_secret)
        print("\n[OK] Toast client initialized")
    except Exception as e:
        print(f"\n[ERROR] Failed to initialize Toast client: {str(e)}")
        return False

    # Get authentication token
    try:
        token = toast_client.get_token()
        if token:
            print("[OK] Authentication successful")
            print(f"     Token: {token[:20]}...")
        else:
            print("[ERROR] Failed to get authentication token")
            return False
    except Exception as e:
        print(f"[ERROR] Authentication failed: {str(e)}")
        return False

    # Fetch orders for first restaurant only (test)
    try:
        test_guid = RESTAURANT_GUIDS[0]
        print(f"\n[INFO] Fetching orders for restaurant {test_guid}")
        print(f"       Date range: {test_date} to {test_date}")

        orders, errors = toast_client.fetch_orders(
            test_guid,
            test_date,
            test_date
        )

        if errors:
            print(f"\n[WARNING] Errors encountered:")
            for error in errors:
                print(f"  - {error}")

        if orders:
            print(f"\n[SUCCESS] Retrieved {len(orders)} orders")

            # Show sample order structure
            if len(orders) > 0:
                sample = orders[0]
                print(f"\nSample Order:")
                print(f"  - Order GUID: {sample.get('guid', 'N/A')}")
                print(f"  - Restaurant: {sample.get('restaurantGuid', 'N/A')}")
                print(f"  - Business Date: {sample.get('businessDate', 'N/A')}")
                print(f"  - Opened: {sample.get('openedDate', 'N/A')}")
                print(f"  - Closed: {sample.get('closedDate', 'N/A')}")
                print(f"  - Number of checks: {len(sample.get('checks', []))}")

                print(f"\nTop-level keys ({len(sample.keys())}):")
                for key in sorted(sample.keys()):
                    print(f"  - {key}")

            return True
        else:
            print(f"\n[INFO] No orders found for {test_date}")
            print("       This might be normal if no orders exist for this date")

            # Try a wider date range
            print("\n[INFO] Trying last 7 days to verify API connectivity...")
            week_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

            orders, errors = toast_client.fetch_orders(
                test_guid,
                week_ago,
                test_date
            )

            if orders:
                print(f"[SUCCESS] Found {len(orders)} orders in last 7 days")
                print("         API connection is working properly")
                return True
            else:
                print("[WARNING] No orders found in last 7 days either")
                return False

    except Exception as e:
        print(f"\n[ERROR] Failed to fetch orders: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_single_day()

    print("\n" + "=" * 60)
    if success:
        print("TEST PASSED - API connection and data retrieval working")
        print("\nNext steps:")
        print("  1. Deploy to Cloud Function: ./deploy.sh")
        print("  2. Load historical data: ./historical_load.sh")
        print("  3. Run ETL transformations: bq query < etl_load_*.sql")
    else:
        print("TEST FAILED - Check errors above")
        print("\nTroubleshooting:")
        print("  1. Verify environment variables are set")
        print("  2. Check Toast API credentials")
        print("  3. Verify restaurant GUIDs are correct")
    print("=" * 60)

    sys.exit(0 if success else 1)
