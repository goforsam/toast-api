"""
Test script to validate Toast API connection and inspect response structure
Run locally to test before deploying to Cloud Function
"""
import os
import json
import requests
from datetime import datetime, timedelta

# Use environment variables or hardcoded test values
TOAST_CLIENT_ID = os.environ.get('TOAST_CLIENT_ID', '')
TOAST_CLIENT_SECRET = os.environ.get('TOAST_CLIENT_SECRET', '')
TEST_RESTAURANT_GUID = '6d035dad-924f-47b4-ba93-fd86575e73a3'  # First GUID from your list

def get_toast_token():
    """Generate OAuth token from Toast API"""
    url = 'https://ws-api.toasttab.com/authentication/v1/authentication/login'

    payload = {
        'clientId': TOAST_CLIENT_ID,
        'clientSecret': TOAST_CLIENT_SECRET,
        'userAccessType': 'TOAST_MACHINE_CLIENT'
    }

    try:
        print('🔑 Requesting Toast API token...')
        resp = requests.post(url, json=payload, timeout=10)
        print(f'Token response status: {resp.status_code}')

        if resp.status_code == 200:
            token_data = resp.json()
            token = token_data.get('token', {}).get('accessToken')
            print('✅ Token acquired successfully')
            return token
        else:
            print(f'❌ Token error: {resp.text}')
            return None
    except Exception as e:
        print(f'❌ Token exception: {str(e)}')
        return None

def test_orders_fetch(token, start_date, end_date):
    """Test fetching orders from Toast API"""
    url = f'https://ws-api.toasttab.com/orders/v2/orders?startDate={start_date}&endDate={end_date}&page=1&pageSize=5'

    headers = {
        'Authorization': f'Bearer {token}',
        'Toast-Restaurant-External-ID': TEST_RESTAURANT_GUID
    }

    print(f'\n📡 Fetching orders from {start_date} to {end_date}...')
    print(f'Restaurant GUID: {TEST_RESTAURANT_GUID}')

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        print(f'Orders response status: {resp.status_code}')

        if resp.status_code == 200:
            data = resp.json()
            orders = data.get('data', [])
            print(f'\n✅ Successfully fetched {len(orders)} orders')

            if orders:
                print('\n📋 Sample order structure (first order):')
                print(json.dumps(orders[0], indent=2))

                # Analyze schema
                print('\n🔍 Schema Analysis:')
                print(f"Total orders in response: {len(orders)}")
                if orders:
                    first_order = orders[0]
                    print(f"Top-level keys: {list(first_order.keys())}")

                return orders
            else:
                print('⚠️  No orders found for this date range')
                return []
        else:
            print(f'❌ Orders fetch error: {resp.text}')
            return None
    except Exception as e:
        print(f'❌ Orders fetch exception: {str(e)}')
        return None

def main():
    print('='*60)
    print('🧪 Toast API Connection Test')
    print('='*60)

    # Check credentials
    if not TOAST_CLIENT_ID or not TOAST_CLIENT_SECRET:
        print('❌ ERROR: Toast credentials not set!')
        print('Set environment variables: TOAST_CLIENT_ID and TOAST_CLIENT_SECRET')
        return

    print(f'Client ID: {TOAST_CLIENT_ID[:10]}...')
    print(f'Restaurant GUID: {TEST_RESTAURANT_GUID}')

    # Get token
    token = get_toast_token()
    if not token:
        print('\n❌ Failed to acquire token. Check credentials.')
        return

    # Test with yesterday's data
    yesterday = datetime.now() - timedelta(days=1)
    start_date = yesterday.strftime('%Y-%m-%d')
    end_date = yesterday.strftime('%Y-%m-%d')

    print(f'\n📅 Testing with date: {start_date}')
    orders = test_orders_fetch(token, start_date, end_date)

    if orders is None:
        print('\n❌ API test failed')
    elif len(orders) == 0:
        print('\n⚠️  No data found - trying a wider date range...')
        # Try last 7 days
        week_ago = datetime.now() - timedelta(days=7)
        start_date = week_ago.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        print(f'Testing with date range: {start_date} to {end_date}')
        orders = test_orders_fetch(token, start_date, end_date)

    if orders and len(orders) > 0:
        print('\n✅ API CONNECTION TEST PASSED')
        print(f'Successfully retrieved {len(orders)} sample orders')
    else:
        print('\n⚠️  API connection works but no orders found')
        print('This might be normal if there are no orders in the date range')

if __name__ == '__main__':
    main()
