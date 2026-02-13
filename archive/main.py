"""
Complete Toast API Data Extraction
Loads ALL raw data sources:
- Orders (ordersBulk)
- Cash Entries (cashmgmt)
- Time Entries (labor)
"""
import functions_framework
import os
import json
import requests
import time
from datetime import datetime, timedelta
from google.cloud import bigquery, secretmanager

# Configuration
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'purpose')
MAX_PAGES = 1000

# Restaurant GUIDs
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

def get_secret(secret_id):
    """Retrieve secret from Google Secret Manager"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{BQ_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        print(f"Failed to retrieve secret {secret_id}: {str(e)}")
        return os.environ.get(secret_id, '')

def get_toast_token_with_creds(client_id, client_secret):
    """Generate OAuth token from Toast API"""
    url = 'https://ws-api.toasttab.com/authentication/v1/authentication/login'
    payload = {
        'clientId': client_id,
        'clientSecret': client_secret,
        'userAccessType': 'TOAST_MACHINE_CLIENT'
    }

    try:
        print('Requesting Toast token...')
        resp = requests.post(url, json=payload, timeout=10)
        print(f'Token response: {resp.status_code}')
        if resp.status_code == 200:
            token_data = resp.json()
            return token_data.get('token', {}).get('accessToken')
        else:
            print(f'Token error: {resp.text}')
            return None
    except Exception as e:
        print(f'Token exception: {str(e)}')
        return None

def fetch_orders(token, guid, start_date, end_date):
    """Fetch orders from /orders/v2/ordersBulk"""
    orders = []
    page = 1

    # Convert to ISO timestamp format
    start_datetime = f'{start_date}T00:00:00.000-0000'
    end_datetime = f'{end_date}T23:59:59.999-0000'

    while page <= MAX_PAGES:
        url = f'https://ws-api.toasttab.com/orders/v2/ordersBulk?startDate={start_datetime}&endDate={end_datetime}&page={page}&pageSize=100'

        headers = {
            'Authorization': f'Bearer {token}',
            'Toast-Restaurant-External-ID': guid
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f'Error fetching orders page {page} for {guid}: {resp.status_code}')
                break

            data = resp.json()
            page_orders = data if isinstance(data, list) else data.get('data', [])

            if not page_orders:
                break

            # Add metadata
            for order in page_orders:
                order['_loaded_at'] = datetime.utcnow().isoformat()
                order['_restaurant_guid'] = guid
                order['_data_source'] = 'orders_api'

            orders.extend(page_orders)
            page += 1

        except Exception as e:
            print(f'Exception fetching orders: {str(e)}')
            break

    return orders

def fetch_cash_entries(token, guid, start_date, end_date):
    """Fetch cash entries from /cashmgmt/v1/entries"""
    entries = []
    page = 1

    # Convert to ISO timestamp format
    start_datetime = f'{start_date}T00:00:00.000-0000'
    end_datetime = f'{end_date}T23:59:59.999-0000'

    while page <= MAX_PAGES:
        url = f'https://ws-api.toasttab.com/cashmgmt/v1/entries?startDate={start_datetime}&endDate={end_datetime}&page={page}&pageSize=100'

        headers = {
            'Authorization': f'Bearer {token}',
            'Toast-Restaurant-External-ID': guid
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f'Error fetching cash entries page {page} for {guid}: {resp.status_code}')
                break

            data = resp.json()
            page_entries = data if isinstance(data, list) else data.get('data', [])

            if not page_entries:
                break

            # Add metadata
            for entry in page_entries:
                entry['_loaded_at'] = datetime.utcnow().isoformat()
                entry['_restaurant_guid'] = guid
                entry['_data_source'] = 'cash_api'

            entries.extend(page_entries)
            page += 1

        except Exception as e:
            print(f'Exception fetching cash entries: {str(e)}')
            break

        # Rate limiting
        time.sleep(1)

    return entries

def fetch_time_entries(token, guid, start_date, end_date):
    """Fetch time entries from /labor/v1/timeEntries"""
    entries = []
    page = 1

    # Convert to ISO timestamp format
    start_datetime = f'{start_date}T00:00:00.000-0000'
    end_datetime = f'{end_date}T23:59:59.999-0000'

    while page <= MAX_PAGES:
        url = f'https://ws-api.toasttab.com/labor/v1/timeEntries?startDate={start_datetime}&endDate={end_datetime}&page={page}&pageSize=100'

        headers = {
            'Authorization': f'Bearer {token}',
            'Toast-Restaurant-External-ID': guid
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f'Error fetching time entries page {page} for {guid}: {resp.status_code}')
                break

            data = resp.json()
            page_entries = data if isinstance(data, list) else data.get('data', [])

            if not page_entries:
                break

            # Add metadata
            for entry in page_entries:
                entry['_loaded_at'] = datetime.utcnow().isoformat()
                entry['_restaurant_guid'] = guid
                entry['_data_source'] = 'labor_api'

            entries.extend(page_entries)
            page += 1

        except Exception as e:
            print(f'Exception fetching time entries: {str(e)}')
            break

        # Rate limiting
        time.sleep(1)

    return entries

def load_to_bigquery(data, table_name):
    """Load data to BigQuery with auto-detect schema"""
    if not data:
        return 0

    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}'

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    print(f'Loading {len(data)} records to {table_id}')
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for completion

    return len(data)

@functions_framework.http
def load_all_data(request):
    """
    Load ALL Toast data sources

    Query parameters:
    - start_date: YYYY-MM-DD (default: yesterday)
    - end_date: YYYY-MM-DD (default: yesterday)
    - mode: 'test' for dry-run
    """
    try:
        # Parse dates
        request_json = request.get_json(silent=True)
        request_args = request.args

        if request_json and 'start_date' in request_json:
            start_date = request_json['start_date']
            end_date = request_json.get('end_date', start_date)
        elif 'start_date' in request_args:
            start_date = request_args['start_date']
            end_date = request_args.get('end_date', start_date)
        else:
            yesterday = datetime.now() - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            end_date = yesterday.strftime('%Y-%m-%d')

        test_mode = (request_json and request_json.get('mode') == 'test') or request_args.get('mode') == 'test'

        print(f'Loading ALL Toast data: {start_date} to {end_date}')
        print(f'Restaurants: {len(RESTAURANT_GUIDS)}, Test mode: {test_mode}')

        # Get credentials
        client_id = get_secret('TOAST_CLIENT_ID')
        client_secret = get_secret('TOAST_CLIENT_SECRET')

        if not client_id or not client_secret:
            return json.dumps({'error': 'Failed to retrieve credentials'}), 500

        # Get token
        token = get_toast_token_with_creds(client_id, client_secret)
        if not token:
            return json.dumps({'error': 'Failed to get Toast token'}), 500

        # Collect all data
        all_orders = []
        all_cash_entries = []
        all_time_entries = []
        errors = []

        for guid in RESTAURANT_GUIDS:
            if not guid:
                continue

            print(f'Processing restaurant: {guid}')

            try:
                # Orders
                orders = fetch_orders(token, guid, start_date, end_date)
                all_orders.extend(orders)
                print(f'  Orders: {len(orders)}')

                # Cash entries
                cash_entries = fetch_cash_entries(token, guid, start_date, end_date)
                all_cash_entries.extend(cash_entries)
                print(f'  Cash entries: {len(cash_entries)}')

                # Time entries
                time_entries = fetch_time_entries(token, guid, start_date, end_date)
                all_time_entries.extend(time_entries)
                print(f'  Time entries: {len(time_entries)}')

            except Exception as e:
                error_msg = f'Error processing {guid}: {str(e)}'
                print(error_msg)
                errors.append(error_msg)

        # Summary
        print(f'\nTotals: {len(all_orders)} orders, {len(all_cash_entries)} cash entries, {len(all_time_entries)} time entries')

        if test_mode:
            result = {
                'status': 'test',
                'message': 'Test mode - data not loaded',
                'orders_fetched': len(all_orders),
                'cash_entries_fetched': len(all_cash_entries),
                'time_entries_fetched': len(all_time_entries),
                'errors': errors
            }
            return json.dumps(result), 200

        # Load to BigQuery
        orders_loaded = load_to_bigquery(all_orders, 'toast_orders_raw')
        cash_loaded = load_to_bigquery(all_cash_entries, 'toast_cash_entries_raw')
        labor_loaded = load_to_bigquery(all_time_entries, 'toast_time_entries_raw')

        result = {
            'status': 'success',
            'message': f'Loaded all data for {start_date} to {end_date}',
            'orders_loaded': orders_loaded,
            'cash_entries_loaded': cash_loaded,
            'time_entries_loaded': labor_loaded,
            'errors': errors
        }

        return json.dumps(result), 200

    except Exception as e:
        error_msg = f'Critical error: {str(e)}'
        print(error_msg)
        import traceback
        traceback.print_exc()

        return json.dumps({'status': 'error', 'message': error_msg}), 500
