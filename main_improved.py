import functions_framework
import os
import json
import requests
from datetime import datetime, timedelta
from google.cloud import bigquery

# Environment variables
TOAST_CLIENT_ID = os.environ.get('TOAST_CLIENT_ID', '')
TOAST_CLIENT_SECRET = os.environ.get('TOAST_CLIENT_SECRET', '')
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'purpose')
RESTAURANT_GUIDS = [g.strip() for g in os.environ.get('RESTAURANT_GUIDS', '').split(',') if g.strip()]

MAX_PAGES = 1000

def get_toast_token():
    """Generate OAuth token from Toast API using machine client credentials"""
    url = 'https://ws-api.toasttab.com/authentication/v1/authentication/login'

    payload = {
        'clientId': TOAST_CLIENT_ID,
        'clientSecret': TOAST_CLIENT_SECRET,
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

def fetch_orders_for_restaurant(token, guid, start_date, end_date):
    """Fetch all orders for a single restaurant"""
    orders = []
    page = 1

    while page <= MAX_PAGES:
        url = f'https://ws-api.toasttab.com/orders/v2/orders?startDate={start_date}&endDate={end_date}&page={page}&pageSize=100'

        headers = {
            'Authorization': f'Bearer {token}',
            'Toast-Restaurant-External-ID': guid
        }

        try:
            resp = requests.get(url, headers=headers, timeout=30)

            if resp.status_code != 200:
                print(f'Error fetching page {page} for {guid}: {resp.status_code} - {resp.text}')
                break

            data = resp.json()
            page_orders = data.get('data', [])

            if not page_orders:
                print(f'No more orders for {guid} at page {page}')
                break

            # Add metadata to each order
            for order in page_orders:
                order['_loaded_at'] = datetime.utcnow().isoformat()
                order['_restaurant_guid'] = guid
                order['_data_source'] = 'toast_api'

            print(f'Got {len(page_orders)} orders from page {page} for {guid}')
            orders.extend(page_orders)
            page += 1

        except Exception as e:
            print(f'Exception fetching page {page} for {guid}: {str(e)}')
            break

    return orders

@functions_framework.http
def orders_daily(request):
    """
    Fetch orders from Toast API and load into BigQuery

    Query parameters:
    - start_date: YYYY-MM-DD (default: yesterday)
    - end_date: YYYY-MM-DD (default: yesterday)
    - mode: 'test' for dry-run without BigQuery load
    """

    try:
        # Parse request parameters
        request_json = request.get_json(silent=True)
        request_args = request.args

        # Get date range from request or default to yesterday
        if request_json and 'start_date' in request_json:
            start_date = request_json['start_date']
            end_date = request_json.get('end_date', request_json['start_date'])
        elif 'start_date' in request_args:
            start_date = request_args['start_date']
            end_date = request_args.get('end_date', start_date)
        else:
            yesterday = datetime.now() - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            end_date = yesterday.strftime('%Y-%m-%d')

        # Test mode flag
        test_mode = (request_json and request_json.get('mode') == 'test') or request_args.get('mode') == 'test'

        print(f'Starting orders fetch for date range: {start_date} to {end_date}')
        print(f'Test mode: {test_mode}')
        print(f'Restaurants to process: {len(RESTAURANT_GUIDS)}')

        # Get token
        token = get_toast_token()
        if not token:
            return json.dumps({'error': 'Failed to get Toast token'}), 500, {'Content-Type': 'application/json'}

        all_orders = []
        errors = []

        # Fetch orders for each restaurant
        for guid in RESTAURANT_GUIDS:
            if not guid:
                continue

            print(f'Fetching orders for restaurant: {guid}')
            try:
                orders = fetch_orders_for_restaurant(token, guid, start_date, end_date)
                all_orders.extend(orders)
                print(f'Total orders for {guid}: {len(orders)}')
            except Exception as e:
                error_msg = f'Failed to fetch orders for {guid}: {str(e)}'
                print(error_msg)
                errors.append(error_msg)

        print(f'Total orders fetched: {len(all_orders)}')

        if not all_orders:
            result = {
                'status': 'success',
                'message': f'No orders found for date range {start_date} to {end_date}',
                'orders_loaded': 0,
                'errors': errors
            }
            return json.dumps(result), 200, {'Content-Type': 'application/json'}

        # Test mode - don't load to BigQuery
        if test_mode:
            result = {
                'status': 'test',
                'message': 'Test mode - data not loaded to BigQuery',
                'orders_fetched': len(all_orders),
                'date_range': f'{start_date} to {end_date}',
                'sample_order': all_orders[0] if all_orders else None,
                'errors': errors
            }
            return json.dumps(result), 200, {'Content-Type': 'application/json'}

        # Load to BigQuery
        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.toast_orders_raw'

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
            # Schema autodetect on first load, then use existing schema
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        )

        print(f'Loading {len(all_orders)} orders to BigQuery table: {table_id}')
        job = client.load_table_from_json(all_orders, table_id, job_config=job_config)
        job.result()  # Wait for job to complete

        print(f'Successfully loaded {len(all_orders)} orders to BigQuery')

        result = {
            'status': 'success',
            'message': f'Successfully loaded orders for {start_date} to {end_date}',
            'orders_loaded': len(all_orders),
            'date_range': f'{start_date} to {end_date}',
            'table': table_id,
            'errors': errors
        }

        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        error_msg = f'Error: {str(e)}'
        print(error_msg)
        import traceback
        traceback.print_exc()

        result = {
            'status': 'error',
            'message': error_msg
        }
        return json.dumps(result), 500, {'Content-Type': 'application/json'}
