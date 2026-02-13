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
RESTAURANT_GUIDS = [g.strip() for g in os.environ.get('RESTAURANT_GUIDS', '').split(',')]

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

@functions_framework.http
def orders_daily(request):
    """Fetch yesterday's orders from Toast API and load into BigQuery"""
    
    try:
        # Get token
        token = get_toast_token()
        if not token:
            return 'Failed to get Toast token', 500
        
        print(f'Got token, fetching orders for {len(RESTAURANT_GUIDS)} restaurants')
        
        # Calculate yesterday's date range
        yesterday = datetime.now() - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        
        all_orders = []
        
        # Fetch orders for each restaurant
        for guid in RESTAURANT_GUIDS:
            if not guid:
                continue
                
            print(f'Fetching orders for restaurant: {guid}')
            
            page = 1
            while page <= MAX_PAGES:
                url = f'https://ws-api.toasttab.com/orders/v2/orders?startDate={start_date}&endDate={end_date}&page={page}&pageSize=100'
                
                headers = {
                    'Authorization': f'Bearer {token}',
                    'Toast-Restaurant-External-ID': guid
                }
                
                resp = requests.get(url, headers=headers, timeout=30)
                
                if resp.status_code != 200:
                    print(f'Error fetching page {page} for {guid}: {resp.status_code}')
                    break
                
                data = resp.json()
                orders = data.get('data', [])
                
                if not orders:
                    print(f'No more orders for {guid} at page {page}')
                    break
                
                print(f'Got {len(orders)} orders from page {page} for {guid}')
                all_orders.extend(orders)
                page += 1
        
        print(f'Total orders fetched: {len(all_orders)}')
        
        if not all_orders:
            return 'No orders found for yesterday', 200
        
        # Load to BigQuery
        client = bigquery.Client(project=BQ_PROJECT_ID)
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.toast_orders_raw'
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True
        )
        
        job = client.load_table_from_json(all_orders, table_id, job_config=job_config)
        job.result()
        
        print(f'Loaded {len(all_orders)} orders to BigQuery')
        return f'Success: Loaded {len(all_orders)} orders', 200
        
    except Exception as e:
        print(f'Error: {str(e)}')
        return f'Error: {str(e)}', 500
