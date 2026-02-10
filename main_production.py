import functions_framework
import os
import json
import requests
import time
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery, secretmanager
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BQ_PROJECT_ID = os.environ.get('BQ_PROJECT_ID', 'possible-coast-439421-q5')
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'purpose')
MAX_PAGES = 100  # Reasonable limit
RATE_LIMIT_DELAY = 12  # seconds (5 requests/minute = 1 per 12 seconds)
REQUEST_TIMEOUT = 90  # seconds for bulk orders
TOKEN_REFRESH_THRESHOLD = 300  # Refresh token if expires in 5 minutes

# Hardcoded restaurant GUIDs
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

# Define explicit BigQuery schema - store complex objects as JSON strings
BQ_SCHEMA = [
    # Required fields
    bigquery.SchemaField("guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("restaurantGuid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("businessDate", "DATE", mode="REQUIRED"),

    # Basic fields
    bigquery.SchemaField("entityType", "STRING"),
    bigquery.SchemaField("externalId", "STRING"),
    bigquery.SchemaField("displayNumber", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("approvalStatus", "STRING"),
    bigquery.SchemaField("numberOfGuests", "INTEGER"),
    bigquery.SchemaField("duration", "INTEGER"),

    # Boolean fields
    bigquery.SchemaField("voided", "BOOLEAN"),
    bigquery.SchemaField("deleted", "BOOLEAN"),
    bigquery.SchemaField("excessFood", "BOOLEAN"),
    bigquery.SchemaField("createdInTestMode", "BOOLEAN"),

    # Timestamp fields
    bigquery.SchemaField("openedDate", "TIMESTAMP"),
    bigquery.SchemaField("closedDate", "TIMESTAMP"),
    bigquery.SchemaField("modifiedDate", "TIMESTAMP"),
    bigquery.SchemaField("paidDate", "TIMESTAMP"),
    bigquery.SchemaField("voidDate", "TIMESTAMP"),
    bigquery.SchemaField("deletedDate", "TIMESTAMP"),
    bigquery.SchemaField("createdDate", "TIMESTAMP"),
    bigquery.SchemaField("promisedDate", "TIMESTAMP"),
    bigquery.SchemaField("estimatedFulfillmentDate", "TIMESTAMP"),
    bigquery.SchemaField("voidBusinessDate", "DATE"),

    # String fields for simple values
    bigquery.SchemaField("requiredPrepTime", "STRING"),
    bigquery.SchemaField("channelGuid", "STRING"),

    # Complex objects stored as JSON strings
    bigquery.SchemaField("checks", "JSON"),
    bigquery.SchemaField("revenueCenter", "JSON"),
    bigquery.SchemaField("restaurantService", "JSON"),
    bigquery.SchemaField("createdDevice", "JSON"),
    bigquery.SchemaField("lastModifiedDevice", "JSON"),
    bigquery.SchemaField("pricingFeatures", "JSON"),
    bigquery.SchemaField("server", "JSON"),
    bigquery.SchemaField("table", "JSON"),
    bigquery.SchemaField("deliveryInfo", "JSON"),
    bigquery.SchemaField("serviceArea", "JSON"),
    bigquery.SchemaField("curbsidePickupInfo", "JSON"),
    bigquery.SchemaField("appliedPackagingInfo", "JSON"),
    bigquery.SchemaField("diningOption", "JSON"),

    # Metadata
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("_restaurant_guid", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_data_source", "STRING", mode="REQUIRED"),
]


def get_secret(secret_id: str) -> str:
    """
    Retrieve secret from Google Secret Manager

    Args:
        secret_id: Secret name (e.g., 'TOAST_CLIENT_ID')

    Returns:
        Secret value as string
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{BQ_PROJECT_ID}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode('UTF-8')
    except Exception as e:
        logger.error(f"Failed to retrieve secret {secret_id}: {str(e)}")
        # Fallback to env vars for development
        return os.environ.get(secret_id, '')


def create_http_session() -> requests.Session:
    """
    Create requests session with retry logic

    Returns:
        Configured requests Session
    """
    session = requests.Session()

    # Retry on network errors and 5xx server errors
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,  # 2, 4, 8 seconds
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


class ToastAPIClient:
    """Toast API client with token management and rate limiting"""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = None
        self.session = create_http_session()
        self.last_request_time = {}  # Track per-restaurant rate limiting

    def get_token(self) -> Optional[str]:
        """
        Get or refresh OAuth token

        Returns:
            Access token or None if failed
        """
        # Check if token is still valid
        if self.token and self.token_expiry:
            if datetime.now() < (self.token_expiry - timedelta(seconds=TOKEN_REFRESH_THRESHOLD)):
                return self.token

        # Request new token
        url = 'https://ws-api.toasttab.com/authentication/v1/authentication/login'
        payload = {
            'clientId': self.client_id,
            'clientSecret': self.client_secret,
            'userAccessType': 'TOAST_MACHINE_CLIENT'
        }

        try:
            logger.info("Requesting Toast API token")
            resp = self.session.post(url, json=payload, timeout=10)
            resp.raise_for_status()

            token_data = resp.json()
            self.token = token_data.get('token', {}).get('accessToken')

            # Toast tokens typically expire in 1 hour
            self.token_expiry = datetime.now() + timedelta(hours=1)

            logger.info("Successfully acquired Toast API token")
            return self.token

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get Toast token: {str(e)}")
            return None

    def _apply_rate_limit(self, restaurant_guid: str):
        """Apply rate limiting per restaurant (5 req/min)"""
        last_request = self.last_request_time.get(restaurant_guid, 0)
        elapsed = time.time() - last_request

        if elapsed < RATE_LIMIT_DELAY:
            sleep_time = RATE_LIMIT_DELAY - elapsed
            logger.info(f"Rate limiting: sleeping {sleep_time:.1f}s for {restaurant_guid}")
            time.sleep(sleep_time)

        self.last_request_time[restaurant_guid] = time.time()

    def fetch_orders(self, restaurant_guid: str, start_date: str, end_date: str) -> Tuple[List[Dict], List[str]]:
        """
        Fetch orders for a single restaurant with pagination and rate limiting

        Args:
            restaurant_guid: Toast restaurant GUID
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)

        Returns:
            Tuple of (orders list, errors list)
        """
        orders = []
        errors = []
        page = 1

        # Refresh token if needed
        token = self.get_token()
        if not token:
            errors.append(f"No valid token for {restaurant_guid}")
            return orders, errors

        # Convert to ISO timestamp format
        start_datetime = f'{start_date}T00:00:00.000-0000'
        end_datetime = f'{end_date}T23:59:59.999-0000'

        while page <= MAX_PAGES:
            # Apply rate limiting
            self._apply_rate_limit(restaurant_guid)

            url = f'https://ws-api.toasttab.com/orders/v2/ordersBulk?startDate={start_datetime}&endDate={end_datetime}&page={page}&pageSize=100'

            headers = {
                'Authorization': f'Bearer {token}',
                'Toast-Restaurant-External-ID': restaurant_guid,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }

            try:
                logger.info(f"Fetching page {page} for restaurant {restaurant_guid}")
                resp = self.session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

                # Handle rate limiting explicitly
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()

                data = resp.json()

                # Handle both response formats: list or dict with 'data' key
                if isinstance(data, list):
                    page_orders = data
                    pagination = {}
                else:
                    page_orders = data.get('data', [])
                    pagination = data.get('pagination', {})

                if not page_orders:
                    logger.info(f"No more orders for {restaurant_guid} at page {page}")
                    break

                # Add restaurant GUID and metadata to each order
                for order in page_orders:
                    # Ensure restaurantGuid is present (API doesn't always include it)
                    if 'restaurantGuid' not in order:
                        order['restaurantGuid'] = restaurant_guid

                    # Normalize timestamps to BigQuery-compatible format
                    normalize_timestamps(order)

                    if not validate_order(order):
                        logger.warning(f"Invalid order skipped: {order.get('guid', 'unknown')}")
                        continue

                    order['_loaded_at'] = datetime.utcnow().isoformat() + 'Z'
                    order['_restaurant_guid'] = restaurant_guid
                    order['_data_source'] = 'toast_api'

                logger.info(f"Retrieved {len(page_orders)} orders from page {page}")
                orders.extend(page_orders)

                # Check pagination info
                if pagination.get('hasNextPage') == False:
                    break

                page += 1

            except requests.exceptions.Timeout:
                error_msg = f"Timeout fetching page {page} for {restaurant_guid}"
                logger.error(error_msg)
                errors.append(error_msg)
                break

            except requests.exceptions.RequestException as e:
                error_msg = f"Error fetching page {page} for {restaurant_guid}: {str(e)}"
                logger.error(error_msg)
                errors.append(error_msg)
                break

        return orders, errors


def normalize_timestamps(order: Dict) -> None:
    """
    Normalize Toast API timestamp and date formats to BigQuery-compatible format

    Toast API returns:
    - Timestamps: 2026-02-08T04:26:03.864+0000
    - Dates: 20260208 (integer)

    BigQuery expects:
    - Timestamps: 2026-02-08T04:26:03.864000Z (RFC 3339)
    - Dates: 2026-02-08

    Args:
        order: Order dict to normalize (modified in-place)
    """
    timestamp_fields = [
        'openedDate', 'closedDate', 'modifiedDate', 'paidDate', 'voidDate',
        'deletedDate', 'createdDate', 'promisedDate', 'estimatedFulfillmentDate'
    ]
    date_fields = ['businessDate', 'voidBusinessDate']

    # Normalize timestamps
    for field in timestamp_fields:
        if field in order and order[field]:
            try:
                # Toast format: 2026-02-08T04:26:03.864+0000
                # Replace +0000 with Z (UTC indicator)
                value = str(order[field])
                if '+0000' in value:
                    value = value.replace('+0000', 'Z')
                elif '-0000' in value:
                    value = value.replace('-0000', 'Z')
                order[field] = value
            except Exception as e:
                logger.warning(f"Failed to normalize timestamp {field}: {e}")

    # Normalize dates (convert 20260208 to 2026-02-08)
    for field in date_fields:
        if field in order and order[field]:
            try:
                value = str(order[field])
                # If it's 8 digits like 20260208, convert to YYYY-MM-DD
                if len(value) == 8 and value.isdigit():
                    order[field] = f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
            except Exception as e:
                logger.warning(f"Failed to normalize date {field}: {e}")


def validate_order(order: Dict) -> bool:
    """
    Validate order has required fields

    Args:
        order: Order dict from Toast API

    Returns:
        True if valid, False otherwise
    """
    required_fields = ['guid', 'restaurantGuid', 'businessDate']

    for field in required_fields:
        if field not in order or order[field] is None:
            logger.warning(f"Order missing required field: {field}")
            return False

    return True


def load_to_bigquery_with_dedup(orders: List[Dict], table_id: str) -> Tuple[int, List[str]]:
    """
    Load orders to BigQuery with deduplication using MERGE

    Args:
        orders: List of order dicts
        table_id: Fully qualified table ID

    Returns:
        Tuple of (rows loaded, errors list)
    """
    if not orders:
        return 0, []

    errors = []
    client = bigquery.Client(project=BQ_PROJECT_ID)

    try:
        # Load to temporary staging table
        temp_table_id = f"{table_id}_temp_{int(time.time())}"

        job_config = bigquery.LoadJobConfig(
            schema=BQ_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        logger.info(f"Loading {len(orders)} orders to staging table {temp_table_id}")
        load_job = client.load_table_from_json(orders, temp_table_id, job_config=job_config)
        load_job.result()

        # Ensure main table exists with correct schema before MERGE
        try:
            table = client.get_table(table_id)
            # Check if schema matches (column count)
            if len(table.schema) != len(BQ_SCHEMA):
                logger.warning(f"Schema mismatch: table has {len(table.schema)} columns, expected {len(BQ_SCHEMA)}. Recreating table.")
                client.delete_table(table_id)
                table = bigquery.Table(table_id, schema=BQ_SCHEMA)
                client.create_table(table)
                logger.info(f"Recreated table {table_id} with correct schema")
            else:
                logger.info(f"Main table {table_id} exists with correct schema")
        except Exception as e:
            logger.info(f"Creating main table {table_id} with {len(BQ_SCHEMA)} columns")
            table = bigquery.Table(table_id, schema=BQ_SCHEMA)
            client.create_table(table)

        # MERGE from staging to main table (deduplication)
        merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON T.guid = S.guid AND T.restaurantGuid = S.restaurantGuid
        WHEN NOT MATCHED THEN
          INSERT ROW
        """

        logger.info(f"Merging data to {table_id} with deduplication")
        merge_job = client.query(merge_query)
        merge_job.result()

        rows_affected = merge_job.num_dml_affected_rows or 0

        # Clean up temp table
        client.delete_table(temp_table_id, not_found_ok=True)

        logger.info(f"Successfully loaded {rows_affected} new orders (deduplicated)")
        return rows_affected, errors

    except Exception as e:
        error_msg = f"BigQuery load error: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        return 0, errors


@functions_framework.http
def orders_daily(request):
    """
    Production-ready Cloud Function to fetch Toast orders and load to BigQuery

    Query parameters:
    - start_date: YYYY-MM-DD (default: yesterday)
    - end_date: YYYY-MM-DD (default: yesterday)
    - restaurant_guids: List of restaurant GUIDs to process (default: all restaurants)
    - mode: 'test' for dry-run without BigQuery load
    """

    try:
        # Parse request parameters
        request_json = request.get_json(silent=True)
        request_args = request.args

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

        # Optional: Filter specific restaurants
        if request_json and 'restaurant_guids' in request_json:
            restaurant_guids = request_json['restaurant_guids']
            if not isinstance(restaurant_guids, list):
                restaurant_guids = [restaurant_guids]
        else:
            restaurant_guids = RESTAURANT_GUIDS

        test_mode = (request_json and request_json.get('mode') == 'test') or request_args.get('mode') == 'test'

        logger.info(f"Starting orders fetch: {start_date} to {end_date}, restaurants={len(restaurant_guids)}, test_mode={test_mode}")

        # Get credentials from Secret Manager
        client_id = get_secret('TOAST_CLIENT_ID')
        client_secret = get_secret('TOAST_CLIENT_SECRET')

        if not client_id or not client_secret:
            return json.dumps({'error': 'Failed to retrieve credentials'}), 500, {'Content-Type': 'application/json'}

        # Initialize Toast API client
        toast_client = ToastAPIClient(client_id, client_secret)

        all_orders = []
        all_errors = []

        # Fetch orders for each restaurant with rate limiting
        for guid in restaurant_guids:
            if not guid:
                continue

            logger.info(f"Processing restaurant: {guid}")
            orders, errors = toast_client.fetch_orders(guid, start_date, end_date)
            all_orders.extend(orders)
            all_errors.extend(errors)

        logger.info(f"Total orders fetched: {len(all_orders)}, errors: {len(all_errors)}")

        if not all_orders:
            result = {
                'status': 'success',
                'message': f'No orders found for {start_date} to {end_date}',
                'orders_loaded': 0,
                'errors': all_errors
            }
            return json.dumps(result), 200, {'Content-Type': 'application/json'}

        # Test mode - don't load to BigQuery
        if test_mode:
            result = {
                'status': 'test',
                'message': 'Test mode - data not loaded',
                'orders_fetched': len(all_orders),
                'date_range': f'{start_date} to {end_date}',
                'sample_order_keys': list(all_orders[0].keys()) if all_orders else [],
                'errors': all_errors
            }
            return json.dumps(result), 200, {'Content-Type': 'application/json'}

        # Load to BigQuery with deduplication
        table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.toast_orders_raw'
        rows_loaded, load_errors = load_to_bigquery_with_dedup(all_orders, table_id)
        all_errors.extend(load_errors)

        result = {
            'status': 'success',
            'message': f'Loaded orders for {start_date} to {end_date}',
            'orders_fetched': len(all_orders),
            'orders_loaded': rows_loaded,
            'duplicates_skipped': len(all_orders) - rows_loaded,
            'date_range': f'{start_date} to {end_date}',
            'table': table_id,
            'errors': all_errors
        }

        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        error_msg = f'Critical error: {str(e)}'
        logger.exception(error_msg)

        result = {
            'status': 'error',
            'message': error_msg
        }
        return json.dumps(result), 500, {'Content-Type': 'application/json'}
