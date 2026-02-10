"""Toast API client with OAuth, rate limiting, and retry logic"""

import requests
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import MAX_PAGES, REQUEST_TIMEOUT, TOKEN_REFRESH_THRESHOLD, RATE_LIMITS

logger = logging.getLogger(__name__)


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
        # Track per-endpoint, per-restaurant rate limiting
        self.last_request_time = {
            'orders': {},
            'cash': {},
            'labor': {},
            'menus': {},
            'config': {}
        }

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

    def _apply_rate_limit(self, endpoint: str, restaurant_guid: str):
        """
        Apply endpoint-specific rate limiting per restaurant

        Args:
            endpoint: API endpoint type (orders, cash, labor, menus, config)
            restaurant_guid: Restaurant GUID
        """
        delay = RATE_LIMITS.get(endpoint, 12)  # Default to orders rate limit
        tracker = self.last_request_time[endpoint]

        last_request = tracker.get(restaurant_guid, 0)
        elapsed = time.time() - last_request

        if elapsed < delay:
            sleep_time = delay - elapsed
            logger.info(f"Rate limiting [{endpoint}]: sleeping {sleep_time:.1f}s for {restaurant_guid}")
            time.sleep(sleep_time)

        tracker[restaurant_guid] = time.time()

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
        from .date_utils import normalize_timestamps, validate_order

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
            self._apply_rate_limit('orders', restaurant_guid)

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
