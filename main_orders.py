"""Toast Orders ETL - Cloud Function
Entry point: orders_daily(request)

Fetches orders from Toast API for 1 restaurant, flattens to fact rows, loads to BigQuery.
Request: {"restaurant_guid": "abc-123", "start_date": "2025-01-01", "end_date": "2025-01-07"}
"""

import json
import logging
from datetime import datetime, timedelta

import functions_framework

from shared.config import RESTAURANT_GUIDS, SCHEMA_FACT_ORDER_ITEMS, SECRET_SUFFIX
from shared.secrets_utils import get_secret
from shared.toast_client import ToastAPIClient
from shared.bigquery_utils import load_to_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def flatten_orders_to_facts(orders, restaurant_guid):
    """
    Flatten nested orders -> checks -> selections into fact rows.
    Each selection (menu item) becomes 1 row in fact_order_items.
    """
    rows = []
    for order in orders:
        business_date = order.get('businessDate')
        server_guid = None
        server = order.get('server')
        if server:
            server_guid = server.get('guid')

        is_voided = order.get('voided', False)
        is_deleted = order.get('deleted', False)

        for check in order.get('checks', []):
            check_total = check.get('totalAmount', 0) or 0
            check_tax = check.get('taxAmount', 0) or 0

            # Sum tips from all payments on this check
            check_tip = 0
            payment_type = None
            for payment in check.get('payments', []):
                check_tip += payment.get('tipAmount', 0) or 0
                if payment_type is None:
                    payment_type = payment.get('type')

            for selection in check.get('selections', []):
                if selection.get('voided'):
                    continue

                # Extract sales category
                sales_cat = selection.get('salesCategory') or {}

                row = {
                    'selection_guid': selection.get('guid'),
                    'order_guid': order.get('guid'),
                    'check_guid': check.get('guid'),
                    'restaurant_guid': restaurant_guid,
                    'business_date': business_date,
                    'menu_item_guid': selection.get('itemGuid') or (selection.get('item') or {}).get('guid'),
                    'server_guid': server_guid,
                    'menu_item_name': selection.get('displayName'),
                    'sales_category_name': sales_cat.get('name'),
                    'item_quantity': selection.get('quantity', 0) or 0,
                    'item_price': selection.get('price', 0) or 0,
                    'pre_discount_price': selection.get('preDiscountPrice', 0) or 0,
                    'discount_amount': selection.get('appliedDiscountAmount', 0) or 0,
                    'tax_amount': selection.get('tax', 0) or 0,
                    'check_total': check_total,
                    'check_tax': check_tax,
                    'check_tip': check_tip,
                    'payment_type': payment_type,
                    'is_voided': is_voided,
                    'is_deleted': is_deleted,
                    '_loaded_at': datetime.utcnow().isoformat() + 'Z',
                }

                # Skip rows missing required dedup keys
                if not row['selection_guid'] or not row['order_guid']:
                    continue

                rows.append(row)

    return rows


@functions_framework.http
def orders_daily(request):
    """
    Cloud Function entry point for orders ETL.

    Request JSON:
        restaurant_guid: Required. Single GUID or "ALL" for all restaurants.
        start_date: Optional. YYYY-MM-DD (default: yesterday)
        end_date: Optional. YYYY-MM-DD (default: yesterday)
    """
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        restaurant_guid = request_json.get('restaurant_guid')
        start_date = request_json.get('start_date')
        end_date = request_json.get('end_date')

        # Default to yesterday
        if not start_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        # Determine which restaurants to process
        if not restaurant_guid:
            return _error_response('restaurant_guid is required'), 400
        elif restaurant_guid == 'ALL':
            guids = RESTAURANT_GUIDS
        else:
            guids = [restaurant_guid]

        logger.info(f"Orders ETL: {len(guids)} restaurant(s), {start_date} to {end_date}")

        # Get Toast credentials
        client_id = get_secret(f'TOAST_CLIENT_ID{SECRET_SUFFIX}')
        client_secret = get_secret(f'TOAST_CLIENT_SECRET{SECRET_SUFFIX}')
        if not client_id or not client_secret:
            return _error_response('Failed to retrieve Toast credentials'), 500

        toast_client = ToastAPIClient(client_id, client_secret)

        # Process each restaurant
        total_orders = 0
        total_items = 0
        total_loaded = 0
        all_errors = []

        for guid in guids:
            logger.info(f"Processing restaurant: {guid}")

            # Fetch orders from Toast API
            orders, fetch_errors = toast_client.fetch_orders(guid, start_date, end_date)
            all_errors.extend(fetch_errors)

            if not orders:
                logger.info(f"No orders for {guid}")
                continue

            total_orders += len(orders)

            # Flatten orders to fact rows
            fact_rows = flatten_orders_to_facts(orders, guid)
            total_items += len(fact_rows)

            if not fact_rows:
                logger.info(f"No item rows after flattening for {guid}")
                continue

            # Load to BigQuery
            rows_loaded, load_errors = load_to_bigquery(
                records=fact_rows,
                table_name='fact_order_items',
                schema=SCHEMA_FACT_ORDER_ITEMS,
                dedup_keys=['selection_guid', 'order_guid'],
            )
            all_errors.extend(load_errors)
            total_loaded += rows_loaded

            logger.info(f"Restaurant {guid}: {len(orders)} orders -> {len(fact_rows)} items -> {rows_loaded} loaded")

        # Response
        result = {
            'status': 'success',
            'start_date': start_date,
            'end_date': end_date,
            'restaurants_processed': len(guids),
            'orders_fetched': total_orders,
            'items_flattened': total_items,
            'rows_loaded': total_loaded,
            'errors': all_errors,
        }

        logger.info(f"Orders ETL complete: {total_loaded} rows loaded")
        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        logger.error(f"Orders ETL failed: {str(e)}")
        return _error_response(str(e)), 500


def _error_response(message):
    return json.dumps({'status': 'error', 'error': message}), {'Content-Type': 'application/json'}
