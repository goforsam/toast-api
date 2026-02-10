"""Toast Orders ETL - Fetch orders and load flattened fact_order_items to BigQuery"""

import functions_framework
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List

# Shared modules
from shared.config import SCHEMA_FACT_ORDER_ITEMS, RESTAURANT_GUIDS, BQ_PROJECT_ID, BQ_DATASET_ID
from shared.secrets import get_secret
from shared.toast_client import ToastAPIClient
from shared.bigquery_utils import load_to_bigquery_with_dedup
from shared.date_utils import normalize_timestamps

# Configure structured logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def normalize_timestamp_value(value):
    """Convert Toast API timestamp format (+0000) to BigQuery format (Z)"""
    if value and isinstance(value, str):
        if '+0000' in value:
            return value.replace('+0000', 'Z')
        elif '-0000' in value:
            return value.replace('-0000', 'Z')
    return value


def flatten_order_to_items(order: Dict) -> List[Dict]:
    """
    Flatten order checks and selections to individual item rows

    Args:
        order: Order dict from Toast API with nested checks[] and selections[]

    Returns:
        List of flattened item dicts for fact_order_items table
    """
    items = []

    for check in order.get('checks', []):
        check_guid = check.get('guid')

        for selection in check.get('selections', []):
            # Extract nested menu item and sales category details (handle null values)
            menu_item = selection.get('item') or {}
            sales_category = selection.get('salesCategory') or {}

            # Calculate prices
            unit_price = selection.get('price', 0.0)
            quantity = selection.get('quantity', 1.0)
            total_price = unit_price * quantity

            item = {
                # Composite key
                'selection_guid': selection.get('guid'),  # Primary dedup key
                'order_guid': order.get('guid'),
                'check_guid': check_guid,

                # Dimension keys
                'restaurant_guid': order.get('restaurantGuid'),
                'business_date': order.get('businessDate'),  # Already normalized
                'menu_item_guid': menu_item.get('guid'),
                'sales_category_guid': sales_category.get('guid'),

                # Denormalized dimension attributes (for BI speed)
                'menu_item_name': menu_item.get('name'),
                'sales_category_name': sales_category.get('name'),

                # Measures
                'quantity': quantity,
                'unit_price': unit_price,
                'total_price': total_price,
                'tax_amount': selection.get('tax', 0.0),
                'discount_amount': selection.get('appliedDiscounts', [{}])[0].get('discountAmount', 0.0) if selection.get('appliedDiscounts') else 0.0,

                # Attributes
                'is_voided': selection.get('voided', False),
                'voided_date': normalize_timestamp_value(selection.get('voidDate')),
                'modifiers': json.dumps(selection.get('modifiers', [])),  # Store as JSON string

                # Timestamps
                'created_date': normalize_timestamp_value(selection.get('createdDate')),
                'modified_date': normalize_timestamp_value(selection.get('modifiedDate')),

                # Metadata (standard across all fact tables)
                '_loaded_at': datetime.utcnow().isoformat() + 'Z',
                '_restaurant_guid': order.get('restaurantGuid'),
                '_data_source': 'toast_api'
            }

            items.append(item)

    return items


@functions_framework.http
def orders_daily(request):
    """
    Cloud Function entry point: Fetch Toast orders and load flattened fact_order_items to BigQuery

    Query parameters:
    - start_date: YYYY-MM-DD (default: yesterday)
    - end_date: YYYY-MM-DD (default: yesterday)
    - mode: 'test' for dry-run without BigQuery load
    - restaurant_guids: Optional list to filter specific restaurants

    Returns:
        JSON response with status and metrics
    """

    try:
        # Parse request parameters
        request_json = request.get_json(silent=True)
        request_args = request.args

        # Get date range
        if request_json and 'start_date' in request_json:
            start_date = request_json['start_date']
            end_date = request_json.get('end_date', request_json['start_date'])
        elif 'start_date' in request_args:
            start_date = request_args['start_date']
            end_date = request_args.get('end_date', start_date)
        else:
            # Default: yesterday
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

        logger.info(f"Starting orders ETL: {start_date} to {end_date}, restaurants={len(restaurant_guids)}, test_mode={test_mode}")

        # Get credentials from Secret Manager
        client_id = get_secret('TOAST_CLIENT_ID')
        client_secret = get_secret('TOAST_CLIENT_SECRET')

        if not client_id or not client_secret:
            return json.dumps({'error': 'Failed to retrieve credentials'}), 500, {'Content-Type': 'application/json'}

        # Initialize Toast API client
        toast_client = ToastAPIClient(client_id, client_secret)

        all_items = []
        all_errors = []
        orders_fetched = 0

        # Fetch orders for each restaurant with rate limiting
        for guid in restaurant_guids:
            if not guid:
                continue

            logger.info(f"Processing restaurant: {guid}")
            orders, errors = toast_client.fetch_orders(guid, start_date, end_date)
            orders_fetched += len(orders)
            all_errors.extend(errors)

            # Flatten orders to items (timestamps normalized inline)
            for order in orders:
                items = flatten_order_to_items(order)
                all_items.extend(items)

        logger.info(f"Total orders fetched: {orders_fetched}, items extracted: {len(all_items)}, errors: {len(all_errors)}")

        if not all_items:
            result = {
                'status': 'success',
                'message': f'No items found for {start_date} to {end_date}',
                'orders_fetched': orders_fetched,
                'items_loaded': 0,
                'duplicates_skipped': 0,
                'errors': all_errors
            }
            return json.dumps(result), 200, {'Content-Type': 'application/json'}

        # Load to BigQuery with deduplication
        if not test_mode:
            table_id = f'{BQ_PROJECT_ID}.{BQ_DATASET_ID}.fact_order_items'

            rows_loaded, load_errors = load_to_bigquery_with_dedup(
                all_items,
                table_id,
                schema=SCHEMA_FACT_ORDER_ITEMS,
                dedup_key='selection_guid'
            )

            all_errors.extend(load_errors)
            duplicates_skipped = len(all_items) - rows_loaded

            logger.info(f"BigQuery load complete: {rows_loaded} new items, {duplicates_skipped} duplicates")

            result = {
                'status': 'success' if not load_errors else 'partial',
                'orders_fetched': orders_fetched,
                'items_loaded': rows_loaded,
                'duplicates_skipped': duplicates_skipped,
                'errors': all_errors
            }
        else:
            # Test mode: just return metrics without loading
            result = {
                'status': 'test_mode',
                'orders_fetched': orders_fetched,
                'items_extracted': len(all_items),
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
