"""Toast Cash ETL - Cloud Function
Entry point: cash_daily(request)

Fetches cash entries and deposits from Toast API for 1 restaurant, loads to BigQuery.
Request: {"restaurant_guid": "abc-123", "start_date": "2025-01-01", "end_date": "2025-01-07"}
"""

import json
import logging
from datetime import datetime, timedelta

import functions_framework

from shared.config import RESTAURANT_GUIDS, SCHEMA_FACT_CASH_ENTRIES, SCHEMA_FACT_CASH_DEPOSITS, SECRET_SUFFIX
from shared.secrets_utils import get_secret
from shared.toast_client import ToastAPIClient
from shared.bigquery_utils import load_to_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def flatten_cash_entries(entries, restaurant_guid):
    """Flatten cash entry API responses into fact rows."""
    rows = []
    for entry in entries:
        biz_date = entry.get('businessDate')
        if biz_date and len(str(biz_date)) == 8 and str(biz_date).isdigit():
            biz_date = f"{str(biz_date)[:4]}-{str(biz_date)[4:6]}-{str(biz_date)[6:8]}"

        # Normalize entry date timestamp
        entry_date = entry.get('date') or entry.get('entryDate')
        if entry_date and '+0000' in str(entry_date):
            entry_date = str(entry_date).replace('+0000', 'Z')
        elif entry_date and '-0000' in str(entry_date):
            entry_date = str(entry_date).replace('-0000', 'Z')

        row = {
            'cash_entry_guid': entry.get('guid'),
            'restaurant_guid': restaurant_guid,
            'business_date': biz_date,
            'employee_guid': (entry.get('employee') or {}).get('guid'),
            'entry_type': entry.get('type'),
            'amount': entry.get('amount', 0) or 0,
            'reason': entry.get('reason'),
            'notes': entry.get('notes'),
            'cash_drawer_guid': (entry.get('cashDrawer') or {}).get('guid'),
            'entry_date': entry_date,
            '_loaded_at': datetime.utcnow().isoformat() + 'Z',
        }

        if not row['cash_entry_guid']:
            continue

        rows.append(row)

    return rows


def flatten_cash_deposits(deposits, restaurant_guid):
    """Flatten cash deposit API responses into fact rows."""
    rows = []
    for deposit in deposits:
        biz_date = deposit.get('businessDate')
        if biz_date and len(str(biz_date)) == 8 and str(biz_date).isdigit():
            biz_date = f"{str(biz_date)[:4]}-{str(biz_date)[4:6]}-{str(biz_date)[6:8]}"

        deposit_date = deposit.get('date')
        if deposit_date and '+0000' in str(deposit_date):
            deposit_date = str(deposit_date).replace('+0000', 'Z')
        elif deposit_date and '-0000' in str(deposit_date):
            deposit_date = str(deposit_date).replace('-0000', 'Z')

        row = {
            'deposit_guid': deposit.get('guid'),
            'restaurant_guid': restaurant_guid,
            'business_date': biz_date,
            'deposit_date': deposit_date,
            'deposit_amount': deposit.get('amount', 0) or 0,
            'cash_amount': deposit.get('cashAmount', 0) or 0,
            'check_amount': deposit.get('checkAmount', 0) or 0,
            '_loaded_at': datetime.utcnow().isoformat() + 'Z',
        }

        if not row['deposit_guid']:
            continue

        rows.append(row)

    return rows


@functions_framework.http
def cash_daily(request):
    """
    Cloud Function entry point for cash ETL.

    Request JSON:
        restaurant_guid: Required. Single GUID or "ALL" for all restaurants.
        start_date: Optional. YYYY-MM-DD (default: yesterday)
        end_date: Optional. YYYY-MM-DD (default: yesterday)
    """
    try:
        request_json = request.get_json(silent=True) or {}
        restaurant_guid = request_json.get('restaurant_guid')
        start_date = request_json.get('start_date')
        end_date = request_json.get('end_date')

        if not start_date:
            yesterday = datetime.utcnow() - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
        if not end_date:
            end_date = start_date

        if not restaurant_guid or restaurant_guid == 'ALL':
            guids = RESTAURANT_GUIDS
        else:
            guids = [restaurant_guid]

        logger.info(f"Cash ETL: {len(guids)} restaurant(s), {start_date} to {end_date}")

        client_id = get_secret(f'TOAST_CLIENT_ID{SECRET_SUFFIX}')
        client_secret = get_secret(f'TOAST_CLIENT_SECRET{SECRET_SUFFIX}')
        if not client_id or not client_secret:
            return _error_response('Failed to retrieve Toast credentials'), 500

        toast_client = ToastAPIClient(client_id, client_secret)

        total_entries = 0
        total_deposits = 0
        entries_loaded = 0
        deposits_loaded = 0
        all_errors = []

        for guid in guids:
            logger.info(f"Processing cash for restaurant: {guid}")

            # Fetch and load cash entries
            raw_entries, entry_errors = toast_client.fetch_cash_entries(guid, start_date, end_date)
            all_errors.extend(entry_errors)

            if raw_entries:
                fact_rows = flatten_cash_entries(raw_entries, guid)
                total_entries += len(fact_rows)

                if fact_rows:
                    loaded, load_errors = load_to_bigquery(
                        records=fact_rows,
                        table_name='fact_cash_entries',
                        schema=SCHEMA_FACT_CASH_ENTRIES,
                        dedup_keys=['cash_entry_guid'],
                    )
                    all_errors.extend(load_errors)
                    entries_loaded += loaded

            # Fetch and load cash deposits
            raw_deposits, deposit_errors = toast_client.fetch_cash_deposits(guid, start_date, end_date)
            all_errors.extend(deposit_errors)

            if raw_deposits:
                fact_rows = flatten_cash_deposits(raw_deposits, guid)
                total_deposits += len(fact_rows)

                if fact_rows:
                    loaded, load_errors = load_to_bigquery(
                        records=fact_rows,
                        table_name='fact_cash_deposits',
                        schema=SCHEMA_FACT_CASH_DEPOSITS,
                        dedup_keys=['deposit_guid'],
                    )
                    all_errors.extend(load_errors)
                    deposits_loaded += loaded

            logger.info(f"Restaurant {guid}: {len(raw_entries)} entries, {len(raw_deposits)} deposits")

        result = {
            'status': 'success',
            'start_date': start_date,
            'end_date': end_date,
            'restaurants_processed': len(guids),
            'entries_flattened': total_entries,
            'entries_loaded': entries_loaded,
            'deposits_flattened': total_deposits,
            'deposits_loaded': deposits_loaded,
            'errors': all_errors,
        }

        logger.info(f"Cash ETL complete: {entries_loaded} entries, {deposits_loaded} deposits loaded")
        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        logger.error(f"Cash ETL failed: {str(e)}")
        return _error_response(str(e)), 500


def _error_response(message):
    return json.dumps({'status': 'error', 'error': message})
