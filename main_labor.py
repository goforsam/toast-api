"""Toast Labor ETL - Cloud Function
Entry point: labor_daily(request)

Fetches labor time entries from Toast API for 1 restaurant, loads to BigQuery.
Request: {"restaurant_guid": "abc-123", "start_date": "2025-01-01", "end_date": "2025-01-07"}
"""

import json
import logging
from datetime import datetime, timedelta

import functions_framework

from shared.config import RESTAURANT_GUIDS, SCHEMA_FACT_LABOR_SHIFTS
from shared.secrets_utils import get_secret
from shared.toast_client import ToastAPIClient
from shared.bigquery_utils import load_to_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def flatten_labor_shifts(entries, restaurant_guid):
    """Flatten labor time entry API responses into fact rows."""
    rows = []
    for entry in entries:
        # Parse business date (integer YYYYMMDD -> YYYY-MM-DD)
        biz_date = entry.get('businessDate')
        if biz_date and len(str(biz_date)) == 8 and str(biz_date).isdigit():
            biz_date = f"{str(biz_date)[:4]}-{str(biz_date)[4:6]}-{str(biz_date)[6:8]}"
        elif not biz_date:
            # Derive from inDate if missing
            in_date = entry.get('inDate')
            if in_date:
                try:
                    biz_date = str(in_date)[:10]
                except Exception:
                    pass

        # Normalize timestamps
        in_date = entry.get('inDate')
        if in_date and '+0000' in str(in_date):
            in_date = str(in_date).replace('+0000', 'Z')
        elif in_date and '-0000' in str(in_date):
            in_date = str(in_date).replace('-0000', 'Z')

        out_date = entry.get('outDate')
        if out_date and '+0000' in str(out_date):
            out_date = str(out_date).replace('+0000', 'Z')
        elif out_date and '-0000' in str(out_date):
            out_date = str(out_date).replace('-0000', 'Z')

        # Extract employee and job references
        employee_ref = entry.get('employeeReference') or entry.get('employee') or {}
        job_ref = entry.get('jobReference') or entry.get('job') or {}

        # Extract wage info
        wage = entry.get('wage') or entry.get('hourlyWage') or 0

        row = {
            'time_entry_guid': entry.get('guid'),
            'restaurant_guid': restaurant_guid,
            'business_date': biz_date,
            'employee_guid': employee_ref.get('guid'),
            'job_guid': job_ref.get('guid'),
            'job_title': job_ref.get('title') or job_ref.get('name'),
            'in_date': in_date,
            'out_date': out_date,
            'regular_hours': entry.get('regularHours', 0) or 0,
            'overtime_hours': entry.get('overtimeHours', 0) or 0,
            'hourly_wage': wage,
            'regular_pay': entry.get('regularPay', 0) or entry.get('nonOvertimeHourlyWages', 0) or 0,
            'overtime_pay': entry.get('overtimePay', 0) or entry.get('overtimeHourlyWages', 0) or 0,
            'total_pay': entry.get('totalPay', 0) or entry.get('totalWages', 0) or 0,
            'declared_tips': entry.get('declaredTips', 0) or entry.get('cashTips', 0) or 0,
            'is_deleted': entry.get('deleted', False),
            '_loaded_at': datetime.utcnow().isoformat() + 'Z',
        }

        if not row['time_entry_guid']:
            continue

        rows.append(row)

    return rows


@functions_framework.http
def labor_daily(request):
    """
    Cloud Function entry point for labor ETL.

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

        if not restaurant_guid:
            return _error_response('restaurant_guid is required'), 400
        elif restaurant_guid == 'ALL':
            guids = RESTAURANT_GUIDS
        else:
            guids = [restaurant_guid]

        logger.info(f"Labor ETL: {len(guids)} restaurant(s), {start_date} to {end_date}")

        client_id = get_secret('TOAST_CLIENT_ID')
        client_secret = get_secret('TOAST_CLIENT_SECRET')
        if not client_id or not client_secret:
            return _error_response('Failed to retrieve Toast credentials'), 500

        toast_client = ToastAPIClient(client_id, client_secret)

        total_entries = 0
        total_shifts = 0
        shifts_loaded = 0
        all_errors = []

        for guid in guids:
            logger.info(f"Processing labor for restaurant: {guid}")

            raw_entries, fetch_errors = toast_client.fetch_labor_time_entries(guid, start_date, end_date)
            all_errors.extend(fetch_errors)

            total_entries += len(raw_entries)

            if not raw_entries:
                logger.info(f"No labor entries for {guid}")
                continue

            fact_rows = flatten_labor_shifts(raw_entries, guid)
            total_shifts += len(fact_rows)

            if fact_rows:
                loaded, load_errors = load_to_bigquery(
                    records=fact_rows,
                    table_name='fact_labor_shifts',
                    schema=SCHEMA_FACT_LABOR_SHIFTS,
                    dedup_keys=['time_entry_guid'],
                )
                all_errors.extend(load_errors)
                shifts_loaded += loaded

            logger.info(f"Restaurant {guid}: {len(raw_entries)} entries -> {len(fact_rows)} shifts -> {shifts_loaded} loaded")

        result = {
            'status': 'success',
            'start_date': start_date,
            'end_date': end_date,
            'restaurants_processed': len(guids),
            'entries_fetched': total_entries,
            'shifts_flattened': total_shifts,
            'shifts_loaded': shifts_loaded,
            'errors': all_errors,
        }

        logger.info(f"Labor ETL complete: {shifts_loaded} shifts loaded")
        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        logger.error(f"Labor ETL failed: {str(e)}")
        return _error_response(str(e)), 500


def _error_response(message):
    return json.dumps({'status': 'error', 'error': message}), {'Content-Type': 'application/json'}
