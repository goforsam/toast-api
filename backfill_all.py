"""Historical backfill orchestrator for Toast ETL.

Calls deployed Cloud Functions for each restaurant x week combination.
Usage:
    python backfill_all.py --type orders [--dataset purpose] [--start 2025-01-01]
    python backfill_all.py --type cash --dataset rodrigos [--start 2025-01-01]
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta

import requests

from shared.clients import CLIENTS

# URL hash is project-level (same for all functions in the project)
URL_HASH = 'vtpo3hu6ba-uw'

def get_function_url(prefix, etl_type):
    return f'https://{prefix}-{etl_type}-etl-{URL_HASH}.a.run.app'


def generate_weeks(start_date, end_date):
    """Generate (week_start, week_end) tuples from start to end."""
    current = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        yield current.strftime('%Y-%m-%d'), week_end.strftime('%Y-%m-%d')
        current = week_end + timedelta(days=1)


def call_function(url, restaurant_guid, start_date, end_date, timeout=600):
    """Call the Cloud Function for one restaurant x date range."""
    payload = {
        'restaurant_guid': restaurant_guid,
        'start_date': start_date,
        'end_date': end_date,
    }

    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        result = resp.json()
        return result
    except requests.exceptions.Timeout:
        return {'status': 'error', 'error': 'Request timed out'}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def format_result_orders(result, progress, week_start, week_end):
    """Format output for orders backfill."""
    if result.get('status') == 'success':
        orders = result.get('orders_fetched', 0)
        items = result.get('items_flattened', 0)
        loaded = result.get('rows_loaded', 0)
        print(f"  {progress} {week_start} to {week_end}: {orders} orders -> {items} items -> {loaded} loaded")
        return {'orders': orders, 'items': items, 'loaded': loaded}
    return None


def format_result_cash(result, progress, week_start, week_end):
    """Format output for cash backfill."""
    if result.get('status') == 'success':
        entries = result.get('entries_loaded', 0)
        deposits = result.get('deposits_loaded', 0)
        print(f"  {progress} {week_start} to {week_end}: {entries} entries, {deposits} deposits loaded")
        return {'entries': entries, 'deposits': deposits}
    return None


def format_result_labor(result, progress, week_start, week_end):
    """Format output for labor backfill."""
    if result.get('status') == 'success':
        shifts = result.get('shifts_loaded', 0)
        print(f"  {progress} {week_start} to {week_end}: {shifts} shifts loaded")
        return {'shifts': shifts}
    return None


FORMAT_FUNCTIONS = {
    'orders': format_result_orders,
    'cash': format_result_cash,
    'labor': format_result_labor,
}


def main():
    parser = argparse.ArgumentParser(description='Toast ETL Historical Backfill')
    parser.add_argument('--type', required=True, choices=['orders', 'cash', 'labor'],
                        help='ETL type to backfill')
    parser.add_argument('--dataset', default='purpose', choices=list(CLIENTS.keys()),
                        help='Target dataset/client (default: purpose)')
    parser.add_argument('--start', default='2025-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--restaurant', default=None, help='Single restaurant GUID (default: all)')
    parser.add_argument('--delay', type=int, default=2, help='Seconds between calls (default: 2)')
    parser.add_argument('--dry-run', action='store_true', help='Print calls without executing')
    args = parser.parse_args()

    if not args.end:
        args.end = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

    config = CLIENTS[args.dataset]
    function_url = get_function_url(config['url_prefix'], args.type)
    format_result = FORMAT_FUNCTIONS[args.type]
    guids = [args.restaurant] if args.restaurant else config['restaurant_guids']
    weeks = list(generate_weeks(args.start, args.end))
    total_calls = len(guids) * len(weeks)

    print(f"Backfill [{args.type}] dataset={args.dataset}: {len(guids)} restaurant(s) x {len(weeks)} weeks = {total_calls} calls")
    print(f"Date range: {args.start} to {args.end}")
    print(f"Function URL: {function_url}")
    print()

    if args.dry_run:
        for guid in guids:
            for week_start, week_end in weeks:
                print(f"  [DRY RUN] {guid[:8]}... {week_start} to {week_end}")
        print(f"\nTotal: {total_calls} calls (dry run)")
        return

    completed = 0
    errors = []

    for guid in guids:
        print(f"\n--- Restaurant: {guid} ---")

        for week_start, week_end in weeks:
            completed += 1
            progress = f"[{completed}/{total_calls}]"

            result = call_function(function_url, guid, week_start, week_end)

            formatted = format_result(result, progress, week_start, week_end)

            if formatted is None:
                error = result.get('error', 'Unknown error')
                print(f"  {progress} {week_start} to {week_end}: ERROR - {error}")
                errors.append(f"{guid[:8]} {week_start}: {error}")
            elif result.get('errors'):
                for err in result['errors']:
                    print(f"    WARNING: {err}")
                    errors.append(f"{guid[:8]} {week_start}: {err}")

            time.sleep(args.delay)

    # Summary
    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE [{args.type.upper()}] dataset={args.dataset}")
    print(f"{'='*60}")
    print(f"Calls: {completed}/{total_calls}")
    print(f"Errors: {len(errors)}")

    if errors:
        print(f"\nError details:")
        for err in errors:
            print(f"  - {err}")


if __name__ == '__main__':
    main()
