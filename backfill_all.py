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

# Client configs: dataset -> (function_url_prefix, restaurant_guids)
CLIENT_CONFIGS = {
    'purpose': {
        'prefix': 'toast',
        'guids': [
            '6d035dad-924f-47b4-ba93-fd86575e73a3',
            '53ae28f1-87c7-4a07-9a43-b619c009b7b0',
            'def5e222-f458-41d0-bff9-48abaf20666a',
            '42f246b1-82f1-4048-93c1-d63554c7d9ef',
            'a405942e-179b-4f3f-a75b-a0d18882bd7f',
            'd587bfe9-9faa-48a8-9938-1a23ad36bc9e',
            'da6f0893-d17c-4f93-b7ee-0c708d2611a9',
            'a6a87c64-734e-4f39-90dc-598b5e743105',
            'e629b6e6-85f5-466f-9427-cfbb4f2a6bfe',
            '290ca643-8ee4-4d8f-9c00-3793e15ae8a6',
            'eaa7b168-db38-45be-82e8-bd25e6647fd1',
            'a4b4a7a2-0309-4451-8b62-ca0c98858a84',
            'd44d5122-3412-459a-946d-f91a5da03ea3',
        ],
    },
    'rodrigos': {
        'prefix': 'rodrigos',
        'guids': [
            'ab3c4f80-5529-4b5f-bba1-cc9abaf33431',
            '3383074f-b565-4501-ae86-41f21c866cba',
            '8cb95c1f-2f82-4f20-9dce-446a956fd4bb',
            'bef05e5c-3b38-49f3-9b8d-ca379130f718',
            '8c37412b-a13b-4edd-bbd8-b26222fcbe68',
            'dedecf4f-ee34-41ab-a740-f3b461eed4eb',
            'eea6e77a-46b2-4631-907e-10d85a845bb8',
            'e2fbc555-2cc4-49ee-bbdc-1e4c652ec6f4',
            'd0bbc362-63d4-4277-af85-2bf2c808bdc7',
            '1903fd30-c0ff-4682-b9af-b184c77d9653',
        ],
    },
}

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
    parser.add_argument('--dataset', default='purpose', choices=list(CLIENT_CONFIGS.keys()),
                        help='Target dataset/client (default: purpose)')
    parser.add_argument('--start', default='2025-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', default=None, help='End date (YYYY-MM-DD, default: yesterday)')
    parser.add_argument('--restaurant', default=None, help='Single restaurant GUID (default: all)')
    parser.add_argument('--delay', type=int, default=2, help='Seconds between calls (default: 2)')
    parser.add_argument('--dry-run', action='store_true', help='Print calls without executing')
    args = parser.parse_args()

    if not args.end:
        args.end = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')

    config = CLIENT_CONFIGS[args.dataset]
    function_url = get_function_url(config['prefix'], args.type)
    format_result = FORMAT_FUNCTIONS[args.type]
    guids = [args.restaurant] if args.restaurant else config['guids']
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
