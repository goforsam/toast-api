"""Toast Config ETL - Cloud Function
Entry point: config_weekly(request)

Fetches dimension data (restaurants, employees, jobs, menu items) from Toast API
and loads to BigQuery using full refresh (WRITE_TRUNCATE).
Request: {} (no params needed, processes all restaurants)
"""

import json
import logging
from datetime import datetime

import functions_framework

from shared.config import (
    RESTAURANT_GUIDS,
    SECRET_SUFFIX,
    SCHEMA_DIM_RESTAURANTS,
    SCHEMA_DIM_EMPLOYEES,
    SCHEMA_DIM_JOBS,
    SCHEMA_DIM_MENU_ITEMS,
)
from shared.secrets_utils import get_secret
from shared.toast_client import ToastAPIClient
from shared.bigquery_utils import load_dimension_to_bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def flatten_restaurant(info, restaurant_guid):
    """Flatten restaurant API response into a dimension row."""
    if not info:
        return None

    general = info.get('general', {}) or {}
    location = info.get('location', {}) or {}
    address = location.get('address', {}) or {}

    return {
        'restaurant_guid': restaurant_guid,
        'restaurant_name': general.get('name'),
        'location_name': info.get('locationName') or general.get('locationName'),
        'address_line1': address.get('addressLine1'),
        'address_line2': address.get('addressLine2'),
        'city': address.get('city'),
        'state': address.get('stateCode') or address.get('state'),
        'zip_code': address.get('zipCode') or address.get('zip'),
        'timezone': general.get('timeZone') or info.get('timeZone'),
        '_loaded_at': datetime.utcnow().isoformat() + 'Z',
    }


def flatten_employee(emp, restaurant_guid):
    """Flatten employee API response into a dimension row."""
    return {
        'employee_guid': emp.get('guid'),
        'restaurant_guid': restaurant_guid,
        'first_name': emp.get('firstName'),
        'last_name': emp.get('lastName'),
        'email': emp.get('email'),
        'external_id': emp.get('externalId') or emp.get('externalEmployeeId'),
        'is_deleted': emp.get('deleted', False),
        '_loaded_at': datetime.utcnow().isoformat() + 'Z',
    }


def flatten_job(job, restaurant_guid):
    """Flatten job API response into a dimension row."""
    return {
        'job_guid': job.get('guid'),
        'restaurant_guid': restaurant_guid,
        'job_title': job.get('title') or job.get('name'),
        'default_wage': job.get('defaultWage', 0) or 0,
        'tipped': job.get('tipped', False),
        'is_deleted': job.get('deleted', False),
        '_loaded_at': datetime.utcnow().isoformat() + 'Z',
    }


def flatten_menu_items(menus, restaurant_guid):
    """
    Flatten nested menu structure into individual item rows.

    Toast menus structure: menus[] -> groups[] -> items[]
    Handles both key naming conventions (groups/menuGroups, items/menuItems).
    Each item becomes 1 row in dim_menu_items.
    """
    rows = []

    def _extract_items_from_group(group, menu_name):
        """Extract items from a group, handling nested subgroups."""
        group_name = group.get('name')

        # Items can be under 'items', 'menuItems', or 'menuItem'
        items = group.get('menuItems') or group.get('items') or []
        for item in items:
            item_guid = item.get('guid')
            if not item_guid:
                continue

            sales_cat = item.get('salesCategory') or {}
            visibility = item.get('visibility')
            if isinstance(visibility, list):
                visibility = ','.join(str(v) for v in visibility)

            row = {
                'menu_item_guid': item_guid,
                'restaurant_guid': restaurant_guid,
                'menu_name': menu_name,
                'menu_group_name': group_name,
                'item_name': item.get('name'),
                'price': item.get('price', 0) or 0,
                'sales_category_name': sales_cat.get('name'),
                'visibility': str(visibility) if visibility else None,
                'is_deleted': item.get('deleted', False),
                '_loaded_at': datetime.utcnow().isoformat() + 'Z',
            }
            rows.append(row)

        # Handle nested subgroups
        subgroups = group.get('subgroups') or group.get('menuGroups') or []
        for subgroup in subgroups:
            _extract_items_from_group(subgroup, menu_name)

    for menu in menus:
        menu_name = menu.get('name')

        # Groups can be under 'groups', 'menuGroups', or 'menus'
        groups = menu.get('menuGroups') or menu.get('groups') or []
        for group in groups:
            _extract_items_from_group(group, menu_name)

    return rows


@functions_framework.http
def config_weekly(request):
    """
    Cloud Function entry point for config/dimension ETL.

    Fetches current state of restaurants, employees, jobs, and menu items.
    Full refresh (WRITE_TRUNCATE) on each dimension table.

    Request JSON (all optional):
        restaurant_guid: Single GUID (default: all restaurants)
    """
    try:
        request_json = request.get_json(silent=True) or {}
        restaurant_guid = request_json.get('restaurant_guid')

        if restaurant_guid:
            guids = [restaurant_guid]
        else:
            guids = RESTAURANT_GUIDS

        logger.info(f"Config ETL: {len(guids)} restaurant(s)")

        client_id = get_secret(f'TOAST_CLIENT_ID{SECRET_SUFFIX}')
        client_secret = get_secret(f'TOAST_CLIENT_SECRET{SECRET_SUFFIX}')
        if not client_id or not client_secret:
            return _error_response('Failed to retrieve Toast credentials'), 500

        toast_client = ToastAPIClient(client_id, client_secret)

        all_restaurants = []
        all_employees = []
        all_jobs = []
        all_menu_items = []
        all_errors = []

        for guid in guids:
            logger.info(f"Fetching config for restaurant: {guid}")

            # Fetch restaurant info
            raw_info, info_errors = toast_client.fetch_restaurant_info(guid)
            all_errors.extend(info_errors)
            if raw_info:
                row = flatten_restaurant(raw_info[0], guid)
                if row:
                    all_restaurants.append(row)

            # Fetch employees
            raw_employees, emp_errors = toast_client.fetch_employees(guid)
            all_errors.extend(emp_errors)
            for emp in raw_employees:
                row = flatten_employee(emp, guid)
                if row.get('employee_guid'):
                    all_employees.append(row)

            # Fetch jobs
            raw_jobs, job_errors = toast_client.fetch_jobs(guid)
            all_errors.extend(job_errors)
            for job in raw_jobs:
                row = flatten_job(job, guid)
                if row.get('job_guid'):
                    all_jobs.append(row)

            # Fetch menus (60s rate limit - slow but fine for weekly)
            raw_menus, menu_errors = toast_client.fetch_menus(guid)
            all_errors.extend(menu_errors)
            if raw_menus:
                items = flatten_menu_items(raw_menus, guid)
                all_menu_items.extend(items)
                logger.info(f"Restaurant {guid}: {len(items)} menu items")

        # Load dimensions (full refresh)
        restaurants_loaded = 0
        employees_loaded = 0
        jobs_loaded = 0
        menu_items_loaded = 0

        if all_restaurants:
            loaded, load_errors = load_dimension_to_bigquery(
                records=all_restaurants,
                table_name='dim_restaurants',
                schema=SCHEMA_DIM_RESTAURANTS,
            )
            all_errors.extend(load_errors)
            restaurants_loaded = loaded

        if all_employees:
            loaded, load_errors = load_dimension_to_bigquery(
                records=all_employees,
                table_name='dim_employees',
                schema=SCHEMA_DIM_EMPLOYEES,
            )
            all_errors.extend(load_errors)
            employees_loaded = loaded

        if all_jobs:
            loaded, load_errors = load_dimension_to_bigquery(
                records=all_jobs,
                table_name='dim_jobs',
                schema=SCHEMA_DIM_JOBS,
            )
            all_errors.extend(load_errors)
            jobs_loaded = loaded

        if all_menu_items:
            loaded, load_errors = load_dimension_to_bigquery(
                records=all_menu_items,
                table_name='dim_menu_items',
                schema=SCHEMA_DIM_MENU_ITEMS,
            )
            all_errors.extend(load_errors)
            menu_items_loaded = loaded

        result = {
            'status': 'success',
            'restaurants_processed': len(guids),
            'restaurants_loaded': restaurants_loaded,
            'employees_loaded': employees_loaded,
            'jobs_loaded': jobs_loaded,
            'menu_items_loaded': menu_items_loaded,
            'errors': all_errors,
        }

        logger.info(f"Config ETL complete: {restaurants_loaded} restaurants, {employees_loaded} employees, {jobs_loaded} jobs, {menu_items_loaded} menu items")
        return json.dumps(result), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        logger.error(f"Config ETL failed: {str(e)}")
        return _error_response(str(e)), 500


def _error_response(message):
    return json.dumps({'status': 'error', 'error': message}), {'Content-Type': 'application/json'}
