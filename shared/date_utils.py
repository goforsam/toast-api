"""Date and timestamp normalization utilities"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)


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
