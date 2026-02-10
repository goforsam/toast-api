"""BigQuery loading utilities with MERGE deduplication pattern"""

import time
import logging
from typing import Dict, List, Tuple
from google.cloud import bigquery

from .config import BQ_PROJECT_ID

logger = logging.getLogger(__name__)


def load_to_bigquery_with_dedup(
    records: List[Dict],
    table_id: str,
    schema: List[bigquery.SchemaField] = None,
    dedup_key: str = None
) -> Tuple[int, List[str]]:
    """
    Load records to BigQuery with deduplication using MERGE

    Args:
        records: List of record dicts
        table_id: Fully qualified table ID (project.dataset.table)
        schema: BigQuery schema (optional, uses table's existing schema if not provided)
        dedup_key: Primary key field name for deduplication (default: 'guid')

    Returns:
        Tuple of (rows loaded, errors list)
    """
    if not records:
        return 0, []

    if dedup_key is None:
        dedup_key = 'guid'

    errors = []
    client = bigquery.Client(project=BQ_PROJECT_ID)

    try:
        # Load to temporary staging table
        temp_table_id = f"{table_id}_temp_{int(time.time())}"

        # Use provided schema or fetch from existing table
        if schema is None:
            try:
                table = client.get_table(table_id)
                schema = table.schema
            except Exception:
                raise ValueError(f"No schema provided and table {table_id} doesn't exist")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        logger.info(f"Loading {len(records)} records to staging table {temp_table_id}")
        load_job = client.load_table_from_json(records, temp_table_id, job_config=job_config)
        load_job.result()

        # Ensure main table exists with correct schema before MERGE
        try:
            table = client.get_table(table_id)
            # Check if schema matches (column count)
            if len(table.schema) != len(schema):
                logger.warning(f"Schema mismatch: table has {len(table.schema)} columns, expected {len(schema)}. Recreating table.")
                client.delete_table(table_id)
                table = bigquery.Table(table_id, schema=schema)
                client.create_table(table)
                logger.info(f"Recreated table {table_id} with correct schema")
            else:
                logger.info(f"Main table {table_id} exists with correct schema")
        except Exception as e:
            logger.info(f"Creating main table {table_id} with {len(schema)} columns")
            table = bigquery.Table(table_id, schema=schema)
            client.create_table(table)

        # MERGE from staging to main table (deduplication)
        # Support composite keys if dedup_key contains comma
        if ',' in dedup_key:
            # Composite key (e.g., "guid,restaurantGuid")
            keys = [k.strip() for k in dedup_key.split(',')]
            on_condition = ' AND '.join([f'T.{k} = S.{k}' for k in keys])
        else:
            # Single key
            on_condition = f'T.{dedup_key} = S.{dedup_key}'

        merge_query = f"""
        MERGE `{table_id}` T
        USING `{temp_table_id}` S
        ON {on_condition}
        WHEN NOT MATCHED THEN
          INSERT ROW
        """

        logger.info(f"Merging data to {table_id} with deduplication on {dedup_key}")
        merge_job = client.query(merge_query)
        merge_job.result()

        rows_affected = merge_job.num_dml_affected_rows or 0

        # Clean up temp table
        client.delete_table(temp_table_id, not_found_ok=True)

        logger.info(f"Successfully loaded {rows_affected} new records (deduplicated)")
        return rows_affected, errors

    except Exception as e:
        error_msg = f"BigQuery load error: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        return 0, errors
