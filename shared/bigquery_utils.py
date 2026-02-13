"""BigQuery loading utilities with staging table + load job deduplication"""

import json
import tempfile
import time
import logging
from typing import Dict, List, Tuple
from google.cloud import bigquery

from .config import BQ_PROJECT_ID, BQ_DATASET_ID

logger = logging.getLogger(__name__)


def get_table_id(table_name: str) -> str:
    """Build fully qualified table ID"""
    return f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"


def load_to_bigquery(
    records: List[Dict],
    table_name: str,
    schema: List[bigquery.SchemaField],
    dedup_keys: List[str],
) -> Tuple[int, List[str]]:
    """
    Load records to BigQuery using staging table + load job with deduplication.

    Flow:
    1. Write records to NDJSON temp file
    2. BigQuery load job into staging table (free, no streaming cost)
    3. INSERT INTO fact SELECT * FROM staging WHERE NOT EXISTS (dedup on keys)
    4. Drop staging table

    Args:
        records: List of record dicts to load
        table_name: Short table name (e.g., 'fact_order_items')
        schema: BigQuery schema fields
        dedup_keys: List of column names for deduplication

    Returns:
        Tuple of (rows_inserted, errors_list)
    """
    if not records:
        return 0, []

    errors = []
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = get_table_id(table_name)
    staging_id = f"{table_id}_staging_{int(time.time())}"

    try:
        # Step 1: Ensure main table exists
        _ensure_table_exists(client, table_id, schema)

        # Step 2: Write NDJSON to temp file and load into staging table
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            for record in records:
                f.write(json.dumps(record, default=str) + '\n')
            temp_path = f.name

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(temp_path, 'rb') as f:
            load_job = client.load_table_from_file(f, staging_id, job_config=job_config)
            load_job.result()

        staging_rows = load_job.output_rows
        logger.info(f"Loaded {staging_rows} rows into staging table")

        # Step 3: INSERT with dedup (only rows that don't already exist)
        on_condition = ' AND '.join(
            [f'main.{k} = staging.{k}' for k in dedup_keys]
        )

        insert_query = f"""
        INSERT INTO `{table_id}`
        SELECT staging.*
        FROM `{staging_id}` AS staging
        WHERE NOT EXISTS (
            SELECT 1 FROM `{table_id}` AS main
            WHERE {on_condition}
        )
        """

        logger.info(f"Dedup insert into {table_name} on keys: {dedup_keys}")
        query_job = client.query(insert_query)
        query_job.result()

        rows_inserted = query_job.num_dml_affected_rows or 0
        duplicates_skipped = staging_rows - rows_inserted

        logger.info(f"Inserted {rows_inserted} new rows, skipped {duplicates_skipped} duplicates")

        # Step 4: Drop staging table
        client.delete_table(staging_id, not_found_ok=True)

        return rows_inserted, errors

    except Exception as e:
        error_msg = f"BigQuery load error: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        # Clean up staging table on error
        try:
            client.delete_table(staging_id, not_found_ok=True)
        except Exception:
            pass
        return 0, errors


def load_dimension_to_bigquery(
    records: List[Dict],
    table_name: str,
    schema: List[bigquery.SchemaField],
) -> Tuple[int, List[str]]:
    """
    Load dimension records using full refresh (WRITE_TRUNCATE).

    Replaces the entire table contents each run. Used for dimension tables
    that represent current state (employees, jobs, restaurants).

    Args:
        records: List of record dicts to load
        table_name: Short table name (e.g., 'dim_employees')
        schema: BigQuery schema fields

    Returns:
        Tuple of (rows_loaded, errors_list)
    """
    if not records:
        return 0, []

    errors = []
    client = bigquery.Client(project=BQ_PROJECT_ID)
    table_id = get_table_id(table_name)

    try:
        # Ensure table exists
        _ensure_table_exists(client, table_id, schema)

        # Write NDJSON to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            for record in records:
                f.write(json.dumps(record, default=str) + '\n')
            temp_path = f.name

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(temp_path, 'rb') as f:
            load_job = client.load_table_from_file(f, table_id, job_config=job_config)
            load_job.result()

        rows_loaded = load_job.output_rows
        logger.info(f"Dimension refresh: {rows_loaded} rows loaded to {table_name}")
        return rows_loaded, errors

    except Exception as e:
        error_msg = f"BigQuery dimension load error: {str(e)}"
        logger.error(error_msg)
        errors.append(error_msg)
        return 0, errors


def _ensure_table_exists(
    client: bigquery.Client,
    table_id: str,
    schema: List[bigquery.SchemaField],
):
    """Create table if it doesn't exist, with partitioning and clustering."""
    try:
        client.get_table(table_id)
    except Exception:
        logger.info(f"Creating table {table_id}")
        table = bigquery.Table(table_id, schema=schema)

        field_names = [f.name for f in schema]
        if 'business_date' in field_names:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field='business_date',
            )
        if 'restaurant_guid' in field_names:
            table.clustering_fields = ['restaurant_guid']

        client.create_table(table)
        logger.info(f"Created table {table_id} with {len(schema)} columns")
