"""
Complete ETL Pipeline - Transform Raw Orders to Dimensional Model
Runs all ETL SQL scripts in sequence
"""
from google.cloud import bigquery
import sys
import time
import io

# Force UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

PROJECT_ID = 'possible-coast-439421-q5'
DATASET_ID = 'purpose'

# Use ASCII-safe check/cross marks for Windows compatibility
CHECK_MARK = '[OK]'
CROSS_MARK = '[X]'

def run_sql_file(client, filename, description):
    """Execute SQL file and report results"""
    print(f"\n{'='*70}")
    print(f"{description}")
    print(f"{'='*70}")
    print(f"File: {filename}")
    print()

    try:
        with open(filename, 'r', encoding='utf-8') as f:
            sql = f.read()

        # Split by semicolons for multiple statements
        statements = [s.strip() for s in sql.split(';') if s.strip()]

        print(f"Executing {len(statements)} SQL statement(s)...")

        for i, statement in enumerate(statements, 1):
            if not statement or statement.startswith('--'):
                continue

            print(f"  [{i}/{len(statements)}] Running...", end=' ')
            start = time.time()

            try:
                job = client.query(statement)
                job.result()  # Wait for completion

                elapsed = time.time() - start
                print(f"OK ({elapsed:.1f}s)")

                # Show row counts if available
                if job.num_dml_affected_rows:
                    print(f"       -> {job.num_dml_affected_rows} rows affected")

            except Exception as e:
                print(f"FAILED")
                print(f"       Error: {str(e)}")
                return False

        print(f"\n{CHECK_MARK} {description} - COMPLETED")
        return True

    except FileNotFoundError:
        print(f"ERROR: File not found: {filename}")
        return False
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False

def check_raw_data(client):
    """Check if raw orders data exists"""
    print(f"\n{'='*70}")
    print("Checking Raw Orders Data")
    print(f"{'='*70}\n")

    try:
        query = f"""
        SELECT
            COUNT(*) as total_orders,
            MIN(businessDate) as earliest_date,
            MAX(businessDate) as latest_date,
            COUNT(DISTINCT restaurantGuid) as num_restaurants
        FROM `{PROJECT_ID}.{DATASET_ID}.toast_orders_raw`
        """

        results = client.query(query).result()
        row = next(results)

        print(f"  Total Orders: {row.total_orders:,}")
        print(f"  Date Range: {row.earliest_date} to {row.latest_date}")
        print(f"  Restaurants: {row.num_restaurants}")

        if row.total_orders == 0:
            print("\n  WARNING: No raw orders found!")
            print("  Run the Cloud Function first to load data")
            return False

        print(f"\n{CHECK_MARK} Raw data ready for transformation")
        return True

    except Exception as e:
        print(f"  ERROR: {str(e)}")
        print("\n  Make sure:")
        print("    1. gcloud auth application-default login")
        print("    2. Cloud Function has loaded data")
        return False

def main():
    print("="*70)
    print("Toast API ETL Pipeline")
    print("="*70)
    print(f"\nProject: {PROJECT_ID}")
    print(f"Dataset: {DATASET_ID}")
    print("\nThis will:")
    print("  1. Create dimensional tables")
    print("  2. Load dimensions (SCD Type 2)")
    print("  3. Load fact tables")
    print()

    # Skip prompt if --auto-run flag is provided
    if '--auto-run' not in sys.argv:
        try:
            input("Press Enter to continue or Ctrl+C to cancel...")
        except (EOFError, KeyboardInterrupt):
            print("\nProceeding automatically...")
            print()

    # Initialize BigQuery client
    try:
        print("\nInitializing BigQuery client...")
        client = bigquery.Client(project=PROJECT_ID)
        print(f"{CHECK_MARK} Connected to BigQuery")
    except Exception as e:
        print(f"\nERROR: Failed to connect to BigQuery")
        print(f"Error: {str(e)}")
        print("\nPlease run: gcloud auth application-default login")
        sys.exit(1)

    # Check raw data
    if not check_raw_data(client):
        print("\nExiting - raw data check failed")
        sys.exit(1)

    # Step 1: Create dimensional tables
    if not run_sql_file(
        client,
        'transform_to_dimensions.sql',
        'Step 1: Creating Dimensional Table Schemas'
    ):
        print("\nETL FAILED at Step 1")
        sys.exit(1)

    # Step 2: Load dimensions
    if not run_sql_file(
        client,
        'etl_load_dimensions.sql',
        'Step 2: Loading Dimension Tables (SCD Type 2)'
    ):
        print("\nETL FAILED at Step 2")
        sys.exit(1)

    # Step 3: Load facts
    if not run_sql_file(
        client,
        'etl_load_facts.sql',
        'Step 3: Loading Fact Tables'
    ):
        print("\nETL FAILED at Step 3")
        sys.exit(1)

    # Summary
    print(f"\n{'='*70}")
    print("ETL COMPLETE - SUCCESS!")
    print(f"{'='*70}\n")

    print("Dimensional Model Created:")
    print("  Dimensions:")
    print("    - DimLocation (Restaurant locations)")
    print("    - DimEmployee (Servers and staff)")
    print("    - DimJob (Job roles)")
    print("    - DimMenuItem (Menu items)")
    print()
    print("  Facts:")
    print("    - FactOrders (1 row per order)")
    print("    - FactChecks (1 row per guest check)")
    print("    - FactPayments (1 row per payment)")
    print("    - FactMenuSelection (1 row per item ordered)")
    print()

    # Show row counts
    print("Checking row counts...")
    tables = [
        'DimLocation', 'DimEmployee', 'DimJob', 'DimMenuItem',
        'FactOrders', 'FactChecks', 'FactPayments', 'FactMenuSelection'
    ]

    for table in tables:
        try:
            query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET_ID}.{table}`"
            result = client.query(query).result()
            count = next(result).cnt
            print(f"  {table}: {count:,} rows")
        except:
            print(f"  {table}: Not found")

    print()
    print("Next steps:")
    print("  1. Connect Power BI to BigQuery dataset 'purpose'")
    print("  2. Setup Cloud Scheduler: bash setup_scheduler.sh")
    print("  3. Create analytical views: Run powerbi_queries.sql")
    print()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
