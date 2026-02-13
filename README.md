# Toast POS to BigQuery ETL Pipeline

Toast API data extraction and loading for 13 restaurant locations into BigQuery fact tables, feeding Power BI dashboards.

## Current Status: Phase 1 - Building Orders ETL

See [TODO.md](TODO.md) for detailed progress and next steps.

## Architecture

```
Toast APIs (13 restaurants)
    |
    v
4 Cloud Functions (daily, parallel)
    |
    v
BigQuery (possible-coast-439421-q5.purpose)
    |
    v
Power BI Dashboards
```

### Cloud Functions
| Function | Entry Point | Target Table | Schedule |
|----------|-------------|-------------|----------|
| `toast-orders-etl` | `orders_daily()` | `fact_order_items` | Daily 2:00 AM |
| `toast-cash-etl` | `cash_daily()` | `fact_cash_entries`, `fact_cash_deposits` | Daily 2:15 AM |
| `toast-labor-etl` | `labor_daily()` | `fact_labor_shifts` | Daily 2:30 AM |
| `toast-config-etl` | `config_weekly()` | `dim_*` tables (SCD2) | Weekly Sun 3 AM |

Each function call processes **1 restaurant x date range** to avoid timeouts.

### Request Format
```json
{
  "restaurant_guid": "def5e222-f458-41d0-bff9-48abaf20666a",
  "start_date": "2025-01-01",
  "end_date": "2025-01-07"
}
```

## Project Structure

```
toast-api/
├── shared/
│   ├── __init__.py          # Package init
│   ├── toast_client.py      # OAuth, rate limiting, retry, pagination
│   ├── bigquery_utils.py    # Staging table + load job with dedup
│   ├── secrets_utils.py     # GCP Secret Manager with env var fallback
│   ├── config.py            # Schemas, restaurant GUIDs, rate limits
│   └── date_utils.py        # Toast timestamp/date normalization
├── main_orders.py           # Orders ETL Cloud Function
├── deploy_all.sh            # Deploy script
├── backfill_all.py          # Historical load orchestrator
├── requirements.txt         # Python dependencies
├── TODO.md                  # Project status and next steps
├── README.md                # This file
└── archive/                 # Legacy files from previous iterations
```

## GCP Resources

| Resource | Value |
|----------|-------|
| Project | `possible-coast-439421-q5` |
| Dataset | `purpose` |
| Region | `us-west1` |
| Runtime | Python 3.12 |
| Secrets | `TOAST_CLIENT_ID`, `TOAST_CLIENT_SECRET` (Secret Manager) |

## Quick Start

```bash
# Deploy orders function
bash deploy_all.sh

# Test with 1 restaurant, 1 day
curl -X POST https://toast-orders-etl-xxx.run.app \
  -H 'Content-Type: application/json' \
  -d '{"restaurant_guid":"def5e222-f458-41d0-bff9-48abaf20666a","start_date":"2026-02-09","end_date":"2026-02-09"}'

# Historical backfill (all 2025 to present)
python backfill_all.py
```

## Resuming Work

When starting a new Claude Code session, say:

> "Read TODO.md and README.md, then continue with the next incomplete step."

This gives Claude full context of the project state, architecture decisions, and what to do next.
