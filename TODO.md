# Toast ETL Pipeline - Project Status

## Current Status: PHASE 1 - Clean Restart (In Progress)

### What's Done
- [x] Archived 40+ legacy files to `archive/` folder
- [x] Clean directory: only `shared/`, `requirements.txt`, `README.md` remain
- [x] Identified production-ready code in `shared/toast_client.py` (OAuth, rate limiting, retry)
- [x] Plan approved (see `.claude/plans/logical-coalescing-dove.md`)
- [x] Updated `shared/config.py` - added `SCHEMA_FACT_ORDER_ITEMS` (21 fields)
- [x] Refactored `shared/bigquery_utils.py` - staging table + load job pattern
- [x] Renamed `shared/secrets.py` to `shared/secrets_utils.py`
- [x] Created `main_orders.py` - entry point `orders_daily(request)`
- [x] Updated `README.md` and `.gcloudignore`

### What's Next
- [x] Created `deploy_all.sh` - deploy script
- [x] Created `main.py` - thin wrapper that re-exports Cloud Function entry points
- [x] Deployed `toast-orders-etl` and tested 1 restaurant x 1 day (758 rows loaded, dedup verified)
- [x] Dropped old `fact_order_items` table (had wrong schema from old deployment)
- [x] Created `backfill_all.py` - orchestrate historical load
- [ ] Run historical backfill (Jan 2025 to present)
- [ ] Create `main_cash.py`, `main_labor.py`, `main_config.py` (after orders works)
- [ ] Setup Cloud Schedulers (daily 2AM)

---

## Key Decisions

### Architecture: 4 Independent Cloud Functions
| Function | Entry Point | Target Table | Schedule |
|----------|-------------|-------------|----------|
| `toast-orders-etl` | `orders_daily()` | `fact_order_items` | Daily 2:00 AM |
| `toast-cash-etl` | `cash_daily()` | `fact_cash_entries`, `fact_cash_deposits` | Daily 2:15 AM |
| `toast-labor-etl` | `labor_daily()` | `fact_labor_shifts` | Daily 2:30 AM |
| `toast-config-etl` | `config_weekly()` | `dim_*` tables (SCD2) | Weekly Sun 3 AM |

### 1 Restaurant Per Call (Avoids Timeouts)
- Each function call processes **1 restaurant only**
- Request format: `{"restaurant_guid": "abc-123", "start_date": "2025-01-01", "end_date": "2025-01-07"}`
- Daily scheduler uses `"restaurant_guid": "ALL"` (1 day x 13 restaurants = ~3 min, safe)
- Backfill orchestrator sends 780 calls (13 restaurants x 60 weeks)

### Direct-to-Fact (No Raw Staging)
- Cloud Functions flatten API data and load directly into fact tables
- No intermediate `toast_orders_raw` table
- Flattening: `order -> check -> selection = 1 fact row`

### Staging Table + Load Job (Not MERGE Per Call)
- Write records to NDJSON -> BigQuery load job into temp staging table (free)
- `INSERT INTO fact SELECT * FROM staging WHERE NOT EXISTS (dedup on keys)`
- Drop staging table after

### Dedup Keys
| Table | Dedup Key |
|-------|-----------|
| `fact_order_items` | `selection_guid, order_guid` |
| `fact_cash_entries` | `cash_entry_guid` |
| `fact_cash_deposits` | `deposit_guid` |
| `fact_labor_shifts` | `time_entry_guid` |

---

## GCP Resources

| Resource | Value |
|----------|-------|
| Project | `possible-coast-439421-q5` |
| Dataset | `purpose` |
| Region | `us-west1` |
| Runtime | Python 3.12 |
| Memory | 1GB (orders), 512MB (others) |
| Timeout | 540s |
| Secrets | `TOAST_CLIENT_ID`, `TOAST_CLIENT_SECRET` (Secret Manager) |

## Restaurants: 13 Locations
All GUIDs in `shared/config.py` -> `RESTAURANT_GUIDS`

## Rate Limits (Toast API)
| Endpoint | Delay | Notes |
|----------|-------|-------|
| `/orders/v2/ordersBulk` | 12s | 5 req/min per location |
| `/cashmgmt/v1/entries` | 3s | Conservative |
| `/labor/v1/timeEntries` | 3s | Conservative |
| `/menus/v3/menus` | 60s | 1 req/sec STRICT |
| `/config/*` | 3s | Conservative |

---

## Known Issues
- Cash API (`/cashmgmt/v1/entries`) returns 400 errors - needs debugging
- Labor API (`/labor/v1/timeEntries`) returns 0 records - needs debugging
- Focus on orders first, debug cash/labor later

## File Structure
```
toast-api/
├── shared/
│   ├── __init__.py          # DONE
│   ├── toast_client.py      # DONE - OAuth, rate limiting, retry
│   ├── bigquery_utils.py    # DONE - Staging + load job pattern
│   ├── secrets_utils.py     # DONE - Secret Manager helper
│   ├── config.py            # DONE - Schemas + constants
│   └── date_utils.py        # DONE - Timestamp normalization
├── main.py                  # DONE - re-exports entry points for Cloud Functions
├── main_orders.py           # DONE - orders_daily()
├── main_cash.py             # TODO - cash_daily()
├── main_labor.py            # TODO - labor_daily()
├── main_config.py           # TODO - config_weekly()
├── deploy_all.sh            # DONE
├── backfill_all.py          # DONE
├── requirements.txt         # DONE
├── README.md                # DONE
├── TODO.md                  # THIS FILE
└── archive/                 # All legacy files
```
