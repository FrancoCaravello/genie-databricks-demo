# Demo Setup Guide — Step by Step

This document describes every step taken to build the Genie Databricks Demo from scratch, in chronological order.

---

## Phase 1 — Initial Medallion Pipeline (single environment)

### 1.1 Source data
- Uploaded `sales_transactions.csv` (500 rows of synthetic sales data) to a Unity Catalog Volume at `genie_demo.de_demo.raw_files`.
- Columns: `order_id`, `order_date`, `customer_id`, `product_name`, `category`, `quantity`, `unit_price`, `discount_pct`, `payment_method`, `region`, `order_status`.

### 1.2 Bronze table
- Created `genie_demo.de_demo.bronze_sales_transactions` using `read_files()` with `inferColumnTypes=false`.
- All columns ingested as STRING to preserve raw fidelity.
- Added `_source_file_path` and `_ingested_at` ingestion metadata columns.
- Added column-level comments via `ALTER TABLE ... ALTER COLUMN ... COMMENT`.

### 1.3 Silver table
- Created `genie_demo.de_demo.silver_sales_transactions` by reading from bronze.
- Applied type casting: `order_date` → DATE, `quantity` → INT, `unit_price`/`discount_pct` → DECIMAL.
- Normalized `order_status` to UPPER CASE.
- Computed derived columns: `gross_amount = quantity × unit_price`, `net_amount = gross_amount × (1 − discount_pct/100)`.

### 1.4 Gold table
- Created `genie_demo.de_demo.gold_sales_summary` by aggregating silver.
- Filter: only `order_status = 'COMPLETED'`.
- Grain: `order_date × region × category`.
- Metrics: `total_transactions`, `total_units_sold`, `total_gross_revenue`, `total_net_revenue`, `avg_transaction_value`.

---

## Phase 2 — Git Integration & Job Orchestration

### 2.1 GitHub connection
- Configured a GitHub Personal Access Token (PAT) in Databricks Settings → Linked Accounts.
- Scopes used: `repo`.

### 2.2 Git folder
- Created a Databricks Git folder linked to `https://github.com/FrancoCaravello/genie-databricks-demo` (branch: `main`).
- Path in workspace: `/Users/franco.caravello@piconsulting.com.ar/genie-databricks-demo`.

### 2.3 Notebooks versioned in Git
- Created 4 notebooks in the Git folder:
  - `nb_ingest_bronze` — bronze ingestion
  - `nb_transform_silver` — silver transformation
  - `nb_build_gold` — gold aggregation
  - `nb_validate` — end-to-end validation (Python assertions that fail the job if any check fails)
- Committed and pushed to `main`.

### 2.4 Lakeflow Job
- Created job `genie_demo_medallion_prd` (originally named `genie_demo_medallion_daily`).
- 4 sequential tasks: `ingest_bronze → transform_silver → build_gold → validate`.
- Each task is a Git-sourced notebook.
- Schedule: daily at 06:00 UTC.
- Compute: serverless.

### 2.5 Workspace organization
- Moved original loose notebooks to `_scratch/` folder.
- Convention established: each project lives in its own Git folder at the workspace root.

---

## Phase 3 — Environment Separation (DEV / QAS / PRD)

### 3.1 Decision: separate catalogs
- Chose separate Unity Catalog catalogs per environment (vs. schemas under one catalog).
- Reason: stronger isolation, catalog-level RBAC, enterprise-aligned.

### 3.2 Decision: separate Git branches
- Three long-lived branches: `dev`, `qas`, `prd`.
- `dev` is for active development. `qas` and `prd` only receive changes via Pull Request.
- Promotion flow: `dev → qas → prd`.

### 3.3 Unity Catalog infrastructure
- Created 3 catalogs with `MANAGED LOCATION` pointing to the existing ADLS storage:
  - `genie_demo_dev`, `genie_demo_qas`, `genie_demo_prd`
- In each catalog:
  - Schema: `de_demo`
  - Volume: `raw_files`
  - CSV copied from `genie_demo.de_demo.raw_files` to each new volume.

### 3.4 Git branches
- Created branches `dev`, `qas`, `prd` from `main`.
- Each branch gets its own `conf/env.json`:

```
dev:  { "catalog": "genie_demo_dev", "schema": "de_demo", "volume_path": "/Volumes/genie_demo_dev/de_demo/raw_files" }
qas:  { "catalog": "genie_demo_qas", ... }
prd:  { "catalog": "genie_demo_prd", ... }
```

### 3.5 Notebook parameterization
- Added an **Environment Setup** Python cell at the top of each notebook:
  - Reads `conf/env.json`
  - Runs `USE CATALOG` and `USE SCHEMA` so all `%sql` cells use unqualified table names
  - Sets a `volume_path` widget for use in `read_files()`
- Replaced all hardcoded `genie_demo.de_demo.<table>` references in SQL cells with unqualified names.
- Changes committed to `dev`, `qas`, and `prd` branches independently.

### 3.6 .gitattributes
- Added `.gitattributes` to all 3 branches:
  ```
  conf/env.json merge=ours
  ```
- Prevents Git from auto-merging `conf/env.json` in future PRs.
- Each branch must always keep its own version of this file.

### 3.7 Lakeflow Jobs per environment

| Job | Branch | Schedule |
|-----|--------|----------|
| `genie_demo_medallion_dev` | `dev` | Manual only |
| `genie_demo_medallion_qas` | `qas` | Daily 02:00 UTC |
| `genie_demo_medallion_prd` | `prd` | Daily 06:00 UTC |

### 3.8 UC grants

Applied explicit grants for `franco.caravello@piconsulting.com.ar`:

| Catalog | Grants |
|---------|--------|
| `genie_demo_dev` | USE CATALOG, USE SCHEMA, SELECT, MODIFY |
| `genie_demo_qas` | USE CATALOG, USE SCHEMA, SELECT |
| `genie_demo_prd` | USE CATALOG, USE SCHEMA, SELECT |

> In a production setup, MODIFY on QAS/PRD would be restricted to a service principal that runs the jobs.

---

## Current State Summary

### Workspace
```
/Users/franco.caravello@piconsulting.com.ar/
├── genie-databricks-demo/   ← Git folder (GitHub)
└── _scratch/                ← pre-Git notebook versions (reference only)
```

### GitHub Repository
```
https://github.com/FrancoCaravello/genie-databricks-demo
Branches: main, dev, qas, prd
```

### Unity Catalog Tables

| Catalog | Schema | Table |
|---------|--------|-------|
| `genie_demo` | `de_demo` | `bronze_sales_transactions`, `silver_sales_transactions`, `gold_sales_summary` |
| `genie_demo_dev` | `de_demo` | (populated when DEV job runs) |
| `genie_demo_qas` | `de_demo` | (populated when QAS job runs) |
| `genie_demo_prd` | `de_demo` | (populated when PRD job runs) |

### Lakeflow Jobs

| Job ID | Name | Branch | Schedule |
|--------|------|--------|----------|
| 972837798198719 | `genie_demo_medallion_dev` | `dev` | Manual |
| 631809223504978 | `genie_demo_medallion_qas` | `qas` | Daily 02:00 UTC |
| 878443287805223 | `genie_demo_medallion_prd` | `prd` | Daily 06:00 UTC |

---

## Next Steps (not yet implemented)

- [ ] Run DEV job end-to-end to validate parameterization works
- [ ] Set up service principals with restricted MODIFY on QAS/PRD
- [ ] Add GitHub branch protection rules on `qas` and `prd`
- [ ] Add email notifications to QAS/PRD jobs on failure
- [ ] Genie Space for business users to query gold tables
