# Genie Databricks Demo — Medallion Pipeline

Standard Databricks medallion architecture demo with environment separation (DEV / QAS / PRD), Git-versioned notebooks, and orchestration via Lakeflow Jobs.

---

## Architecture Overview

```
UC Volume (raw_files)
       │
       ▼
 [nb_ingest_bronze]   → bronze_sales_transactions   (raw CSV, all STRING)
       │
       ▼
 [nb_transform_silver] → silver_sales_transactions  (typed, cleaned, computed)
       │
       ▼
 [nb_build_gold]       → gold_sales_summary          (aggregated by date/region/category)
       │
       ▼
 [nb_validate]         → assertions (fails job if checks don't pass)
```

### Medallion Layers

| Layer  | Table                        | Description |
|--------|------------------------------|-------------|
| Bronze | `bronze_sales_transactions`  | Raw CSV ingested as-is, all columns STRING. Adds `_source_file_path` and `_ingested_at` metadata. |
| Silver | `silver_sales_transactions`  | Type-cast (DATE, INT, DECIMAL), trimmed strings, UPPER-CASE status, computed `gross_amount` and `net_amount`. |
| Gold   | `gold_sales_summary`         | Daily aggregation by `region` × `category`. Only `COMPLETED` orders. Metrics: transactions, units, gross/net revenue, avg value. |

---

## Environment Separation

Three isolated environments, each with its own Unity Catalog catalog:

| Environment | Catalog          | Git Branch | Job                          | Schedule        |
|-------------|------------------|------------|------------------------------|-----------------|
| DEV         | `genie_demo_dev` | `dev`      | `genie_demo_medallion_dev`   | Manual only     |
| QAS         | `genie_demo_qas` | `qas`      | `genie_demo_medallion_qas`   | Daily 02:00 UTC |
| PRD         | `genie_demo_prd` | `prd`      | `genie_demo_medallion_prd`   | Daily 06:00 UTC |

### Unity Catalog Asset Inventory

Each environment has the identical structure:

```
<catalog>.de_demo.raw_files          ← UC Volume (source CSV)
<catalog>.de_demo.bronze_sales_transactions
<catalog>.de_demo.silver_sales_transactions
<catalog>.de_demo.gold_sales_summary
```

Source file: `sales_transactions.csv` (500 rows, synthetic sales data)

---

## Repository Structure

```
genie-databricks-demo/
├── nb_ingest_bronze.py       # Bronze ingestion notebook
├── nb_transform_silver.py    # Silver transformation notebook
├── nb_build_gold.py          # Gold aggregation notebook
├── nb_validate.py            # End-to-end validation notebook
├── conf/
│   └── env.json              # Environment config (catalog, schema, volume_path)
│                             # ⚠️  DO NOT MERGE between branches — intentionally different per branch
├── .gitattributes            # Prevents auto-merge of conf/env.json
├── README.md                 # This file
└── DEMO_GUIDE.md             # Step-by-step setup guide
```

---

## Notebook Details

### nb_ingest_bronze
Reads `sales_transactions.csv` from the UC Volume using `read_files()` with `inferColumnTypes=false`. All columns land as STRING to preserve raw fidelity. Adds ingestion metadata (`_source_file_path`, `_ingested_at`). Adds column-level comments via `ALTER TABLE`.

### nb_transform_silver
Reads from bronze. Applies: type casting (STRING → DATE/INT/DECIMAL), string trimming, UPPER-CASE normalization of `order_status`. Computes `gross_amount = quantity × unit_price` and `net_amount = gross_amount × (1 − discount_pct/100)`.

### nb_build_gold
Reads from silver. Filters to `order_status = 'COMPLETED'`. Aggregates by `order_date × region × category`: total transactions, units sold, gross/net revenue, avg transaction value.

### nb_validate
Runs 5 assertions that fail the Databricks Job if any check does not pass:
1. Bronze table has rows
2. Silver row count equals bronze row count
3. No NULLs in silver key columns (`order_id`, `order_date`, `net_amount`)
4. Gold table has rows
5. Gold net revenue reconciles with silver completed net revenue (diff = 0)

---

## Environment Configuration

Each branch has a `conf/env.json` that tells the notebooks which catalog to use:

```json
{
  "catalog": "genie_demo_qas",
  "schema": "de_demo",
  "volume_path": "/Volumes/genie_demo_qas/de_demo/raw_files"
}
```

Notebooks read this file at runtime via a Python setup cell and issue `USE CATALOG` / `USE SCHEMA` so all SQL cells use unqualified table names.

---

## Promotion Workflow

```
developer works on dev branch
        │
        ▼
  opens PR: dev → qas
  (resolve conf/env.json conflict: keep qas version)
        │
        ▼
  QAS job validates ✓
        │
        ▼
  opens PR: qas → prd
  (resolve conf/env.json conflict: keep prd version)
        │
        ▼
  PRD job runs on schedule
```

> **Note on conf/env.json conflicts:** this file is intentionally different per branch. When resolving PR conflicts, always keep the **target branch** version. The `.gitattributes` file marks it with `merge=ours` to prevent silent overwrites.

---

## UC Access Model

| Principal                           | DEV                  | QAS     | PRD     |
|-------------------------------------|----------------------|---------|---------|
| `franco.caravello@piconsulting.com.ar` | USE + SELECT + MODIFY | USE + SELECT | USE + SELECT |
| Job runner (same user, demo)         | MODIFY               | MODIFY  | MODIFY  |

In a production setup, MODIFY on QAS/PRD would be restricted to a dedicated service principal.

---

## How to Run

1. Open the `genie_demo_medallion_qas` job
2. Click **Run now**
3. Monitor the 4 tasks: `ingest_bronze → transform_silver → build_gold → validate`
4. If `validate` passes, all 5 assertions are green
5. Check the tables at `genie_demo_qas.de_demo.*`
