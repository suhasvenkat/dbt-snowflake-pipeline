# dbt Snowflake Data Pipeline

[![dbt](https://img.shields.io/badge/dbt-Core-FF694B?style=flat&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?style=flat&logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![AWS S3](https://img.shields.io/badge/AWS-S3%20Source-FF9900?style=flat&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/s3/)
[![SQL](https://img.shields.io/badge/SQL-Advanced-336791?style=flat)](https://www.postgresql.org/)

End-to-end analytics engineering pipeline on the modern data stack. Implements medallion architecture (bronze/silver/gold), SCD Type 2 historical tracking, incremental models, and automated dbt tests.

Built to demonstrate the architecture I used at TCS to standardize KPIs across 4 business units — reducing ad-hoc data requests by 40%.

---

## Architecture

```
AWS S3 (raw source data)
        │
        ▼  (Snowflake COPY INTO / external stage)
┌─────────────────────────────────────────────┐
│              SNOWFLAKE                       │
│                                             │
│  BRONZE          SILVER          GOLD       │
│  ───────         ───────         ────────   │
│  Raw copy   →   Cleaned    →   KPI-ready   │
│  No transform    Typed          Aggregated  │
│  Full audit      Deduped        BI-ready    │
│  trail           Business       Fact +      │
│                  logic starts   Dim tables  │
└─────────────────────────────────────────────┘
        │
        ▼  (dbt models + tests + docs)
        │
        ▼
Power BI / Tableau / BI tools
```

**Design decisions:**
- **Bronze** — raw source copy, zero transformations, full audit trail preserved
- **Silver** — cleaned, typed, deduplicated; all business logic lives here
- **Gold** — aggregated KPI tables, optimized for BI query patterns
- **SCD Type 2** — full history on customer/product dimensions via `dbt snapshot`
- **Incremental** — fact tables only process new/updated records

---

## Project structure

```
dbt-snowflake-pipeline/
├── models/
│   ├── bronze/
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── stg_products.sql
│   ├── silver/
│   │   ├── int_orders_cleaned.sql
│   │   └── int_customers_enriched.sql
│   └── gold/
│       ├── dim_customer.sql
│       ├── dim_product.sql
│       └── fct_revenue.sql
├── snapshots/
│   └── customer_snapshot.sql
├── tests/
│   └── assert_revenue_positive.sql
├── macros/
│   └── generate_schema_name.sql
├── seeds/
│   └── date_spine.csv
├── dbt_project.yml
├── profiles.yml.example
└── README.md
```

---

## Key models explained

### Bronze — `stg_orders.sql`
Raw copy of source data with column renames and basic type casting only. No business logic.

### Silver — `int_orders_cleaned.sql`
Removes duplicates, handles nulls, applies business rules (e.g., exclude test orders, normalize statuses).

### Gold — `fct_revenue.sql` (incremental)
Revenue fact table, only processes records newer than last run. Joins to dim tables for enriched analysis.

### Gold — `dim_customer.sql`
Customer dimension table. Flat snapshot of current state (use `customer_snapshot` for history).

### Snapshot — `customer_snapshot.sql`
SCD Type 2 history of customer records. Every change creates a new row with `dbt_valid_from` / `dbt_valid_to`. Full history queryable.

---

## Setup

### Prerequisites
- Python 3.9+
- dbt Core with Snowflake adapter: `pip install dbt-snowflake`
- Snowflake account (free trial works)

### 1. Clone
```bash
git clone https://github.com/suhasvenkat/dbt-snowflake-pipeline.git
cd dbt-snowflake-pipeline
pip install dbt-snowflake
```

### 2. Configure Snowflake connection
```bash
cp profiles.yml.example ~/.dbt/profiles.yml
```

Edit `~/.dbt/profiles.yml`:
```yaml
dbt_snowflake_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>        # e.g. xy12345.eu-west-1
      user: <your_username>
      password: <your_password>
      role: TRANSFORMER
      database: ANALYTICS_DEV
      warehouse: COMPUTE_WH
      schema: DBT_DEV
      threads: 4
```

### 3. Test connection
```bash
dbt debug
```

### 4. Run the pipeline
```bash
# Full run — all layers
dbt run

# Run tests
dbt test

# Run specific layer
dbt run --select bronze
dbt run --select silver
dbt run --select gold

# Run snapshot (SCD Type 2)
dbt snapshot

# Generate + view docs
dbt docs generate
dbt docs serve
```

---

## Data quality tests

Every model has automated tests that run on `dbt test`:

| Test | Models | What it checks |
|------|--------|----------------|
| `not_null` | All PKs | No null primary keys |
| `unique` | All PKs | No duplicate records |
| `accepted_values` | `order_status` | Only valid status codes |
| `relationships` | Fact → Dim | FK integrity |
| `assert_revenue_positive` | `fct_revenue` | Revenue > 0 (custom test) |

---

## Why this matters

At TCS, I built a version of this architecture to standardize KPIs across 4 business units. Before the semantic layer existed, every team had their own Excel definition of "revenue" — and they all disagreed. After centralizing metric logic in dbt, ad-hoc data requests dropped 40% because analysts had one trusted source to query instead of asking "which number is right?"

The key insight: **data quality is a modeling problem, not a tooling problem.** dbt tests + a governed layer make trust systematic.

---

## Author

**Suhas Venkat** — Analytics Engineer  
[suhasvenkat.github.io](https://suhasvenkat.github.io) · [linkedin.com/in/suhas-venkat](https://linkedin.com/in/suhas-venkat)
