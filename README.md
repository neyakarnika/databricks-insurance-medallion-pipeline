# Databricks Insurance Medallion Pipeline
Medallion architecture (Bronze/Silver/Gold) ETL pipeline built on Databricks

A mini end-to-end data pipeline built on Databricks Community Edition, using Delta Lake and the medallion architecture.

## Architecture
Bronze (raw ingestion) → Silver (cleansed, deduplicated, MERGE-based upserts) → Gold (business aggregates)

## Highlights
- Synthetic insurance data (customers, policies, claims)
- Delta Lake MERGE INTO for incremental/CDC-style upserts
- Data quality checks: row count reconciliation, null checks, referential integrity
- Gold-layer business aggregates: claims by region, loss ratio by policy type, fraud summary
- Orchestrated via Databricks Workflows (Bronze → Silver → Gold)

## Tech stack
Databricks, PySpark, Spark SQL, Delta Lake, Databricks Workflows
