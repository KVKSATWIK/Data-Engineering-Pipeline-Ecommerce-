# End-to-End E-Commerce Data Engineering Pipeline

A hands-on Data Engineering project that extracts e-commerce data from PostgreSQL, transforms it using PySpark, validates data quality, and orchestrates the workflow with Apache Airflow.

## Overview

This project demonstrates a data pipeline from relational database extraction to an analytics-ready dataset. Airflow coordinates the workflow through 11 dependent tasks.

## Architecture

PostgreSQL → Python Ingestion → Raw Data Validation → PySpark Transformations → Data Quality Validation → Analytics Dataset

Apache Airflow orchestrates the workflow and manages task dependencies.

## Technology Stack

- PostgreSQL
- Python
- PySpark
- Apache Airflow
- CSV
- Windows and Ubuntu on WSL

## Data Sources

The pipeline extracts data from five PostgreSQL tables:

- Customers
- Products
- Orders
- Order Items
- Payments

## Pipeline Workflow

The `ecommerce_pipeline` DAG contains 11 tasks:

1. Extract data from PostgreSQL.
2. Validate raw CSV files.
3. Transform customer data.
4. Transform product data.
5. Transform order data.
6. Transform order-item data.
7. Transform payment data.
8. Validate processed tables.
9. Build the analytics order-line dataset.
10. Validate the analytics dataset.
11. Validate order/payment consistency.

Tasks execute according to their dependencies. A failed task stops downstream execution.

## Data Quality

The project validates raw files, processed tables, analytics output, and order/payment consistency. Inconsistencies are reported for review.

## Output

The pipeline generates an analytics-ready order-line CSV dataset for downstream analysis and business intelligence.

## Project Status

- [x] PostgreSQL extraction
- [x] Raw data validation
- [x] PySpark transformations
- [x] Processed data validation
- [x] Analytics dataset generation
- [x] Airflow orchestration with 11 tasks
- [ ] Power BI dashboard
- [ ] Automated dashboard refresh
- [ ] Cloud deployment

## Local Setup

The project currently uses Windows and Ubuntu on WSL. PostgreSQL extraction runs in Windows, while PySpark processing runs in WSL.

To reproduce the workflow:

1. Install PostgreSQL and create the required database and tables.
2. Configure database credentials in a local `.env` file.
3. Install the required Python dependencies.
4. Configure WSL, Java, and PySpark.
5. Configure Apache Airflow and place the DAG in the Airflow DAGs directory.
6. Run the pipeline and inspect task statuses in the Airflow UI.

Local configuration, credentials, virtual environments, and generated datasets are excluded from the repository.

## Learning Outcomes

- Data ingestion from a relational database
- Data transformation with PySpark
- Data quality validation
- Analytics dataset generation
- Workflow orchestration with Apache Airflow
- Working across Windows and Linux environments

## Future Improvements

- Build a Power BI dashboard
- Configure automated dashboard refresh
- Add scheduled pipeline execution
- Explore cloud deployment
