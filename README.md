# Airflow Data Ingestion Pipelines

This repository contains Apache Airflow DAGs for automated data ingestion pipelines supporting multiple data sources and target databases (Snowflake and PostgreSQL).

## Overview

This project implements a robust ETL framework with custom operators for:
- Data acquisition from S3 buckets or local file systems
- Schema validation and data quality checks
- Multi-layer data loading (Mirror/Stage for Snowflake, Bronze/Silver for PostgreSQL)
- Automated data transformation and testing

## Architecture

The pipelines follow a multi-layer architecture:

**Snowflake Pipelines:**
- **Mirror Layer (MIRROR_DB.MIRROR)**: Raw data landing zone with temporal tables
- **Stage Layer (STAGE_DB.STAGE)**: Processed and validated data

**PostgreSQL Pipelines:**
- **Bronze Layer (ETL_PIPELINES_DB.BRONZE)**: Raw data landing zone
- **Silver Layer (ETL_PIPELINES_DB.SILVER)**: Processed and validated data

## Directory Structure

```
airflow_dags/
├── dags/                          # Airflow DAG definitions
│   ├── csse_covid_19_daily_reports_dag.py
│   ├── csse_covid_19_daily_reports_postgres_dag.py
│   ├── csse_covid_19_daily_reports_snowflake_dag.py
│   ├── netflix_movies_and_tv_shows_dag.py
│   ├── cricket_league_ipl_data_dag.py
│   ├── events_dag.py
│   ├── orders_dag.py
│   ├── order_items_dag.py
│   ├── products_dag.py
│   ├── reviews_dag.py
│   └── users_dag.py
├── configs/                       # Dataset configuration files
│   ├── {dataset_name}/
│   │   ├── {dataset_name}.json           # Main configuration
│   │   ├── mirror/                        # Mirror layer configs
│   │   └── stage/                         # Stage layer configs
├── ddls/                          # Database DDL scripts
│   ├── combined_ddl.sql                  # Combined DDL for all tables
│   ├── STG_*.sql                         # Stage table definitions
│   ├── T_ML_*.sql                        # Mirror/Bronze table definitions
│   └── T_STG_*.sql                       # Stage table definitions
├── snowflake_pipelines/          # Snowflake-specific pipeline configs
├── requirements.txt              # Python dependencies
└── README.md
```

## Available DAGs

### COVID-19 Data Pipelines
- **csse_covid_19_daily_reports_dag.py**: Base COVID-19 daily reports pipeline
- **csse_covid_19_daily_reports_postgres_dag.py**: COVID-19 data to PostgreSQL
- **csse_covid_19_daily_reports_snowflake_dag.py**: COVID-19 data to Snowflake

### Entertainment Data
- **netflix_movies_and_tv_shows_dag.py**: Netflix catalog data ingestion
- **cricket_league_ipl_data_dag.py**: IPL cricket league data
- **cricket_league_ipl_data_local_dag.py**: Local IPL data source

### E-commerce Data
- **orders_dag.py**: Orders data pipeline
- **order_items_dag.py**: Order items data pipeline
- **products_dag.py**: Products catalog pipeline
- **users_dag.py**: User data pipeline
- **reviews_dag.py**: Product reviews pipeline
- **events_dag.py**: Events tracking pipeline

## Custom Operators

The project uses custom operators from the `custom-operators` package:

**Acquisition & Download:**
- `AcquisitionOperator`: Check file availability on S3 or local filesystem
- `DownloadOperator`: Download files from S3 to Airflow temp area

**Snowflake Operators:**
- `MoveFileToSnowflakeOperator`: Move files to Snowflake internal stage
- `FileSnowflakeTableSchemaCheckOperator`: Validate schema against config
- `SnowflakeCopyOperator`: Copy data from stage to table
- `FileSnowflakeTableDataCheckOperator`: Validate data integrity
- `SnowflakeLoadToMirrorOperator`: Load data to mirror layer
- `SnowflakeLoadToStageOperator`: Load data to stage layer

**PostgreSQL Operators:**
- `FilePostgresTableSchemaCheckOperator`: Validate schema for PostgreSQL
- `CopyFileToPostgresOperator`: Copy file data to PostgreSQL
- `FilePostgresTableDataCheckOperator`: Validate PostgreSQL data integrity
- `PostgresLoadToMirrorOperator`: Load to bronze layer
- `PostgresMirrorTestsOperator`: Run mirror layer tests
- `PostgresLoadToStageOperator`: Load to silver layer
- `PostgresStageTestsOperator`: Run stage layer tests

## Configuration Structure

Each dataset has a JSON configuration file in the `configs/` directory:

```json
{
    "dataset_name": "dataset_name",
    "snowflake_stage_name": "STG_TABLE_NAME",
    "bucket": "s3-bucket-name",
    "tasks": ["task1", "task2", ...],
    "mirror_layer": {
        "database": "MIRROR_DB",
        "schema": "MIRROR"
    },
    "stage_layer": {
        "database": "STAGE_DB",
        "schema": "STAGE"
    },
    "s3_connection_id": "S3_CONN_ID",
    "db_conn_id": "SNOWFLAKE_CONN_ID",
    "start_date": "2021,1,1",
    "load_historical_data": true,
    "schedule_interval": "0 23 * * 1-5"
}
```

## Prerequisites

- Apache Airflow 2.x
- Python 3.8+
- Snowflake account (for Snowflake pipelines)
- PostgreSQL database (for PostgreSQL pipelines)
- AWS S3 access (for S3-based pipelines)
- Access to custom packages repository

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd airflow_dags
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure Airflow connections:
   - `S3_CONN_ID`: AWS S3 connection
   - `SNOWFLAKE_CONN_ID`: Snowflake database connection
   - `POSTGRES_CONN_ID`: PostgreSQL connection

4. Place the DAGs in your Airflow DAGs folder or configure Airflow to scan this directory.

5. Run database DDL scripts to create required tables:
```bash
# Execute combined_ddl.sql or individual DDL files as needed
```

## Usage

### Manual Trigger
DAGs can be manually triggered from the Airflow UI:

1. Go to Airflow UI
2. Select the DAG
3. Click "Trigger DAG"

### Scheduled Execution
DAGs are scheduled based on their `schedule_interval` parameter:
- COVID-19 Snowflake: `0 23 * * 1-5` (11 PM, Mon-Fri)
- Netflix: `@once` (manual trigger only)
- Others: Varies by DAG

## Data Flow

Typical pipeline flow:

1. **Acquisition**: Check for file availability (S3 or local)
2. **Download**: Download file to Airflow temporary area
3. **Schema Validation**: Validate file schema against configuration
4. **Load to Temporary**: Load data to temporary table
5. **Data Validation**: Verify data integrity
6. **Load to Mirror/Bronze**: Load to raw data layer
7. **Data Testing**: Run quality tests
8. **Load to Stage/Silver**: Load to processed data layer

## Dependencies

- `core-utils==1.0.0`: Core utility functions
- `custom-operators==1.0.0`: Custom Airflow operators

## Development

### Adding a New DAG

1. Create a new DAG file in `dags/`
2. Create configuration in `configs/{dataset_name}/`
3. Add DDL scripts in `ddls/` if needed
4. Follow the existing DAG pattern using custom operators

### Testing

Test files are available:
- `test.py`: Basic testing utilities
- `test2.py`: Additional testing utilities

## Troubleshooting

- **Connection Errors**: Verify Airflow connections are properly configured
- **Schema Mismatches**: Check configuration files match source data structure
- **S3 Access**: Ensure IAM permissions are correctly set up
- **Snowflake Stage**: Verify Snowflake internal stage exists and is accessible

## License

[Specify your license here]

## Contact

[Add contact information]