# Cocoa Supply Chain Data Platform

This project is a containerized data platform designed to generate, process, store, and visualize synthetic data for Ghanaian cocoa shipments. It uses modern open-source tools to create a scalable ETL (Extract, Transform, Load) pipeline, orchestrated with Docker Compose for local development and testing.

The platform includes:
- **Data Generation**: An Airflow DAG generates synthetic CSV files hourly and uploads them to MinIO.
- **Data Ingestion**: CSVs are uploaded to MinIO (S3-compatible storage) in the `landing/` bucket.
- **Data Processing**: An Airflow DAG processes CSVs in batches using manifests, stages data as Parquet, and loads into PostgreSQL with UPSERT.
- **Data Storage**: PostgreSQL stores processed data; MinIO manages raw, staged, archived files, and manifests.
- **Visualization**: Metabase provides interactive dashboards for insights.
- **CI/CD**: GitHub Actions automates testing and deployment.

## Table of Contents
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
  - [Generate Data](#generate-data)
  - [Process Data with Airflow](#process-data-with-airflow)
  - [Visualize with Metabase](#visualize-with-metabase)
- [CI/CD Pipeline](#cicd-pipeline)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features
- **Containerized Services**: Dockerized MinIO, PostgreSQL, Airflow, and Metabase with health checks.
- **Automated Data Generation**: Airflow DAG (`hourly_cocoa_data_generation_minio`) generates and uploads 10 CSVs hourly (1000–10,000 records each) to MinIO.
- **Scalable ETL Pipeline**: Airflow DAG (`cocoa_supply_chain_dag`) processes CSVs in batches (size=5), handles large files in chunks (50k rows), stages as Parquet, and performs UPSERT into PostgreSQL.
- **Idempotency**: Uses `ON CONFLICT` for UPSERT on `shipment_id` and file archiving to prevent reprocessing.
- **Data Generation**: Python script creates realistic cocoa shipment CSVs with configurable record counts.
- **Visualization**: Metabase dashboards for metrics like shipment value by region.
- **CI/CD**: GitHub Actions for code linting, DAG testing, and Docker image deployment.
- **Secure Configuration**: Environment variables for sensitive data; custom Airflow image with pinned dependencies.

## Architecture
<img src="images/Mini Data Platform architecture.png" alt="Logo" width="1000"/>
<img src="images/DAG.png" alt="Logo" width="1000"/>

The platform uses a layered architecture:
1. **Data Generation**: The `hourly_cocoa_data_generation_minio` DAG generates 10 CSVs hourly with random records (1000–10,000) using `generate_data.py` and uploads to MinIO’s `landing/` bucket.
2. **Ingestion**: CSVs are stored in MinIO’s `landing/` bucket.
3. **Storage**:
   - **MinIO**: Manages raw CSVs in `landing/`, staged Parquet in `staging/`, archived files in `archive/`, and manifests in `manifests/`.
   - **PostgreSQL**: Stores structured data in the `cocoa_shipments` table.
4. **Processing**: The `cocoa_supply_chain_dag` (daily schedule):
   - **Check Files**: Scans `landing/` and creates a manifest if files exist.
   - **Generate Batches**: Splits manifest into batches (size=5) and saves batch manifests.
   - **Process Batch**: For each batch, reads CSVs in chunks, validates columns, enriches (e.g., adds `shipment_value_usd`), stages as Parquet, loads to a temp table, and UPSERTs to the main table.
   - **Cleanup Batch**: Archives landing files and deletes staging files for processed batches.
5. **Visualization**: Metabase queries PostgreSQL for dashboards.
6. **Orchestration**: Docker Compose manages services and networking.
7. **CI/CD**: GitHub Actions validates and deploys.

**Table Schema** (`cocoa_shipments`):
| Column              | Type       | Description                          |
|---------------------|------------|--------------------------------------|
| `shipment_id`      | TEXT (PK) | Unique shipment ID                   |
| `timestamp`        | TIMESTAMPTZ | Shipment datetime                    |
| `farm_id`          | TEXT      | Farm ID (e.g., FARM-123)             |
| `region`           | TEXT      | Ghanaian region (e.g., Ashanti)      |
| `bean_type`        | TEXT      | Bean variety (e.g., Forastero)       |
| `quality_score`    | NUMERIC   | Quality rating (7.5-9.8)             |
| `shipment_weight_kg` | NUMERIC | Weight in kg (500-5000)             |
| `temperature_celsius` | NUMERIC | Temperature (°C, 18-25)             |
| `shipment_value_usd` | NUMERIC | Value ($/kg * weight)               |
| `processed_at`     | TIMESTAMPTZ | Processing timestamp                 |

## Prerequisites
- **Docker & Docker Compose**: Version 20+.
- **Git**: For cloning the repository.
- **Python 3.11+**: For local data generation (optional).
- **Ports**: 5432 (PostgreSQL), 9000/9001 (MinIO), 8080 (Airflow), 4000 (Metabase).
- **GitHub Account**: For CI/CD (optional).

## Setup
1. **Clone Repository**:
   ```bash
   git clone <your-repo-url>
   cd <project-dir>
   ```

2. **Configure Environment**:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with:
   - `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
   - `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
   - `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_ADMIN_EMAIL`, `AIRFLOW_WEBSERVER_SECRET_KEY`, `AIRFLOW_UID`
   - `AIRFLOW_CONN_MINIO` (e.g., `aws://user:pass@minio:9000/?endpoint_url=http%3A%2F%2Fminio%3A9000`)
   - `AIRFLOW_CONN_POSTGRES` (e.g., `postgresql://user:pass@postgres:5432/db`)

3. **Install Python Dependencies** (for Airflow):
   Ensure `airflow` directory’s `requirements.txt` includes:
   ```
   apache-airflow==2.9.2
   apache-airflow-providers-amazon
   apache-airflow-providers-postgres
   pandas
   pyarrow
   ```

4. **Start Services**:
   ```bash
   docker compose up --build -d
   ```
   - Builds Airflow image, initializes database, creates admin user, and starts services.
   - Check status: `docker compose ps`.
   - Monitor logs: `docker compose logs -f`.

## Usage

### Generate Data
The `hourly_cocoa_data_generation_minio` DAG runs hourly, generating 10 CSVs (1000–10,000 records each) and uploading them to MinIO’s `landing/` bucket. To run manually or test locally:

1. Install dependencies locally (optional):
   ```bash
   pip install faker pandas
   ```
2. Run script locally to generate files:
   ```bash
   python scripts/generate_data.py
   ```
   - Creates CSVs in `/tmp` with random records.
   - Edit `NUM_FILES`, `MIN_RECORDS`, `MAX_RECORDS` in `generate_data.py` to adjust.

3. To trigger the DAG:
   - Enable `hourly_cocoa_data_generation_minio` in Airflow UI (http://localhost:8080, use `.env` credentials).
   - Trigger manually or wait for hourly schedule.
   - Files are uploaded to `landing/` in MinIO.

### Process Data with Airflow
The `cocoa_supply_chain_dag` processes files daily:
1. Access Airflow: http://localhost:8080 (use `.env` admin credentials).
2. Enable `cocoa_supply_chain_dag`.
3. Trigger manually or wait for daily schedule.
   - Checks `landing/`, creates manifests, processes batches (validate, enrich, stage as Parquet, UPSERT to PostgreSQL), and cleans up.
   - Check logs in Airflow UI for details (e.g., processed files, rows inserted).
4. Customize in `cocoa_processing_dag.py`:
   - `BATCH_SIZE=5`: Number of files per batch.
   - `CHUNK_SIZE=50_000`: Rows per chunk for processing.
   - `PRICE_PER_KG_USD=2.50`: Price for calculating `shipment_value_usd`.

### Visualize with Metabase
1. Access Metabase: http://localhost:4000.
2. Set up admin account on first login.
3. Add PostgreSQL data source (host: `postgres`, port: 5432, use `.env` creds).
4. Create queries (e.g., `SELECT region, SUM(shipment_value_usd) FROM cocoa_shipments GROUP BY region`).
5. Build dashboards for metrics like shipment value, quality trends, or regional distribution.

## CI/CD Pipeline
Defined in `.github/workflows/ci-cd-pipeline.yml`:
- **Pull Request**:
  - Lints code with Ruff.
  - Tests Airflow DAG integrity.
  - Builds Docker images.
- **Push to main**:
  - Builds and pushes Airflow image to GHCR (`ghcr.io/<owner>/data-platform-airflow:latest` and `:<sha>`).

Enable by pushing to a branch and opening a PR.

## Project Structure
```
.
├── .github/
│   └── workflows/
│       └── ci-cd-pipeline.yml          # CI/CD workflow
├── airflow/
│   ├── dags/
│   │   ├── cocoa_processing_dag.py    # ETL DAG
│   │   └── cocoa_data_generation_dag.py # Data generation DAG
│   ├── Dockerfile                     # Airflow image
│   └── requirements.txt               # Airflow dependencies
├── data/                              # Local CSVs (gitignored)
├── scripts/
│   └── generate_data.py               # Data generator
├── .env                               # Secrets (gitignored)
├── .env.example                       # Env template
├── .gitignore                         # Ignores data, .env
├── docker-compose.yml                 # Service orchestration
└── README.md                          # Documentation
```

## Troubleshooting
- **Airflow Init Fails**: Check `docker compose logs airflow-init`. Verify `.env` vars; restart: `docker compose down -v && docker compose up --build`.
- **DAG Errors**: Ensure `requirements.txt` matches Airflow 2.9.2; rebuild image. Check `scripts/` is mounted in `docker-compose.yml`.
- **MinIO Issues**: Create `data` bucket in console (http://localhost:9001) if missing.
- **Metabase Connection**: Verify PostgreSQL health (`docker compose logs postgres`); use `postgres` as host.
- **Port Conflicts**: Update ports in `docker-compose.yml` (e.g., `4001:3000` for Metabase).
- **Disk Space**: Run `docker system prune -f`.
- **No Files Detected**: Ensure CSVs are in `landing/`; check manifests in `manifests/`.
- **Data Generation Fails**: Verify `/tmp` is writable; check `NUM_FILES_TO_GENERATE` in `cocoa_data_generation_dag.py`.

## Contributing
1. Fork and create a branch: `git checkout -b feature/new-feature`.
2. Commit: `git commit -m "Add new feature"`.
3. Push: `git push origin feature/new-feature`.
4. Open a PR; CI validates automatically.

Ideas: Add more DAGs, integrate real data sources, or deploy to cloud (e.g., AWS ECS, Kubernetes).