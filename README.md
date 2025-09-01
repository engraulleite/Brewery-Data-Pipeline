# Brewery-Data-Pipeline 🍻
An end-to-end data pipeline ingesting data from Open Brewery DB API, processing it through a Medallion (Bronze-Silver-Gold) architecture using Airflow and PySpark, with containerized execution.

## 📋Project Overview
An end-to-end data engineering project demonstrating the extraction, transformation, and loading (ETL) of brewery data from the [Open Brewery DB](https://www.openbrewerydb.org/) API. The pipeline is built following the Medallion architecture (Bronze, Silver, Gold layers) and is orchestrated with Apache Airflow, utilizing PySpark for distributed data processing. The entire solution is containerized with Docker for easy execution and reproducibility.

## 🏅Medallion Architecture
The pipeline follows a classic ETL pattern orchestrated by Airflow. Each layer of the data lake is represented by a directory on the local filesystem, simulating a real-world data lake environment like AWS S3 or Azure Data Lake Storage.

### 🥉Bronze
-  This layer contains the raw, unmodified data ingested directly from the Open Brewery DB API. Each file corresponds to one batch of data fetched from the API, timestamped for lineage.
-  Saved to /data/bronze/<date_process>/breweries_pag_<n>.json

### 🥈Silver
-  Resilient reading of multiple JSON files
-  Delta writing partitioned by date_process and state
-  Storage: /data/silver/date_process=.../state=.../

### 🥇Gold
-  Aggregations by state and brewery_type, with brewery_count
-  Delta writing partitioned by date_process
-  Storage: /data/gold/date_process=.../

## 🛠️Tech Stack
-  Orchestration: Apache Airflow
-  Language: Python (Pyspark), SQL
-  Containerization: Docker, Docker Compose
-  Data Format: JSON, Parquet, Delta
-  Testing: Pytest

## 📁Project Structure
```
Brewery-Data-Pipeline/
├── airflow/
│   ├── dags/
│   └── config/webserver_config.py
├── docker/
├── scripts/
├── src/
├── tests/
├── Dockerfile
├── .dockerignore
├── Makefile
├── README.MD
├── docker-compose.yml
├── requirements.txt
└── spark-defaults.conf
```
## 📑Metadata and Versioning
### Delta Storage
-  Each Delta table supports versioning and change history (Time Travel).
-  Access to snapshots with schema evolution control.
### Metadata
-  Standard structure adopted for columns (e.g., consistent naming between layers).
-  Use of column comments in full mode for documentation via ALTER COLUMN.

## 🚀How to Run the Project
Prerequisites
-  Docker Desktop installed and running
-  Spark 3.4.1 (spark-3.4.1-bin-hadoop3.tgz) [download](https://archive.apache.org/dist/spark/spark-3.4.1/)
### Installation & Execution
Build Docker Services
```bash
docker compose build # Builds all services defined in docker-compose.yml
```
Start Services in Detached Mode
```bash
docker compose up -d # Runs all services in the background
```
Execute Main Pipeline
```bash
docker exec -it spark-container python3 /home/project/scripts/main.py # Triggers the main data processing pipeline
```
## ✅ Quality Assurance
### Automated Validation Framework
Test Suites
-  pytest Integration: Comprehensive test coverage
-  verify_all.py: Master validation script

Validation Checks
-  ✅ Data Existence Verification
-  ✅ Null Value Detection
-  ✅ Duplicate Record Identification
-  ✅ Schema Compliance Validation

### Test Structure
#### Unit Tests
-  test_transform.py: Transformation logic validation
-  test_gold_quality.py: Gold layer quality checks

#### Specialized Validation
-  verify_*.py: Layer-specific verification scripts
-  check_duplicates_silver.py: Dedicated duplicate detection for silver layer

#### Master Validation Script
-  verify_all.py: Unified validation execution
-  Clean Logging: Professional output format

#### Makefile Commands
```bash
make verify    # Run all quality verification checks
make test      # Execute comprehensive test suite
make all       # Run complete validation pipeline
Note: The Makefile provides simplified commands for executing the quality assurance framework
```
