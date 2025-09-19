# Retail Sales Data Warehouse ETL Project

## Project Overview
This project implements an ETL (Extract, Transform, Load) pipeline for a **Retail Sales Data Warehouse** using Python, MySQL, and Docker. The ETL workflow ingests CSV files containing customer, product, store, and sales data, processes them, and loads them into a dimensional star schema in MySQL.

The warehouse design uses **dimension tables** (`DimCustomer`, `DimProduct`, `DimStore`, `DimDate`) and a **fact table** (`FactSales`) to enable analytics and reporting.

---

## Tech Stack
- **Python 3.x** – for ETL scripting
- **Pandas** – for CSV processing and data transformation
- **SQLAlchemy** – for database connection and SQL execution
- **MySQL 8.0** – relational database for data warehouse
- **Docker & Docker Compose** – to run MySQL in a containerized environment

---

## Repository Structure

.
├── data/ # CSV input files
│ ├── customers.csv
│ ├── products.csv
│ ├── stores.csv
│ └── sales.csv
├── ddl.sql # SQL script for creating tables (dimensions, fact, staging)
├── etl_load.py # Python ETL script
├── docker-compose.yml # Docker configuration for MySQL
├── samplequery.sql # Sample SQL script for testing
└── README.md

---

## Setup Instructions

### 1. Clone the repository

git clone <repository_url>
cd <repository_folder>

### 2. Start MySQL with Docker Compose

docker-compose up -d
MySQL will be accessible at localhost:3307

Credentials:

User: user
Password
Database: retail_dw

### 3. Initialize Database

Run the DDL script to create the schema:
mysql -u user -p -P 3307 < ddl.sql

### 4. Load Data via ETL Script

Ensure your CSV files are in the data/ folder, then run:
python etl_load.py


The script performs:

Loading raw CSVs into staging table StgSales.
Upserting dimension tables (DimCustomer, DimProduct, DimStore, DimDate).
Aggregating and inserting sales data into the fact table FactSales.

Optional: truncate staging table after ETL (can be uncommented in etl_load.py).

Star Schema Design

Dimensions
DimCustomer – stores customer details
DimProduct – stores product details
DimStore – stores store details
DimDate – stores date information

Fact
FactSales – stores aggregated sales metrics (quantity and total amount) for each combination of customer, product, store, and date.

Staging
StgSales – temporary table for raw CSV data before transformation.

Sample Queries

Total sales by product:

SELECT p.ProductName, SUM(f.TotalAmount) AS TotalSales
FROM FactSales f
JOIN DimProduct p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductName;


Monthly sales by store:

SELECT d.Month, s.StoreName, SUM(f.TotalAmount) AS TotalSales
FROM FactSales f
JOIN DimStore s ON f.StoreKey = s.StoreKey
JOIN DimDate d ON f.DateKey = d.DateKey
GROUP BY d.Month, s.StoreName;

Notes

The ETL script is idempotent, so running it multiple times will not create duplicate records.
Staging data can be kept for auditing or cleared after loading into the fact table.
The project uses ON DUPLICATE KEY UPDATE to handle upserts in MySQL.

Author

Dheeraj Mohanbabu
