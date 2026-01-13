# SQL-Data-Warehouse
Building an SQL Data Warehouse Solution from scratch

## 📌 Overview

This project is a hands-on exploration of **data engineering** concepts, focusing on data extraction, transformation, loading (ETL), and pipeline automation. 
It serves as a learning project to gain practical experience with real-world datasets while developing a structured approach to data warehousing.

**Welcome to the Project**  
🔗 [Project Overview](https://www.notion.so/Welcome-to-the-Project-fccab1cbaaf843d584d237ec6dce641e?pvs=4)  

📌 **Access:** View-only access is granted upon request and can be revoked at any time.

## ✅ Prerequisites

- **SQL Server 2019+** (or **Azure SQL Edge** for local containers).
- **ODBC Driver 17 for SQL Server** (required if you use the Python notebooks).
- **Python 3.10+** (optional; only needed for the Jupyter notebooks in `scripts/`).
- Access to the `datasets/` directory from the SQL Server host (required for `BULK INSERT`).

## 📁 Repository Layout

| Path | Purpose |
| --- | --- |
| `datasets/` | CRM + ERP CSV extracts used as the Bronze layer source. |
| `scripts/` | SQL scripts for schema creation, Bronze/Silver loads, and notebooks. |
| `tests/` | Data quality checks for Bronze/Silver/Gold layers. |

## ⚙️ Setup

1. **Clone the repo and open SQL Server tooling**  
   Use SQL Server Management Studio (SSMS), Azure Data Studio, or any T-SQL client connected to your SQL Server instance.

2. **Confirm dataset access**  
   `BULK INSERT` reads files from the SQL Server host, not your local client. If SQL Server runs on another machine or container, copy the `datasets/` folder there or use a shared path.

3. **Optional: Python notebooks**  
   If you want to explore `scripts/python_sql_server_connection.ipynb`, install Python 3.10+ and the ODBC driver, then configure your connection string.

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

## ▶️ Execution Guide (Manual)

Run the scripts in this order:

1. **Initialize database + schemas**  
   `scripts/init.database.sql` (drops/recreates `DataWarehouse`, then creates `bronze`, `silver`, `gold` schemas).
2. **Create Bronze tables**  
   `scripts/bronze_layer/create_table_bronze_layer.SQL`
3. **Create the Bronze load procedure**  
   `scripts/bronze_layer/bulk_insert_crm_cust_info.sql`
4. **Load Bronze data**  
   `scripts/bronze_layer/bronze-load-bronze.sql`  
   *Override the default base path if needed:*  
   ```
   EXEC bronze.load_bronze @base_path = N'C:\data\SQL-Data-Warehouse\datasets';
   ```
5. **Create Silver tables**  
   `scripts/silver_layer/create-silver-table-structure.sql`
6. **Run Silver cleansing scripts**  
   - `scripts/silver_layer/cleansing_crm_cust_info.sql` (loads cleaned customer data into Silver)
   - `scripts/silver_layer/cleansing_crm_prd_info.sql` (currently exploratory; returns a cleaned projection for product data)
7. **Gold layer views**  
   Create the Gold layer views with `scripts/gold_layer/create_gold_views.sql`:
   - `gold.dim_customers`
   - `gold.dim_products`
   - `gold.fact_sales`
8. **Run quality checks**  
   Execute the SQL scripts in `tests/` for Bronze/Silver/Gold data validation.

> Note: `BULK INSERT` reads files from the SQL Server machine. Ensure the SQL Server service account has access to the path or share you provide.

## 🧰 One-Click Orchestration (SQLCMD Mode)

If you use SSMS/ADS with SQLCMD mode enabled, you can run the full pipeline via:

```
:r .\scripts\run_pipeline.sql
```

This script chains the database init, Bronze load, and Silver cleansing steps in order.

## ✅ CI Automation (GitHub Actions + Local Runner)

This repo ships with a lightweight GitHub Actions workflow that provisions a SQL Server
container, runs the ETL pipeline, and executes the CI-grade quality checks in
`tests/quality_checks_ci.sql`. The workflow runs on every push and pull request to catch
schema or data integrity regressions early.

You can also run the same automation locally with Docker:

```
scripts/ci/run_ci_checks.sh
```

Optional environment variables for local runs:
- `SA_PASSWORD` (defaults to `YourStrong!Passw0rd1`)
- `MSSQL_IMAGE` (defaults to `mcr.microsoft.com/mssql/server:2022-latest`)
- `MSSQL_TOOLS_IMAGE` (defaults to `mcr.microsoft.com/mssql-tools:latest`)
- `SQLCMD_SERVER` (defaults to `localhost`)
- `SQLCMD_NETWORK` (defaults to `host`)

If you already have SQL Server running, set `START_CONTAINER=false` to skip container
startup and only run the pipeline + checks.

## 🐍 One-Click Orchestration (Python + sqlcmd)

If you prefer a single command outside SSMS/ADS, use the Python runner that calls `sqlcmd`
for each step:

```
python scripts/orchestrate_pipeline.py --server localhost --database master --trusted-connection
```

For SQL authentication, pass `--username` and `--password` instead of `--trusted-connection`.

## 🧾 Data Description

The project uses small CRM and ERP CSV extracts (included in `datasets/`) for demo purposes. These are **sample datasets** intended for learning and testing ETL logic.

**CRM extracts (`datasets/source_crm/`)**
- `cst_info.csv`: customer identifiers, names, marital status, gender, create date.
- `prd_info.csv`: product identifiers, names, cost, line, start/end dates.
- `sales_details.csv`: sales order facts (order/ship/due dates, sales, quantity, price).

**ERP extracts (`datasets/source_erp/`)**
- `CST_AZ12.csv`: customer demographics (birthdate, gender).
- `LOC_A101.csv`: customer location (country).
- `PX_CAT_G1V2.csv`: product category, subcategory, and maintenance indicator.

## 📚 Data Dictionary & Governance Notes

The tables below summarize the core warehouse entities, key fields, and notable transformations so you can understand the business context without reading every SQL file.

### Bronze layer (raw ingest)

| Table | Grain | Key fields | Description |
| --- | --- | --- | --- |
| `bronze.crm_cust_info` | One row per customer per CRM extract | `cust_id`, `cust_key`, `cust_firstname`, `cust_lastname`, `cust_marital_status`, `cust_gender`, `cust_create_date` | Raw customer records from the CRM extract. |
| `bronze.crm_prd_info` | One row per product per CRM extract | `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt` | Raw product master data from the CRM extract. |
| `bronze.crm_sales_details` | One row per sales order line | `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price` | Raw sales transactions with dates stored as integer yyyymmdd values. |
| `bronze.erp_cust_az12` | One row per customer per ERP extract | `cid`, `bdate`, `gen` | ERP customer demographics (birthdate, gender). |
| `bronze.erp_loc_a101` | One row per customer per ERP extract | `cid`, `cntry` | ERP customer location details (country). |
| `bronze.erp_px_cat_g1v2` | One row per product category | `id`, `cat`, `subcat`, `maintenance` | ERP product category hierarchy and maintenance flag. |

### Silver layer (cleansed & standardized)

| Table | Grain | Key fields | Transformations |
| --- | --- | --- | --- |
| `silver.crm_cust_info` | Latest row per customer | `cust_id`, `cust_key`, `cust_firstname`, `cust_lastname`, `cust_marital_status`, `cust_gender`, `cust_create_date`, `cust_is_future` | Trims names, standardizes marital status and gender, keeps latest record per customer, and flags future-dated creates via `cust_is_future`. |
| `silver.crm_prd_info` | One row per product | `prd_id`, `prd_key`, `prd_nm`, `prd_cost`, `prd_line`, `prd_start_dt`, `prd_end_dt` | Mirrors CRM product master data for cleansing and downstream transformations. |
| `silver.crm_sales_details` | One row per sales order line | `sls_ord_num`, `sls_prd_key`, `sls_cust_id`, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`, `sls_sales`, `sls_quantity`, `sls_price` | Retains CRM sales details for conversion to analytical dates in Gold. |
| `silver.erp_cust_az12` | One row per customer | `cid`, `bdate`, `gen` | ERP customer demographics retained for enrichment. |
| `silver.erp_loc_a101` | One row per customer | `cid`, `cntry` | ERP customer locations retained for enrichment. |
| `silver.erp_px_cat_g1v2` | One row per product category | `id`, `cat`, `subcat`, `maintenance` | ERP product category hierarchy retained for enrichment. |

### Gold layer (analytics-ready views)

| View | Grain | Key fields | Business rules |
| --- | --- | --- | --- |
| `gold.dim_customers` | One row per customer | `customer_key`, `customer_id`, `customer_number`, `first_name`, `last_name_hash`, `marital_status`, `gender`, `birth_date`, `country` | Joins CRM and ERP sources; `gender` prioritizes cleaned CRM values and falls back to ERP `gen` when CRM is `n/a`. Last names are hashed in Gold to demonstrate data minimization. |
| `gold.dim_products` | One row per product | `product_key`, `product_id`, `product_number`, `product_name`, `product_cost`, `product_line`, `category`, `subcategory`, `maintenance` | Standardizes product line codes (M/R/S/T) to descriptive labels and enriches with ERP categories. |
| `gold.fact_sales` | One row per sales order line | `order_number`, `customer_key`, `product_key`, `order_date`, `ship_date`, `due_date`, `sales_amount`, `quantity`, `price` | Converts integer dates to `DATE` and joins to the customer/product dimensions. |

### Privacy & GDPR considerations

This project uses **fictional, synthetic data** included in the `datasets/` folder for learning and testing. It does **not** contain real personal data, and no production identifiers are used. To demonstrate privacy-by-design, the Gold customer view hashes last names (`last_name_hash`) so analytical consumers can join and segment without direct identifiers. If you adapt these pipelines for real customer data, apply privacy-by-design controls such as masking, tokenization, encryption, or row-level security before loading to analytics layers.

## 🚀 Performance & Scalability Considerations (Future Work)

The current datasets are intentionally small, so performance is a non-issue. If the warehouse were scaled to larger volumes, the following optimizations would make the model more production-ready:

- **Add primary and foreign keys**  
  Define primary keys on dimension tables (for example, `customer_key` on `gold.dim_customers`) and foreign key relationships from fact tables to dimensions. This enforces referential integrity and helps the SQL optimizer produce better execution plans.
- **Index common join/filter columns**  
  Add non-clustered indexes on high-usage join keys such as `silver.crm_cust_info.cust_key` and `silver.crm_sales_details.sls_cust_id` to speed up joins between CRM and ERP data.
- **Store dates as DATE/DATETIME in Silver**  
  Convert integer date fields (for example, `sls_order_dt`, `sls_ship_dt`, `sls_due_dt`) into `DATE`/`DATETIME` types in the Silver layer to simplify filtering and enable date functions without repeated conversions.
- **Partition large fact tables when volumes grow**  
  If `gold.fact_sales` becomes large, partition by a date column (such as `order_date`) to improve manageability and query performance.
- **Materialize Gold views if needed**  
  If the Gold layer moves from views to materialized tables for performance, apply the same indexing strategy used for facts/dimensions.

## ✅ Outcomes & Example Queries

**Current output layers**
- **Bronze**: raw CSV loads into `bronze.*` tables via `BULK INSERT`.
- **Silver**: cleaned `crm_cust_info` data (trimmed strings, standardized values, future-date flag).
- **Gold**: dimensional model views (`dim_customers`, `dim_products`, `fact_sales`) referenced by the gold QA script.

**Example validation query**
```
SELECT TOP 10
    cust_id,
    cust_firstname,
    cust_lastname,
    cust_gender,
    cust_is_future
FROM silver.crm_cust_info
ORDER BY cust_id;
```

Expected outcome: rows return with cleaned first/last names, standardized gender values, and `cust_is_future` indicating invalid future dates.

## 📜 License

This project is licensed under the terms of the **MIT License**. See the [LICENSE.txt](LICENSE.txt) file for details.

## 🧑‍💻 About Me  

Hi! I'm **Konstantin Milonas** – a Certified Commercial Specialist (IHK), Senior Retail Consultant, and Aspiring Data Analyst. I'm currently enrolled 
in a **7-month Bootcamp**, concluding on **April 21, 2025**, to enhance my expertise in **database design, statistical analysis, and data tools**, 
including Tableau, Python, PostgreSQL, Excel, Google Sheets, and Machine Learning.
