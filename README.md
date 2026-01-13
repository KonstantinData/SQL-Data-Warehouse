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

## ▶️ Execution Guide (Manual)

Run the scripts in this order:

1. **Initialize database + schemas**  
   `scripts/init.database.sql` (drops/recreates `DataWarehouse`, then creates `bronze`, `silver`, `gold` schemas).
2. **Create Bronze tables**  
   `scripts/bronze_layer/create_table_bronze_layer.SQL`
3. **Create the Bronze load procedure**  
   `scripts/bronze_layer/bulk_insert_crm_cst_info.sql`
4. **Load Bronze data**  
   `scripts/bronze_layer/bronze-load-bronze.sql`  
   *Override the default base path if needed:*  
   ```
   EXEC bronze.load_bronze @base_path = N'C:\data\SQL-Data-Warehouse\datasets';
   ```
5. **Create Silver tables**  
   `scripts/silver_layer/create-silver-table-structure.sql`
6. **Run Silver cleansing scripts**  
   - `scripts/silver_layer/cleansing_crm_cst_info.sql` (loads cleaned customer data into Silver)
   - `scripts/silver_layer/cleansing_crm_prd_info.sql` (currently exploratory; returns a cleaned projection for product data)
7. **Gold layer views (planned)**  
   The `scripts/gold_layer/` folder is a placeholder. The target tables referenced in `tests/quality_checks_gold.sql` are:
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

## ✅ Outcomes & Example Queries

**Current output layers**
- **Bronze**: raw CSV loads into `bronze.*` tables via `BULK INSERT`.
- **Silver**: cleaned `crm_cst_info` data (trimmed strings, standardized values, future-date flag).
- **Gold**: planned dimensional model (`dim_customers`, `dim_products`, `fact_sales`) referenced by the gold QA script.

**Example validation query**
```
SELECT TOP 10
    cst_id,
    cst_firstname,
    cst_lastname,
    cst_gender,
    cst_is_future
FROM silver.crm_cst_info
ORDER BY cst_id;
```

Expected outcome: rows return with cleaned first/last names, standardized gender values, and `cst_is_future` indicating invalid future dates.

## 📜 License

This project is licensed under the terms of the **MIT License**. See the [LICENSE.txt](LICENSE.txt) file for details.

## 🧑‍💻 About Me  

Hi! I'm **Konstantin Milonas** – a Certified Commercial Specialist (IHK), Senior Retail Consultant, and Aspiring Data Analyst. I'm currently enrolled 
in a **7-month Bootcamp**, concluding on **April 21, 2025**, to enhance my expertise in **database design, statistical analysis, and data tools**, 
including Tableau, Python, PostgreSQL, Excel, Google Sheets, and Machine Learning.
