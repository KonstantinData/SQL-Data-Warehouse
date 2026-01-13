# SQL-Data-Warehouse
Building an SQL Data Warehouse Solution from scratch

## 📌 Overview

This project is my hands-on exploration of **data engineering** concepts, focusing on data extraction, transformation, loading (ETL), and pipeline automation. 
It serves as a learning project to gain practical experience with real-world datasets while developing a structured approach to data warehousing.


**Welcome to the Project**  
🔗 [Project Overview](https://www.notion.so/Welcome-to-the-Project-fccab1cbaaf843d584d237ec6dce641e?pvs=4)  

📌 **Access:** View-only access is granted upon request and can be revoked at any time.


## 🎯 What I'm Learning

- **Data Extraction**: Retrieving data from APIs, databases, or flat files.
- **Data Transformation**: Cleaning, structuring, and preparing data for storage.
- **ETL Pipeline**: Automating data ingestion and processing steps.
- **Data Warehousing**: Implementing a structured database for efficient querying.
- **Automation & Scheduling**: Utilizing **Apache Airflow** or similar tools for workflow automation.
- **Data Visualization**: Presenting insights using **BI tools** like Power BI or Tableau.

## 🛠️ Technologies I'm Using

- **Python**: For scripting and data processing.
- **SQL**: For querying and managing structured data.
- **Apache Airflow**: For orchestrating data workflows.
- **Docker**: For creating containerized environments.
- **Cloud Services** (optional): AWS, GCP, or Azure for scalable storage and compute solutions.
- **BigQuery / Snowflake** (optional): Exploring cloud-based data warehousing solutions.

This repository will evolve as I explore data engineering, with a stronger focus on analytics engineering while 
still incorporating elements of data analysis

## 🚀 Running the Bronze Layer Load

The Bronze layer loader uses `BULK INSERT` with a configurable base path. By default, it expects the `datasets` folder in the repository root. If your CSV files live elsewhere, pass the absolute path to the stored procedure.

**Default (repository-relative) path**
```
EXEC bronze.load_bronze;
```

**Custom absolute path (example)**
```
EXEC bronze.load_bronze @base_path = N'C:\data\SQL-Data-Warehouse\datasets';
```

> Note: `BULK INSERT` reads files from the SQL Server machine. Ensure the SQL Server service account has access to the path or share you provide.

## 📜 License

This project is licensed under the terms of the **MIT License**. See the [LICENSE.txt](LICENSE.txt) file for details.

## 🧑‍💻 About Me  

Hi! I'm **Konstantin Milonas** – a Certified Commercial Specialist (IHK), Senior Retail Consultant, and Aspiring Data Analyst. I'm currently enrolled 
in a **7-month Bootcamp**, concluding on **April 21, 2025**, to enhance my expertise in **database design, statistical analysis, and data tools**, 
including Tableau, Python, PostgreSQL, Excel, Google Sheets, and Machine Learning.
