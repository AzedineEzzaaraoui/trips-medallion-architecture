# 🚗 Trip Data Engineering Pipeline (Medallion Architecture)

## 📌 Project Overview

This project implements an **end-to-end data engineering pipeline** using **Databricks, PySpark, and Delta Lake**.
It follows the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw trip data into business-ready insights.

The goal is to simulate a real-world **transportation analytics system** (similar to Uber/Careem use cases).

---

## 🏗️ Architecture

```
Raw CSV Files (Volumes)
        │
        ▼
Bronze Layer (Raw Ingestion - Auto Loader)
        │
        ▼
Silver Layer (Cleaned & Transformed Data)
        │
        ▼
Gold Layer (Aggregated Business KPIs)
        │
        ▼
BI / Analytics (Power BI / SQL)
```

---

## 🧩 Data Model

### Fact Table

* **trips**

  * trip_id
  * business_date
  * city_id
  * distance_kms
  * sales_amt
  * passenger_rating
  * driver_rating

### Dimension Tables

* **city**
* **calendar**

---

## 🥉 Bronze Layer

* Ingest raw CSV files using **Databricks Auto Loader**
* Store data in Delta format
* Add ingestion metadata (timestamp, source file)

📂 Example:

```
/Volumes/trips/landing/trips
```

---

## 🥈 Silver Layer

* Clean and standardize data
* Rename columns
* Apply data quality checks
* Handle CDC (Change Data Capture)

---

## 🥇 Gold Layer

* Create business-level aggregated tables:

  * trips_kpi_daily
  * revenue_by_city
  * trips_by_month
  * customer_satisfaction

---

## 📊 Business Use Cases

* Revenue analysis by city and time
* Trip trends over time
* Customer satisfaction analysis
* Driver performance insights

---

## ⚙️ Technologies Used

* **Databricks**
* **PySpark**
* **Delta Lake**
* **Auto Loader (cloudFiles)**
* **SQL**
* **Power BI (optional)**

---

## 🚀 How to Run

1. Upload CSV files to:

```
/Volumes/trips/landing/trips
```

2. Run Bronze pipeline (Auto Loader)

3. Run Silver transformations

4. Run Gold aggregations

5. Query results:

```sql
SELECT * FROM trips.gold.trips_kpi_daily;
```

---

## 🧠 Key Learnings

* Building scalable data pipelines
* Implementing Medallion Architecture
* Handling streaming ingestion with Auto Loader
* Designing analytical data models

---

## 📌 Future Improvements

* Add real-time streaming (Kafka / Kinesis)
* Add data quality framework
* Add orchestration (Airflow)
* Deploy CI/CD pipeline

---

## 👤 Author

Data Engineer | PySpark | Databricks | Data Pipelines

---
