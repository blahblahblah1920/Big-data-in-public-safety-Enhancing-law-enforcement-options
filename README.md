# 🚔 Big Data in Public Safety: Enhancing Law Enforcement Options

> **ADTA 5240 – Harvesting, Storing, and Retrieving Data** | University of North Texas  
> Group 4 | Guided by Professor Anthony Fantasia

A big data analytics project demonstrating how the **Dallas Police Department** can leverage arrest data to support smarter, fairer, and more effective policing.

---

## 📌 Project Overview

This project applies the full data lifecycle — from collection to interpretation — to Dallas Police arrest data sourced from the [Dallas Open Data Portal](https://www.dallasopendata.com/Public-Safety/PoliceArrests/sdr7-6v3j/about_data). The goal is to uncover trends in crime patterns, racial disparities, drug enforcement, and use-of-force incidents to inform data-driven policy decisions.

---

## 🗂️ Data Lifecycle

| Stage | Description |
|-------|-------------|
| **Generation** | Dallas Open Data Portal — daily-updated arrest records |
| **Collection** | Socrata API; 33 of 65 columns selected; static snapshot taken April 12 |
| **Processing** | Google Cloud OpenRefine — deduplication, standardization, type alignment |
| **Storage** | Google BigQuery + Apache Spark with HDFS |
| **Management** | Dataproc cluster (1 namenode, 2 worker nodes); IAM service accounts |
| **Analysis** | SQL queries via BigQuery; Spark jobs on HDFS |
| **Visualization** | Google Looker Studio & Tableau |
| **Interpretation** | Equity insights and policy recommendations |

---

## ⚡ My Contribution — Automated Data Streaming

I built the **automated daily data ingestion pipeline** that keeps both storage systems up to date without manual intervention.

### How It Works

The pipeline uses the `upzdate` column from the source dataset as a **watermark** — on each run, only records newer than the latest timestamp already in the database are fetched and appended. This prevents duplicate ingestion and ensures the databases stay current.

### BigQuery Streaming (`bigquery_stream.py`)

- Connects to the Dallas Open Data API via `sodapy` + direct CSV URL
- Queries records where `ararrestdate = yesterday`
- Applies type transformations (height to float, dates to datetime, booleans, etc.)
- Appends new rows to the BigQuery table using `WRITE_APPEND`

```python
job = bq_client.load_table_from_dataframe(df, table_id, job_config=bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND"
))
```

### HDFS / Apache Spark Streaming (`spark_stream.py`)

- Initializes a Spark session with Hive support on the Dataproc cluster
- Reads the current max `upzdate` from the existing Hive table
- Fetches only records newer than that timestamp from the API
- Cleans and casts all columns to match the Hive schema
- Appends new data using `insertInto` in append mode

```python
spark_df.write.mode("append").insertInto("arrest_data_dallas")
```

### Results

| System | Before Script | After Script |
|--------|--------------|--------------|
| BigQuery | Latest date: April 12 | Latest date: April 16 |
| HDFS (Hive) | Latest `upzdate`: April 19 (prior run) | Updated with latest records |

---

## 🛠️ Tech Stack

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Google BigQuery](https://img.shields.io/badge/BigQuery-4285F4?style=flat&logo=googlebigquery&logoColor=white)
![Apache Hive](https://img.shields.io/badge/Apache%20Hive-FDEE21?style=flat&logo=apachehive&logoColor=black)
![HDFS](https://img.shields.io/badge/HDFS-66CCFF?style=flat&logo=apache&logoColor=white)
![Google Cloud](https://img.shields.io/badge/Google%20Cloud-4285F4?style=flat&logo=googlecloud&logoColor=white)

- **Data Source:** Dallas Open Data Portal (Socrata API)
- **Data Cleaning:** Google Cloud OpenRefine
- **Storage:** Google BigQuery, Apache Spark + HDFS (Dataproc)
- **Streaming:** Python (`pandas`, `sodapy`, `pyspark`, `google-cloud-bigquery`)
- **Visualization:** Google Looker Studio, Tableau
- **Infrastructure:** Google Cloud Dataproc (1 namenode + 2 worker nodes), IAM service accounts

---

## 📊 Key Insights

- **Black and Hispanic/Latino individuals** account for the highest arrest counts across both districts over a 10-year span
- **Drug-related arrests** peak in 2018 and 2025, indicating sustained enforcement focus
- **Students and service-sector workers** have disproportionately high arrest rates, pointing to socio-economic factors
- **52%+ of arrests** involved unarmed individuals, yet use-of-force was still recorded in some of those cases
- **Midnight (00:00)** is the most common arrest hour, with 1,163 incidents

---

## 💡 Recommendations

1. Conduct regular **equity audits** on racial arrest patterns
2. Expand **diversion programs** for drug-related offenses
3. Launch **outreach initiatives** for youth and low-income workers
4. Improve **de-escalation training** and use-of-force protocols
5. Publish **public dashboards** to build community trust and accountability

---

## 👥 Team
| Esther Eze | Gauri Mahalle |  Nisha Ali |  Pardhiv Vasireddy | Pranav Abishai Moses | Srilakshmi Savithena |
---

*Data sourced from the [Dallas Open Data Portal](https://www.dallasopendata.com/Public-Safety/PoliceArrests/sdr7-6v3j/about_data) · University of North Texas · ADTA 5240*
