#  Distributed E-Commerce Sessionization & Bot Fraud Detection

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-F68A1E?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00AADC?style=for-the-badge&logo=databricks&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)

## 📌 Executive Summary
In large-scale retail environments, identifying automated scraper bots and click-fraud is critical to protecting ad spend and ensuring accurate business metrics. 

This project is an end-to-end, distributed machine learning pipeline built on **Databricks**. It processes over **40 million raw clickstream events (5GB+)**, enforces rigid data quality gates, engineers stateful user sessions from stateless clicks, and deploys an unsupervised machine learning model to isolate malicious bot traffic in the wild.

## 🏗️ Architecture & Tech Stack
The pipeline follows a strict **Medallion Architecture** (Bronze, Silver, Gold) backed by ACID-compliant **Delta Lake** tables and orchestrated via Databricks Workflows.

* **Compute:** Databricks, Apache Spark (PySpark)
* **Storage:** AWS S3, Delta Lake
* **Machine Learning:** Spark MLlib (K-Means Clustering)
* **Visualization:** Matplotlib, Seaborn

## ⚙️ The Pipeline

### 🥉 Bronze Layer: Raw Ingestion
* Utilizes **Databricks Auto Loader (`cloudFiles`)** to incrementally ingest raw CSV event logs from AWS S3.
* Preserves the raw state of the data while appending engineering audit columns (`_ingest_timestamp`, `_source_file`) for complete data lineage.

### 🥈 Silver Layer: Data Quality & Sessionization
* **Data Quality Gates:** Filters nulls, drops duplicates, explicitly casts schema types, and safely generates unique event IDs using `monotonically_increasing_id()`. 
* **Distributed Sessionization:** Applies highly-optimized PySpark `Window` functions (partitioned by `user_id`, ordered by `event_time`) to calculate time gaps between individual clicks. Events with >30 minutes of inactivity are split into logically distinct `session_ids`.

### 🥇 Gold Layer: Feature Engineering & ML
* **Behavioral Aggregation:** Compresses millions of events into session-level features, calculating metrics such as `session_duration_sec`, `click_rate_per_sec`, and `unique_products_viewed`.
* **Anomaly Detection:** Trains an unsupervised **K-Means Clustering** model on scaled features. Because real-world bot behavior constantly evolves, this unsupervised approach mathematically isolates hyper-active sessions (e.g., scrapers viewing 9 products in 1 second) without relying on historical labels.

## 📊 Key Results & Visualizations

### Bot Behavior vs. Human Behavior
<img width="840" height="518" alt="scatter" src="https://github.com/user-attachments/assets/7200e16b-f2f5-4075-9e82-d0492175e3a0" />

> *The scatter plot demonstrates the stark behavioral difference between normal human traffic (gradual clicks over long durations) and scraper bots (hundreds of clicks in near-zero seconds).*

### The "Smoking Gun": Click Rate Density
<img width="840" height="482" alt="bar" src="https://github.com/user-attachments/assets/55473840-e09d-47de-81b8-8ba7340f0103" />

> *The density distribution highlights normal human navigation peaking near 0.05 clicks/sec, while the ML-flagged bot cluster spikes at mathematically impossible speeds.*

## 🚀 How to Run
1. Clone this repository to your local machine or directly into Databricks Repos.
2. Download the [REES46 eCommerce Dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) from Kaggle.
3. Upload `2019-Oct.csv` to your designated AWS S3 raw landing zone.
4. Execute the notebooks sequentially from `01_ingestion` through `05_fraud_detection`, or orchestrate them via a Databricks Workflow DAG.
