# 📺 YouTube Trending Videos Data Pipeline (Multi-Country)

This project processes, cleans, validates, and loads YouTube trending video data from multiple countries into a PostgreSQL database using **PySpark**. It ensures schema integrity, handles null checks, removes duplicates, and stores a cleaned version in both Parquet and PostgreSQL for further analysis (e.g., with Power BI).

---

## 📂 Project Overview

- 🔁 **ETL Pipeline** built in **PySpark**
- ✅ Schema validation & null checks against **PostgreSQL**
- 💾 Cleaned data stored in **Parquet** and PostgreSQL
- 🌍 Supports **multi-country** processing
- 📊 Ready for integration with **Power BI** dashboards

---

## 🧰 Tech Stack

| Tool         | Usage                                      |
|--------------|---------------------------------------------|
| PySpark      | Data cleaning, transformation, loading     |
| PostgreSQL   | Structured storage                         |
| Power BI     | Visualization (optional, not in repo)     |
| Python       | Core scripting                             |
| JSON         | Configurable input paths and parameters    |
| Logging      | Structured logging per run                 |

---

## 🚀 How to Run

### 1. Prepare Your Environment

Ensure the following:
- PySpark is installed and configured
- PostgreSQL is running
- JDBC driver is downloaded (e.g., `postgresql-42.7.2.jar`)
- Update the config path in `main()` and JDBC JAR path

### 2. Set Up Your Config

Example `config_template.json`

📦 Output
✅ Cleaned and validated data per country

🔄 Existing records for the country are deleted before insert

🧊 Saved as Parquet ({country}_cleaned.parquet)

🛢️ Appended to PostgreSQL table: youtube_trending

📌 Key Features
Schema enforcement against defined PySpark StructType

Data cleaning includes:

Date parsing (trending + publish time)

Dropping invalid or null critical records

Removing duplicates (video_id + country)

Joins with a category mapping

Validates against PostgreSQL NOT NULL constraints

Includes retry mechanism on failure

📝 Logging
Logs are saved to C:/path/youtube_data_cleaning_<timestamp>.log

Logging levels: INFO, WARNING, ERROR

📊 Power BI Visualization (Optional)
After loading into PostgreSQL, you can:

Import the youtube_trending table

Build visualizations around:

Views, Likes, Comments trends

Country/category breakdowns

Engagement metrics

⚠️ .pbix file and raw CSVs are excluded due to file size

🧪 Example Schema
PostgreSQL Table: youtube_trending

Column	Type
video_id	TEXT
trending_date	TIMESTAMP
title	TEXT
channel_title	TEXT
category_id	INTEGER
publish_time	TIMESTAMP
tags	TEXT
views	INTEGER
likes	INTEGER
dislikes	INTEGER
comment_count	INTEGER
thumbnail_link	TEXT
comments_disabled	BOOLEAN
ratings_disabled	BOOLEAN
video_error_or_removed	BOOLEAN
description	TEXT
country	TEXT
category	TEXT

🛠️ To Do / Improvements
 Add unit tests for schema validation

 Improve retry logic with exponential backoff

 Add CLI parameters instead of hardcoded paths

 Dockerize for reproducibility