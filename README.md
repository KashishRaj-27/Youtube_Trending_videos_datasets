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
