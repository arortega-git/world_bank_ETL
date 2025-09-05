# 🌍 World Bank Unemployment ETL with Airflow

## 📖 Project Overview
This project automates the extraction, transformation, and loading (ETL) of **unemployment indicators** from the [World Bank API](https://data.worldbank.org/).
It uses **Apache Airflow** to orchestrate tasks and stores results in **CSV files** and (optionally) in a **PostgreSQL database**.

The pipeline is modular and easy to extend to other indicators, countries, or time ranges.

---

## ⚙️ Tech Stack
- 🐍 **Python** (requests, pandas, SQLAlchemy)
- 🎛️ **Apache Airflow**
- 🗄️ **PostgreSQL**
- ☁️ **World Bank API**

---

## 📊 Indicators Covered
- **General unemployment**: total, male, female
- **Age-based unemployment**: youth (15–24), male, female
- **Education-level unemployment**: basic, intermediate, advanced (male & female)

---

## 🏗️ Project Structure
```text
/opt/airflow/dags/
│── get_data.py              # ETL functions (extract, transform, save)
│── unemployment_etl_dag.py  # Airflow DAG definition
/opt/airflow/data/
│── raw/                     # Raw JSON files
│── processed/               # Processed CSV files
│── merged/                  # Merged CSVs per country
```

---

## 🚀 Usage

### 1) Start Airflow
```bash
airflow standalone
```

### 2) Place DAG files
Ensure both `get_data.py` and `unemployment_etl_dag.py` live in your Airflow DAGs folder (e.g., `/opt/airflow/dags/`).

### 3) Trigger the DAG
- Open the **Airflow UI** at `http://localhost:8080`
- Enable the DAG **`unemployment_etl_dag`**
- Click **Trigger DAG** ▶️

### 4) Outputs
- 📂 Raw JSON → `/opt/airflow/data/raw/`
- 📂 Processed CSV → `/opt/airflow/data/processed/`
- 📂 Merged CSV per country → `/opt/airflow/data/merged/`
- 🗄️ **PostgreSQL tables** (optional, see notes below)

---

## 🧩 Configuration

### Countries
Edit the `country_codes` dictionary in `get_data.py`:
```python
country_codes = {
    "Czech Republic": "CZ",
    "European Union": "EUU",
    # "Spain": "ESP",  # add more as needed
}
```

### Indicators & date ranges
Indicators are grouped by category; change or extend them in `get_data.py`.
Date ranges use World Bank format `YYYY:YYYY`:
```python
date_ranges = {
    "general": "2000:2023",
    "age": "2000:2023",
    "education": "2000:2023",
}
```

### PostgreSQL (optional)
By default the pipeline saves JSON/CSV. To also write to Postgres, ensure:
- The SQLAlchemy engine is correct in `get_data.py` (example below).
- You call `save_to_db` after merging or extend the DAG with a task that loads the merged CSV into Postgres.

```python
ENGINE = create_engine("postgresql+psycopg2://airflow:airflow@localhost:5432/unemployment")
# Tip: make `engine` optional in run_etl to match the DAG:
# def run_etl(country_codes, indicators, date_ranges, engine=ENGINE):
```

---

## 🔄 Workflow
1. **Extract** → Fetch unemployment data from the World Bank API
2. **Transform** → Convert JSON to a clean pandas DataFrame
3. **Load** → Save as JSON, CSV, and (optionally) into PostgreSQL
4. **Merge** → Combine all indicators per country into a single dataset

---

## 🌐 Example Countries
- 🇨🇿 Czech Republic (`CZ`)
- 🇪🇺 European Union (`EUU`)

---

## 🧪 Quick SQL sanity check (optional)
After loading to Postgres, you can run a simple query to inspect data:
```sql
SELECT *
FROM information_schema.tables
WHERE table_schema = 'public';
```

---

## 🛠️ Troubleshooting
- **Missing `engine` in DAG call**: The provided DAG calls `run_etl` without the `engine` arg. Either pass it in `op_kwargs` **or** make `engine` optional in the function signature (`engine=ENGINE`).
- **Permissions/paths**: Make sure Airflow has read/write permissions for `/opt/airflow/data/`.
- **API rate limiting**: A short `sleep` is included; increase it if you hit rate limits.

---

## 📌 Next Steps
- Add more countries 🌍
- Automate DB loading 📥
- Build dashboards (Power BI / Tableau) 📊

---

✍️ **Author**: Learning project for Airflow + ETL pipelines.
