# ✈️ OpenSky ETL Pipeline with Azure Databricks

This project implements a scalable ETL pipeline using **Azure Databricks** to ingest real-time flight data from the **OpenSky API**, process it using the **medallion architecture (Bronze → Silver → Gold)**, and store the results in **Azure SQL** for downstream analytics with **Power BI**.

---

## 📊 Architecture

```
OpenSky API → Azure Databricks
     ↓
 Bronze → Silver → Gold (Delta Lake)
     ↓
 Azure SQL / Azure Storage
     ↓
     Power BI
```

---

## 🧱 Project Structure

```
.
├── notebooks/               # Databricks notebooks (Python or ipynb format)
│   ├── 1_bronze_ingest.py
│   ├── 2_silver_transform.py
│   └── 3_gold_model.py
├── requirements.txt         # Python dependencies for Databricks environment
├── .github/workflows/       # (Optional) GitHub Actions for CI/CD
└── README.md
```

---

## ⚙️ Setup Instructions

### 1. Clone this Repo
```bash
git clone https://github.com/your-username/opensky-etl-pipeline.git
cd opensky-etl-pipeline
```

### 2. Upload Notebooks to Databricks
- Open your Databricks workspace.
- Import notebooks from the `notebooks/` folder.

### 3. Configure Azure Resources
- Set up **Azure SQL** or **Azure Storage** for data persistence.
- Generate and store **Databricks secrets** for:
  - OpenSky API authentication (if needed)
  - Azure Storage keys / SQL credentials

### 4. Install Python Dependencies
In a Databricks cluster:
```bash
pip install -r requirements.txt
```

---

## 🛠️ Key Components

| Layer   | Description                                  |
|---------|----------------------------------------------|
| Bronze  | Raw ingest from OpenSky API (JSON format)    |
| Silver  | Cleaned and structured data                  |
| Gold    | Aggregated data ready for BI consumption     |

---

## 📈 Power BI Integration

- Connect **Power BI Desktop** to **Azure SQL Database** using the same schema used in the Gold layer.
- Refresh dashboards on a schedule or via pipeline triggers.

---

## 📦 Dependencies

Main packages used:
- `pyspark`, `delta-spark`
- `requests`, `pandas`, `numpy`
- `azure-identity`, `azure-storage-blob`, `pyodbc`

See [`requirements.txt`](./requirements.txt) for details.

---


