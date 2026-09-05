# Smart Retail Data Platform – Internship Project

## 📌 Project Overview
This project builds a local retail data platform that ingests, cleans, models, and visualizes sales, inventory, and customer data.  
The goal is to turn messy retail data into clear business answers using a medallion architecture (Raw → Bronze → Silver → Gold → Dashboard).

---

## 🛒 Retail Scenario
- Business type: [e.g., small grocery chain / fashion store / electronics shop]
- Data sources:
  - POS sales (CSV)
  - E‑commerce orders (JSON)
  - Warehouse stock (PostgreSQL/CSV)
  - CRM customers (CSV/API mock)
  - Supplier deliveries (JSON/Parquet)

---

## 📂 Source Data Plan
| Source              | Format | Purpose                  | Issues to Simulate |
|---------------------|--------|--------------------------|--------------------|
| POS sales           | CSV    | Store transactions       | Duplicate IDs, invalid prices |
| E‑commerce orders   | JSON   | Online orders/events     | Missing customer IDs, mixed timestamps |
| Warehouse stock     | CSV/DB | Inventory snapshots      | Negative quantities |
| CRM customers       | CSV/API| Customer profiles        | Invalid email formats |
| Supplier deliveries | JSON   | Purchase orders          | Future‑dated records |

---

## 🏗️ Architecture Diagram
*(Insert diagram image here – Medallion flow)*  
Raw → Bronze → Silver → Gold → Dashboard

Tools:
- Storage: MinIO (local S3)
- Orchestration: Apache Airflow
- Database: PostgreSQL/DuckDB
- Transformations: Python + dbt
- Quality: Great Expectations
- Dashboard: Apache Superset
- Environment: Docker Compose
- Version Control: GitHub + Actions

---

## 📊 ERD Draft (Gold Schema)
**Dimensions**
- `dim_customer` – Clean customer profile (hashed email)
- `dim_product` – Unified product catalogue
- `dim_store` – Store and region info
- `dim_date` – Calendar fields

**Facts**
- `fact_sales_transaction` – One row per sales line item
- `fact_inventory_snapshot` – Daily stock position by product/location

*(Insert ERD diagram here)*

---

## 📅 Eight‑Week Plan
1. Plan – Sources, architecture, ERD
2. Setup – Docker, MinIO, DB, seed data
3. Ingest – Raw landing + Bronze parsing
4. Clean – Silver cleaning + quality checks
5. Model – Gold schema + dbt tests
6. Dashboards – Superset charts + quality view
7. Reliability – CI/CD, logging, fixes
8. Handover – Report + demo

---

## ✅ Week 1 Deliverables
- GitHub repo initialized
- Task board created (GitHub Projects/Trello)
- Architecture diagram
- ERD draft
- Source data plan documented

---

## 🔗 Evidence
- Repo link: [Insert here]
- Task board screenshot: [Insert here]
- Architecture diagram: [Insert here]
- ERD draft: [Insert here]
- Source data plan: [Insert here]
