import pandas as pd
from sqlalchemy import create_engine
import json

# ✅ Connection string (your password is secretsd)
engine = create_engine("postgresql://postgres:secretsd@127.0.0.1:5432/retail")

# ✅ Load files from dataset folder (matching actual names)
df_pos = pd.read_csv("dataset/pos_sales.csv")
df_ecom = pd.read_json("dataset/ecommerce_orders.json")
df_wh = pd.read_csv("dataset/warehouse_stock.csv")
df_crm = pd.read_csv("dataset/crm_customers.csv")
df_sup = pd.read_json("dataset/supplier_deliveries.json")

# ✅ Write DataFrames into Postgres tables
df_pos.to_sql("raw_pos_sales", engine, if_exists="replace", index=False)
df_ecom.to_sql("raw_ecommerce_orders", engine, if_exists="replace", index=False)
df_wh.to_sql("raw_warehouse_stock", engine, if_exists="replace", index=False)
df_crm.to_sql("raw_crm_customers", engine, if_exists="replace", index=False)
df_sup.to_sql("raw_supplier_deliveries", engine, if_exists="replace", index=False)

print("✅ All datasets loaded into Postgres successfully!")
