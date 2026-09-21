from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine

# --- ETL Functions ---
def extract_data(**kwargs):
    dataset_path = "/opt/airflow/dags/dataset"

    # Read raw files
    customers = pd.read_csv(os.path.join(dataset_path, "crm_customers.csv"))
    products = pd.read_csv(os.path.join(dataset_path, "warehouse_stock.csv"))
    sales = pd.read_csv(os.path.join(dataset_path, "pos_sales.csv"))
    suppliers = pd.read_json(os.path.join(dataset_path, "supplier_deliveries.json"))
    ecommerce = pd.read_json(os.path.join(dataset_path, "ecommerce_orders.json"))

    # Push to XCom
    kwargs['ti'].xcom_push(key='customers', value=customers.to_dict())
    kwargs['ti'].xcom_push(key='products', value=products.to_dict())
    kwargs['ti'].xcom_push(key='sales', value=sales.to_dict())
    kwargs['ti'].xcom_push(key='suppliers', value=suppliers.to_dict())

def transform_data(**kwargs):
    customers = pd.DataFrame(kwargs['ti'].xcom_pull(key='customers'))
    products = pd.DataFrame(kwargs['ti'].xcom_pull(key='products'))
    sales = pd.DataFrame(kwargs['ti'].xcom_pull(key='sales'))
    suppliers = pd.DataFrame(kwargs['ti'].xcom_pull(key='suppliers'))

    # Example transformation: join sales with customers + products
    fact_sales = sales.merge(customers, on='customer_id') \
                      .merge(products, on='product_id')

    # Push transformed data
    kwargs['ti'].xcom_push(key='fact_sales', value=fact_sales.to_dict())
    kwargs['ti'].xcom_push(key='customers', value=customers.to_dict())
    kwargs['ti'].xcom_push(key='products', value=products.to_dict())
    kwargs['ti'].xcom_push(key='suppliers', value=suppliers.to_dict())

def load_data(**kwargs):
    fact_sales = pd.DataFrame(kwargs['ti'].xcom_pull(key='fact_sales'))
    sales = pd.DataFrame(kwargs['ti'].xcom_pull(key='sales'))
    customers = pd.DataFrame(kwargs['ti'].xcom_pull(key='customers'))
    products = pd.DataFrame(kwargs['ti'].xcom_pull(key='products'))
    suppliers = pd.DataFrame(kwargs['ti'].xcom_pull(key='suppliers'))
    ecommerce = pd.read_json("/opt/airflow/dags/dataset/ecommerce_orders.json")

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@retail_postgres:5432/retail"
    )
    source_tables = {
        "raw_crm_customers": customers,
        "raw_warehouse_stock": products,
        "raw_pos_sales": sales,
        "raw_supplier_deliveries": suppliers,
        "raw_ecommerce_orders": ecommerce,
        "fact_sales": sales,
        "fact_sales_loaded": sales,
    }
    for table_name, dataframe in source_tables.items():
        dataframe.to_sql(table_name, engine, if_exists="replace", index=False)

    customer_dimension = sales[["customer_id"]].dropna().drop_duplicates().reset_index(drop=True)
    customer_profiles = customers.drop_duplicates("customer_id").reset_index(drop=True)
    profile_indexes = customer_dimension.index % len(customer_profiles)
    customer_dimension["customer_name"] = customer_profiles.loc[
        profile_indexes, "name"
    ].to_numpy()
    customer_dimension["hashed_email"] = customer_profiles.loc[
        profile_indexes, "email"
    ].to_numpy()
    customer_dimension["loyalty_tier"] = customer_profiles.loc[
        profile_indexes, "loyalty_tier"
    ].to_numpy()
    customer_dimension["region"] = customer_profiles.loc[
        profile_indexes, "region"
    ].to_numpy()

    product_dimension = products.drop_duplicates("product_id").copy()
    product_dimension["product_name"] = product_dimension["product_id"].map(
        lambda product_id: f"product_{product_id}"
    )
    product_dimension["category"] = "unknown"
    supplier_by_product = suppliers.drop_duplicates("product_id").set_index("product_id")[
        "supplier_id"
    ]
    product_dimension["supplier_id"] = product_dimension["product_id"].map(
        supplier_by_product
    )
    product_dimension = product_dimension[
        ["product_id", "product_name", "category", "supplier_id"]
    ]

    supplier_dimension = suppliers.drop_duplicates("supplier_id").copy()
    supplier_dimension["supplier_name"] = suppliers.drop_duplicates("supplier_id")[
        "supplier_id"
    ].map(lambda supplier_id: f"supplier_{supplier_id}").to_numpy()
    supplier_dimension["contact_info"] = "unknown"
    supplier_dimension = supplier_dimension[["supplier_id", "supplier_name", "contact_info"]]

    warehouse_dimension = products[["store_id"]].drop_duplicates().rename(
        columns={"store_id": "warehouse_id"}
    )
    warehouse_dimension["warehouse_name"] = warehouse_dimension["warehouse_id"].map(
        lambda warehouse_id: f"warehouse_{warehouse_id}"
    )
    warehouse_dimension["location"] = "unknown"
    warehouse_dimension = warehouse_dimension[
        ["warehouse_id", "warehouse_name", "location"]
    ]

    store_dimension = sales[["store_id"]].drop_duplicates().copy()
    store_dimension["store_name"] = store_dimension["store_id"].map(
        lambda store_id: f"store_{store_id}"
    )
    regions = sorted(customers["region"].dropna().unique())
    store_dimension["region"] = store_dimension["store_id"].map(
        lambda store_id: regions[(int(store_id) - 1) % len(regions)]
    )
    store_dimension = store_dimension[["store_id", "store_name", "region"]]

    dimensions = {
        "dim_customers": customer_dimension,
        "dim_products": product_dimension,
        "dim_suppliers": supplier_dimension,
        "dim_warehouse": warehouse_dimension,
        "dim_store": store_dimension,
    }
    for table_name, dataframe in dimensions.items():
        dataframe.to_sql(table_name, engine, if_exists="replace", index=False)

    engine.dispose()

# --- DAG Definition ---
with DAG(
    dag_id="etl_pipeline_week3",
    start_date=datetime(2026, 9, 11),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load
