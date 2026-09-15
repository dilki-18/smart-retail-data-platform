from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# --- ETL Functions ---
def extract_data(**kwargs):
    # Define dataset path relative to project root
    dataset_path = "/opt/airflow/dags/dataset"

    # Read raw files
    customers = pd.read_csv(os.path.join(dataset_path, "crm_customers.csv"))
    products = pd.read_csv(os.path.join(dataset_path, "warehouse_stock.csv"))
    sales = pd.read_csv(os.path.join(dataset_path, "pos_sales.csv"))

    # Push to XCom
    kwargs['ti'].xcom_push(key='customers', value=customers.to_dict())
    kwargs['ti'].xcom_push(key='products', value=products.to_dict())
    kwargs['ti'].xcom_push(key='sales', value=sales.to_dict())

def transform_data(**kwargs):
    customers = pd.DataFrame(kwargs['ti'].xcom_pull(key='customers'))
    products = pd.DataFrame(kwargs['ti'].xcom_pull(key='products'))
    sales = pd.DataFrame(kwargs['ti'].xcom_pull(key='sales'))

    # Example transformation: join sales with customers + products
    fact_sales = sales.merge(customers, on='customer_id') \
                      .merge(products, on='product_id')

    # Push transformed data
    kwargs['ti'].xcom_push(key='fact_sales', value=fact_sales.to_dict())

def load_data(**kwargs):
    fact_sales = pd.DataFrame(kwargs['ti'].xcom_pull(key='fact_sales'))

    conn = psycopg2.connect(
        host="retail_postgres",
        database="retail",
        user="postgres",
        password="postgres"
    )
    cur = conn.cursor()

    # Create fact_sales table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_sales (
            transaction_id SERIAL PRIMARY KEY,
            customer_id INT,
            product_id INT,
            store_id INT,
            date DATE,
            quantity INT,
            net_sales_amount NUMERIC
        );
    """)

    # Insert rows
    for _, row in fact_sales.iterrows():
        cur.execute("""
            INSERT INTO fact_sales (customer_id, product_id, store_id, date, quantity, net_sales_amount)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            row.get('customer_id'),
            row.get('product_id'),
            row.get('store_id', 1),  # default store_id if missing
            row.get('date'),
            row.get('quantity'),
            row.get('net_sales_amount')
        ))

    conn.commit()
    cur.close()
    conn.close()

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
