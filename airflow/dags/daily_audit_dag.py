import os
from datetime import datetime, timedelta
import pandas as pd
import pymssql
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------
# Global Configurations
# ----------------------------------------------------
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = int(os.getenv("DB_PORT", 1433))

if not DB_USER or not DB_PASSWORD or not DB_NAME:
    raise ValueError("CRITICAL SECURITY ERROR: Database credentials ('DB_USER', 'DB_PASSWORD', 'DB_NAME') are not fully defined in environment variables!")

MSSQL_CONN_PARAMS = {
    "server": "mssql",
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "port": DB_PORT
}

MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError("CRITICAL SECURITY ERROR: MinIO credentials ('MINIO_ROOT_USER' / 'MINIO_ROOT_PASSWORD') are not defined!")

MINIO_STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}


def run_daily_audit(**kwargs):
    """
    Reads clean orders from MinIO S3 Parquet and caught fraud records from SQL Server,
    calculates daily aggregated business metrics, and writes results to SQL Server.
    Designed to be fully idempotent (safe to re-execute for any execution date).
    """
    execution_date = kwargs.get('templates_dict', {}).get('ds')
    if not execution_date:
        execution_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
    print(f"Executing daily audit aggregation for date: {execution_date}")
    
    # 1. Connect to MS SQL Server
    conn = pymssql.connect(**MSSQL_CONN_PARAMS)
    cursor = conn.cursor()
    
    # 2. Query anomaly stats and fraud order IDs for the target date
    try:
        # Get count of fraud events
        query_count = """
            SELECT COUNT(*) 
            FROM supheli_siparisler 
            WHERE CAST(order_timestamp AS DATE) = %s;
        """
        cursor.execute(query_count, (execution_date,))
        total_fraud_count = cursor.fetchone()[0]
        
        # Get list of fraud order IDs to filter out from raw S3 dataset
        query_ids = """
            SELECT order_id 
            FROM supheli_siparisler 
            WHERE CAST(order_timestamp AS DATE) = %s;
        """
        cursor.execute(query_ids, (execution_date,))
        fraud_order_ids = set([row[0] for row in cursor.fetchall()])
        
    except Exception as e:
        print(f"Error querying SQL Server anomalies: {str(e)}")
        total_fraud_count = 0
        fraud_order_ids = set()
        
    # 3. Read raw records from S3 Data Lake and filter out caught fraud IDs
    try:
        df_raw = pd.read_parquet(
            "s3://ecommerce-lake/orders", 
            storage_options=MINIO_STORAGE_OPTIONS
        )
        
        if not df_raw.empty:
            df_raw['date'] = pd.to_datetime(df_raw['order_timestamp']).dt.strftime('%Y-%m-%d')
            # Filter for target date and exclude order IDs registered in the fraud table
            df_target = df_raw[
                (df_raw['date'] == execution_date) & 
                (~df_raw['order_id'].isin(fraud_order_ids))
            ]
            
            total_clean_orders = len(df_target)
            total_clean_revenue = (df_target['price'] * df_target['quantity']).sum()
        else:
            total_clean_orders = 0
            total_clean_revenue = 0.0
            
    except Exception as e:
        print(f"Warning: S3 bucket or parquet path not initialized yet: {str(e)}")
        total_clean_orders = 0
        total_clean_revenue = 0.0
        
    total_total_orders = total_clean_orders + total_fraud_count
    fraud_ratio = (total_fraud_count / total_total_orders) if total_total_orders > 0 else 0.0
    
    # 3. Write summary metrics using an idempotent upsert pattern
    try:
        cursor.execute("SELECT COUNT(*) FROM gunluk_ozet WHERE summary_date = %s", (execution_date,))
        exists = cursor.fetchone()[0] > 0
        
        if exists:
            update_query = """
                UPDATE gunluk_ozet 
                SET total_orders = %s, total_clean_revenue = %s, total_fraud_count = %s, fraud_ratio = %s
                WHERE summary_date = %s;
            """
            cursor.execute(
                update_query, 
                (total_total_orders, total_clean_revenue, total_fraud_count, fraud_ratio, execution_date)
            )
        else:
            insert_query = """
                INSERT INTO gunluk_ozet (summary_date, total_orders, total_clean_revenue, total_fraud_count, fraud_ratio)
                VALUES (%s, %s, %s, %s, %s);
            """
            cursor.execute(
                insert_query, 
                (execution_date, total_total_orders, total_clean_revenue, total_fraud_count, fraud_ratio)
            )
        conn.commit()
        print(f"Audit summary successfully saved for date: {execution_date}")
    except Exception as e:
        print(f"Failed to write audit summary: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def run_data_quality_checks(**kwargs):
    """
    Executes automated data quality queries against MS SQL Server.
    Raises ValueError to halt downstream tasks if quality anomalies are found.
    """
    execution_date = kwargs.get('templates_dict', {}).get('ds')
    if not execution_date:
        execution_date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
    print(f"Executing data quality checks for date: {execution_date}")
    
    conn = pymssql.connect(**MSSQL_CONN_PARAMS)
    cursor = conn.cursor()
    
    try:
        # Check 1: Anomaly reason column must not contain null or blank values
        cursor.execute(
            "SELECT COUNT(*) FROM supheli_siparisler WHERE anomaly_reason IS NULL OR anomaly_reason = '';"
        )
        null_reasons = cursor.fetchone()[0]
        if null_reasons > 0:
            raise ValueError(f"Data Quality Alert: Found {null_reasons} records with null/empty anomaly reasons!")
            
        # Check 2: Transactions flagged as ZERO_PRICE must have a price value <= 0
        cursor.execute(
            "SELECT COUNT(*) FROM supheli_siparisler WHERE anomaly_reason = 'ZERO_PRICE' AND price > 0;"
        )
        invalid_prices = cursor.fetchone()[0]
        if invalid_prices > 0:
            raise ValueError(f"Data Quality Alert: Found {invalid_prices} price anomalies with price > 0!")
            
        print("All data quality checks passed successfully.")
    finally:
        cursor.close()
        conn.close()


# ----------------------------------------------------
# DAG Configuration
# ----------------------------------------------------
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ecommerce_daily_audit',
    default_args=default_args,
    description='Idempotent daily business aggregation and data quality check pipeline',
    schedule_interval='@daily',
    catchup=False
) as dag:

    audit_task = PythonOperator(
        task_id='daily_audit_aggregation',
        python_callable=run_daily_audit,
        templates_dict={'ds': '{{ ds }}'}
    )

    dq_task = PythonOperator(
        task_id='data_quality_verification',
        python_callable=run_data_quality_checks,
        templates_dict={'ds': '{{ ds }}'}
    )

    audit_task >> dq_task
