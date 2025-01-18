from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.config import load_config
from utils.extract import extract_products_data, extract_transactions_data
from utils.transform import clean_products, clean_transactions, products_revenue, sales_channel_performance, customer_frequency
from utils.load import load_to_postgres
import os

#get proper working directory
os.chdir(os.path.dirname(__file__))
print(os.getcwd())
CONFIG_FILE = "config/config.yaml"
config = load_config(CONFIG_FILE)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='etl pipeline from local csv as primary source and json api as secondary source',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    def extract_task(**kwargs):
        products_data = extract_products_data(config)
        transactions_data = extract_transactions_data(config)
        kwargs['ti'].xcom_push(key='products_data', value=products_data)
        kwargs['ti'].xcom_push(key='transactions_data', value=transactions_data)

    def transform_task(**kwargs):
        products_data = kwargs['ti'].xcom_pull(task_ids='extract_product', key='products_data')
        clean_products_data = clean_products(products_data)
        transactions_data = kwargs['ti'].xcom_pull(task_ids='extract_transactions', key='transactions_data') 
        clean_transactions_data = clean_transactions(transactions_data)
        product_revenue_data = products_revenue(clean_transactions_data)
        sales_channel_data = sales_channel_performance(clean_transactions_data)
        customer_frequency_data = customer_frequency(clean_transactions_data)
        kwargs['ti'].xcom_push(key='transformed_products_data', value=clean_products_data)
        kwargs['ti'].xcom_push(key='transformed_transactions_data', value=clean_transactions_data)
        kwargs['ti'].xcom_push(key='product_revenue_data', value=product_revenue_data)
        kwargs['ti'].xcom_push(key='sales_channel_data', value=sales_channel_data)
        kwargs['ti'].xcom_push(key='customer_frequency_data', value=customer_frequency_data)

    def load_task(**kwargs):
        transformed_product_data = kwargs['ti'].xcom_pull(task_ids='transformed_product', key='transformed_products_data')
        transformed_transactions_data = kwargs['ti'].xcom_pull(task_ids='transformed_transactions', key = 'transformed_transactions_data')
        load_product_revenue_data = kwargs['ti'].xcom_pull(task_ids='product_revenue', key = 'product_revenue_data')
        load_sales_channel_data = kwargs['ti'].xcom_pull(task_ids='sales_channel', key = 'sales_channel_data')
        load_customer_frequency_data = kwargs['ti'].xcom_pull(task_ids='customer_frequency', key = 'customer_frequency_data')
        load_to_postgres(transformed_product_data, config['database']['table_name_products'], config['database']['connection_string'])
        load_to_postgres(transformed_transactions_data, config['database']['table_name_transactions'], config['database']['connection_string'])
        load_to_postgres(load_product_revenue_data, config['database']['table_name_product_revenue'], config['database']['connection_string'])
        load_to_postgres(load_sales_channel_data, config['database']['table_name_sales_channel'], config['database']['connection_string'])
        load_to_postgres(load_customer_frequency_data, config['database']['table_name_customer_frequency'], config['database']['connection_string'])


    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_task
    )

    extract >> transform >> load