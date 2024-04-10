from sqlalchemy import create_engine
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import DAG
from scripts.main import df_ordenado


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 31, 11, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_and_store',
    default_args=default_args,
    description='Scrape MercadoLibre and store results in database',
    schedule_interval='30 11 * * *',
)

# SQL code for table creation
create_table_sql = """
CREATE TABLE IF NOT EXISTS ml_celu_mas_vendido (
    Puesto VARCHAR(50),
    Producto VARCHAR(255),
    Precio VARCHAR(100),
    Fecha_Actual DATE   
);
"""

def create_table():
    engine = create_engine('mysql://airflow:airflow@mysql:3306/airflow')
    engine.execute(create_table_sql)

def load_dataframe_to_database():
    engine = create_engine('mysql://airflow:airflow@mysql:3306/airflow')
    df_ordenado.to_sql('ml_celu_mas_vendido', con=engine, if_exists='replace', index=False)

def store_in_mysql():
    engine = create_engine('mysql://airflow:airflow@mysql:3306/airflow')
    connection = engine.connect()
    df_ordenado.to_sql('ml_celu_mas_vendido', con=connection, if_exists='append', index=False)
    connection.close()

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

load_dataframe_task = PythonOperator(
    task_id='load_dataframe_to_database',
    python_callable=load_dataframe_to_database,
    dag=dag,
)

scrape_and_store_task = PythonOperator(
    task_id='scrape_and_store',
    python_callable=store_in_mysql,
    dag=dag,
)

create_table_task >> load_dataframe_task >> scrape_and_store_task
