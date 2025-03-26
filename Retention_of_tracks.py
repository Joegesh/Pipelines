from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging


def delete_records():
    client = bigquery.Client()

    check_query = """
        SELECT COUNT(*) as Total
        FROM `dev-index-453506-u7.Practice.Ilayaraja_hits`
        WHERE ID IS NOT NULL
        """
    results = client.query(check_query).result()
    row = list(results)[0]
    count = row.get("Total")

    if count == 0:
        logging.info("No more records to delete. Task completed")
        return

    delete_query = """
        DELETE FROM `dev-index-453506-u7.Practice.Ilayaraja_hits`
        WHERE ID IN(
        SELECT ID FROM `dev-index-453506-u7.Practice.Ilayaraja_hits`
        ORDER BY Popularity ASC 
        LIMIT 20)
    """
    delete_job = client.query(delete_query)
    delete_job.result()
    logging.info(f"Deleted 20 records! {count - 20 if count > 20 else 0} remaining...")


default_args = {
    'start_date': datetime(2025,3,13,13,0,0),
    'retries':1,
    'retry_delay':timedelta(minutes=5),
}

dag = DAG(
    'delete_20_records',
    default_args=default_args,
    description='Deletes 20 records daily from Ilayaraja_hits table',
    schedule_interval='@daily',
    catchup=False,
    tags=['bigquery', 'cleanup']
)

delete_task = PythonOperator(
    task_id='delete_records_task',
    python_callable=delete_records,
    dag=dag,
)

