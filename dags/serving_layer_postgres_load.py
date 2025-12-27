"""
Airflow DAG - Serving Layer PostgreSQL Load
Merges batch and speed layer data and loads to PostgreSQL for Metabase
Supports both full and incremental loads with file tracking
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
import sys
sys.path.insert(0, '/opt/airflow')

from src.serving.postgres_loader import load_serving_layer, initial_full_load, incremental_load


# =============================================================================
# DAG DEFINITION
# =============================================================================

default_args = {
    'owner': 'aviation',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'serving_layer_postgres_load',
    default_args=default_args,
    description='Load merged batch and speed layer data to PostgreSQL',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['aviation', 'serving-layer', 'postgres', 'metabase'],
    params={
        'full_load': Param(
            default=False,
            type='boolean',
            description='Set to True for initial full load, False for incremental'
        )
    }
)


# =============================================================================
# TASKS
# =============================================================================

def load_postgres_task(**context):
    """Load serving layer to PostgreSQL."""
    # Get full_load parameter from DAG run config
    full_load = context['params'].get('full_load', False)
    
    if full_load:
        print("="*60)
        print("RUNNING FULL LOAD (overwriting all data)")
        print("="*60)
        initial_full_load()
    else:
        print("="*60)
        print("RUNNING INCREMENTAL LOAD (only new files)")
        print("="*60)
        incremental_load()
    
    print("Serving layer load complete")


load_postgres = PythonOperator(
    task_id='load_postgres',
    python_callable=load_postgres_task,
    dag=dag,
)


# =============================================================================
# TASK DEPENDENCIES
# =============================================================================

load_postgres
