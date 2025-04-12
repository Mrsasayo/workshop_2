from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd
import sys
import os
import logging

logger_dag = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO) # Asegurarse de que el logging esté configurado

dags_path = os.path.dirname(__file__)
if dags_path not in sys.path:
    sys.path.append(dags_path)
    logger_dag.info(f"dag.py: Appended to sys.path: {dags_path}")

try:
    from task_to_dag import (
        task_extract_api,
        task_read_csv,
        task_read_db,
        task_transform_api,
        task_transform_csv,
        task_transform_db,
        task_merge,
        task_load,
        task_store
    )
    logger_dag.info("dag.py: Successfully imported task wrapper functions from task_to_dag.")
except ImportError as e:
     logger_dag.error(f"dag.py: Error importing from task_to_dag: {e}. DAG definition might fail or tasks will use placeholders.", exc_info=True)
     def task_extract_api(): logger_dag.error("Placeholder task_extract_api called"); return pd.DataFrame()
     def task_read_csv(): logger_dag.error("Placeholder task_read_csv called"); return pd.DataFrame()
     def task_read_db(): logger_dag.error("Placeholder task_read_db called"); return pd.DataFrame()
     def task_transform_api(df): logger_dag.error("Placeholder task_transform_api called"); return pd.DataFrame()
     def task_transform_csv(df): logger_dag.error("Placeholder task_transform_csv called"); return pd.DataFrame()
     def task_transform_db(df): logger_dag.error("Placeholder task_transform_db called"); return pd.DataFrame()
     def task_merge(*dfs): logger_dag.error("Placeholder task_merge called"); return pd.DataFrame()
     def task_load(df): logger_dag.error("Placeholder task_load called"); return False # O un DF vacío si la tarea siguiente lo espera
     def task_store(df): logger_dag.error("Placeholder task_store called"); return False


default_args = {
    'owner': 'nico',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id="workshop",
    default_args=default_args,
    description='ETL pipeline using task wrappers: Extract -> Transform -> Merge -> Load -> Store',
    schedule=None, 
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'workshop_2', 'refactored', 'youtube', 'spotify', 'grammy']
)
def etl_pipeline_v2_refactored():


    @task
    def extract_api_task() -> pd.DataFrame:
        """DAG Task: Extrae datos de YouTube API llamando al wrapper."""
        logger_dag.info("Executing DAG task: extract_api_task")
        df = task_extract_api() 
        if not isinstance(df, pd.DataFrame):
             logger_dag.error("extract_api_task: Wrapper did not return a DataFrame. Raising error.")
             raise TypeError(f"Task wrapper task_extract_api failed to return DataFrame (got {type(df)})")
        logger_dag.info(f"DAG task extract_api_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def transform_api_task(df_api: pd.DataFrame) -> pd.DataFrame:
        """DAG Task: Transforma datos API llamando al wrapper."""
        logger_dag.info("Executing DAG task: transform_api_task")
        df = task_transform_api(df_api)
        if not isinstance(df, pd.DataFrame):
             logger_dag.error("transform_api_task: Wrapper did not return a DataFrame. Returning empty.")
             return pd.DataFrame()
        logger_dag.info(f"DAG task transform_api_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def read_csv_task() -> pd.DataFrame:
        """DAG Task: Lee datos CSV (Spotify) llamando al wrapper."""
        logger_dag.info("Executing DAG task: read_csv_task")
        df = task_read_csv()
        if not isinstance(df, pd.DataFrame):
            logger_dag.error("read_csv_task: Wrapper did not return a DataFrame. Raising error.")
            raise TypeError(f"Task wrapper task_read_csv failed to return DataFrame (got {type(df)})")
        logger_dag.info(f"DAG task read_csv_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def transform_csv_task(df_csv: pd.DataFrame) -> pd.DataFrame:
        """DAG Task: Transforma datos CSV llamando al wrapper."""
        logger_dag.info("Executing DAG task: transform_csv_task")
        df = task_transform_csv(df_csv)
        if not isinstance(df, pd.DataFrame):
             logger_dag.error("transform_csv_task: Wrapper did not return a DataFrame. Returning empty.")
             return pd.DataFrame()
        logger_dag.info(f"DAG task transform_csv_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def read_db_task() -> pd.DataFrame:
        """DAG Task: Lee datos DB (Grammy) llamando al wrapper."""
        logger_dag.info("Executing DAG task: read_db_task")
        df = task_read_db()
        if not isinstance(df, pd.DataFrame):
            logger_dag.error("read_db_task: Wrapper did not return a DataFrame. Raising error.")
            raise TypeError(f"Task wrapper task_read_db failed to return DataFrame (got {type(df)})")
        logger_dag.info(f"DAG task read_db_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def transform_db_task(df_db: pd.DataFrame) -> pd.DataFrame:
        """DAG Task: Transforma datos DB llamando al wrapper."""
        logger_dag.info("Executing DAG task: transform_db_task")
        df = task_transform_db(df_db)
        if not isinstance(df, pd.DataFrame):
             logger_dag.error("transform_db_task: Wrapper did not return a DataFrame. Returning empty.")
             return pd.DataFrame()
        logger_dag.info(f"DAG task transform_db_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def merge_task(df_api: pd.DataFrame, df_csv: pd.DataFrame, df_db: pd.DataFrame) -> pd.DataFrame:
        """DAG Task: Mergea los DataFrames transformados llamando al wrapper."""
        logger_dag.info("Executing DAG task: merge_task")
        df = task_merge(df_api, df_csv, df_db)
        if not isinstance(df, pd.DataFrame):
             logger_dag.error("merge_task: Wrapper did not return a DataFrame. Returning empty.")
             return pd.DataFrame()
        logger_dag.info(f"DAG task merge_task finished. DataFrame shape: {df.shape}")
        return df

    @task
    def load_task(df_merged: pd.DataFrame) -> pd.DataFrame:
        """DAG Task: Carga los datos mergeados a la DB llamando al wrapper.
           Devuelve el DF por si store lo necesita (alternativamente, store podría leer de XCom)."""
        logger_dag.info("Executing DAG task: load_task")
        success = task_load(df_merged)
        if not success:
             logger_dag.error("load_task: Wrapper reported failure. Raising error to fail DAG.")
             raise ValueError("Load task wrapper failed.")
        logger_dag.info("DAG task load_task finished.")
        return df_merged

    @task
    def store_task(df_to_store: pd.DataFrame):
        """DAG Task: Almacena el dataset final llamando al wrapper."""
        logger_dag.info("Executing DAG task: store_task")
        success = task_store(df_to_store)
        if not success:
            logger_dag.error("store_task: Wrapper reported failure. Raising error.")
            raise ValueError("Store task wrapper failed.")
        logger_dag.info("DAG task store_task finished.")


    api_data = extract_api_task()
    api_transformed = transform_api_task(api_data)

    csv_data = read_csv_task()
    csv_transformed = transform_csv_task(csv_data)

    db_data = read_db_task()
    db_transformed = transform_db_task(db_data)

    merged_data = merge_task(api_transformed, csv_transformed, db_transformed)

    df_loaded = load_task(merged_data)

    store_task(df_loaded)

etl_dag_instance = etl_pipeline_v2_refactored()