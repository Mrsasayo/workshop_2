from datetime import timedelta, datetime
from airflow.decorators import dag, task
import pandas as pd
# Assuming these functions exist elsewhere and handle the core logic
# from some_module import (
#     extract_api_data, transform_api_data,
#     read_csv_data, transform_csv_data,
#     read_db_data, transform_db_data,
#     merge_dataframes, load_data_to_db, store_data_to_gdrive
# )

# Placeholder implementations for demonstration
def extract_api_data(): print("Extracting API data..."); return pd.DataFrame({'api_col': [1, 2]})
def transform_api_data(df): print("Transforming API data..."); return df.assign(api_transformed=True)
def read_csv_data(): print("Reading CSV data..."); return pd.DataFrame({'csv_col': ['a', 'b']})
def transform_csv_data(df): print("Transforming CSV data..."); return df.assign(csv_transformed=True)
def read_db_data(): print("Reading DB data..."); return pd.DataFrame({'db_col': [10.1, 10.2]})
def transform_db_data(df): print("Transforming DB data..."); return df.assign(db_transformed=True)
def merge_dataframes(df1, df2, df3): print("Merging dataframes..."); return pd.concat([df1, df2, df3], axis=1) # Simple concat for example
def load_data_to_db(df): print(f"Loading data to DB:\n{df}"); return df # Return df for store task
def store_data_to_gdrive(df): print(f"Storing data to GDrive/CSV:\n{df}"); return True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    dag_id="workshop_2_updated",
    default_args=default_args,
    description='ETL pipeline extracting from API, CSV, DB, transforming, merging, loading to DB, and storing to CSV.',
    schedule=timedelta(days=1),
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'workshop_2', 'postgres', 'gdrive'] 
)
def etl_pipeline():

    @task
    def extract() -> pd.DataFrame:
        """Task: Extracts data from the source API (WWW)."""
        df_api = extract_api_data()
        return df_api

    @task
    def transform(df_api: pd.DataFrame) -> pd.DataFrame:
        """Task: Transforms the data extracted from the API."""
        df_api_transformed = transform_api_data(df_api)
        return df_api_transformed

    @task
    def read_csv() -> pd.DataFrame:
        """Task: Reads data from the source CSV file."""
        df_csv = read_csv_data()
        return df_csv

    @task
    def transform_csv(df_csv: pd.DataFrame) -> pd.DataFrame:
        """Task: Transforms the data read from the CSV file."""
        df_csv_transformed = transform_csv_data(df_csv)
        return df_csv_transformed

    @task
    def read_db() -> pd.DataFrame:
        """Task: Reads data from the source SQL database."""
        df_db = read_db_data()
        return df_db

    @task
    def transform_db(df_db: pd.DataFrame) -> pd.DataFrame:
        """Task: Transforms the data read from the SQL database."""
        df_db_transformed = transform_db_data(df_db)
        return df_db_transformed

    @task
    def merge(df_api_transformed: pd.DataFrame, df_csv_transformed: pd.DataFrame, df_db_transformed: pd.DataFrame) -> pd.DataFrame:
        """Task: Merges the three transformed data streams (from API, CSV, DB sources)."""
        df_merged = merge_dataframes(df_api_transformed, df_csv_transformed, df_db_transformed)
        return df_merged

    @task
    def load(df_merged: pd.DataFrame) -> pd.DataFrame:
        """Task: Loads the merged and transformed data into the target SQL database."""
        df_loaded = load_data_to_db(df_merged)
        return df_loaded

    @task
    def store(df_to_store: pd.DataFrame):
        """Task: Stores the final transformed dataset as a CSV file in Google Drive (or similar file storage)."""
        store_success = store_data_to_gdrive(df_to_store)
        if not store_success:
            raise ValueError("Storing data to CSV/Gdrive failed.")

    api_data = extract()
    api_transformed = transform(api_data)

    csv_data = read_csv()
    csv_transformed = transform_csv(csv_data)

    db_data = read_db()
    db_transformed = transform_db(db_data)

    merged_data = merge(api_transformed, csv_transformed, db_transformed)
    loaded_data = load(merged_data)
    store(loaded_data)

etl_pipeline()