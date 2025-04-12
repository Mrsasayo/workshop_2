# ---------INICIO DEL CAMBIO-----------
import pandas as pd
import logging
import sys
import os

# Configuración del logging (opcional aquí, el DAG principal también puede configurar)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - TASK_WRAPPER - %(message)s')
logger = logging.getLogger(__name__)

# --- Gestión de PYTHONPATH ---
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if src_path not in sys.path:
    sys.path.append(src_path)
    logger.info(f"task_to_dag.py: Appended to sys.path: {src_path}")

# --- Importar Funciones de Lógica Real ---
try:
    # Extract
    from extract.extract_api import extract_youtube_data_api
    from extract.extract_csv import extract_spotify_data_csv
    from extract.extract_sql import extract_grammy_data_sql
    # Transform
    from transform.transform_api import clean_youtube_data
    from transform.transform_csv import clean_spotify_data
    from transform.transform_sql import clean_grammy_data
    # Merge
    from transform.transform_merge import merge_all_data
    # Load <<< --- NUEVA IMPORTACIÓN --- >>>
    from load.load_to_db import load_dataframe_to_postgres
    # --- Importaciones futuras (placeholders por ahora) ---
    # from load.store_to_gdrive import store_to_gdrive_logic # O store_to_csv
    logger.info("task_to_dag.py: Successfully imported logic functions (extract, transform, merge, load).")
except ImportError as e:
    logger.error(f"task_to_dag.py: Error importing logic functions: {e}. DAG tasks might fail.", exc_info=True)
    # Definir placeholders para que el módulo cargue sin error fatal
    def extract_youtube_data_api(): logger.error("Placeholder: extract_youtube_data_api called"); return pd.DataFrame()
    def extract_spotify_data_csv(): logger.error("Placeholder: extract_spotify_data_csv called"); return pd.DataFrame()
    def extract_grammy_data_sql(): logger.error("Placeholder: extract_grammy_data_sql called"); return pd.DataFrame()
    def clean_youtube_data(df): logger.error("Placeholder: clean_youtube_data called"); return pd.DataFrame()
    def clean_spotify_data(df): logger.error("Placeholder: clean_spotify_data called"); return pd.DataFrame()
    def clean_grammy_data(df): logger.error("Placeholder: clean_grammy_data called"); return pd.DataFrame()
    def merge_all_data(df1, df2, df3): logger.error("Placeholder: merge_all_data called"); return pd.DataFrame()
    # Placeholder para load
    def load_dataframe_to_postgres(df, table, db, chunk): logger.error("Placeholder: load_dataframe_to_postgres called"); return False
    # Define placeholders para el resto si es necesario

# === Funciones Wrapper para cada Tarea del DAG ===

# --- EXTRACT Wrappers (Sin cambios) ---
def task_extract_api() -> pd.DataFrame:
    logger.info("Executing task wrapper: task_extract_api")
    try:
        df = extract_youtube_data_api();
        if not isinstance(df, pd.DataFrame): logger.error(f"Logic function extract_youtube_data_api did not return a DataFrame (got {type(df)})"); return pd.DataFrame()
        logger.info(f"Task wrapper task_extract_api finished. DataFrame shape: {df.shape}"); return df
    except Exception as e: logger.error(f"Error during task_extract_api execution: {e}", exc_info=True); return pd.DataFrame()

def task_read_csv() -> pd.DataFrame:
    logger.info("Executing task wrapper: task_read_csv")
    try:
        df = extract_spotify_data_csv();
        if not isinstance(df, pd.DataFrame): logger.error(f"Logic function extract_spotify_data_csv did not return a DataFrame (got {type(df)})"); return pd.DataFrame()
        logger.info(f"Task wrapper task_read_csv finished. DataFrame shape: {df.shape}"); return df
    except Exception as e: logger.error(f"Error during task_read_csv execution: {e}", exc_info=True); return pd.DataFrame()

def task_read_db() -> pd.DataFrame:
    logger.info("Executing task wrapper: task_read_db")
    try:
        df = extract_grammy_data_sql();
        if not isinstance(df, pd.DataFrame): logger.error(f"Logic function extract_grammy_data_sql did not return a DataFrame (got {type(df)})"); return pd.DataFrame()
        logger.info(f"Task wrapper task_read_db finished. DataFrame shape: {df.shape}"); return df
    except Exception as e: logger.error(f"Error during task_read_db execution: {e}", exc_info=True); return pd.DataFrame()

# --- TRANSFORM Wrappers (Sin cambios) ---
def task_transform_api(df_api_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("Executing task wrapper: task_transform_api")
    if not isinstance(df_api_raw, pd.DataFrame): logger.warning(f"Input to task_transform_api is not a DataFrame (got {type(df_api_raw)}). Returning empty DataFrame."); return pd.DataFrame()
    if df_api_raw.empty: logger.warning("Input DataFrame to task_transform_api is empty. Returning empty DataFrame."); return pd.DataFrame()
    try:
        df_transformed = clean_youtube_data(df_api_raw);
        logger.info(f"Task wrapper task_transform_api finished. Output shape: {df_transformed.shape if isinstance(df_transformed, pd.DataFrame) else 'N/A'}")
        if not isinstance(df_transformed, pd.DataFrame): logger.error("clean_youtube_data function did not return a DataFrame!"); return pd.DataFrame()
        return df_transformed
    except Exception as e: logger.error(f"Error during task_transform_api execution: {e}", exc_info=True); return pd.DataFrame()

def task_transform_csv(df_csv_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("Executing task wrapper: task_transform_csv")
    if not isinstance(df_csv_raw, pd.DataFrame): logger.warning(f"Input to task_transform_csv is not a DataFrame (got {type(df_csv_raw)}). Returning empty DataFrame."); return pd.DataFrame()
    if df_csv_raw.empty: logger.warning("Input DataFrame to task_transform_csv is empty. Returning empty DataFrame."); return pd.DataFrame()
    try:
        df_transformed = clean_spotify_data(df_csv_raw);
        logger.info(f"Task wrapper task_transform_csv finished. Output shape: {df_transformed.shape if isinstance(df_transformed, pd.DataFrame) else 'N/A'}")
        if not isinstance(df_transformed, pd.DataFrame): logger.error("clean_spotify_data function did not return a DataFrame!"); return pd.DataFrame()
        return df_transformed
    except Exception as e: logger.error(f"Error during task_transform_csv execution: {e}", exc_info=True); return pd.DataFrame()

def task_transform_db(df_db_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("Executing task wrapper: task_transform_db")
    if not isinstance(df_db_raw, pd.DataFrame): logger.warning(f"Input to task_transform_db is not a DataFrame (got {type(df_db_raw)}). Returning empty DataFrame."); return pd.DataFrame()
    if df_db_raw.empty: logger.warning("Input DataFrame to task_transform_db is empty. Returning empty DataFrame."); return pd.DataFrame()
    try:
        df_transformed = clean_grammy_data(df_db_raw);
        logger.info(f"Task wrapper task_transform_db finished. Output shape: {df_transformed.shape if isinstance(df_transformed, pd.DataFrame) else 'N/A'}")
        if not isinstance(df_transformed, pd.DataFrame): logger.error("clean_grammy_data function did not return a DataFrame!"); return pd.DataFrame()
        return df_transformed
    except Exception as e: logger.error(f"Error during task_transform_db execution: {e}", exc_info=True); return pd.DataFrame()

# --- MERGE Wrapper (Sin Cambios) ---
def task_merge(df_spotify_cleaned: pd.DataFrame, df_grammy_cleaned: pd.DataFrame, df_youtube_cleaned: pd.DataFrame) -> pd.DataFrame:
    logger.info("Executing task wrapper: task_merge")
    if not isinstance(df_spotify_cleaned, pd.DataFrame): logger.warning("Spotify input for merge is not a DataFrame."); df_spotify_cleaned=pd.DataFrame()
    if not isinstance(df_grammy_cleaned, pd.DataFrame): logger.warning("Grammy input for merge is not a DataFrame."); df_grammy_cleaned=pd.DataFrame()
    if not isinstance(df_youtube_cleaned, pd.DataFrame): logger.warning("YouTube input for merge is not a DataFrame."); df_youtube_cleaned=pd.DataFrame()
    logger.info(f"Input shapes for merge - Spotify: {df_spotify_cleaned.shape}, Grammy: {df_grammy_cleaned.shape}, YouTube: {df_youtube_cleaned.shape}")
    try:
        df_merged = merge_all_data(df_spotify_clean=df_spotify_cleaned, df_grammy_clean=df_grammy_cleaned, df_youtube_clean=df_youtube_cleaned);
        logger.info(f"Task wrapper task_merge finished. Output shape: {df_merged.shape if isinstance(df_merged, pd.DataFrame) else 'N/A'}")
        if not isinstance(df_merged, pd.DataFrame): logger.error("merge_all_data function did not return a DataFrame!"); return pd.DataFrame()
        return df_merged
    except Exception as e: logger.error(f"Error during task_merge execution: {e}", exc_info=True); return pd.DataFrame()

# --- LOAD Wrapper <<< --- ACTUALIZADO --- >>> ---
# Define constantes para la tabla y DB destino aquí o pásalas desde el DAG
TARGET_DB_NAME = "artists"
TARGET_TABLE_NAME = "artists_merged_final" # Nombre para la tabla final
LOAD_CHUNK_SIZE = 40000 # Chunksize para la carga

def task_load(df_merged: pd.DataFrame) -> bool:
    """Wrapper para la tarea de carga a DB que llama a la lógica real."""
    logger.info("Executing task wrapper: task_load")
    if not isinstance(df_merged, pd.DataFrame) or df_merged.empty:
        logger.warning(f"Input to task_load is not a valid/non-empty DataFrame (got {type(df_merged)}). Skipping load.")
        return False # Indicar skip o fallo

    logger.info(f"DataFrame received for loading to '{TARGET_TABLE_NAME}'. Shape: {df_merged.shape}")
    try:
        # --- ¡Llama a la función de carga real! ---
        success = load_dataframe_to_postgres(
            df_to_load=df_merged,
            target_table_name=TARGET_TABLE_NAME,
            db_name=TARGET_DB_NAME, # Especificar la DB aquí
            chunk_size=LOAD_CHUNK_SIZE # Usar el chunksize definido
        )
        # -----------------------------------------
        if success:
            logger.info(f"Task wrapper task_load finished successfully for table '{TARGET_TABLE_NAME}'.")
        else:
            logger.warning(f"Task wrapper task_load reported failure for table '{TARGET_TABLE_NAME}'.")
        return success
    except Exception as e:
        logger.error(f"Error during task_load execution: {e}", exc_info=True)
        return False # Indicar fallo

# --- STORE Wrapper (Placeholder - Sin cambios) ---
def task_store(df_loaded_data: pd.DataFrame) -> bool:
    # Nota: Este wrapper asume que la tarea 'load' devuelve el DataFrame.
    # Si task_load solo devuelve True/False, necesitarías que esta tarea
    # recibiera df_merged de la tarea merge vía XCom.
    logger.info("Executing task wrapper: task_store")
    if not isinstance(df_loaded_data, pd.DataFrame) or df_loaded_data.empty:
        logger.warning(f"Input to task_store is not a valid/non-empty DataFrame (got {type(df_loaded_data)}). Skipping store.")
        return False

    logger.info(f"DataFrame received for storing. Shape: {df_loaded_data.shape}")
    try:
        # --- ¡¡¡Aquí llamarías a tu lógica de almacenamiento real!!! ---
        logger.info(f"Placeholder Store: Simulating storing DataFrame head:\n{df_loaded_data.head().to_string()}")
        success = True # Placeholder
        # ---------------------------------------------------------
        if success: logger.info("Task wrapper task_store finished successfully.")
        else: logger.warning("Task wrapper task_store reported failure.")
        return success
    except Exception as e: logger.error(f"Error during task_store execution: {e}", exc_info=True); return False

# ----------FIN DEL CAMBIO-------------