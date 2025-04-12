# ---------INICIO DEL CAMBIO-----------
import pandas as pd
import numpy as np
import logging
import warnings
import io # Para loguear info()

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - TRANSFORM_CSV - %(message)s')
logger = logging.getLogger(__name__)

# Suprimir warnings específicos si es necesario
# warnings.filterwarnings('ignore', category=FutureWarning)

def clean_spotify_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el DataFrame de datos de Spotify Dataset.

    Args:
        df_raw (pd.DataFrame): DataFrame crudo extraído del CSV spotify_dataset.

    Returns:
        pd.DataFrame: DataFrame limpio y procesado. Devuelve DataFrame vacío en caso de error.
    """
    logger.info(f"Iniciando limpieza de datos Spotify. Filas iniciales: {len(df_raw) if isinstance(df_raw, pd.DataFrame) else 'N/A'}")

    # --- Validación de Entrada ---
    if not isinstance(df_raw, pd.DataFrame):
        logger.error("La entrada no es un DataFrame de Pandas.")
        return pd.DataFrame()
    if df_raw.empty:
        logger.warning("El DataFrame de entrada está vacío. No hay datos para limpiar.")
        return pd.DataFrame()

    df_clean = df_raw.copy()
    initial_rows = len(df_clean)

    try:
        # --- Inicio de la Limpieza (basado en 003_cleandata_spotify_dataset.ipynb) ---

        # 1. Eliminar duplicados exactos
        num_duplicados_before = df_clean.duplicated().sum()
        if num_duplicados_before > 0:
            df_clean.drop_duplicates(inplace=True)
            logger.info(f"Se eliminaron {num_duplicados_before} filas duplicadas exactas.")
        else:
            logger.info("No se encontraron filas duplicadas exactas.")
        rows_after_duplicates = len(df_clean)

        # 2. Eliminar filas con nulos en columnas críticas
        # (Basado en EDA del notebook: 'artists', 'album_name', 'track_name')
        cols_to_check_nulls = ['artists', 'album_name', 'track_name']
        logger.info(f"Verificando nulos en columnas críticas: {cols_to_check_nulls}")
        # Calcular nulos ANTES de dropear para loguear correctamente
        nulls_in_critical = df_clean[cols_to_check_nulls].isnull().any(axis=1)
        num_rows_with_nulls = nulls_in_critical.sum()

        if num_rows_with_nulls > 0:
            df_clean.dropna(subset=cols_to_check_nulls, inplace=True)
            rows_dropped_nulls = rows_after_duplicates - len(df_clean) # Calcular diferencia
            logger.info(f"Se eliminaron {rows_dropped_nulls} filas debido a nulos en {cols_to_check_nulls}.")
        else:
            logger.info(f"No se encontraron nulos en las columnas críticas {cols_to_check_nulls}.")

        # 3. Limpieza de Texto (Espacios)
        logger.info("Eliminando espacios en blanco iniciales/finales de columnas de texto...")
        # Seleccionar columnas de tipo 'object' que probablemente sean texto
        object_columns = df_clean.select_dtypes(include=['object']).columns
        cols_stripped = []
        for col in object_columns:
            # Verificar si realmente contiene strings antes de intentar strip
            if pd.api.types.is_string_dtype(df_clean[col]) or df_clean[col].dtype == 'object':
                 # Comprobar si hay valores no nulos para evitar errores en columnas totalmente nulas (aunque ya dropeamos algunas)
                 if df_clean[col].notnull().any():
                    try:
                        # Rellenar NaNs temporales con '' si aún existen antes de strip
                        df_clean[col] = df_clean[col].fillna('').astype(str).str.strip()
                        cols_stripped.append(col)
                    except Exception as e:
                        logger.warning(f"No se pudo aplicar .str.strip() a la columna '{col}': {e}")
            else:
                 logger.debug(f"Columna '{col}' no es de tipo object/string, omitiendo strip.")

        if cols_stripped:
            logger.info(f"Espacios eliminados en columnas: {cols_stripped}")

        # 4. Conversión a Tipos de Pandas Óptimos (String)
        logger.info("Optimizando tipos de datos de texto a 'string' de Pandas...")
        # Re-identificar columnas de texto después de strip/fillna
        string_cols_final = df_clean.select_dtypes(include=['object']).columns # O usar la lista object_columns si se prefiere
        converted_to_string = []
        for col in string_cols_final:
             if col in df_clean.columns: # Verificar que la columna exista
                 try:
                     # Convertir a 'string' de pandas si sigue siendo 'object'
                     if df_clean[col].dtype == 'object':
                        df_clean[col] = df_clean[col].astype('string')
                        converted_to_string.append(col)
                 except Exception as e:
                     logger.warning(f"No se pudo convertir la columna '{col}' a 'string': {e}")
        if converted_to_string:
             logger.info(f"Columnas convertidas a 'string' de Pandas: {converted_to_string}")


        # --- Fin de la Limpieza ---
        final_rows = len(df_clean)
        logger.info(f"Limpieza de datos Spotify finalizada. Filas restantes: {final_rows} (de {initial_rows})")

        # Loguear info del DF limpio
        logger.info("Información del DataFrame limpio:")
        buffer = io.StringIO()
        df_clean.info(buf=buffer)
        logger.info(buffer.getvalue())

    except Exception as e:
        logger.error(f"Error inesperado durante la limpieza de datos Spotify: {e}", exc_info=True)
        return pd.DataFrame() # Devolver DataFrame vacío en caso de error

    return df_clean
# ----------FIN DEL CAMBIO-------------