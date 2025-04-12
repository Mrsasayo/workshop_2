# ---------INICIO DEL CAMBIO-----------
import pandas as pd
import numpy as np
import logging
import warnings

# Configuración del logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - TRANSFORM_SQL - %(message)s')
logger = logging.getLogger(__name__)

# Suprimir warnings específicos si es necesario (ej. FutureWarning de Pandas)
# warnings.filterwarnings('ignore', category=FutureWarning)

def clean_grammy_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el DataFrame de datos de The Grammy Awards.

    Args:
        df_raw (pd.DataFrame): DataFrame crudo extraído de la tabla the_grammy_awards.

    Returns:
        pd.DataFrame: DataFrame limpio y procesado. Devuelve DataFrame vacío en caso de error.
    """
    logger.info(f"Iniciando limpieza de datos Grammy. Filas iniciales: {len(df_raw) if isinstance(df_raw, pd.DataFrame) else 'N/A'}")

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
        # --- Inicio de la Limpieza (basado en 003_cleandata_grammy_awards.ipynb) ---

        # 1. Conversión de Tipos de Datos (Fechas) - Asegurar UTC
        logger.info("Convirtiendo columnas de fecha a datetime (UTC)...")
        date_cols = ['published_at', 'updated_at']
        initial_non_nulls = df_clean[date_cols].notnull().sum()

        for col in date_cols:
            if col in df_clean.columns:
                original_dtype = df_clean[col].dtype
                try:
                    # Forzar conversión a UTC. Si ya tiene timezone, lo convierte. Si no, asume UTC.
                    # `errors='coerce'` convierte inválidos a NaT.
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', utc=True)
                    logger.info(f"Columna '{col}' convertida a datetime (UTC). Nuevo tipo: {df_clean[col].dtype}")
                except Exception as e:
                     logger.error(f"Error convirtiendo '{col}' (dtype: {original_dtype}) a datetime: {e}. Se mantendrá como estaba.")
                     # No continuar si falla la conversión de fecha, puede indicar problema mayor.
                     # Alternativamente, podrías intentar restaurar la columna original: df_clean[col] = df_raw[col]

        # Verificar errores de conversión (NaT)
        final_non_nulls = df_clean[date_cols].notnull().sum()
        for col in date_cols:
             if col in df_clean.columns and col in initial_non_nulls.index: # Check existence
                 errors_count = initial_non_nulls.get(col, 0) - final_non_nulls.get(col, 0)
                 if errors_count > 0:
                     logger.warning(f"Se encontraron {errors_count} errores de formato en '{col}' que se convirtieron a NaT.")

        # 2. Manejo de Valores Nulos
        logger.info("Manejando valores nulos...")
        # Eliminar filas con 'nominee' nulo (crítico)
        rows_before_drop_nominee = len(df_clean)
        df_clean.dropna(subset=['nominee'], inplace=True)
        rows_dropped_nominee = rows_before_drop_nominee - len(df_clean)
        if rows_dropped_nominee > 0:
            logger.info(f"Se eliminaron {rows_dropped_nominee} filas por nulos en 'nominee'.")
        else:
            logger.info("No se encontraron nulos en 'nominee'.")

        # Rellenar nulos en otras columnas clave con placeholders
        fill_values = {
            'artist': 'No Especificado',
            'workers': 'No Especificado',
            'img': 'Sin URL'
        }
        logger.info(f"Rellenando nulos en {list(fill_values.keys())}...")
        for col, placeholder in fill_values.items():
            if col in df_clean.columns:
                 null_count_before = df_clean[col].isnull().sum()
                 if null_count_before > 0:
                     df_clean[col].fillna(placeholder, inplace=True)
                     logger.info(f"  - Nulos en '{col}' rellenados con '{placeholder}'. ({null_count_before} valores)")

        # 3. Limpieza Básica de Texto (Espacios)
        logger.info("Eliminando espacios en blanco iniciales/finales de columnas de texto...")
        # Columnas identificadas como object/string en el notebook
        text_cols_to_strip = ['title', 'category', 'nominee', 'artist', 'workers', 'img']
        cols_stripped = []
        for col in text_cols_to_strip:
            if col in df_clean.columns and df_clean[col].dtype == 'object':
                try:
                    df_clean[col] = df_clean[col].str.strip()
                    cols_stripped.append(col)
                except AttributeError:
                    logger.warning(f"No se pudo aplicar .str.strip() a la columna '{col}', aunque es de tipo object.")
        if cols_stripped:
            logger.info(f"Espacios eliminados en columnas: {cols_stripped}")

        # 4. Conversión a Tipos de Pandas Óptimos (Opcional pero recomendado)
        logger.info("Optimizando tipos de datos de texto a 'string' de Pandas...")
        string_cols = ['title', 'category', 'nominee', 'artist', 'workers', 'img']
        for col in string_cols:
            if col in df_clean.columns:
                try:
                    # Convertir columnas de texto limpias a 'string' de pandas
                    if df_clean[col].dtype == 'object': # Solo convertir si es object
                       df_clean[col] = df_clean[col].astype('string')
                except Exception as e:
                    logger.warning(f"No se pudo convertir la columna '{col}' a 'string': {e}")
        logger.info("Conversión a tipo 'string' de Pandas completada (si aplica).")


        # --- Fin de la Limpieza ---
        final_rows = len(df_clean)
        logger.info(f"Limpieza de datos Grammy finalizada. Filas restantes: {final_rows} (de {initial_rows})")

        # Loguear info del DF limpio
        logger.info("Información del DataFrame limpio:")
        # Usar buffer para capturar info() y loguearlo
        buffer = io.StringIO()
        df_clean.info(buf=buffer)
        logger.info(buffer.getvalue())


    except Exception as e:
        logger.error(f"Error inesperado durante la limpieza de datos Grammy: {e}", exc_info=True)
        return pd.DataFrame() # Devolver DataFrame vacío en caso de error

    return df_clean

# Importar io para capturar info() en logs (añadir al inicio del script si no está)
import io
# ----------FIN DEL CAMBIO-------------