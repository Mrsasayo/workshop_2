                                       
import pandas as pd
import numpy as np
import logging
import warnings
import io                      

                           
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - TRANSFORM_API - %(message)s')
logger = logging.getLogger(__name__)

                                               
                                                           

def clean_youtube_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia el DataFrame de datos de estadísticas de YouTube API.

    Args:
        df_raw (pd.DataFrame): DataFrame crudo con los datos de youtube_stats.

    Returns:
        pd.DataFrame: DataFrame limpio y procesado. Devuelve DataFrame vacío en caso de error.
    """
    logger.info(f"Iniciando limpieza de datos YouTube API. Filas iniciales: {len(df_raw) if isinstance(df_raw, pd.DataFrame) else 'N/A'}")

                                   
    if not isinstance(df_raw, pd.DataFrame):
        logger.error("La entrada no es un DataFrame de Pandas.")
        return pd.DataFrame()
    if df_raw.empty:
        logger.warning("El DataFrame de entrada está vacío. No hay datos para limpiar.")
        return pd.DataFrame()

    df_clean = df_raw.copy()
    initial_rows = len(df_clean)

    try:
                                                                           

                                                 
        cols_numericas = ['subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']
        cols_texto = ['artist_query', 'channel_id_found', 'channel_title_verified']

                                                                   
        logger.info("Rellenando valores NaN y manejando -1 en columnas numéricas...")
        nulos_rellenados = {}
        for col in cols_numericas:
            if col in df_clean.columns:
                nulos_antes = df_clean[col].isnull().sum()
                                                                                
                if col == 'subscriber_count':
                    df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')                                  
                    replaced_neg_one = (df_clean[col] == -1.0).sum()
                    if replaced_neg_one > 0:
                        df_clean[col] = df_clean[col].replace(-1.0, np.nan)
                        logger.info(f"  - Columna '{col}': {replaced_neg_one} valores -1 reemplazados por NaN.")
                        nulos_antes = df_clean[col].isnull().sum()                   

                                              
                if nulos_antes > 0:
                    df_clean[col].fillna(0, inplace=True)
                    nulos_rellenados[col] = nulos_antes
                    logger.info(f"  - Columna '{col}': {nulos_antes} NaN (incluyendo ex -1) rellenados con 0.")
                else:
                    logger.info(f"  - Columna '{col}': No se encontraron NaN para rellenar.")
            else:
                logger.warning(f"  - Columna numérica esperada '{col}' no encontrada.")
        if nulos_rellenados:
            logger.info("Relleno de NaN en columnas numéricas completado.")

                                                     
        logger.info("Convirtiendo columnas numéricas a tipo entero (int64)...")
        for col in cols_numericas:
                if col in df_clean.columns:
                    try:
                                                                       
                        df_clean[col] = df_clean[col].astype('int64')
                        logger.info(f"  - Columna '{col}' convertida a int64.")
                    except Exception as e:
                        logger.error(f"  - Error al convertir '{col}' a entero: {e}. Se mantendrá como {df_clean[col].dtype}.", exc_info=True)
                                                                                           

                                                 
        logger.info("Limpiando y asegurando tipo texto en columnas de texto...")
        cols_stripped = []
        for col in cols_texto:
            if col in df_clean.columns:
                                                                  
                                                                                      
                df_clean[col].fillna('', inplace=True)
                                                           
                try:
                    df_clean[col] = df_clean[col].astype(str).str.strip()
                    cols_stripped.append(col)
                except Exception as e:
                    logger.warning(f"No se pudo aplicar strip a la columna '{col}': {e}")
            else:
                 logger.warning(f"  - Columna de texto esperada '{col}' no encontrada.")

        if cols_stripped:
            logger.info(f"Limpieza de espacios/nulos completada en columnas: {cols_stripped}")

                                                          
        logger.info("Optimizando tipos de datos de texto a 'string' de Pandas...")
        converted_to_string = []
        for col in cols_texto:
            if col in df_clean.columns:
                 try:
                                                     
                     df_clean[col] = df_clean[col].astype('string')
                     converted_to_string.append(col)
                 except Exception as e:
                     logger.warning(f"No se pudo convertir la columna '{col}' a 'string': {e}")
        if converted_to_string:
             logger.info(f"Columnas convertidas a 'string' de Pandas: {converted_to_string}")


                                    
        final_rows = len(df_clean)
                                                                       
        logger.info(f"Limpieza de datos YouTube API finalizada. Filas procesadas: {final_rows} (de {initial_rows})")

                                    
        logger.info("Información del DataFrame limpio:")
        buffer = io.StringIO()
        df_clean.info(buf=buffer)
        logger.info(buffer.getvalue())

    except Exception as e:
        logger.error(f"Error inesperado durante la limpieza de datos YouTube API: {e}", exc_info=True)
        return pd.DataFrame()                                            

    return df_clean
                                       