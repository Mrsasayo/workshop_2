                                                                            
                                       
import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text
                                                                     
from sqlalchemy import Table, Column, MetaData, inspect
from sqlalchemy import BigInteger, Integer, String, Text, Float, Boolean, TIMESTAMP                                    
from dotenv import load_dotenv
import time

                                                    
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - LOAD_TO_DB - %(message)s')
logger = logging.getLogger(__name__)

def load_dataframe_to_postgres(df_to_load: pd.DataFrame,
                               target_table_name: str,
                               db_name: str = None,                           
                               chunk_size: int = 10000) -> bool:
    """
    Carga un DataFrame de Pandas en una tabla de PostgreSQL.

    Args:
        df_to_load (pd.DataFrame): El DataFrame a cargar.
        target_table_name (str): El nombre de la tabla destino en PostgreSQL.
        db_name (str, optional): El nombre de la base de datos. Si es None, usa POSTGRES_DB de .env.
        chunk_size (int, optional): Tamaño del lote para la carga. Defaults to 10000.

    Returns:
        bool: True si la carga fue exitosa (o al menos no lanzó excepción), False en caso contrario.
    """
    logger.info(f"Iniciando carga de DataFrame a PostgreSQL. Tabla destino: '{target_table_name}', DB: '{db_name or 'desde .env'}'")

                                   
    if not isinstance(df_to_load, pd.DataFrame):
        logger.error("La entrada no es un DataFrame de Pandas. No se puede cargar.")
        return False
    if df_to_load.empty:
        logger.warning("El DataFrame de entrada está vacío. No hay datos para cargar.")
                                                                                           
        return True

                                        
    dotenv_path = '/home/nicolas/Escritorio/workshops/workshop_2/env/.env'                    
    load_dotenv(dotenv_path=dotenv_path)
    logger.debug(f"Cargando variables de entorno desde: {dotenv_path}")

    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_HOST = os.getenv('POSTGRES_HOST')
    DB_PORT = os.getenv('POSTGRES_PORT')
                                                 
    DB_NAME_TARGET = db_name if db_name else os.getenv('POSTGRES_DB')

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME_TARGET]):
        logger.error("Faltan variables de entorno para la conexión a la base de datos.")
        return False

    engine = None
    success = False

    try:
                                             
        logger.info(f"Creando motor SQLAlchemy para la base de datos '{DB_NAME_TARGET}'...")
        db_url = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME_TARGET}'
        engine = create_engine(db_url)
        logger.info("Motor SQLAlchemy creado exitosamente.")

                                                    
                                                                                       
                                                                                             
        sql_types = {
                                                                             
            'track_id': Text(),
            'artists': Text(),
            'album_name': Text(),
            'track_name': Text(),
            'popularity': Integer(),
            'duration_ms': Integer(),
            'explicit': Boolean(),
            'danceability': Float(),
            'energy': Float(),
            'key': Integer(),
            'loudness': Float(),
            'mode': Integer(),
            'speechiness': Float(),
            'acousticness': Float(),
            'instrumentalness': Float(),
            'liveness': Float(),
            'valence': Float(),
            'tempo': Float(),
            'time_signature': Integer(),
            'track_genre': Text(),
                                                 
            'channel_id_found': Text(),
            'channel_title_verified': Text(),
            'subscriber_count': BigInteger(),              
            'view_count': BigInteger(),                    
            'video_count': BigInteger(),                   
            'total_top10_video_likes': BigInteger(),              
                                                
            'grammy_year': Integer(),                                                                    
            'grammy_category': Text(),
            'grammy_winner': Boolean()
        }
        logger.info(f"Usando tipos SQL definidos para la tabla '{target_table_name}'.")
        logger.debug(f"Mapeo de tipos SQL: {sql_types}")


                                   
        logger.info(f"Cargando {len(df_to_load)} filas en la tabla '{target_table_name}' (if_exists='replace') en chunks de {chunk_size}...")
        start_upload_time = time.time()

        df_to_load.to_sql(
            name=target_table_name,
            con=engine,
            if_exists='replace',                                  
            index=False,                                                          
            method='multi',                                                     
            dtype=sql_types,                                          
            chunksize=chunk_size                  
        )

        end_upload_time = time.time()
        logger.info(f"DataFrame cargado exitosamente a '{target_table_name}' en {end_upload_time - start_upload_time:.2f} segundos.")

                                       
        try:
            with engine.connect() as connection:
                query_count = text(f'SELECT COUNT(*) FROM "{target_table_name}"')
                result = connection.execute(query_count)
                num_db_rows = result.scalar_one()
            logger.info(f"Verificación: Número de filas en la tabla '{target_table_name}': {num_db_rows}")
            if len(df_to_load) == num_db_rows:
                logger.info("¡Verificación de conteo de filas exitosa!")
                success = True
            else:
                logger.warning(f"Discrepancia en el número de filas: DataFrame ({len(df_to_load)}) vs DB ({num_db_rows}).")
                success = True                                                                    
        except Exception as e_verify:
            logger.error(f"Error durante la verificación de la carga: {e_verify}")
            success = True                                              

    except Exception as e:
        logger.error(f"Error durante la carga del DataFrame a PostgreSQL: {e}", exc_info=True)
        success = False                    

    finally:
        if engine:
            engine.dispose()
            logger.info("Conexión a la base de datos cerrada.")

    logger.info(f"Proceso de carga finalizado. Estado: {'Éxito' if success else 'Fallo'}")
    return success

                                                    
if __name__ == '__main__':
    logger.info("Ejecutando load_to_db.py como script independiente para pruebas...")

                                                                         
    test_data = {
        'track_id': ['t1', 't2', 't3'],
        'artists': ['Artist A', 'Artist B', 'Artist C;Artist D'],
        'track_name': ['Song Alpha', 'Song Beta', 'Song Gamma'],
        'popularity': [80, 70, 60],
        'duration_ms': [180000, 210000, 150000],
        'explicit': [False, True, False],
        'danceability': [0.7, 0.5, 0.8],
        'energy': [0.8, 0.6, 0.9],
        'key': [1, 5, 8],
        'loudness': [-5.0, -7.5, -4.0],
        'mode': [1, 0, 1],
        'speechiness': [0.05, 0.1, 0.08],
        'acousticness': [0.1, 0.5, 0.05],
        'instrumentalness': [0.0, 0.0, 0.2],
        'liveness': [0.15, 0.2, 0.1],
        'valence': [0.8, 0.4, 0.9],
        'tempo': [120.0, 100.0, 130.0],
        'time_signature': [4, 4, 4],
        'track_genre': ['pop', 'rock', 'electronic'],
        'channel_id_found': ['chA', 'chB', 'chCD'],
        'channel_title_verified': ['Artist A Verified', 'Artist B Channel', 'C and D Music'],
        'subscriber_count': [1000, 500, 2000],
        'view_count': [100000, 20000, 50000],
        'video_count': [10, 5, 20],
        'total_top10_video_likes': [5000, 1000, 3000],
        'grammy_year': [2020, pd.NA, 2021],                              
        'grammy_category': ['Best Song', '', 'Best Collab'],
        'grammy_winner': [True, False, False]
    }
    df_test = pd.DataFrame(test_data)

                                                         
    df_test['grammy_year'] = df_test['grammy_year'].astype('Int64')                                    
    df_test['explicit'] = df_test['explicit'].astype(bool)
    df_test['grammy_winner'] = df_test['grammy_winner'].astype(bool)
                                                                           

    logger.info("DataFrame de prueba creado:")
    print(df_test.head().to_string())
    df_test.info()

                                          
    TARGET_TABLE = "artists_merged_final_test"                       
    TARGET_DB = "artists"                               
    load_successful = load_dataframe_to_postgres(df_test, TARGET_TABLE, db_name=TARGET_DB, chunk_size=500)

    if load_successful:
        logger.info(f"Carga standalone completada exitosamente en la tabla '{TARGET_TABLE}'.")
    else:
        logger.error("La carga standalone falló.")

                                       