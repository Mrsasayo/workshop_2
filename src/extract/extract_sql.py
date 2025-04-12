                                       
import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys

                           
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

                                                    
                                                                                            
                                                                    
                                                                           
                                                                                     
                                  
                                  
                                                                 
                                                                                   

def extract_grammy_data_sql() -> pd.DataFrame:
    """
    Extrae datos de la tabla 'the_grammy_awards' desde PostgreSQL.

    Returns:
        pd.DataFrame: DataFrame con los datos de la tabla the_grammy_awards.
                      Devuelve un DataFrame vacío en caso de error.
    """
    logging.info("Iniciando la extracción de datos SQL para The Grammy Awards...")

                                 
                                                                                 
                                                                       
                                                                           
    dotenv_path = '/home/nicolas/Escritorio/workshops/workshop_2/env/.env'                    
    load_dotenv(dotenv_path=dotenv_path)
    logging.info(f"Cargando variables de entorno desde: {dotenv_path}")

    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_HOST = os.getenv('POSTGRES_HOST')
    DB_PORT = os.getenv('POSTGRES_PORT')
    DB_NAME = os.getenv('POSTGRES_DB')
    TABLE_NAME = 'the_grammy_awards'                                           

    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
        logging.error("Faltan variables de entorno para la base de datos en " + dotenv_path)
        return pd.DataFrame()                                    

    engine = None
    df_grammy = pd.DataFrame()                                   

    try:
        logging.info(f"Creando motor SQLAlchemy para la base de datos '{DB_NAME}'...")
        db_url = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(db_url)
        logging.info(f"Conectando a '{DB_NAME}' y extrayendo datos de la tabla '{TABLE_NAME}'...")

                                             
                                                                                                     
        query = text(f'SELECT * FROM "{TABLE_NAME}"')                                    

        with engine.connect() as connection:
            df_grammy = pd.read_sql_query(query, con=connection)

        logging.info(f"Datos de '{TABLE_NAME}' extraídos exitosamente. Filas: {len(df_grammy)}")
        if not df_grammy.empty:
             logging.info("Primeras 5 filas extraídas:")
                                                                                                
             print(df_grammy.head().to_string())
             logging.info("Información del DataFrame extraído:")
             df_grammy.info()
        else:
             logging.warning(f"La tabla '{TABLE_NAME}' está vacía o no se pudieron leer datos.")


    except Exception as e:
        logging.error(f"Error durante la extracción de datos SQL desde '{TABLE_NAME}': {e}", exc_info=True)
                                                   

    finally:
        if engine:
            engine.dispose()
            logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"Extracción SQL finalizada. Devolviendo DataFrame con {len(df_grammy)} filas.")
    return df_grammy

                                                                      
if __name__ == "__main__":
    logging.info("Ejecutando extract_sql.py como script independiente.")
    df_result = extract_grammy_data_sql()
    if not df_result.empty:
        logging.info(f"Script standalone finalizado. Filas extraídas: {len(df_result)}")
                                                              
                                               
    else:
        logging.error("Script standalone finalizado. No se pudieron extraer datos.")
                                       