                                       
import pandas as pd
import os
import logging
import sys

                           
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

                                                                                           
                                                                                     
                                  
                                  

def extract_spotify_data_csv() -> pd.DataFrame:
    """
    Extrae datos del archivo CSV 'spotify_dataset.csv'.

    Returns:
        pd.DataFrame: DataFrame con los datos del archivo CSV.
                      Devuelve un DataFrame vacío en caso de error.
    """
    logging.info("Iniciando la extracción de datos desde CSV para Spotify...")

                                                                           
    CSV_FILE_PATH = '/home/nicolas/Escritorio/workshops/workshop_2/data/spotify_dataset.csv'                    
    logging.info(f"Intentando leer archivo CSV desde: {CSV_FILE_PATH}")

    df_spotify = pd.DataFrame()                                   

    try:
                                       
                                                                                  
        df_spotify = pd.read_csv(CSV_FILE_PATH)
        num_csv_rows = len(df_spotify)
        logging.info(f"Archivo CSV '{os.path.basename(CSV_FILE_PATH)}' cargado exitosamente. Filas: {num_csv_rows}")

                                                                           
        if 'Unnamed: 0' in df_spotify.columns:
            df_spotify = df_spotify.drop(columns=['Unnamed: 0'])
            logging.info("Columna 'Unnamed: 0' eliminada del DataFrame.")

        if not df_spotify.empty:
            logging.info("Primeras 5 filas del DataFrame cargado:")
                                                          
            print(df_spotify.head().to_string())
            logging.info("Información del DataFrame cargado:")
            df_spotify.info()
        else:
            logging.warning(f"El archivo CSV '{os.path.basename(CSV_FILE_PATH)}' está vacío.")


    except FileNotFoundError:
        logging.error(f"Error: El archivo CSV no se encontró en la ruta: {CSV_FILE_PATH}")
                                                    
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV '{CSV_FILE_PATH}': {e}", exc_info=True)
                                                    

    logging.info(f"Extracción CSV finalizada. Devolviendo DataFrame con {len(df_spotify)} filas.")
    return df_spotify

                                                                      
if __name__ == "__main__":
    logging.info("Ejecutando extract_csv.py como script independiente.")
    df_result = extract_spotify_data_csv()
    if not df_result.empty:
        logging.info(f"Script standalone finalizado. Filas extraídas: {len(df_result)}")
                                                              
                                               
    else:
        logging.error("Script standalone finalizado. No se pudieron extraer datos.")
                                       