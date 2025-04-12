                                       
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import Integer, String, Text, TIMESTAMP, Boolean, VARCHAR, Float, BigInteger            
from dotenv import load_dotenv
import logging
import time
import numpy as np
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import sys
import warnings

                           
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                                                                
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='file_cache is only supported with oauth2client')                                

                                                                                           
                                                                                     
                                  
                                  

                                                                                               

def search_for_channel(youtube_service, artist_name):
    """Busca un canal por nombre y devuelve el ID del primer resultado."""
    try:
        search_query = f"{artist_name} Topic"                               
        logging.debug(f"  API CALL: youtube.search().list (channel, q='{search_query}')")
        request = youtube_service.search().list(
            part="snippet", q=search_query, type="channel", maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            channel_id = response['items'][0]['id']['channelId']
            channel_title = response['items'][0]['snippet']['title']
            logging.info(f"    Canal encontrado (con Topic): '{channel_title}' (ID: {channel_id}) para '{artist_name}'")
            return channel_id
        else:
                                  
            logging.info(f"  No se encontró canal con 'Topic'. Intentando búsqueda directa para '{artist_name}'...")
            logging.debug(f"  API CALL: youtube.search().list (channel, q='{artist_name}')")
            request = youtube_service.search().list(
                part="snippet", q=artist_name, type="channel", maxResults=1
            )
            response = request.execute()
            if response.get('items'):
                channel_id = response['items'][0]['id']['channelId']
                channel_title = response['items'][0]['snippet']['title']
                logging.info(f"    Canal encontrado (directo): '{channel_title}' (ID: {channel_id}) para '{artist_name}'")
                return channel_id
            else:
                logging.warning(f"  No se encontró canal para '{artist_name}' en búsqueda directa tampoco.")
                return None
    except googleapiclient.errors.HttpError as e:
        logging.error(f"  Error HTTP buscando canal para '{artist_name}': {e}")
        if 'quotaExceeded' in str(e):
            raise e                                                  
        return None
    except Exception as e:
        logging.error(f"  Error inesperado buscando canal para '{artist_name}': {e}", exc_info=True)
        return None

def get_channel_stats(youtube_service, channel_id):
    """Obtiene estadísticas de un canal por su ID."""
    if not channel_id: return None
    try:
        logging.debug(f"    API CALL: youtube.channels().list (id='{channel_id}')")
        request = youtube_service.channels().list(part="statistics,snippet", id=channel_id)
        response = request.execute()
        if response.get('items'):
            item = response['items'][0]
            stats = item.get('statistics', {})
            snippet = item.get('snippet', {})
            try: subscriber_count = int(stats.get('subscriberCount', 0))
            except (ValueError, TypeError): subscriber_count = 0
            try: view_count = int(stats.get('viewCount', 0))
            except (ValueError, TypeError): view_count = 0
            try: video_count = int(stats.get('videoCount', 0))
            except (ValueError, TypeError): video_count = 0

            hidden_subs = stats.get('hiddenSubscriberCount', False)
            logging.info(f"      Stats obtenidas para {channel_id}: Subs={subscriber_count if not hidden_subs else 'HIDDEN'}, Views={view_count}, Videos={video_count}")
            return {
                'channel_title_verified': snippet.get('title'),
                'subscriber_count': subscriber_count if not hidden_subs else -1,                       
                'view_count': view_count,
                'video_count': video_count,
            }
        else:
            logging.warning(f"    No se encontraron estadísticas para el channel ID: {channel_id}")
            return None
    except googleapiclient.errors.HttpError as e:
        logging.error(f"    Error HTTP obteniendo estadísticas para {channel_id}: {e}")
        if 'quotaExceeded' in str(e): raise e           
        return None
    except Exception as e:
        logging.error(f"    Error inesperado obteniendo estadísticas para {channel_id}: {e}", exc_info=True)
        return None

def search_top_videos(youtube_service, channel_id):
    """Busca los top 10 videos musicales más vistos de un canal."""
    video_ids = []
    if not channel_id: return []
    try:
        logging.debug(f"    API CALL: youtube.search().list (video, channelId='{channel_id}', order=viewCount, videoCategoryId=10)")
        request = youtube_service.search().list(
            part="id", channelId=channel_id, order="viewCount", type="video",
            videoCategoryId="10",         
            maxResults=10
        )
        response = request.execute()
        video_ids = [item['id']['videoId'] for item in response.get('items', [])]
        logging.info(f"      Se encontraron {len(video_ids)} IDs de videos musicales top para {channel_id}.")
        return video_ids
    except googleapiclient.errors.HttpError as e:
        if 'quotaExceeded' in str(e): raise e           
        if 'videoCategoryId filter is not supported' in str(e) or 'Forbidden' in str(e):                                          
            logging.warning(f"      Búsqueda por categoría musical no soportada/permitida para {channel_id}. Intentando sin categoría...")
            try:
                logging.debug(f"    API CALL: youtube.search().list (video, channelId='{channel_id}', order=viewCount)")
                request = youtube_service.search().list(
                    part="id", channelId=channel_id, order="viewCount", type="video", maxResults=10
                )
                response = request.execute()
                video_ids = [item['id']['videoId'] for item in response.get('items', [])]
                logging.info(f"      Se encontraron {len(video_ids)} IDs de videos top (sin filtro categoría) para {channel_id}.")
                return video_ids
            except googleapiclient.errors.HttpError as inner_e:                                       
                 if 'quotaExceeded' in str(inner_e): raise inner_e           
                 logging.error(f"      Error HTTP en búsqueda sin categoría para {channel_id}: {inner_e}")
                 return []
            except Exception as inner_e:
                logging.error(f"      Error inesperado en búsqueda sin categoría para {channel_id}: {inner_e}", exc_info=True)
                return []
        else:
            logging.error(f"    Error HTTP buscando videos top para {channel_id}: {e}")
            return []
    except Exception as e:
        logging.error(f"    Error inesperado buscando videos top para {channel_id}: {e}", exc_info=True)
        return []

def get_video_likes(youtube_service, video_ids_list):
    """Obtiene el conteo de likes para una lista de IDs de video (en lotes)."""
    total_likes = 0
    if not video_ids_list:
        logging.info("      No hay IDs de video para buscar likes.")
        return 0

    logging.info(f"    Obteniendo likes para {len(video_ids_list)} videos top...")
    batch_size = 50                                   
    likes_found_count = 0
    for i in range(0, len(video_ids_list), batch_size):
        batch_ids = video_ids_list[i:i + batch_size]
        ids_string = ",".join(batch_ids)
        try:
            logging.debug(f"      API CALL: youtube.videos().list (ids='{ids_string[:50]}...')")            
            request = youtube_service.videos().list(part="statistics", id=ids_string)
            response = request.execute()
            for item in response.get('items', []):
                likes_str = item.get('statistics', {}).get('likeCount')
                if likes_str is not None:
                    try:
                        total_likes += int(likes_str)
                        likes_found_count += 1
                    except (ValueError, TypeError):
                        logging.warning(f"      Valor de like no numérico encontrado para video {item.get('id')}: '{likes_str}'")
                else:
                                                                                    
                    logging.debug(f"      Likes no disponibles (None) para video {item.get('id')}.")
        except googleapiclient.errors.HttpError as e:
            if 'quotaExceeded' in str(e): raise e           
            logging.error(f"    Error HTTP obteniendo likes para lote ({len(batch_ids)} IDs): {e}")
            break                                        
        except Exception as e:
            logging.error(f"    Error inesperado obteniendo likes para lote: {e}", exc_info=True)
            break                           

    logging.info(f"      Likes sumados de {likes_found_count}/{len(video_ids_list)} videos: {total_likes}")
    return total_likes

                                             

def extract_youtube_data_api() -> pd.DataFrame:
    """
    Orquesta la extracción de datos de artistas de la DB,
    enriquecimiento con YouTube API, guardado de progreso en CSV,
    carga a la tabla 'youtube_stats' en DB y devuelve los datos finales.

    Returns:
        pd.DataFrame: DataFrame con las estadísticas de YouTube para los artistas.
                      Devuelve un DataFrame vacío si ocurre un error crítico.
    """
    logging.info("Iniciando la extracción y enriquecimiento con API de YouTube...")

                                  
    dotenv_path = '/home/nicolas/Escritorio/workshops/workshop_2/env/.env'                    
    load_dotenv(dotenv_path=dotenv_path)
    logging.info(f"Cargando variables de entorno desde: {dotenv_path}")

                    
    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_NAME = os.getenv('POSTGRES_DB')

                                                 
    YOUTUBE_CLIENT_ID = os.getenv("YOUTUBE_CLIENT_ID")
    YOUTUBE_CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
    YOUTUBE_PROJECT_ID = os.getenv("YOUTUBE_PROJECT_ID")                             
    YOUTUBE_AUTH_URI = os.getenv("YOUTUBE_AUTH_URI", "https://accounts.google.com/o/oauth2/auth")
    YOUTUBE_TOKEN_URI = os.getenv("YOUTUBE_TOKEN_URI", "https://oauth2.googleapis.com/token")
    YOUTUBE_REDIRECT_URIS= os.getenv("YOUTUBE_REDIRECT_URIS", "http://localhost")                 
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    YOUTUBE_SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]

                         
    TABLE_GRAMMY_CLEAN = 'the_grammy_awards_clean'                                 
    TABLE_SPOTIFY_CLEAN = 'spotify_dataset_clean'                                 
    TABLE_YOUTUBE_RAW = 'youtube_stats'                                    
    PROGRESS_CSV_PATH = '/home/nicolas/Escritorio/workshops/workshop_2/data/youtube_stats.csv'                    
    CHUNK_SIZE_UPLOAD = 10000                                          

                                 
    if not all([POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME]):
        logging.error("Faltan variables de entorno para la base de datos.")
        return pd.DataFrame()
    if not YOUTUBE_CLIENT_ID or not YOUTUBE_CLIENT_SECRET:
        logging.error("Faltan YOUTUBE_CLIENT_ID y/o YOUTUBE_CLIENT_SECRET.")
        return pd.DataFrame()

                                         
    engine = None
    df_final_results = pd.DataFrame()                       
    try:
        logging.info(f"Creando motor SQLAlchemy para la base de datos '{POSTGRES_NAME}'...")
        db_url = f'postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}'
        engine = create_engine(db_url)
        logging.info("Motor SQLAlchemy creado exitosamente.")
    except Exception as e:
        logging.error(f"Error al crear el motor SQLAlchemy: {e}", exc_info=True)
        return pd.DataFrame()

                                                                 
    unique_artists_full_list = []
    try:
        logging.info(f"Extrayendo artistas únicos de '{TABLE_SPOTIFY_CLEAN}' y '{TABLE_GRAMMY_CLEAN}'...")
                                                
        query_spotify = f'SELECT DISTINCT artists AS artist_name FROM "{TABLE_SPOTIFY_CLEAN}" WHERE artists IS NOT NULL'
        df_artists_spotify = pd.read_sql(query_spotify, con=engine)
        logging.info(f"Artistas extraídos de Spotify: {len(df_artists_spotify)}")

                                              
        query_grammy = f'SELECT DISTINCT artist AS artist_name FROM "{TABLE_GRAMMY_CLEAN}" WHERE artist IS NOT NULL AND artist != \'No Especificado\''
        df_artists_grammy = pd.read_sql(query_grammy, con=engine)
        logging.info(f"Artistas extraídos de Grammys: {len(df_artists_grammy)}")

                                            
        df_combined_artists = pd.concat([df_artists_spotify, df_artists_grammy], ignore_index=True)
        unique_artists_series = df_combined_artists['artist_name'].drop_duplicates().dropna()
        excluded_names = {'Various Artists', '(Various Artists)', 'No Especificado', '', 'Soundtrack'}                           
        unique_artists_series = unique_artists_series[~unique_artists_series.astype(str).str.strip().isin(excluded_names)]
        unique_artists_full_list = unique_artists_series.astype(str).str.strip().unique().tolist()
        unique_artists_full_list = [artist for artist in unique_artists_full_list if artist]                             

        logging.info(f"Lista completa de artistas únicos a procesar: {len(unique_artists_full_list)}")
        if not unique_artists_full_list:
            logging.warning("No se encontraron artistas únicos válidos para procesar.")
                                                                    
                                   

    except Exception as e:
        logging.error(f"Error al extraer artistas de la base de datos: {e}", exc_info=True)
                                                                         
                                                                                           
        if engine: engine.dispose()
        return pd.DataFrame()

                                                                          
    df_processed_so_far = pd.DataFrame()
    artists_already_processed = set()
    try:
        logging.info(f"Intentando cargar progreso anterior desde: {PROGRESS_CSV_PATH}")
        df_processed_so_far = pd.read_csv(PROGRESS_CSV_PATH)
        if 'artist_query' in df_processed_so_far.columns:
            artists_already_processed = set(df_processed_so_far['artist_query'].dropna().unique())
            logging.info(f"Progreso cargado. {len(artists_already_processed)} artistas ya procesados.")
        else:
            logging.warning(f"El archivo CSV de progreso '{PROGRESS_CSV_PATH}' no contiene la columna 'artist_query'. Se procesarán todos los encontrados en la DB.")
            df_processed_so_far = pd.DataFrame()                           
    except FileNotFoundError:
        logging.info(f"No se encontró archivo de progreso '{PROGRESS_CSV_PATH}'. Se iniciará desde cero.")
    except Exception as e:
        logging.error(f"Error al cargar el archivo de progreso CSV '{PROGRESS_CSV_PATH}': {e}. Se iniciará desde cero.", exc_info=True)
        df_processed_so_far = pd.DataFrame()           

    artists_to_process = sorted([artist for artist in unique_artists_full_list if artist not in artists_already_processed])                            
    logging.info(f"Artistas pendientes de procesar en esta ejecución: {len(artists_to_process)}")

                                                                       
    youtube = None
    if artists_to_process:
        logging.info("Iniciando autenticación con YouTube API...")
        try:
                                                                                      
            os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
                                                      
            redirect_uris_list = [uri.strip() for uri in YOUTUBE_REDIRECT_URIS.split(',') if uri.strip()]
            if not redirect_uris_list:
                 redirect_uris_list = ["http://localhost"]                                   

            client_config = {
                "installed": {
                    "client_id": YOUTUBE_CLIENT_ID,
                    "client_secret": YOUTUBE_CLIENT_SECRET,
                    "project_id": YOUTUBE_PROJECT_ID,              
                    "auth_uri": YOUTUBE_AUTH_URI,
                    "token_uri": YOUTUBE_TOKEN_URI,
                    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                    "redirect_uris": redirect_uris_list
                 }
            }
                                                               
            flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_config(
                client_config, YOUTUBE_SCOPES
            )
                                                                    
            credentials = flow.run_local_server(port=0)                                    
            youtube = googleapiclient.discovery.build(
                YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, credentials=credentials
            )
            logging.info("Autenticación con YouTube API exitosa.")
        except Exception as e:
            logging.error(f"Error durante la autenticación con YouTube API: {e}", exc_info=True)
            youtube = None                   
    else:
        logging.info("No hay nuevos artistas para procesar con la API en esta ejecución.")

                                                                        
    current_run_data = []
    processed_count_this_run = 0
    quota_exceeded = False
    api_call_delay = 0.8                                               

    if youtube and artists_to_process:
        logging.info(f"Iniciando procesamiento API para {len(artists_to_process)} artistas pendientes...")
        processing_start_time = time.time()

        for i, artist in enumerate(artists_to_process):
            processed_count_this_run += 1
            logging.info(f"Procesando artista {processed_count_this_run}/{len(artists_to_process)}: {artist}")
            artist_info = {'artist_query': artist}                          

            try:
                channel_id = search_for_channel(youtube, artist)
                artist_info['channel_id_found'] = channel_id
                time.sleep(api_call_delay / 4)              

                if channel_id:
                    stats = get_channel_stats(youtube, channel_id)
                    time.sleep(api_call_delay / 4)

                    if stats:
                        artist_info.update(stats)
                        top_video_ids = search_top_videos(youtube, channel_id)
                        time.sleep(api_call_delay / 4)
                        total_top10_likes = get_video_likes(youtube, top_video_ids)
                        artist_info['total_top10_video_likes'] = total_top10_likes
                        time.sleep(api_call_delay / 4)
                    else:
                        artist_info.update({'channel_title_verified': None, 'subscriber_count': None,
                                            'view_count': None, 'video_count': None,
                                            'total_top10_video_likes': None})
                else:
                    artist_info.update({'channel_id_found': None, 'channel_title_verified': None,
                                        'subscriber_count': None, 'view_count': None,
                                        'video_count': None, 'total_top10_video_likes': None})

                current_run_data.append(artist_info)

            except googleapiclient.errors.HttpError as e:
                if 'quotaExceeded' in str(e):
                    logging.error(f"¡CUOTA EXCEDIDA de YouTube API procesando a {artist}! Deteniendo esta ejecución.")
                    quota_exceeded = True
                                                                                     
                    artist_info.setdefault('channel_id_found', f"Error: Quota Exceeded")
                    artist_info.setdefault('channel_title_verified', None)                     
                    artist_info.setdefault('subscriber_count', None)
                    artist_info.setdefault('view_count', None)
                    artist_info.setdefault('video_count', None)
                    artist_info.setdefault('total_top10_video_likes', None)
                    current_run_data.append(artist_info)
                    break                      
                else:
                    logging.error(f"Error HTTP inesperado procesando {artist}: {e}")
                    artist_info.setdefault('channel_id_found', f"Error: HTTP {e.resp.status}")
                    current_run_data.append(artist_info)                                 
            except Exception as e:
                logging.error(f"Error general inesperado procesando {artist}: {e}", exc_info=True)
                artist_info.setdefault('channel_id_found', f"Error: {type(e).__name__}")
                current_run_data.append(artist_info)                                 

                                                                           
            if not quota_exceeded:
                 time.sleep(api_call_delay)


        processing_end_time = time.time()
        if processed_count_this_run > 0:
            logging.info(f"Procesamiento API de {processed_count_this_run} artistas finalizado en {processing_end_time - processing_start_time:.2f} segundos.")

    elif not artists_to_process:
        logging.info("No había artistas pendientes para procesar con la API.")
    elif not youtube:
         logging.error("El objeto de servicio de YouTube no está disponible (falló la autenticación o no había pendientes). No se procesó con API.")

                                                     
    df_combined_results = pd.DataFrame()
    if current_run_data:                                       
        df_current_run = pd.DataFrame(current_run_data)
        logging.info(f"Combinando {len(df_processed_so_far)} registros previos con {len(df_current_run)} nuevos.")
        df_combined_results = pd.concat([df_processed_so_far, df_current_run], ignore_index=True)
                                                                                  
        df_combined_results.drop_duplicates(subset=['artist_query'], keep='last', inplace=True)
        logging.info(f"DataFrame combinado tiene {len(df_combined_results)} filas únicas por 'artist_query'.")
    elif not df_processed_so_far.empty:
        logging.info("No se procesaron nuevos artistas. Usando los datos del CSV anterior.")
        df_combined_results = df_processed_so_far
    else:
        logging.info("No hay datos previos ni nuevos para combinar.")

                                                                     
    if not df_combined_results.empty:
        try:
                                               
            os.makedirs(os.path.dirname(PROGRESS_CSV_PATH), exist_ok=True)
            logging.info(f"Guardando {len(df_combined_results)} registros combinados en {PROGRESS_CSV_PATH}...")
            df_combined_results.to_csv(PROGRESS_CSV_PATH, index=False)
            logging.info(f"Resultados combinados guardados exitosamente en CSV.")
        except Exception as e:
            logging.error(f"Error al guardar los resultados combinados en CSV '{PROGRESS_CSV_PATH}': {e}", exc_info=True)
    else:
        logging.warning(f"No hay resultados combinados para guardar en {PROGRESS_CSV_PATH}.")


                                                                   
                                                                       
    if os.path.exists(PROGRESS_CSV_PATH) and engine is not None:
        logging.info(f"Iniciando carga final desde {PROGRESS_CSV_PATH} a la tabla '{TABLE_YOUTUBE_RAW}'...")
        try:
            df_to_upload = pd.read_csv(PROGRESS_CSV_PATH)
            logging.info(f"Leído {len(df_to_upload)} filas desde {PROGRESS_CSV_PATH} para cargar a DB.")

                                                              
            sql_types = {
                'artist_query': Text(),
                'channel_id_found': Text(),                             
                'channel_title_verified': Text(),                             
                'subscriber_count': BigInteger(),                                                 
                'view_count': BigInteger(),
                'video_count': BigInteger(),                           
                'total_top10_video_likes': BigInteger()
            }

                                                                               
                                                                                                  
                                                                                               
            for col in ['subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']:
                 if col in df_to_upload.columns:
                                                                                                    
                     df_to_upload[col] = pd.to_numeric(df_to_upload[col], errors='coerce')
                                                                                                       
                     if col == 'subscriber_count':
                        df_to_upload[col] = df_to_upload[col].replace(-1.0, np.nan)                     
                                                                          
                     df_to_upload[col] = df_to_upload[col].fillna(0)
                                                                                                          
                                                                                  
                     try:
                         df_to_upload[col] = df_to_upload[col].astype('int64')
                         logging.debug(f"Columna '{col}' pre-procesada a int64.")
                     except ValueError as ve:
                         logging.error(f"Error al convertir columna '{col}' a int64 después de fillna(0): {ve}. Datos problemáticos:\n{df_to_upload[col][df_to_upload[col].isna()]}")
                                                                       
                         df_to_upload[col] = pd.to_numeric(df_to_upload[col], errors='coerce').fillna(0).astype('int64')


            logging.info(f"Cargando DataFrame procesado a la tabla '{TABLE_YOUTUBE_RAW}' (reemplazando)...")
            start_upload_time = time.time()
            df_to_upload.to_sql(
                TABLE_YOUTUBE_RAW,
                con=engine,
                if_exists='replace',                                                  
                index=False,
                method='multi',                                           
                dtype=sql_types,
                chunksize=CHUNK_SIZE_UPLOAD
            )
            end_upload_time = time.time()
            logging.info(f"DataFrame cargado a '{TABLE_YOUTUBE_RAW}' en {end_upload_time - start_upload_time:.2f} segundos.")

                                      
            with engine.connect() as connection:
                query_count = text(f'SELECT COUNT(*) FROM "{TABLE_YOUTUBE_RAW}"')
                result = connection.execute(query_count)
                num_db_final_rows = result.scalar_one()
            if len(df_to_upload) == num_db_final_rows:
                logging.info(f"¡Verificación de carga final exitosa! {num_db_final_rows} filas en '{TABLE_YOUTUBE_RAW}'.")
            else:
                logging.warning(f"Discrepancia en filas: CSV ({len(df_to_upload)}) vs DB ({num_db_final_rows}) en tabla '{TABLE_YOUTUBE_RAW}'.")

        except Exception as e:
            logging.error(f"Error al cargar el CSV final '{PROGRESS_CSV_PATH}' en la tabla '{TABLE_YOUTUBE_RAW}': {e}", exc_info=True)
    elif not os.path.exists(PROGRESS_CSV_PATH):
        logging.error(f"No se encontró el archivo {PROGRESS_CSV_PATH} para cargar a la base de datos.")
    elif engine is None:
        logging.error("No se puede cargar a la base de datos porque la conexión (engine) no está definida.")


                                                              
                                                                                        
    if os.path.exists(PROGRESS_CSV_PATH):
        try:
            logging.info(f"Leyendo el archivo final {PROGRESS_CSV_PATH} para devolver como resultado.")
            df_final_results = pd.read_csv(PROGRESS_CSV_PATH)
            logging.info(f"DataFrame final leído desde CSV. Filas: {len(df_final_results)}")
                                                                           
            df_final_results = df_final_results.fillna({
                 'channel_id_found': '', 'channel_title_verified': '',
                 'subscriber_count': 0, 'view_count': 0, 'video_count': 0, 'total_top10_video_likes': 0
            })
                                                    
            for col in ['subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']:
                 if col in df_final_results.columns:
                     df_final_results[col] = df_final_results[col].astype(int)

        except Exception as e:
             logging.error(f"Error al leer el archivo CSV final {PROGRESS_CSV_PATH} para devolver: {e}", exc_info=True)
             df_final_results = pd.DataFrame()                                           
    else:
        logging.warning(f"El archivo {PROGRESS_CSV_PATH} no existe al final del proceso. Devolviendo DataFrame vacío.")
        df_final_results = pd.DataFrame()

                            
    if engine:
        engine.dispose()
        logging.info("Conexión a la base de datos cerrada.")

    logging.info(f"Extracción y enriquecimiento API finalizada. Devolviendo DataFrame con {len(df_final_results)} filas.")
    return df_final_results

                                                                      
if __name__ == "__main__":
    logging.info("Ejecutando extract_api.py como script independiente.")
    df_result = extract_youtube_data_api()
    if not df_result.empty:
        logging.info(f"Script standalone finalizado. Filas en el DataFrame resultante: {len(df_result)}")
        print("\nDataFrame Resultante (primeras 10 filas):")
                                                             
        print(df_result.head(10).to_string())
        print("\nInformación del DataFrame final:")
        df_result.info()
    else:
        logging.error("Script standalone finalizado. No se generó DataFrame final o hubo un error.")

                                       