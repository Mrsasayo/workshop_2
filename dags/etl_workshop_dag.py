# dags/etl_workshop_dag.py
import os
import json
from datetime import datetime, timedelta
import pendulum # Mejor manejo de zonas horarias

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook # Para obtener credenciales de GDrive
from airflow.providers.postgres.hooks.postgres import PostgresHook # Ya importado en db.connection
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Importar funciones de nuestros scripts
# Ajusta sys.path si Airflow no encuentra los módulos (depende de cómo esté configurado PYTHONPATH en Airflow)
# Una forma común es añadir el directorio raíz del proyecto al PYTHONPATH.
# Otra es usar rutas relativas o absolutas si se conoce la estructura en el worker de Airflow.
# Aquí asumimos que la carpeta 'scripts' es accesible.
try:
    from src.db.connection import get_sqlalchemy_engine, test_connection
    from src.extract.spotify_client import SpotifyClient
    from src.transform.transformations import (
        transform_spotify_csv_data,
        transform_grammys_db_data,
        transform_spotify_api_data,
        merge_data
    )
    # Para Google Drive, necesitamos la lógica de subida
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload
    from io import StringIO, BytesIO
    import google.auth # Usado por GoogleBaseHook implícitamente o explícitamente
except ImportError as e:
    print(f"Error importando módulos: {e}")
    # Podrías querer lanzar una excepción aquí para que el DAG no se cargue si faltan dependencias
    raise

# --- Constantes y Configuración ---
# Rutas relativas a la carpeta 'dags' o absolutas en el worker de Airflow
# Airflow < 2.8: {{ dag_run.dag.folder }} no existe, usa rutas relativas o variables/config
# Airflow >= 2.8: Puedes usar {{ dag_run.dag.folder }}
# Asumiremos que los datos están en una ubicación accesible por los workers de Airflow
# Si usas Docker, monta los volúmenes correspondientes.
# Puedes usar Variables de Airflow para estas rutas si son dinámicas.
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow') # Ajusta si es diferente
DATA_DIR = os.path.join(AIRFLOW_HOME, 'data') # Asumiendo que 'data' está en AIRFLOW_HOME
SPOTIFY_CSV_FILENAME = 'spotify_dataset.csv' # Nombre del archivo CSV de Spotify
SPOTIFY_CSV_PATH = os.path.join(DATA_DIR, SPOTIFY_CSV_FILENAME)
GRAMMY_TABLE_RAW = 'grammy_awards_raw' # Tabla donde se cargaron los Grammys inicialmente
FINAL_TABLE_NAME = 'spotify_grammy_merged' # Tabla destino en PostgreSQL
GDRIVE_CSV_FILENAME = 'spotify_grammy_merged_data.csv'

# IDs de Conexiones de Airflow (¡DEBES CREARLAS EN LA UI DE AIRFLOW!)
POSTGRES_CONN_ID = 'postgres_workshop' # Cambia si usas otro ID
SPOTIFY_CONN_ID = 'spotify_workshop'   # Cambia si usas otro ID
GDRIVE_CONN_ID = 'google_drive_workshop' # Conexión tipo Google Cloud Platform

# ID de la carpeta en Google Drive (Obtenido de tu .env o como Variable de Airflow)
# Es mejor usar una Variable de Airflow para esto.
# from airflow.models import Variable
# GDRIVE_FOLDER_ID = Variable.get("gdrive_folder_id", default_var="ID_CARPETA_POR_DEFECTO")
# Por simplicidad aquí, lo leeremos de .env (aunque no es ideal para producción)
# Cargar .env (esto funciona si el worker tiene acceso al archivo .env, no siempre garantizado)
from dotenv import load_dotenv
dotenv_path = os.path.join(AIRFLOW_HOME, 'env', '.env') # Asumiendo que env está en AIRFLOW_HOME
if os.path.exists(dotenv_path):
     load_dotenv(dotenv_path=dotenv_path)
     print(".env file loaded for DAG configuration.") # Logging para debug
else:
     print(f".env file not found at {dotenv_path}. Relying on Airflow Variables/Connections.")


GDRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "ID_CARPETA_POR_DEFECTO_SI_NO_HAY_ENV_O_VARIABLE")
if GDRIVE_FOLDER_ID == "ID_CARPETA_POR_DEFECTO_SI_NO_HAY_ENV_O_VARIABLE":
    print("ADVERTENCIA: GOOGLE_DRIVE_FOLDER_ID no encontrado en .env. Usando valor por defecto.")
    # Considera fallar el DAG si esto es crítico: raise ValueError("...")


# --- Funciones Python para las Tareas ---

def _extract_spotify_csv(**context):
    """Lee el CSV de Spotify usando Pandas."""
    logging.info(f"Leyendo archivo CSV desde: {SPOTIFY_CSV_PATH}")
    if not os.path.exists(SPOTIFY_CSV_PATH):
        raise FileNotFoundError(f"El archivo CSV de Spotify no se encontró en {SPOTIFY_CSV_PATH}. Asegúrate de que esté accesible para el worker de Airflow.")
    try:
        df = pd.read_csv(SPOTIFY_CSV_PATH)
        logging.info(f"CSV de Spotify leído exitosamente. Shape: {df.shape}")
        # Pasar DataFrame como JSON a través de XCom (más seguro para tamaños variables)
        context['ti'].xcom_push(key='spotify_csv_data', value=df.to_json(orient='split'))
        return df.shape # Retorna algo pequeño para XCom default
    except Exception as e:
        logging.error(f"Error al leer el CSV de Spotify: {e}")
        raise

def _extract_grammys_db(**context):
    """Lee los datos de Grammys desde la tabla PostgreSQL."""
    logging.info(f"Extrayendo datos de la tabla '{GRAMMY_TABLE_RAW}' usando la conexión '{POSTGRES_CONN_ID}'")
    try:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # Podrías seleccionar columnas específicas aquí para optimizar
        df = hook.get_pandas_df(sql=f"SELECT * FROM {GRAMMY_TABLE_RAW};")
        logging.info(f"Datos de Grammys extraídos exitosamente desde PostgreSQL. Shape: {df.shape}")
        if df.empty:
             logging.warning(f"La tabla '{GRAMMY_TABLE_RAW}' está vacía o no se pudieron extraer datos.")
        # Pasar DataFrame como JSON
        context['ti'].xcom_push(key='grammys_db_data', value=df.to_json(orient='split'))
        return df.shape
    except Exception as e:
        logging.error(f"Error al extraer datos de Grammys desde PostgreSQL: {e}")
        raise

def _extract_spotify_api(**context):
    """
    Interactúa con la API de Spotify para obtener datos adicionales.
    Ejemplo: Buscar tracks del CSV para obtener IDs/popularidad más reciente y géneros de artistas.
    """
    logging.info(f"Extrayendo datos adicionales desde la API de Spotify usando la conexión '{SPOTIFY_CONN_ID}'")
    try:
        # Obtener datos del CSV (requiere que extract_spotify_csv se ejecute primero)
        ti = context['ti']
        spotify_csv_json = ti.xcom_pull(task_ids='extraction_group.extract_spotify_csv', key='spotify_csv_data')
        if not spotify_csv_json:
             raise ValueError("No se encontraron datos del CSV de Spotify en XCom.")
        df_spotify = pd.read_json(spotify_csv_json, orient='split')
        logging.info(f"Datos CSV de Spotify recuperados de XCom para referencia. Shape: {df_spotify.shape}")

        # Inicializar cliente de Spotify
        spotify_client = SpotifyClient(spotify_conn_id=SPOTIFY_CONN_ID)

        # --- Lógica de qué buscar ---
        # Estrategia de ejemplo: Para una muestra de tracks del CSV (o todos si es pequeño),
        # busca el track en la API para obtener:
        # 1. ID de Spotify más reciente (si el del CSV es viejo/incorrecto).
        # 2. Popularidad actual.
        # 3. ID del artista principal.
        # 4. Luego, usa el ID del artista para obtener sus géneros.
        # 5. Opcional: Obtener audio features usando el ID del track.

        # Limitación/Muestra: Para evitar demasiadas llamadas API en desarrollo/ejemplo
        sample_size = 50 # ¡AJUSTA ESTO! Ponlo a len(df_spotify) para procesar todo.
        tracks_to_query = df_spotify.head(sample_size) # O usa .sample(n=...)

        api_results = []
        processed_artist_ids = set() # Para no buscar géneros del mismo artista múltiples veces

        logging.info(f"Consultando API de Spotify para {len(tracks_to_query)} tracks...")
        for index, row in tracks_to_query.iterrows():
            track_name = row.get('track_name')
            # Asumiendo que 'artists' en CSV es string, tomar el primero si hay varios
            artist_name = str(row.get('artists', '')).split(';')[0].strip()

            if not track_name or not artist_name:
                logging.warning(f"Saltando fila {index} por falta de track_name o artist_name.")
                continue

            # 1. Buscar el track en la API
            logging.debug(f"Buscando API: Track='{track_name}', Artista='{artist_name}'")
            track_info = spotify_client.search_track(track_name=track_name, artist_name=artist_name, limit=1)

            if track_info:
                track_id = track_info.get('id')
                artist_id = track_info.get('artists', [{}])[0].get('id') # ID del primer artista

                # Añadir info básica del track al resultado
                result_item = track_info.copy() # Copiar info base
                result_item['query_track_name'] = track_name # Guardar lo que buscamos
                result_item['query_artist_name'] = artist_name

                # 2. Obtener géneros del artista (si no los hemos buscado ya)
                if artist_id and artist_id not in processed_artist_ids:
                    logging.debug(f"Obteniendo géneros para artista ID: {artist_id}")
                    genres = spotify_client.get_artist_genres(artist_id)
                    result_item['artist_genres'] = genres # Añadir géneros al diccionario
                    processed_artist_ids.add(artist_id)
                elif artist_id:
                     logging.debug(f"Géneros para artista {artist_id} ya obtenidos previamente.")
                     # Podríamos buscar los géneros en resultados anteriores si fuera necesario
                     # Por simplicidad, no lo hacemos aquí. La tarea de transformación puede manejarlo.

                # 3. Opcional: Obtener audio features
                if track_id:
                    logging.debug(f"Obteniendo audio features para track ID: {track_id}")
                    features = spotify_client.get_audio_features(track_id)
                    if features:
                         result_item['audio_features'] = features # Añadir features
                    else:
                         result_item['audio_features'] = {} # Añadir vacío si falla

                api_results.append(result_item)
            else:
                logging.warning(f"No se encontró track en API para: Track='{track_name}', Artista='{artist_name}'")
                # Podrías guardar un registro de los no encontrados si es útil
                api_results.append({
                     'query_track_name': track_name,
                     'query_artist_name': artist_name,
                     'found_in_api': False
                })


        logging.info(f"Extracción de API de Spotify completada. {len(api_results)} resultados obtenidos.")
        # Pasar lista de diccionarios como JSON
        context['ti'].xcom_push(key='spotify_api_data', value=json.dumps(api_results))
        return len(api_results)

    except Exception as e:
        logging.error(f"Error durante la extracción de la API de Spotify: {e}")
        # Imprimir traceback podría ser útil para debug:
        # import traceback
        # logging.error(traceback.format_exc())
        raise

# --- Funciones para Tareas de Transformación ---

def _transform_spotify_csv(**context):
    """Transforma los datos del CSV de Spotify."""
    logging.info("Iniciando tarea: Transformar CSV de Spotify")
    ti = context['ti']
    spotify_csv_json = ti.xcom_pull(task_ids='extraction_group.extract_spotify_csv', key='spotify_csv_data')
    if not spotify_csv_json:
        raise ValueError("No se encontraron datos del CSV de Spotify en XCom para transformar.")
    try:
        df = pd.read_json(spotify_csv_json, orient='split')
        df_transformed = transform_spotify_csv_data(df)
        logging.info("Transformación de CSV de Spotify completada.")
        # Pasar JSON a XCom
        ti.xcom_push(key='transformed_spotify_csv', value=df_transformed.to_json(orient='split'))
        return df_transformed.shape
    except Exception as e:
        logging.error(f"Error en la transformación del CSV de Spotify: {e}")
        raise

def _transform_grammys_db(**context):
    """Transforma los datos de Grammys."""
    logging.info("Iniciando tarea: Transformar datos de Grammys DB")
    ti = context['ti']
    grammys_db_json = ti.xcom_pull(task_ids='extraction_group.extract_grammys_db', key='grammys_db_data')
    if not grammys_db_json:
        # Si la tabla estaba vacía, puede ser normal. Crear un DF vacío con schema esperado.
        logging.warning("No se encontraron datos de Grammys en XCom. Probablemente la tabla estaba vacía. Creando DataFrame vacío.")
        # Define las columnas esperadas después de la transformación para consistencia
        expected_columns = ['grammy_year', 'category', 'grammy_nominee', 'grammy_artist', 'winner',
                            'join_artist_grammy', 'join_track_grammy'] # Añade las columnas que crea tu función transform
        df_transformed = pd.DataFrame(columns=expected_columns)

    else:
        try:
             df = pd.read_json(grammys_db_json, orient='split')
             df_transformed = transform_grammys_db_data(df)
             logging.info("Transformación de datos de Grammys completada.")
        except Exception as e:
             logging.error(f"Error en la transformación de datos de Grammys: {e}")
             raise

    # Pasar JSON a XCom (incluso si está vacío)
    ti.xcom_push(key='transformed_grammys_db', value=df_transformed.to_json(orient='split'))
    return df_transformed.shape


def _transform_spotify_api(**context):
    """Transforma los datos obtenidos de la API de Spotify."""
    logging.info("Iniciando tarea: Transformar datos de API Spotify")
    ti = context['ti']
    spotify_api_json = ti.xcom_pull(task_ids='extraction_group.extract_spotify_api', key='spotify_api_data')
    if not spotify_api_json:
         # Si no se encontraron datos en la API, puede ser normal. Crear DF vacío.
         logging.warning("No se encontraron datos de la API de Spotify en XCom. Creando DataFrame vacío.")
         # Define columnas esperadas por la función de transformación API
         expected_columns = [
             'api_track_id', 'api_track_name', 'api_artist_name', 'api_artist_id',
             'api_all_artists', 'api_album_name', 'api_release_date', 'api_popularity',
             'api_duration_ms', 'api_spotify_url', 'api_artist_genres',
             'api_danceability', 'api_energy' # Añade las que crea tu función transform
             ]
         df_transformed = pd.DataFrame(columns=expected_columns)
    else:
         try:
              api_data_list = json.loads(spotify_api_json) # Cargar desde JSON string
              df_transformed = transform_spotify_api_data(api_data_list)
              logging.info("Transformación de datos de API Spotify completada.")
         except Exception as e:
              logging.error(f"Error en la transformación de datos de API Spotify: {e}")
              raise

    # Pasar JSON a XCom
    ti.xcom_push(key='transformed_spotify_api', value=df_transformed.to_json(orient='split'))
    return df_transformed.shape


# --- Función para Tarea de Fusión ---

def _merge_data(**context):
    """Fusiona los tres conjuntos de datos transformados."""
    logging.info("Iniciando tarea: Fusionar datos")
    ti = context['ti']

    # Recuperar los 3 DataFrames transformados de XCom
    try:
        spotify_json = ti.xcom_pull(task_ids='transformation_group.transform_spotify_csv', key='transformed_spotify_csv')
        grammys_json = ti.xcom_pull(task_ids='transformation_group.transform_grammys_db', key='transformed_grammys_db')
        api_json = ti.xcom_pull(task_ids='transformation_group.transform_spotify_api', key='transformed_spotify_api')

        if not spotify_json:
             raise ValueError("Datos transformados de Spotify CSV no encontrados en XCom.")
             # Decide cómo manejar esto. ¿Fallar? ¿Continuar sin ellos?

        df_spotify = pd.read_json(spotify_json, orient='split')

        # Grammys y API pueden estar vacíos si no hubo datos o fallaron antes, manejado en sus transform
        df_grammys = pd.read_json(grammys_json, orient='split') if grammys_json else pd.DataFrame() # Crear DF vacío si no hay XCom
        df_api = pd.read_json(api_json, orient='split') if api_json else pd.DataFrame() # Crear DF vacío si no hay XCom

        logging.info(f"DataFrames recuperados para fusión: Spotify={df_spotify.shape}, Grammys={df_grammys.shape}, API={df_api.shape}")

        # Realizar la fusión
        df_merged = merge_data(df_spotify, df_grammys, df_api)
        logging.info(f"Fusión completada. Shape final: {df_merged.shape}")

        # Pasar resultado a XCom
        ti.xcom_push(key='merged_data', value=df_merged.to_json(orient='split'))
        return df_merged.shape
    except Exception as e:
        logging.error(f"Error durante la fusión de datos: {e}")
        raise


# --- Funciones para Tareas de Carga ---

def _load_to_postgres(**context):
    """Carga el DataFrame fusionado a una tabla final en PostgreSQL."""
    logging.info(f"Iniciando tarea: Cargar datos a PostgreSQL (Tabla: {FINAL_TABLE_NAME})")
    ti = context['ti']
    merged_json = ti.xcom_pull(task_ids='merge_data', key='merged_data')
    if not merged_json:
        raise ValueError("No se encontraron datos fusionados en XCom para cargar a PostgreSQL.")

    try:
        df_merged = pd.read_json(merged_json, orient='split')
        logging.info(f"Datos fusionados recuperados para carga a PG. Shape: {df_merged.shape}")

        if df_merged.empty:
             logging.warning("El DataFrame fusionado está vacío. No se cargará nada a PostgreSQL.")
             return # Salir exitosamente si no hay datos que cargar

        # Obtener engine de SQLAlchemy usando la conexión de Airflow
        engine = get_sqlalchemy_engine(postgres_conn_id=POSTGRES_CONN_ID)

        # Estrategia de carga: 'replace' (borra y crea) o 'append'
        # 'replace' es más simple para un batch completo.
        # 'append' requeriría manejo de duplicados (ej. ON CONFLICT DO NOTHING/UPDATE) o limpieza previa.
        load_strategy = 'replace'
        logging.info(f"Usando estrategia de carga '{load_strategy}' para la tabla '{FINAL_TABLE_NAME}'")

        with engine.connect() as connection:
             # Nota: df.to_sql con 'replace' puede fallar si hay vistas dependientes.
             # Considera TRUNCATE + INSERT para 'append' o DROP + CREATE para 'replace' si hay problemas.
             df_merged.to_sql(
                 FINAL_TABLE_NAME,
                 con=connection,
                 if_exists=load_strategy,
                 index=False,
                 method='multi', # Eficiente para inserción
                 chunksize=1000 # Ajusta según el tamaño de tus datos y memoria
             )
        logging.info(f"Datos cargados exitosamente a la tabla '{FINAL_TABLE_NAME}' en PostgreSQL.")
        return f"Loaded {len(df_merged)} rows"

    except Exception as e:
        logging.error(f"Error al cargar datos a PostgreSQL: {e}")
        raise


def _load_to_gdrive(**context):
    """Carga el DataFrame fusionado como CSV a Google Drive."""
    logging.info("Iniciando tarea: Cargar CSV a Google Drive")
    ti = context['ti']
    merged_json = ti.xcom_pull(task_ids='merge_data', key='merged_data')
    if not merged_json:
        raise ValueError("No se encontraron datos fusionados en XCom para cargar a Google Drive.")

    try:
        df_merged = pd.read_json(merged_json, orient='split')
        logging.info(f"Datos fusionados recuperados para carga a GDrive. Shape: {df_merged.shape}")

        if df_merged.empty:
             logging.warning("El DataFrame fusionado está vacío. No se cargará nada a Google Drive.")
             return # Salir exitosamente si no hay datos

        # Obtener credenciales de Google Cloud desde la conexión de Airflow
        gcp_conn_id = GDRIVE_CONN_ID
        try:
             hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
             credentials = hook.get_credentials()
             logging.info(f"Credenciales de Google Cloud obtenidas desde la conexión '{gcp_conn_id}'.")
        except Exception as e:
             logging.error(f"Error al obtener credenciales de Google Cloud desde la conexión '{gcp_conn_id}': {e}")
             logging.error("Asegúrate de que la conexión exista y esté configurada correctamente (tipo 'Google Cloud Platform', con Keyfile Path o JSON).")
             raise

        # Construir el servicio de Google Drive API
        service = build('drive', 'v3', credentials=credentials)
        logging.info("Servicio de Google Drive API V3 construido.")

        # Convertir DataFrame a CSV en memoria
        csv_buffer = StringIO()
        df_merged.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_content = csv_buffer.getvalue()
        csv_buffer.close()

        # Usar BytesIO para MediaIoBaseUpload
        csv_bytes_io = BytesIO(csv_content.encode('utf-8'))

        # Metadata del archivo en Google Drive
        file_metadata = {
            'name': GDRIVE_CSV_FILENAME,
            'parents': [GDRIVE_FOLDER_ID] # Especifica la carpeta de destino
            # Puedes añadir más metadata si es necesario
        }

        # Media para la subida
        media = MediaIoBaseUpload(
            csv_bytes_io,
            mimetype='text/csv',
            resumable=True # Recomendado para archivos más grandes
        )

        # Realizar la subida
        logging.info(f"Subiendo archivo '{GDRIVE_CSV_FILENAME}' a la carpeta de Google Drive ID: '{GDRIVE_FOLDER_ID}'...")
        request = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink' # Campos a retornar después de la creación
        )
        response = request.execute()

        file_id = response.get('id')
        file_name = response.get('name')
        file_link = response.get('webViewLink')
        logging.info(f"Archivo '{file_name}' subido exitosamente a Google Drive.")
        logging.info(f"  ID del archivo: {file_id}")
        logging.info(f"  Enlace de vista: {file_link}")

        return file_id # Retornar el ID del archivo creado

    except Exception as e:
        logging.error(f"Error al cargar el archivo CSV a Google Drive: {e}")
        # Si es un error de API de Google, el mensaje puede dar pistas (permisos, ID de carpeta inválido, etc.)
        # import traceback
        # logging.error(traceback.format_exc())
        raise


# --- Definición del DAG ---

with DAG(
    dag_id='spotify_grammy_etl_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), # Usa pendulum para timezone
    schedule=None, # O define un schedule: '@daily', '0 0 * * *', timedelta(days=1)
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1, # Número de reintentos en caso de fallo
        'retry_delay': timedelta(minutes=2),
        'depends_on_past': False,
        'email_on_failure': False, # Configura si quieres notificaciones
        'email_on_retry': False,
        # 'postgres_conn_id': POSTGRES_CONN_ID, # Puedes ponerlo aquí si todas las tareas PG lo usan
    },
    description='ETL pipeline para integrar datos de Spotify (CSV, API) y Grammys (DB)',
    tags=['etl', 'spotify', 'grammys', 'workshop'],
) as dag:

    start = EmptyOperator(task_id='start')

    # --- Grupo de Tareas de Extracción ---
    with TaskGroup('extraction_group') as extraction_group:
        extract_spotify_csv = PythonOperator(
            task_id='extract_spotify_csv',
            python_callable=_extract_spotify_csv,
            # provide_context=True, # Ya es True por defecto en Airflow >= 2.0
        )

        extract_grammys_db = PythonOperator(
            task_id='extract_grammys_db',
            python_callable=_extract_grammys_db,
        )

        extract_spotify_api = PythonOperator(
            task_id='extract_spotify_api',
            python_callable=_extract_spotify_api,
            # Nota: Esta tarea depende implícitamente de los datos de extract_spotify_csv vía XCom.
            # Airflow no fuerza esta dependencia automáticamente por XCom pull,
            # así que la definimos explícitamente abajo.
        )

        # Definir dependencias DENTRO del grupo si aplica, o fuera.
        # Aquí, la API necesita el CSV, así que lo indicamos.
        extract_spotify_csv >> extract_spotify_api


    # --- Grupo de Tareas de Transformación ---
    with TaskGroup('transformation_group') as transformation_group:
        transform_spotify_csv = PythonOperator(
            task_id='transform_spotify_csv',
            python_callable=_transform_spotify_csv,
        )

        transform_grammys_db = PythonOperator(
            task_id='transform_grammys_db',
            python_callable=_transform_grammys_db,
        )

        transform_spotify_api = PythonOperator(
            task_id='transform_spotify_api',
            python_callable=_transform_spotify_api,
        )

        # Las transformaciones dependen de sus respectivas extracciones


    # --- Tarea de Fusión ---
    merge_data_task = PythonOperator(
        task_id='merge_data',
        python_callable=_merge_data,
        # Esta tarea depende de que TODAS las transformaciones terminen
    )


    # --- Grupo de Tareas de Carga ---
    with TaskGroup('load_group') as load_group:
        load_to_postgres = PythonOperator(
            task_id='load_to_postgres',
            python_callable=_load_to_postgres,
        )

        load_to_gdrive = PythonOperator(
            task_id='load_to_gdrive',
            python_callable=_load_to_gdrive,
        )
        # Las cargas dependen de la fusión

    end = EmptyOperator(
        task_id='end',
        # Asegurarse que 'end' solo se ejecute si ambas cargas son exitosas
        trigger_rule=TriggerRule.ALL_SUCCESS # Default para EmptyOperator, pero explícito es bueno
        )


    # --- Definir Flujo Principal de Dependencias ---
    start >> extraction_group

    # Conectar extracciones a sus transformaciones correspondientes
    extraction_group.extract_spotify_csv >> transformation_group.transform_spotify_csv
    extraction_group.extract_grammys_db >> transformation_group.transform_grammys_db
    extraction_group.extract_spotify_api >> transformation_group.transform_spotify_api

    # La fusión depende de que todas las transformaciones terminen
    transformation_group >> merge_data_task

    # Las cargas dependen de la fusión
    merge_data_task >> load_group

    # El fin depende de que todas las cargas terminen
    load_group >> end