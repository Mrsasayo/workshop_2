# Proyecto: Pipeline ETL con Apache Airflow - Spotify y Grammys

## Descripción General

Este proyecto implementa un pipeline ETL utilizando Apache Airflow para extraer, transformar y cargar datos relacionados con música desde tres fuentes diferentes:
1.  Un dataset de canciones de Spotify (archivo CSV).
2.  Un dataset de premios Grammy (inicialmente CSV, cargado en PostgreSQL).
3.  La API Web de Spotify para enriquecer los datos.

El objetivo es fusionar estos datos en un conjunto coherente y cargarlo en una base de datos PostgreSQL y como archivo CSV en Google Drive.

## Flujo del Pipeline ETL

El pipeline está orquestado por un DAG de Airflow (`spotify_grammy_etl_pipeline`) y sigue estos pasos:

1.  **Extracción (Paralela):**
    *   `extract_spotify_csv`: Lee datos del archivo `spotify_dataset.csv`.
    *   `extract_grammys_db`: Lee datos de la tabla `grammy_awards_raw` en PostgreSQL.
    *   `extract_spotify_api`: Consulta la API de Spotify para obtener información adicional (popularidad actual, géneros de artistas, IDs, etc.) basada en los datos del CSV.
2.  **Transformación (Paralela):**
    *   `transform_spotify_csv`: Limpia, selecciona columnas y ajusta tipos del dataset de Spotify CSV.
    *   `transform_grammys_db`: Limpia y estandariza los datos de Grammys (nombres de artistas, etc.).
    *   `transform_spotify_api`: Procesa los datos JSON/dict de la API a un formato tabular (DataFrame).
3.  **Fusión:**
    *   `merge_data`: Combina los tres DataFrames transformados. La lógica principal une Spotify CSV con API (por `track_id`) y luego el resultado con Grammys (por `track_name` y `artist_name` normalizados). Se manejan datos faltantes y se crea una bandera para indicar nominaciones/premios Grammy.
4.  **Carga (Paralela):**
    *   `load_to_postgres`: Carga/Reemplaza el DataFrame fusionado en la tabla `spotify_grammy_merged` de PostgreSQL.
    *   `load_to_gdrive`: Guarda el DataFrame fusionado como un archivo CSV (`spotify_grammy_merged_data.csv`) en una carpeta específica de Google Drive.

## Estructura del Proyecto

.
├── config/ # (Opcional) Archivos de configuración (no usado activamente por el DAG)
│ └── db_config.ini
├── dags/ # DAGs de Airflow
│ └── etl_workshop_dag.py
├── data/ # Datos CSV originales
│ ├── spotify_dataset.csv # ¡ASEGÚRATE QUE ESTE ARCHIVO EXISTA AQUÍ!
│ └── the_grammy_awards.csv
├── env/ # Archivo de entorno (¡NO SUBIR A GIT!)
│ └── .env
├── notebooks/ # Notebooks para Análisis Exploratorio (EDA)
│ ├── eda_grammy_awards.ipynb
│ └── eda_spotify_tracks.ipynb
├── scripts/ # Scripts Python reutilizables
│ ├── api/
│ │ ├── init.py
│ │ └── spotify_client.py # Cliente API Spotify
│ ├── db/
│ │ ├── connection.py # Utilidades conexión DB (usando Airflow Hooks)
│ │ ├── init.py
│ │ └── load_grammys.py # Script para carga inicial de Grammys a PG (ejecutar 1 vez)
│ ├── init.py
│ └── transform/
│ ├── init.py
│ └── transformations.py # Funciones de transformación y fusión
├── requirements.txt # Dependencias Python
└── README.md # Este archivo


## Configuración del Entorno

1.  **Clonar Repositorio:**
    ```bash
    git clone <tu-repo-url>
    cd tu_proyecto_airflow
    ```
2.  **Crear Entorno Virtual y Activar:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
3.  **Instalar Dependencias:**
    ```bash
    pip install --upgrade pip
    pip install -r requirements.txt
    ```
4.  **Configurar PostgreSQL:**
    *   Asegúrate de tener un servidor PostgreSQL corriendo.
    *   Crea una base de datos (ej: `spotify_etl_db`).
    *   Crea un usuario y otórgale permisos sobre la base de datos.
5.  **Configurar Credenciales (`env/.env`):**
    *   Copia `env/.env.example` a `env/.env` (o crea `env/.env`).
    *   Rellena los valores para `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_HOST`, `POSTGRES_PORT`.
    *   Registra una aplicación en [Spotify for Developers](https://developer.spotify.com/dashboard/) para obtener `SPOTIFY_CLIENT_ID` y `SPOTIFY_CLIENT_SECRET`. Añádelos al `.env`.
    *   Configura Google Cloud:
        *   Crea un proyecto en [Google Cloud Console](https://console.cloud.google.com/).
        *   Habilita la API de Google Drive.
        *   Crea una Cuenta de Servicio (Service Account).
        *   Descarga el archivo JSON de la clave de la cuenta de servicio.
        *   Añade la ruta a este archivo en `GOOGLE_SERVICE_ACCOUNT_FILE` en `.env`.
        *   Crea una carpeta en Google Drive y obtén su ID (la parte final de la URL de la carpeta). Añádela como `GOOGLE_DRIVE_FOLDER_ID` en `.env`.
        *   **Importante:** Comparte la carpeta de Google Drive con el email de la cuenta de servicio creada (dándole permisos de Editor).
    *   **¡Añade `env/.env` a tu archivo `.gitignore`!**
6.  **Carga Inicial de Datos de Grammys:**
    *   Asegúrate de que el archivo `data/the_grammy_awards.csv` exista.
    *   Ejecuta el script de carga una vez (asegúrate que tu venv esté activado y el `.env` configurado):
        ```bash
        python scripts/db/load_grammys.py
        ```
    *   Verifica que la tabla `grammy_awards_raw` se haya creado y poblado en tu base de datos PostgreSQL.
7.  **Configurar Apache Airflow:**
    *   Instala y configura Airflow (si no lo tienes ya). Consulta la [documentación oficial de Airflow](https://airflow.apache.org/docs/).
    *   Asegúrate de que el directorio `dags` de tu proyecto esté incluido en la carpeta de DAGs de Airflow.
    *   Copia o monta el directorio `data` y `scripts` a una ubicación accesible por los workers de Airflow (si usas Docker, configura volúmenes). Usualmente ponerlos dentro de `$AIRFLOW_HOME` es una opción. El DAG asume que `data` y `scripts` son accesibles.
    *   **Configurar Conexiones de Airflow (UI: Admin -> Connections):**
        *   **PostgreSQL:**
            *   `Conn Id`: `postgres_workshop` (o el que uses en el DAG)
            *   `Conn Type`: `Postgres`
            *   `Host`: Tu host de PostgreSQL (ej: `localhost` o IP/hostname del servidor PG)
            *   `Schema`: El nombre de tu base de datos (ej: `spotify_etl_db`)
            *   `Login`: Tu usuario de PostgreSQL
            *   `Password`: Tu contraseña de PostgreSQL
            *   `Port`: Tu puerto de PostgreSQL (ej: `5432`)
        *   **Spotify API:**
            *   `Conn Id`: `spotify_workshop` (o el que uses en el DAG)
            *   `Conn Type`: `HTTP`
            *   `Host`: `https://accounts.spotify.com` (o `https://api.spotify.com` también podría funcionar, aunque el token se pide a `accounts`)
            *   `Login`: Tu `SPOTIFY_CLIENT_ID`
            *   `Password`: Tu `SPOTIFY_CLIENT_SECRET`
        *   **Google Drive (Google Cloud):**
            *   `Conn Id`: `google_drive_workshop` (o el que uses en el DAG)
            *   `Conn Type`: `Google Cloud`
            *   `Keyfile Path`: Ruta *absoluta* en el worker de Airflow al archivo JSON de tu cuenta de servicio (ej: `/opt/airflow/config/gcp-key.json`). Asegúrate de que este archivo exista en esa ruta dentro del entorno de Airflow (worker).
            *   *Alternativa:* `Keyfile JSON`: Pega el contenido completo del archivo JSON aquí.
            *   `Project Id`: (Opcional pero recomendado) El ID de tu proyecto de Google Cloud.
            *   `Scopes`: `https://www.googleapis.com/auth/drive` (necesario para interactuar con Google Drive)
    *   **(Opcional) Configurar Variables de Airflow (UI: Admin -> Variables):**
        *   Podrías usar una variable para `gdrive_folder_id` en lugar de leerla desde `.env` en el DAG.
            *   `Key`: `gdrive_folder_id`
            *   `Val`: `tu_id_de_carpeta_en_google_drive`

## Ejecución del Pipeline

1.  Asegúrate de que Airflow Scheduler y Webserver estén corriendo.
2.  Abre la interfaz web de Airflow.
3.  Busca el DAG `spotify_grammy_etl_pipeline`.
4.  Despausa el DAG (toggle a 'On').
5.  Puedes dispararlo manualmente usando el botón 'Trigger DAG'.
6.  Monitorea la ejecución en la vista de Grid o Graph. Verifica los logs de cada tarea si hay errores.

## Transformaciones Clave y Decisiones de Diseño

*   **Spotify CSV:** Se seleccionan columnas relevantes, se limpian datos básicos (nulos en IDs, nombres), se convierten tipos, y se extrae el artista principal si hay varios. Se eliminan duplicados por `track_id`.
*   **Grammys DB:** Se limpian nombres de artista y nominado, se estandariza la columna `winner` a booleano. Se preparan columnas para la unión (`join_artist_grammy`, `join_track_grammy`).
*   **Spotify API:** Se procesa la lista de resultados de la API (asumiendo que son diccionarios de info de tracks, géneros, features) en un DataFrame. Se prefijan las columnas con `api_` para evitar colisiones.
*   **Fusión:**
    *   Se usa `df_spotify` (CSV) como base.
    *   Se hace un `left join` con `df_api` usando `track_id` / `api_track_id`.
    *   Se hace un `left join` del resultado con `df_grammys` usando nombres normalizados de track y artista (`join_track_spotify` vs `join_track_grammy`, `join_artist_spotify` vs `join_artist_grammy`). *Nota: Esta unión basada en nombres puede ser imperfecta.*
    *   Se añade una columna `is_grammy_nominated_or_winner` y se limpia `is_grammy_winner`.
*   **Carga:**
    *   **PostgreSQL:** Se usa `to_sql` con `if_exists='replace'`, borrando la tabla `spotify_grammy_merged` y recreándola con los nuevos datos en cada ejecución.
    *   **Google Drive:** Se sube el DataFrame como un archivo CSV usando la API de Google Drive v3 y autenticación de cuenta de servicio.

## Verificación

*   **PostgreSQL:** Conéctate a tu base de datos y ejecuta:
    ```sql
    SELECT COUNT(*) FROM spotify_grammy_merged;
    SELECT * FROM spotify_grammy_merged LIMIT 10;
    -- Revisa filas donde hubo match con Grammys:
    SELECT * FROM spotify_grammy_merged WHERE is_grammy_nominated_or_winner = TRUE LIMIT 10;
    ```
*   **Google Drive:** Navega a la carpeta especificada en Google Drive y verifica que el archivo `spotify_grammy_merged_data.csv` se haya creado/actualizado. Descárgalo y revisa su contenido.
*   **Airflow UI:** Revisa la vista de Grid/Graph para ver ejecuciones exitosas (en verde). Examina los logs de las tareas para detalles o errores.

