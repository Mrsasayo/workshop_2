# Workshop 2: Pipeline ETL de Datos de Artistas con Airflow

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) utilizando Apache Airflow para procesar y combinar datos de artistas desde tres fuentes diferentes:

1.  **Spotify:** Datos de canciones y características desde un archivo CSV (`spotify_dataset.csv`).
2.  **The Grammy Awards:** Historial de nominaciones y premios desde una tabla PostgreSQL (`the_grammy_awards`).
3.  **YouTube Data API v3:** Estadísticas de canales de artistas (suscriptores, vistas, etc.) obtenidas mediante la API de Google.

El pipeline extrae los datos, los limpia, los combina para crear un dataset enriquecido, y finalmente carga el resultado en una tabla PostgreSQL (`artists_merged_final`) en la base de datos `artists`.

## Prerrequisitos

*   **Python:** Versión 3.10 o superior recomendada.
*   **pip:** El instalador de paquetes de Python.
*   **venv:** Módulo para crear entornos virtuales (generalmente incluido con Python 3).
*   **PostgreSQL:** Una instancia de PostgreSQL en ejecución, accesible desde donde se ejecute Airflow. Debes tener creada la base de datos `artists` y las tablas iniciales cargadas (`the_grammy_awards`, `spotify_dataset` - los scripts de carga iniciales no están incluidos en este DAG, se asume que ya existen).
*   **Credenciales de YouTube Data API v3:** Necesitas credenciales OAuth 2.0 (Client ID, Client Secret) de un proyecto en Google Cloud Console con la API habilitada. Consulta la [guía oficial de Google](https://developers.google.com/youtube/v3/getting-started).
*   **Git:** Para clonar el repositorio.

## Instrucciones de Configuración

1.  **Clonar el Repositorio:**
    ```bash
    git clone https://github.com/Mrsasayo/workshop_2.git
    cd workshop_2 # Muévete al directorio del proyecto
    ```
    *(Nota: Asegúrate de estar en el directorio `workshop_2` para los siguientes comandos)*

2.  **Crear y Activar Entorno Virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
    *(En Windows usa: `venv\Scripts\activate`)*

3.  **Instalar Dependencias Base:**
    ```bash
    pip install pandas==2.1.4 numpy==1.26.4 python-dotenv SQLAlchemy psycopg2-binary google-api-python-client google-auth-httplib2 google-auth-oauthlib
    ```
    *(Nota: `psycopg2-binary` es más fácil de instalar que `psycopg2` que requiere dependencias del sistema. `SQLAlchemy` es necesario para interactuar con la DB. Se añadieron las librerías de Google)*.

4.  **Configurar Variables de Entorno:**
    *   Copia o renombra el archivo `env/.env.example` a `env/.env` (si existiera un ejemplo, sino créalo).
    *   Edita el archivo `env/.env` y rellena **TODAS** las variables requeridas:

        ```dotenv
        # PostgreSQL Credentials
        POSTGRES_USER=tu_usuario_postgres
        POSTGRES_PASSWORD=tu_contraseña_postgres
        POSTGRES_HOST=localhost # o la IP/hostname de tu servidor DB
        POSTGRES_PORT=5432 # o el puerto de tu servidor DB
        POSTGRES_DB=artists # Nombre de la base de datos

        # YouTube API OAuth 2.0 Credentials
        # Obtenidas desde Google Cloud Console
        YOUTUBE_CLIENT_ID=TU_CLIENT_ID.apps.googleusercontent.com
        YOUTUBE_CLIENT_SECRET=TU_CLIENT_SECRET
        YOUTUBE_PROJECT_ID=tu-project-id # Opcional pero bueno tenerlo
        # URIs - Generalmente no necesitas cambiarlas
        YOUTUBE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
        YOUTUBE_TOKEN_URI=https://oauth2.googleapis.com/token
        # Asegúrate que esta URI esté registrada en tus credenciales OAuth en Google Console
        YOUTUBE_REDIRECT_URIS=http://localhost
        ```

5.  **Instalar Apache Airflow:**
    *   Define las variables de entorno para la instalación y el directorio HOME de Airflow:
        ```bash
        export AIRFLOW_HOME="$(pwd)/airflow" # Define el directorio para Airflow dentro del proyecto
        # Ajusta la versión de Airflow si necesitas una diferente
        export AIRFLOW_VERSION=2.10.0
        # Detecta tu versión de Python (ej. 3.10)
        export PYTHON_VERSION="$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
        export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
        echo "AIRFLOW_HOME set to: ${AIRFLOW_HOME}"
        echo "Using Airflow ${AIRFLOW_VERSION} with Python ${PYTHON_VERSION}"
        echo "Constraint URL: ${CONSTRAINT_URL}"
        ```
    *   Instala Airflow con el proveedor de PostgreSQL:
        ```bash
        pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
        pip install apache-airflow-providers-postgres
        ```
    *   **Solución de Problemas (Error `google-re2` / `pybind11`):**
        *   Si la instalación de Airflow falla con errores relacionados con `Building wheel for google-re2` o `pybind11`, puede ser necesario instalar dependencias de compilación y `google-re2` por separado. Ejecuta los siguientes comandos **SI Y SOLO SI** la instalación anterior falló:
            ```bash
            echo "--- Applying google-re2 workaround if needed ---"
            # Instala herramientas de compilación (puede variar en otros OS)
            sudo apt update && sudo apt install -y build-essential python3-dev
            # Intenta instalar/actualizar pybind11 primero
            pip install --upgrade pip setuptools wheel pybind11
            # Exporta flags de compilación (puede que no siempre sea necesario pero ayuda)
            export CXXFLAGS="-std=c++17 $(python3 -m pybind11 --includes)"
            echo "CXXFLAGS set to: ${CXXFLAGS}"
            # Intenta instalar google-re2 sin aislamiento de build
            pip install --no-build-isolation google-re2
            echo "--- google-re2 workaround attempt finished ---"
            # Vuelve a intentar instalar Airflow después del workaround
            pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
            pip install apache-airflow-providers-postgres
            ```

6.  **Inicializar Airflow Standalone (Primera Vez):**
    *   La primera vez que ejecutes `standalone`, Airflow inicializará la base de datos (en `airflow/airflow.db` por defecto con SQLite, aunque podrías configurarlo para usar PostgreSQL) y creará un usuario admin.
    *   Asegúrate de que tu variable `AIRFLOW_HOME` esté correctamente exportada en tu terminal actual.
        ```bash
        echo $AIRFLOW_HOME # Debería mostrar la ruta a tu carpeta airflow
        ```
    *   Ejecuta el comando standalone. Te pedirá crear un usuario admin la primera vez.
        ```bash
        airflow standalone
        ```
    *   Anota el usuario y contraseña que creaste.

## Ejecutando el Pipeline

1.  **Activar Entorno Virtual:** Si no está activo:
    ```bash
    source venv/bin/activate
    ```
2.  **Establecer AIRFLOW_HOME:** Asegúrate de que la variable esté definida en tu sesión actual:
    ```bash
    export AIRFLOW_HOME="$(pwd)/airflow"
    ```
3.  **Iniciar Airflow Standalone:**
    ```bash
    airflow standalone
    ```
    Esto iniciará el scheduler, webserver y otros componentes necesarios.
4.  **Acceder a la UI de Airflow:** Abre tu navegador y ve a `http://localhost:8080` (o el puerto que indique Airflow).
5.  **Iniciar Sesión:** Usa el usuario y contraseña admin que creaste.
6.  **Encontrar y Activar el DAG:**
    *   Busca el DAG con ID `workshop_2_pipeline_v2_refactored`.
    *   Activa el interruptor (toggle) para que el DAG esté activo (`ON`).
7.  **Ejecutar el DAG:**
    *   Haz clic en el nombre del DAG.
    *   Haz clic en el botón de "Play" (Trigger DAG) en la esquina superior derecha. Puedes elegir ejecutarlo sin configuración adicional o con configuración (si la hubieras definido).
8.  **Monitorear la Ejecución:** Observa el progreso del DAG en la vista de "Grid" o "Graph". Puedes hacer clic en cada tarea para ver sus logs detallados.
9.  **Autorización de YouTube (Primera Ejecución):**
    *   Durante la primera ejecución de la tarea `extract_api_task` (o si el token de autenticación almacenado expira), verás un mensaje en los logs de esa tarea pidiéndote visitar una URL de Google para autorizar la aplicación.
    *   Copia esa URL, pégala en tu navegador, inicia sesión con tu cuenta de Google y concede los permisos solicitados.
    *   Google te redirigirá a `http://localhost:PORT` (el puerto será aleatorio) o mostrará un código de autorización. Airflow capturará esto automáticamente si `run_local_server` funciona correctamente.
    *   La tarea continuará después de la autorización exitosa. Las ejecuciones posteriores (mientras el refresh token sea válido) no requerirán esta interacción manual.
10. **Verificar Resultados:** Una vez que el DAG se complete exitosamente, verifica la tabla `artists_merged_final` en tu base de datos PostgreSQL `artists`.

## Estructura del Proyecto
workshop_2/
├── airflow/ # Directorio AIRFLOW_HOME
│ ├── dags/ # Contiene los archivos de definición de DAGs
│ │ ├── dag.py
│ │ └── task_to_dag.py
│ ├── logs/
│ ├── airflow.cfg # Configuración de Airflow
│ └── ... # Otros archivos de Airflow
├── data/ # Archivos de datos de entrada y salida intermedia
│ ├── spotify_dataset.csv
│ ├── the_grammy_awards.csv
│ └── youtube_stats.csv # Archivo de progreso/resultado de YouTube API
├── env/ # Archivos de configuración de entorno
│ └── .env # Variables de entorno (DB, API keys)
├── src/ # Código fuente de la lógica ETL
│ ├── extract/ # Scripts para la fase de extracción
│ │ ├── extract_api.py
│ │ ├── extract_csv.py
│ │ └── extract_sql.py
│ ├── transform/ # Scripts para la fase de transformación y merge
│ │ ├── transform_api.py
│ │ ├── transform_csv.py
│ │ ├── transform_sql.py
│ │ └── merge_data.py
│ └── load/ # Scripts para la fase de carga
│ └── load_to_db.py
├── venv/ # Entorno virtual de Python (ignorado por git)
├── .gitignore # Archivos a ignorar por Git
└── README.md # Este archivo

## Variables de Entorno (`env/.env`)

Asegúrate de que este archivo exista y contenga las siguientes variables con tus valores:

```dotenv
# PostgreSQL Credentials
POSTGRES_USER=tu_usuario_postgres
POSTGRES_PASSWORD=tu_contraseña_postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=artists

# YouTube API OAuth 2.0 Credentials
YOUTUBE_CLIENT_ID=TU_CLIENT_ID.apps.googleusercontent.com
YOUTUBE_CLIENT_SECRET=TU_CLIENT_SECRET
YOUTUBE_PROJECT_ID=tu-project-id
YOUTUBE_AUTH_URI=https://accounts.google.com/o/oauth2/auth
YOUTUBE_TOKEN_URI=https://oauth2.googleapis.com/token
YOUTUBE_REDIRECT_URIS=http://localhost
