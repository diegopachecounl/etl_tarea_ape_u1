#Importacion de las librerias necesarias: manejo de DAG,uso de fechas, llamadas a API, manejo de dataframe, directorios y errores

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
from tenacity import retry, stop_after_attempt, wait_exponential,retry_if_exception_type

#Configuracion por defecto para todas las tareas del DAG
default_args = {
    'owner': 'Procesos_ETL_DP',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

#Definicion del DAG a aplicarse
@dag(
    dag_id='DP_owd_etl_robusto',
    default_args=default_args,
    start_date=datetime(2025, 2, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['iot', 'tenacity', 'etl','security', 'Tarea_APE_U1'],
    description='ETL robusto para datos de OpenWeather con manejo de errores y seguridad mejorada'
)

#Definicion de la funcion que ejecutará el DAG
def pipeline_weather_raw():
    BASE_DIR = '/opt/airflow/data/data_lake'

    @task
    def extraer_raw() -> str:   #Definicion de la tarea para extraer la informacion de la API
        print("📋Iniciando la extracción de datos de OpenWeather...")
        API_KEY = os.getenv("OPENWEATHER_APIKEY_DP")
        ciudades = ["Quito", "Loja", "Guayaquil", "Cuenca", "London", "New York", "Tokyo", "Sydney", "Paris", "Berlin"]

        #Resiliencia con tenacity para manejar errores de red
        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(requests.exceptions.RequestException)
        )
        
        #Definicion de la funcion que realiza la conexion con la API
        def safe_api_call(city):
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=10)
            
            # Manejo explícito de códigos críticos
            if response.status_code in [429, 500, 401]:
                raise requests.exceptions.RequestException(f"Error {response.status_code}")
            
            # Para otros errores HTTP
            response.raise_for_status()
            return response.json()
        
        #Inicializacion de la variable a almancenar los resultados de la consulta
        datos_crudos = []

        #Consulta individual para cada una de las ciudades
        for ciudad in ciudades:
            try:
                print(f"Obteniendo datos para {ciudad}...")
                datos_crudos.append(safe_api_call(ciudad))
            except Exception as e:
                print(f"Error al obtener datos para {ciudad}: {e}")
     
        if not datos_crudos:
            raise ValueError("Falló: No se pudieron obtener datos de ninguna ciudad después de varios intentos.")
        
        #Definicion de la ruta para almacenar los datos extraidos
        df_raw = pd.DataFrame(datos_crudos)
        dir_path=f"{BASE_DIR}/raw/fecha={datetime.now().strftime('%Y-%m-%d')}"
        os.makedirs(dir_path, exist_ok=True)
        raw_path = f"{dir_path}/datos.json"
        
        #Guardado del archivo JSON en el datalake
        df_raw.to_json(raw_path, orient="records", indent=2)
        print(f"💾(RAW) Datos crudos guardados en {raw_path}")

        return raw_path #Usando XComs. Retornamos la ruta del archivo guardado para su uso en tareas posteriores

    # Flujo de tareas
    path_raw = extraer_raw()

#instaciación del DAG
dag_instance = pipeline_weather_raw()