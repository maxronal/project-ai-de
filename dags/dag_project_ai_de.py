import os
import logging
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import google.auth.transport.requests
import google.oauth2.id_token
from google.oauth2 import service_account

# -----------------------------------------------------------------------------
# CONFIGURACIÓN Y CONSTANTES DEL ENTORNO
# -----------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
AIRFLOW_GCP_CONN_ID = "GCP_INVOKER_FN"  # ID de la conexión creada en Airflow UI

# URLs de las Cloud Functions Gen2 (Cloud Run)
URL_FN_GCS_INGEST = os.environ.get("FN_INGEST_RAW")
URL_FN_BQ_INGEST = os.environ.get("FN_RAW_TO_BQ")

DEFAULT_ARGS = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 17),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


# -----------------------------------------------------------------------------
# OBTENCIÓN DEL ID TOKEN MEDIANTE AIRFLOW CONNECTION HOOK
# -----------------------------------------------------------------------------
def get_id_token_from_connection(target_url: str, gcp_conn_id: str = AIRFLOW_GCP_CONN_ID) -> str:
    """
    Extrae las credenciales JSON de la conexión de Airflow y genera un ID Token para Cloud Functions.
    """
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    
    # 1. Obtener los extras formateados como diccionario
    conn_extra = hook.get_connection(gcp_conn_id).extra_dejson
    keyfile_dict = conn_extra.get("keyfile_dict")

    if keyfile_dict:
        # Si la clave keyfile_dict está presente en la conexión, instanciar Service Account
        credentials = service_account.Credentials.from_service_account_info(
            keyfile_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    else:
        # Fallback a credenciales obtenidas por el Hook
        credentials = hook.get_credentials()

    # 2. Generar y firmar el ID Token para la Cloud Function
    auth_req = google.auth.transport.requests.Request()
    id_token = google.oauth2.id_token.fetch_id_token(auth_req, target_url)

    return id_token


# -----------------------------------------------------------------------------
# TAREAS DE EJECUCIÓN
# -----------------------------------------------------------------------------
def run_gcs_pre_raw_to_raw(**kwargs):
    """
    Fase 1: Ingesta GCS Pre-Raw a Raw utilizando el token obtenido vía Connection.
    """
    token = get_id_token_from_connection(URL_FN_GCS_INGEST)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(URL_FN_GCS_INGEST, headers=headers, json={}, timeout=300)
    response.raise_for_status()
    
    res_data = response.json()
    logging.info("Respuesta Fase 1 (GCS Ingest): %s", res_data)
    return res_data


def run_raw_to_bigquery_entity(entity_name: str, **kwargs):
    """
    Fase 2: Ingesta Parametrizada a BigQuery enviando entidad y fecha lógica de Airflow.
    """
    token = get_id_token_from_connection(URL_FN_BQ_INGEST)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Fecha de ejecución lógica del DAG (ds = YYYY-MM-DD)
    execution_date = kwargs.get('ds', datetime.now().strftime("%Y-%m-%d"))
    
    payload = {
        "entity": entity_name,
        "ingestion_date": execution_date
    }
    
    logging.info("Invocando Cloud Function para entidad '%s' y fecha '%s'...", entity_name, execution_date)
    response = requests.post(URL_FN_BQ_INGEST, headers=headers, json=payload, timeout=300)
    response.raise_for_status()
    
    res_data = response.json()
    logging.info("Respuesta Fase 2 (%s): %s", entity_name, res_data)
    return res_data


# -----------------------------------------------------------------------------
# DEFINICIÓN DEL DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id='dag_medallion_ingestion_pipeline',
    default_args=DEFAULT_ARGS,
    description='Pipeline ELT Medallion usando Airflow GCP Connection para autenticación HTTP Token',
    catchup=False,
    tags=['gcp', 'elt', 'medallion', 'raw_dataset', 'airflow_connection'],
) as dag:

    # Tarea 1: Copia entre buckets GCS
    task_gcs_ingest_raw = PythonOperator(
        task_id='fase_1_gcs_pre_raw_to_raw',
        python_callable=run_gcs_pre_raw_to_raw,
    )

    # Tarea 2.A: Carga Sales
    task_gcs_raw_to_ingest_bq = PythonOperator(
        task_id='fase_2_raw_ingest_to_bq',
        python_callable=run_raw_to_bigquery_entity,
        op_kwargs={'entity_name': 'sales'},
    )


    # Flujo paralelo tras Fase 1
    task_gcs_ingest_raw >> task_gcs_raw_to_ingest_bq