import os
import json
import logging
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import google.auth.transport.requests
import google.oauth2.id_token
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from airflow.models import Variable

# -----------------------------------------------------------------------------
# CONFIGURACIÓN Y CONSTANTES DEL ENTORNO
# -----------------------------------------------------------------------------
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
AIRFLOW_GCP_CONN_ID = "GCP_INVOKER_FN"  # ID de la conexión creada en Airflow UI

# URLs de las Cloud Functions Gen2 (Cloud Run)
URL_FN_GCS_INGEST = Variable.get("FN_INGEST_RAW")
URL_FN_BQ_INGEST = Variable.get("FN_RAW_TO_BQ")

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
    Extrae el diccionario JSON de la conexión de Airflow y genera un ID Token
    firmado para invocar Cloud Functions o Cloud Run.
    """
    hook = GoogleBaseHook(gcp_conn_id=gcp_conn_id)
    
    # Obtener el objeto extra de la conexión de Airflow
    conn_extra = hook.get_connection(gcp_conn_id).extra_dejson
    keyfile_dict = conn_extra.get("keyfile_dict")

    if not keyfile_dict:
        raise ValueError(f"No se encontró 'keyfile_dict' en los Extras de la conexión {gcp_conn_id}")

    # Si keyfile_dict viene serializado como string, convertirlo a diccionario
    if isinstance(keyfile_dict, str):
        keyfile_dict = json.loads(keyfile_dict)

    # Sanear posibles saltos de línea mal escapados en private_key
    if "private_key" in keyfile_dict and isinstance(keyfile_dict["private_key"], str):
        keyfile_dict["private_key"] = keyfile_dict["private_key"].replace("\\n", "\n")

    # 1. Crear la credencial específica para generar ID Tokens apuntando a la URL destino (target_audience)
    id_credentials = service_account.IDTokenCredentials.from_service_account_info(
        keyfile_dict,
        target_audience=target_url
    )

    # 2. Refrescar el token utilizando un transporte HTTP estándar
    auth_req = Request()
    id_credentials.refresh(auth_req)

    # 3. Retornar el token JWT generado
    return id_credentials.token


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

def run_pass(entity_name: str, **kwargs):
    pass

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

    # Tarea 1: Copiar fuente hacis buckets GCS RAW
    task_fuente_ingest_raw = PythonOperator(
        task_id='fase_1_gcs_pre_raw_to_raw',
        python_callable=run_gcs_pre_raw_to_raw,
    )

    # Tarea 2: Cloud storage ingesta tablas raw
    task_gcs_ingest_raw_bq = EmptyOperator(
        task_id='fase_2_raw_ingest_to_bq'
        # task_id='fase_2_raw_ingest_to_bq',
        # python_callable=run_raw_to_bigquery_entity,
        # op_kwargs={'entity_name': 'sales'},
    )

    # Tarea 3.A: Aplicar reglas (calidadm rejectados), transformar, ingestar staging
    task_rules_transform_ingest_stg = EmptyOperator(
        task_id='fase_3_rules_transform_ingest_stg'
    )

    # Tarea 4.A: Staging ingesta analytics
    task_stg_ingest_analytics = EmptyOperator(
        task_id='fase_4_stg_ingest_analytics'
    )

    # Tarea 5.A: Staging ingesta analytics
    task_analytics_marts_sales = EmptyOperator(
        task_id='fase_5_analytics_marts_sales'
    )

    # Tarea 6.A: Staging ingesta analytics
    task_analytics_customer_360 = EmptyOperator(
        task_id='fase_6_analytics_customer_360'
    )
    
# Flujo secuencial tras Fase 1
    (
        task_fuente_ingest_raw 
        >> task_gcs_ingest_raw_bq 
        >> task_rules_transform_ingest_stg 
        >> task_stg_ingest_analytics 
        >> task_analytics_marts_sales 
        >> task_analytics_customer_360
    )