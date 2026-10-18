project_ai_de/
├── README.md                          # Instrucciones de ejecución y visión general
├── docker-compose.yaml                # Entorno local para Apache Airflow
├── .gitignore                         # Inclusión de secretos (*.json, .env, venv/)
├── requirements.txt                   # Dependencias de Python
│
├── architecture/
│   └── architecture_diagram.png       # Diagrama de la arquitectura del proyecto[cite: 10]
│
├── ingestion/
│   └── cloud_function/                # Código desplegado en GCP Cloud Function[cite: 4]
│       ├── main.py                    # Transferencia de pre-raw a raw
│       └── requirements.txt
│
├── processing/
│   └── bigquery_quality_check.sql     # Validación bloqueante (Fail Fast)[cite: 6]
│
├── dbt/                               # Proyecto dbt Core[cite: 3, 7, 10]
│   ├── dbt_project.yml
│   ├── profiles.yml                   # Configuración del adaptador a BigQuery
│   └── models/
│       ├── staging/
│       │   ├── stg_sales.sql
│       │   ├── stg_customers.sql
│       │   └── stg_products.sql
│       ├── intermediate/
│       │   └── int_customer_metrics.sql
│       └── marts/
│           ├── customer_360.sql
│           └── schema.yml             # Tests de dbt (unique, not_null, relationships)[cite: 7]
│
├── airflow/                            # Entorno del orquestador local[cite: 3, 7, 10]
│   ├── dags/
│   │   └── customer_360_end_to_end.py # DAG principal del pipeline[cite: 7]
│   └── config/                        # Claves del Service Account de GCP
│
├── ml/                                # Capa de Feature Engineering y MLOps[cite: 1, 7, 10]
│   ├── feature_engineering.py         # Creación de variables (RFM, Recencia)[cite: 7]
│   └── train_and_track.py             # Entrenamiento del modelo y tracking con MLflow[cite: 7]
│
├── api/                               # Capa de consumo final REST[cite: 3, 6, 10]
│   ├── main.py                        # Endpoint en FastAPI (Churn scoring)[cite: 6]
│   └── Dockerfile                     # Contenedor para desplegar en Google Cloud Run[cite: 4]
│
└── docs/                              # Documentación y Gobierno[cite: 10]
    ├── decisions.md                   # Matriz de decisiones técnicas y trade-offs[cite: 10]
    ├── data_dictionary.md             # Diccionario de datos de las capas de BigQuery[cite: 10]
    └── governance.md                  # Políticas de trazabilidad, calidad y MLOps[cite: 10]