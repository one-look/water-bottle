"""
Module: college_db_pipeline DAG
Purpose: Orchestrates RDBMS extraction, JSON transformation, and Elasticsearch loading.
"""

import warnings
import logging

# Warnings and Logging setup
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", module="skops")
logging.getLogger("skops").setLevel(logging.ERROR)

from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from typing import Any, Dict, List

# Internal imports
from src.python.credentials.factory import CredentialFactory
from src.python.connectors.factory import ConnectorFactory
from src.python.extractors.factory import ExtractorFactory
from src.python.transformers.factory import TransformerFactory
from src.python.loaders.factory import LoaderFactory
from src.python.utils.reader import load_yml

logger = logging.getLogger(__name__)

# --- Task Functions ---

def psqlcredential_task(**kwargs: Any) -> Dict[str, Any]:
    """
    Retrieves PostgreSQL credentials via the CredentialFactory.
    args:
    **kwargs: Airflow context arguments.
    returns:
    Dict[str, Any]: Database connection parameters (host, user, pass, etc).
    """
    logger.info("Fetching Postgres credentials.")
    return CredentialFactory.get_provider(mode="airflow", conn_id="postgres_college_db").get_credentials()

def escredential_task(**kwargs: Any) -> Dict[str, Any]:
    """
    Retrieves Elasticsearch credentials via the CredentialFactory.
    args:
    **kwargs: Airflow context arguments.
    returns:
    Dict[str, Any]: ES connection parameters.
    """
    logger.info("Fetching Elasticsearch credentials.")
    return CredentialFactory.get_provider(mode="airflow", conn_id="elasticsearch").get_credentials()

def extraction_task(ti: Any, **kwargs: Any) -> Dict[str, Any]:
    """
    Connects to RDBMS and extracts raw data as a Python dictionary.
    args:
    ti: Airflow Task Instance for XCom access.
    returns:
    Dict[str, Any]: Raw data extracted from specified tables.
    """
    CONFIG_PATH = Variable.get("college_db_config")

    creds = ti.xcom_pull(task_ids='psql_creds')
    config = load_yml(CONFIG_PATH).get("postgres", {}).get("extraction", {})

    connector = ConnectorFactory.get_connector(connector_type="rdbms", config=creds)
    connection = connector() # Established SQLAlchemy connection

    try:
        extractor = ExtractorFactory.get_extractor(
            extractor_type="rdbms", 
            connection=connection, 
            config=config
        )
        return extractor() 
    finally:
        connection.close()

def transformation_task(ti: Any, **kwargs: Any) -> List[Dict[str, Any]]:
    """
    Transforms raw RDBMS rows into Elasticsearch-ready JSON actions.
    args:
    ti: Airflow Task Instance for XCom access.
    returns:
    List[Dict[str, Any]]: List of transformed actions for bulk loading.
    """
    CONFIG_PATH = Variable.get("college_db_config")

    raw_data = ti.xcom_pull(task_ids='extract_college_data')
    # Load configuration for indexing logic
    config = load_yml(CONFIG_PATH).get("elasticsearch", {}).get("load", {})

    transformer = TransformerFactory.get_transformer(
        transformer_type="json",
        data=raw_data,
        config=config
    )
    
    # We convert the generator to a list to pass through XCom
    return list(transformer())

def loading_task(ti: Any, **kwargs: Any) -> None:
    """
    Ingests transformed JSON data into Elasticsearch via Bulk API.
    args:
    ti: Airflow Task Instance for XCom access.
    returns:
    None: Logs loading status.
    """
    CONFIG_PATH = Variable.get("college_db_config")
    
    transformed_data = ti.xcom_pull(task_ids='transform_college_data')
    es_creds = ti.xcom_pull(task_ids='es_creds')
    config = load_yml(CONFIG_PATH).get("elasticsearch", {}).get("load", {})

    connector = ConnectorFactory.get_connector(connector_type="elasticsearch", config=es_creds)
    es_connection = connector()

    loader = LoaderFactory.get_loader(
        load_type="elasticsearch",
        connection=es_connection,
        config=config
    )

    loader(data=transformed_data)
    logger.info("Health data ingestion successful.")

# --- DAG Definition ---

default_args = {
    'owner': 'alpha_team',
    'retries': 0
}

with DAG(
    'college_db_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["structure", "college", "postgres"]
) as dag:

    psql_creds = PythonOperator(
        task_id='psql_creds',
        python_callable=psqlcredential_task
    )

    es_creds = PythonOperator(
        task_id='es_creds',
        python_callable=escredential_task
    )

    extract_college_data = PythonOperator(
        task_id='extract_college_data',
        python_callable=extraction_task
    )

    transform_college_data = PythonOperator(
        task_id='transform_college_data',
        python_callable=transformation_task
    )

    load_to_es = PythonOperator(
        task_id='load_to_es',
        python_callable=loading_task
    )

    # Dependency Flow
    psql_creds >> extract_college_data >> transform_college_data >> load_to_es

    es_creds

    [transform_college_data, es_creds] >> load_to_es