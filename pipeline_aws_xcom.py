import os
import boto3
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import json

default_args = {
    "owner": "Paulo César",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag_aws = DAG(
    "aws_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

def dsa_extrai_dados(**kwargs):
    URL_BASE = "https://api.openweathermap.org/data/2.5/weather?"
    API_KEY = "1b100cc2a8884aa0d2f4bec425f0a8c2"
    CIDADE = "Fortaleza"

    url = f"{URL_BASE}q={CIDADE}&appid={API_KEY}"

    response = requests.get(url).json()

    kwargs['ti'].xcom_push(key='api_clima', value=response)

def dsa_transforma_dados(**kwargs):
    ti = kwargs['ti']  # Pegando ti via kwargs
    response = ti.xcom_pull(key='api_clima', task_ids='t01_extrai_dados')

    dsa_dados_tempo = {
        "date": datetime.utcfromtimestamp(response['dt']).strftime('%Y-%m-%d'),
        "temperature": round(response['main']['temp'] - 273.15, 2),
        "weather": response['weather'][0]['description']
    }

    dados_tempo_json = json.dumps(dsa_dados_tempo)

    # Converter o dicionário para JSON
    ti.xcom_push(key='resultados_clima', value=dados_tempo_json)

def dsa_carrega_dados(**kwargs):
    ti = kwargs['ti']  # Pegando ti via kwargs
    # Buscando os dados da instancia da tarefa anterior com o XCom
    task_instance = ti.xcom_pull(key='resultados_clima', task_ids='t02_transforma_dados')
        
        # Converte a string JSON em um objeto Python (dicionário ou lista de dicionários)
    try:
        data = json.loads(task_instance)
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o JSON: {e}")
        raise

    df = pd.DataFrame([data])


    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Configura as credenciais da AWS
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
        region_name=Variable.get('AWS_DEFAULT_REGION')
    )

    # Gera um timestamp único para o nome do arquivo
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')

    # Faz o upload do arquivo para um bucket S3
    s3_bucket = "s3-bucket-017820686119"
    s3_key = f"dados_previsao_tempo_{timestamp}.txt"

    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

t01_extrai_dados = PythonOperator(
    task_id='t01_extrai_dados',
    python_callable=dsa_extrai_dados,
    provide_context=True,  # Mantendo o provide_context=True
    dag=dag_aws
)

t02_transforma_dados = PythonOperator(
    task_id='t02_transforma_dados',
    python_callable=dsa_transforma_dados,
    provide_context=True,  # Mantendo o provide_context=True
    dag=dag_aws
)

t03_carrega_dados = PythonOperator(
    task_id='t03_carrega_dados',
    python_callable=dsa_carrega_dados,
    provide_context=True,  # Mantendo o provide_context=True
    dag=dag_aws
)

t01_extrai_dados >> t02_transforma_dados >> t03_carrega_dados