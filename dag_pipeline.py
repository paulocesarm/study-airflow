from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import boto3
import pandas as pd
from io import StringIO

default_args = {
    "owner": "Paulo César",
    "depends_on_past": False,
    "start_date": datetime(2024,9,19),
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag_pipeline = DAG(
    "pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

url_base = "https://api-football-v1.p.rapidapi.com/v3"
league_id = 71
season_id = 2023
country_id = "Brazil"

querystring = {"country":country_id}

params_players = {
                "league":league_id,
                  "season":season_id
                }
params_times = {
                "league":league_id,
                  "season":season_id,
                  "country": country_id
                }
params_partidas = {
                "league":league_id,
                  "season":season_id
                }

headers = {
	"X-RapidAPI-Key": Variable.get('X-RapidAPI-Key'),
	"X-RapidAPI-Host": Variable.get('X-RapidAPI-Host')
}

s3_client = boto3.client(
    's3',
    aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    region_name=Variable.get('AWS_DEFAULT_REGION')
)

s3_bucket = "s3-bucket-017820686119"
## Extração de dados
def extrai_dados_estadio(**kwargs):
    response = requests.get(f"{url_base}/venues", headers=headers, params=querystring).json()
    kwargs['ti'].xcom_push(key='estadio', value=response)

def extrai_dados_jogadores(**kwargs):
    response = requests.get(f"{url_base}/players", headers=headers, params=params_players).json()
    kwargs['ti'].xcom_push(key='jogadores', value=response)

def extrai_dados_times(**kwargs):
    response = requests.get(f"{url_base}/teams", headers=headers, params=params_times).json()
    kwargs['ti'].xcom_push(key='times', value=response)

def extrai_dados_partidas(**kwargs):
    response = requests.get(f"{url_base}/fixtures", headers=headers, params=params_partidas).json()
    kwargs['ti'].xcom_push(key='partidas', value=response)
    
##Caregando dados da camada bronze
def estadio_bronze(**kwargs):
    ti = kwargs['ti']
    task_instance = ti.xcom_pull(key='estadio', task_ids='camada_bronze.dados_estadio')
    s3_client.put_object(Bucket=s3_bucket, Key="bronze/estadio.json", Body=json.dumps(task_instance))

def jogadores_bronze(**kwargs):
    ti = kwargs['ti']
    task_instance_jogadores = ti.xcom_pull(key='jogadores', task_ids='camada_bronze.dados_jogadores')
    s3_client.put_object(Bucket=s3_bucket, Key="bronze/jogadores.json", Body=json.dumps(task_instance_jogadores))

def times_bronze(**kwargs):
    ti = kwargs['ti']
    task_instance_times = ti.xcom_pull(key='times', task_ids='camada_bronze.dados_times')
    s3_client.put_object(Bucket=s3_bucket, Key="bronze/times.json", Body=json.dumps(task_instance_times))

def partidas_bronze(**kwargs):
    ti = kwargs['ti']
    task_instance_partidas = ti.xcom_pull(key='partidas', task_ids='camada_bronze.dados_partidas')
    s3_client.put_object(Bucket=s3_bucket, Key="bronze/partidas.json", Body=json.dumps(task_instance_partidas))

def transf_estadio_prata(**kwargs):
    ti = kwargs['ti']
    task_instance = ti.xcom_pull(key='estadio', task_ids='camada_bronze.dados_estadio')
    data_json = json.loads(task_instance)
    estadio_df = pd.DataFrame.from_dict(data_json['response'])

    estadio_df.rename(columns={
    "id":"id_estadio",
    "name":"nm_estadio",
    "adress":"endereco_estadio",
    "city":"nm_cidade",
    "country":"nm_pais",
    "capacity":"vl_capacidade",
    "surface":"tp_grama",
    "image":"link_imagem"}, inplace=True)

    estadio_df['id_estadio'] = estadio_df['id_estadio'].astype(str)

    ti.xcom_push(key='transf_estadio', value=json.dumps(estadio_df))



with TaskGroup(group_id='camada_bronze', dag=dag_pipeline) as camada_bronze:

    dados_estadio = PythonOperator(
        task_id='dados_estadio',
        python_callable=extrai_dados_estadio,
        provide_context=True,
        dag=dag_pipeline
    )
    t05_estadio_bronze = PythonOperator(
        task_id='t05_estadio_bronze',
        python_callable=estadio_bronze,
        provide_context=True,
        dag=dag_pipeline
    )

    dados_jogadores = PythonOperator(
        task_id='dados_jogadores',
        python_callable=extrai_dados_jogadores,
        provide_context=True,
        dag=dag_pipeline
    )
    dados_times = PythonOperator(
        task_id='dados_times',
        python_callable=extrai_dados_times,
        provide_context=True,
        dag=dag_pipeline
    )
    dados_partidas = PythonOperator(
        task_id='dados_partidas',
        python_callable=extrai_dados_partidas,
        provide_context=True,
        dag=dag_pipeline
    )

    t06_jogadores_bronze = PythonOperator(
        task_id='t06_jogadores_bronze',
        python_callable=jogadores_bronze,
        provide_context=True,
        dag=dag_pipeline
    )
    t07_times_bronze = PythonOperator(
        task_id='t07_times_bronze',
        python_callable=times_bronze,
        provide_context=True,
        dag=dag_pipeline
    )
    t08_partidas_bronze = PythonOperator(
        task_id='t08_partidas_bronze',
        python_callable=partidas_bronze,
        provide_context=True,
        dag=dag_pipeline
    )

    dados_estadio >> t05_estadio_bronze
    dados_jogadores >> t06_jogadores_bronze
    dados_times >> t07_times_bronze
    dados_partidas >> t08_partidas_bronze