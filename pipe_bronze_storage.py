import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
import json
from airflow.hooks.base import BaseHook

connection= BaseHook.get_connection("azure-blob-storage")
extras = connection.extra_dejson ## utilizado para acessar dados adicionais da conexão
connection_string = extras['connection_string']
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name = "bronze"

url = "https://api-football-v1.p.rapidapi.com/v3"

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
	"X-RapidAPI-Key": "3f522028a9msh6a2b71cf58194f7p189419jsnf95c5d5dcc8d",
	"X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
}


def bronze_estadio():
    response = requests.get(f"{url}/venues", headers=headers, params = querystring)
    response = response.json()
    dados_json = json.dumps(response)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob="estadios.json")
    blob_client.upload_blob(dados_json, overwrite=True,content_settings=ContentSettings(content_type='application/json'))

    print("JSON Estadios enviado com sucesso.")

def bronze_jogadores():
    response = requests.get(f"{url}/players", headers=headers, params=params_players)
    # response = response.json()
    # dados_json = json.dumps(response)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob="jogadores.json")
    blob_client.upload_blob(response,overwrite=True, content_settings=ContentSettings(content_type='application/json'))

    print("JSON Jogadores enviado com sucesso.")

def bronze_times():
    response = requests.get(f"{url}/teams", headers=headers, params=params_players)
    response = response.json()
    dados_json = json.dumps(response)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob="times.json")
    blob_client.upload_blob(dados_json, overwrite=True,content_settings=ContentSettings(content_type='application/json'))

    print("JSON times enviado com sucesso.")

def bronze_partidas():
    response = requests.get(f"{url}/fixtures", headers=headers, params=params_partidas)
    response = response.json()
    dados_json = json.dumps(response)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob="partidas.json")
    blob_client.upload_blob(dados_json, overwrite=True,content_settings=ContentSettings(content_type='application/json'))

    print("JSON partidas enviado com sucesso.")

def raw_pipeline():
    estadio = bronze_estadio()
    jogadores = bronze_jogadores()
    times = bronze_times()
    partidas = bronze_partidas()

    return ("Pipeline raw executado")
