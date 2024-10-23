import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from airflow.models import Variable


connection_string = Variable.get('AZURE_STORAGE_ACCESS_KEY')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name_leitura = "bronze"
container_name_carga = "silver"

def silver_estadios():
    blob_client = blob_service_client.get_blob_client(container=container_name_leitura, blob="estadios.json")
    data_json = blob_client.download_blob().readall()
    data_json = data_json.decode('utf-8')
    data_json = json.loads(data_json)
    dados_df = pd.DataFrame.from_dict(data_json['response'])


    dados_df.rename(columns={
        "id":"id_estadio",
        "name":"nm_estadio",
        "adress":"endereco_estadio",
        "city":"nm_cidade",
        "country":"nm_pais",
        "capacity":"vl_capacidade",
        "surface":"tp_grama",
        "image":"link_imagem"}, inplace=True)
    dados_df['id_estadio'] = dados_df['id_estadio'].astype(str)
    table = pa.Table.from_pandas(dados_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="estadios.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("Estadios Silver Carregado")
def silver_jogadores():
    blob_client = blob_service_client.get_blob_client(container=container_name_leitura, blob="jogadores.json")
    data_json = blob_client.download_blob().readall()
    data_json = data_json.decode('utf-8')
    data_json = json.loads(data_json)
    players_data = [item['player'] for item in data_json['response'] if 'player' in item]

    dados_df = pd.json_normalize(players_data)
    dados_df = pd.DataFrame.from_dict(dados_df)
    

    dados_df.rename(columns={
        "age":"idade",
        "firstname":"nm_primeiro_nome",
        "height":"altura",
        "id":"id_jogador",
        "injured":"contudido",
        "lastname":"nm_sobrenome",
        "name":"nm_jogador",
        "nationality":"nm_pais",
        "photo":"link_foto",
        "weight":"peso",
        "birth.date":"dt_aniversario",
        "birth.place":"place",
        "birth.country":"country"}, inplace=True
        )
    
    dados_df['idade'] = dados_df['idade'].astype(int)
    dados_df['id_jogador'] = dados_df['id_jogador'].astype(str)
    dados_df['dt_aniversario'] = pd.to_datetime(dados_df['dt_aniversario'])
    dados_df['dt_aniversario']= dados_df['dt_aniversario'].dt.date

    table = pa.Table.from_pandas(dados_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="jogadores.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("Jogadores Silver Carregado")
def silver_partidas():
    blob_client = blob_service_client.get_blob_client(container=container_name_leitura, blob="partidas.json")
    data_json = blob_client.download_blob().readall()
    data_json = data_json.decode('utf-8')
    data_json = json.loads(data_json)
    dados_df = pd.json_normalize(data_json['response'])
    dados_df = pd.DataFrame.from_dict(dados_df)

    dados_df.rename(columns={
        "fixture.id": "id_partida",
        "fixture.referee": "nm_juiz",
        "fixture.date": "dt_partida",
        "fixture.venue.id": "id_estadio",
        "fixture.status.long": "ds_status",
        "league.id": "id_liga",
        "league.season": "ano_temporada",
        "league.round": "nr_partida",
        "teams.home.id": "id_time_casa",
        "teams.away.id": "id_time_fora",
        "goals.home": "qt_gols_casa",
        "goals.away": "qt_gols_fora",
        "score.halftime.home": "qt_gols_pr_tempo_casa",
        "score.halftime.away": "qt_gols_pr_tempo_fora",
        "score.fulltime.home": "qt_gols_jogo_casa",
        "score.fulltime.away": "qt_gols_jogo_fora"
    }, inplace=True)


    dados_df = dados_df[
        ['id_partida',
        'nm_juiz',
        'dt_partida',
        'id_estadio',
        'ds_status',
        'id_liga',
        'ano_temporada',
        'nr_partida',
        'id_time_casa',
        'id_time_fora',
        'qt_gols_casa',
        'qt_gols_fora',
        'qt_gols_pr_tempo_casa',
        'qt_gols_pr_tempo_fora',
        'qt_gols_jogo_casa',
        'qt_gols_jogo_fora']]


    dados_df['id_partida'] = dados_df['id_partida'].astype(str)
    dados_df['id_estadio'] = dados_df['id_estadio'].astype(str)
    dados_df['nm_juiz'] = dados_df['nm_juiz'].astype(str)

    dados_df['dt_partida'] = pd.to_datetime(dados_df['dt_partida'])
    dados_df['dt_partida']= dados_df['dt_partida'].dt.date
    dados_df['ds_status'] = dados_df['ds_status'].astype(str)

    dados_df['ds_status'] = dados_df['ds_status'].astype(str)
    dados_df['id_liga'] = dados_df['id_liga'].astype(str)

    dados_df['ano_temporada'] = dados_df['ano_temporada'].astype(int)
    dados_df['nr_partida'] = dados_df['nr_partida'].astype(str)

    dados_df['id_time_casa'] = dados_df['id_time_casa'].astype(str)
    dados_df['id_time_fora'] = dados_df['id_time_fora'].astype(str)

    dados_df['qt_gols_casa'] = dados_df['qt_gols_casa'].astype(float)
    dados_df['qt_gols_fora'] = dados_df['qt_gols_fora'].astype(float)

    dados_df['qt_gols_pr_tempo_casa'] = dados_df['qt_gols_pr_tempo_casa'].astype(float)
    dados_df['qt_gols_pr_tempo_fora'] = dados_df['qt_gols_pr_tempo_fora'].astype(float)

    dados_df['qt_gols_jogo_casa'] = dados_df['qt_gols_jogo_casa'].astype(float)
    dados_df['qt_gols_jogo_fora'] = dados_df['qt_gols_jogo_fora'].astype(float)

    table = pa.Table.from_pandas(dados_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="partidas.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("Partidas Silver Carregado")
def silver_times():
    blob_client = blob_service_client.get_blob_client(container=container_name_leitura, blob="times.json")
    data_json = blob_client.download_blob().readall()
    data_json = data_json.decode('utf-8')
    data_json = json.loads(data_json)
    dados_df = pd.json_normalize(data_json['response'])
    dados_df = pd.DataFrame.from_dict(dados_df)

    dados_df.rename(columns={
        "team.id":"id_time",
        "team.name":"nm_time",
        "team.code":"cd_time",
        "team.country":"nm_pais",
        "team.founded":"ano_fundacao",
        "team.national":"e_nacional",
        "team.logo":"link_logo",
        "venue.city":"nm_cidade",
        "venue.capacity":"capacidade"}, inplace=True)

    dados_df['id_time'] = dados_df['id_time'].astype(str)
    dados_df['nm_time'] = dados_df['nm_time'].astype(str)
    dados_df['cd_time'] = dados_df['cd_time'].astype(str)
    dados_df['nm_pais'] = dados_df['nm_pais'].astype(str)
    dados_df['ano_fundacao'] = dados_df['ano_fundacao'].astype(int)
    dados_df['e_nacional'] = dados_df['e_nacional'].astype(bool)
    dados_df['link_logo'] = dados_df['link_logo'].astype(str)
    dados_df['nm_cidade'] = dados_df['nm_cidade'].astype(str)
    dados_df['capacidade'] = dados_df['capacidade'].astype(int)

    table = pa.Table.from_pandas(dados_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="times.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("Times Silver Carregado")
def silver_pipeline():
    estadio = silver_estadios()
    jogadores = silver_jogadores()
    times = silver_times()
    partidas = silver_partidas()

    return ("Pipeline raw executado")
