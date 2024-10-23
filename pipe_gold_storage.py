import pandas as pd
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContentSettings
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import io
import numpy as np
from airflow.models import Variable


connection_string = Variable.get('AZURE_STORAGE_ACCESS_KEY')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_name_leitura = "silver"
container_name_carga = "gold"
blob_stream_partidas = io.BytesIO()
blob_stream_estadios = io.BytesIO()
blob_stream_jogadores = io.BytesIO()
blob_stream_times = io.BytesIO()

def dim_estadios():
    blob_client_estadios = blob_service_client.get_blob_client(container=container_name_leitura, blob="estadios.parquet")
    blob_client_estadios.download_blob().download_to_stream(blob_stream_estadios)
    # Voltar para o início do stream
    blob_stream_estadios.seek(0)
    # Ler o arquivo Parquet no DataFrame
    estadios_df = pd.read_parquet(blob_stream_estadios)
    estadios_df['nm_pais'] = estadios_df['nm_pais'].str.replace('Brazil','Brasil')
    estadios_df['cidade_pais'] = estadios_df['nm_cidade'] + ', ' + estadios_df['nm_pais']

    estadios_df = estadios_df[[
        'id_estadio',
        'nm_estadio',
        'nm_cidade',
        'nm_pais',
        'vl_capacidade',
        'link_imagem',
        'cidade_pais'
    ]]

    table = pa.Table.from_pandas(estadios_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="dim_estadios.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("dim_estadios carregada")
    return estadios_df
def fatos_partidas():
    blob_client_partidas = blob_service_client.get_blob_client(container=container_name_leitura, blob="partidas.parquet")
    blob_client_partidas.download_blob().download_to_stream(blob_stream_partidas)
    # Voltar para o início do stream
    blob_stream_partidas.seek(0)
    # Ler o arquivo Parquet no DataFrame
    partidas_df = pd.read_parquet(blob_stream_partidas)
    partidas_df['id_estadio'] = partidas_df['id_estadio'].str.replace('.0','')
    estadio_df = dim_estadios()

    fatos_partidas_df = pd.merge(partidas_df,estadio_df[['cidade_pais','id_estadio']],on ='id_estadio', how = 'left')
    table = pa.Table.from_pandas(fatos_partidas_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="fatos_partidas.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("fatos_partidas carregada")
def dim_jogadores():
    blob_client_jogadores = blob_service_client.get_blob_client(container=container_name_leitura, blob="jogadores.parquet")
    blob_client_jogadores.download_blob().download_to_stream(blob_stream_jogadores)
    # Voltar para o início do stream
    blob_stream_jogadores.seek(0)
    # Ler o arquivo Parquet no DataFrame
    jogadores_df = pd.read_parquet(blob_stream_jogadores)

    jogadores_df.fillna(value=np.nan, inplace=True)
    jogadores_df['nm_pais'] = jogadores_df['nm_pais'].str.replace('Brazil','Brasil')
    jogadores_df['nm_completo'] = jogadores_df['nm_primeiro_nome'] + " " + jogadores_df['nm_sobrenome']
    jogadores_df = jogadores_df.drop(['country','nm_jogador','nm_primeiro_nome','nm_sobrenome'], axis=1)

    table = pa.Table.from_pandas(jogadores_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="dim_jogadores.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("dim_jogadores carregada")
def dim_times():
    blob_client_times = blob_service_client.get_blob_client(container=container_name_leitura, blob="times.parquet")
    blob_client_times.download_blob().download_to_stream(blob_stream_times)
    # Voltar para o início do stream
    blob_stream_times.seek(0)
    # Ler o arquivo Parquet no DataFrame
    times_df = pd.read_parquet(blob_stream_times)

    times_df['nm_pais'] = times_df['nm_pais'].str.replace('Brazil','Brasil')
    times_df = times_df.drop(['venue.name','venue.address','venue.surface','venue.image','venue.id'],axis=1)

    table = pa.Table.from_pandas(times_df)
    buf = BytesIO()
    pq.write_table(table, buf)
    blob_client = blob_service_client.get_blob_client(container=container_name_carga, blob="dim_times.parquet")
    buf.seek(0)
    blob_client.upload_blob(buf.getvalue(), overwrite=True)
    print("dim_times carregada")
def gold_pipeline():
    partidas = fatos_partidas()
    jogadores = dim_jogadores()
    times = dim_times()
    
    return ("Pipeline raw executado")


