import json
import boto3
import csv
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO, BytesIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_cleaned = 'cleaned-bucket-bovespa'
    bucket_business = 'business-bucket-bovespa'

    # Obter o nome do arquivo do evento
    file_key = event['Records'][0]['s3']['object']['key']

    try:
        # Baixar o arquivo CSV do bucket cleaned
        obj = s3.get_object(Bucket=bucket_cleaned, Key=file_key)
        data = obj['Body'].read().decode('utf-8')

        # Ler os dados do CSV
        csv_reader = csv.DictReader(StringIO(data))
        rows = list(csv_reader)
        fieldnames = csv_reader.fieldnames

        # Converter para Parquet
        table = pa.Table.from_pylist(rows, schema=pa.schema([(field, pa.string()) for field in fieldnames]))
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)

        # Definir o nome do arquivo Parquet
        parquet_key = file_key.replace('.csv', '.parquet')

        # Fazer upload do arquivo Parquet para o bucket business
        s3.put_object(Bucket=bucket_business, Key=parquet_key, Body=parquet_buffer.getvalue())

        print(f"Dados convertidos e carregados com sucesso no bucket {bucket_business} para o arquivo {parquet_key}")

    except Exception as e:
        print(f"Erro ao processar o arquivo {file_key}: {e}")
        raise e

# Função para carregar e executar o evento localmente
def test_lambda_locally():
    with open('event.json') as f:
        event = json.load(f)
    lambda_handler(event, None)

if __name__ == "__main__":
    test_lambda_locally()
