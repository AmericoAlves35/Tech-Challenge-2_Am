import boto3
import pandas as pd
from io import StringIO

# Configuração do cliente S3
s3 = boto3.client('s3', region_name='sa-east-1')  # Substitua pela sua região correta
bucket_raw = 'raw-bucket-bovespa'
bucket_cleaned = 'cleaned-bucket-bovespa'

# Listar arquivos no bucket raw
response = s3.list_objects_v2(Bucket=bucket_raw)

# Verificar se há arquivos no bucket
if 'Contents' in response:
    print("Arquivos no bucket 'raw-bucket-bovespa':")
    for obj in response['Contents']:
        print(obj['Key'])
        file_key = obj['Key']  # Usar a chave do arquivo encontrado

        try:
            # Baixar o arquivo do bucket raw
            obj = s3.get_object(Bucket=bucket_raw, Key=file_key)
            data = obj['Body'].read().decode('utf-8')

            # Ler os dados em um DataFrame
            df = pd.read_csv(StringIO(data))

            # Limpar os dados (exemplo simples: remover linhas com valores faltantes)
            df_cleaned = df.dropna()

            # Salvar os dados limpos em um buffer
            csv_buffer = StringIO()
            df_cleaned.to_csv(csv_buffer, index=False)

            # Fazer upload dos dados limpos para o bucket cleaned
            s3.put_object(Bucket=bucket_cleaned, Key=file_key, Body=csv_buffer.getvalue())

            print(f"Dados limpos carregados com sucesso no bucket {bucket_cleaned} para o arquivo {file_key}")

        except Exception as e:
            print(f"Erro ao carregar dados no S3 para o arquivo {file_key}: {e}")
else:
    print("Nenhum arquivo encontrado no bucket 'raw-bucket-bovespa'.")
