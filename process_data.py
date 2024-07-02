import boto3
import pandas as pd
from io import StringIO

# Configuração do cliente S3
s3 = boto3.client('s3', region_name='sa-east-1')
bucket_cleaned = 'cleaned-bucket-bovespa'
bucket_business = 'business-bucket-bovespa'

# Listar arquivos no bucket cleaned
response = s3.list_objects_v2(Bucket=bucket_cleaned)

# Verificar se há arquivos no bucket
if 'Contents' in response:
    print("Arquivos no bucket 'cleaned-bucket-bovespa':")
    for obj in response['Contents']:
        print(obj['Key'])
        file_key = obj['Key']  # Usar a chave do arquivo encontrado

        try:
            # Baixar o arquivo do bucket cleaned
            obj = s3.get_object(Bucket=bucket_cleaned, Key=file_key)
            data = obj['Body'].read().decode('utf-8')

            # Ler os dados em um DataFrame
            df = pd.read_csv(StringIO(data))

            # Converter colunas para strings antes de limpar caracteres não numéricos
            df['Qtde. Teórica'] = df['Qtde. Teórica'].astype(str).str.replace('[^0-9]', '', regex=True)
            df['Part. (%)'] = df['Part. (%)'].astype(str).str.replace('[^0-9.]', '', regex=True)

            # Converter colunas relevantes para o tipo numérico
            df['Qtde. Teórica'] = pd.to_numeric(df['Qtde. Teórica'], errors='coerce')
            df['Part. (%)'] = pd.to_numeric(df['Part. (%)'], errors='coerce')

            if df['Qtde. Teórica'].isnull().any():
                raise ValueError("Erro na conversão de valores na coluna 'Qtde. Teórica' para numérico.")

            # Salvar os dados processados em um buffer
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Fazer upload dos dados processados para o bucket business
            s3.put_object(Bucket=bucket_business, Key=file_key, Body=csv_buffer.getvalue())

            print(f"Dados processados carregados com sucesso no bucket {bucket_business} para o arquivo {file_key}")

        except ValueError as ve:
            print(str(ve))
        except Exception as e:
            print(f"Erro ao carregar dados no S3 para o arquivo {file_key}: {e}")
else:
    print("Nenhum arquivo encontrado no bucket 'cleaned-bucket-bovespa'.")
