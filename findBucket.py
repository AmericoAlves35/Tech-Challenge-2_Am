import boto3

# Configuração do cliente S3
s3 = boto3.client('s3', region_name='sa-east-1')  # Substitua pela sua região correta
bucket_raw = 'raw-bucket-bovespa'

# Listar arquivos no bucket raw
response = s3.list_objects_v2(Bucket=bucket_raw)

# Verificar se há arquivos no bucket
if 'Contents' in response:
    print("Arquivos no bucket 'raw-bucket-bovespa':")
    for obj in response['Contents']:
        print(obj['Key'])
else:
    print("Nenhum arquivo encontrado no bucket 'raw-bucket-bovespa'.")
