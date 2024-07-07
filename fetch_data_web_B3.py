import csv
import os
import time
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from datetime import datetime
from io import StringIO, BytesIO

# Função para gerar o próximo nome de arquivo com contador
def get_next_filename(base_name, extension, directory):
    counter = 1
    while True:
        filename = f"{counter:02d}_{base_name}.{extension}"
        filepath = os.path.join(directory, filename)
        if not os.path.exists(filepath):
            return filename
        counter += 1

# Configurando as opções do Chrome para rodar em modo headless
chrome_options = Options()
chrome_options.add_argument('--headless')  # Roda o navegador sem abrir uma janela GUI

# Configurando o serviço do ChromeDriver
service = Service(ChromeDriverManager().install())

# Inicializando o navegador com as opções configuradas
driver = webdriver.Chrome(service=service, options=chrome_options)

# URL base da página da B3 com dados do pregão D-1
base_url = 'https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br&page={}'

# Inicializando variáveis
dados_pregao = []
dados_rodape = {}  # Dicionário para armazenar os valores do rodapé dinâmicos

# Nomes das colunas da tabela de dados de pregão
colunas_tabela = ['Setor', 'Código', 'Ação', 'Tipo', 'Qtde. Teórica', 'Part. (%)', 'Part. (%)Acum.']

try:
    # Acessando a primeira página
    pagina = 1
    url = base_url.format(pagina)
    driver.get(url)

    # Esperando até 10 segundos para o elemento de tabela ser carregado
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

    # Capturando os textos "Carteira do Dia" e "Carteira Teórica"
    carteira_dia = driver.find_element(By.XPATH, '//h2').text
    carteira_teorica = driver.find_element(By.XPATH, '//p[contains(text(), "Carteira Teórica")]').text

    # Selecionar "Setor de Atuação" no seletor "Consulta por"
    select = Select(wait.until(EC.presence_of_element_located((By.ID, "segment"))))
    select.select_by_visible_text("Setor de Atuação")

    # Clicar no botão "BUSCAR"
    buscar_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'BUSCAR')]")))
    buscar_button.click()

    # Esperar a tabela carregar
    wait.until(EC.presence_of_element_located((By.CLASS_NAME, "table-responsive-md")))

    # Loop para coletar dados de todas as páginas disponíveis
    while True:
        # Obtendo todas as tabelas na página atual
        tabelas = driver.find_elements(By.TAG_NAME, 'table')

        # Extraindo e armazenando os dados das tabelas da página atual
        for tabela in tabelas:
            linhas = tabela.find_elements(By.TAG_NAME, 'tr')
            for linha in linhas:
                # Verificando se a linha contém os dados esperados
                colunas = linha.find_elements(By.TAG_NAME, 'td')
                if len(colunas) >= len(colunas_tabela):  # Verifica se há pelo menos as colunas esperadas
                    dados_linha = [col.text.strip() for col in colunas[:len(colunas_tabela)]]
                    dados_pregao.append(dados_linha)

        # Capturando valores dinâmicos do rodapé (apenas na primeira página)
        if pagina == 1:
            rodape_element = driver.find_element(By.XPATH, '//tfoot')
            linhas_rodape = rodape_element.find_elements(By.TAG_NAME, 'tr')

            for i, linha in enumerate(linhas_rodape):
                colunas = linha.find_elements(By.TAG_NAME, 'td')
                chave_rodape = ['Quantidade Teórica Total', 'Redutor'][i]  # Definindo as chaves do rodapé dinamicamente
                valores_rodape = [col.text.strip().replace(',', '.') for col in colunas[1:]]
                dados_rodape[chave_rodape] = valores_rodape if len(valores_rodape) > 1 else valores_rodape[0]

        # Verificando se há um botão "Próximo" disponível
        proximo_button = None
        try:
            proximo_button = driver.find_element(By.XPATH, '//li[@class="pagination-next"]/a')
        except:
            break  # Sai do loop se o botão "Próximo" não estiver presente

        # Verificando se o botão "Próximo" está habilitado
        if 'disabled' in proximo_button.get_attribute('class'):
            break  # Sai do loop se o botão "Próximo" estiver desabilitado

        # Clicando no botão "Próximo" para ir para a próxima página
        proximo_button.click()

        # Aguardando um pequeno intervalo antes de continuar para evitar bloqueios
        time.sleep(2)

        # Atualizando o contador de página
        pagina += 1

        # Aguardando até 10 segundos para o elemento de tabela ser carregado na próxima página
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

except Exception as e:
    print(f"Ocorreu um erro: {str(e)}")

finally:
    # Fechando o navegador
    driver.quit()

    # Salvando os dados em um arquivo CSV no mesmo diretório do script
    diretorio_atual = os.getcwd()
    nome_arquivo_csv = get_next_filename('dados_pregao_b3', 'csv', diretorio_atual)
    caminho_arquivo_csv = os.path.join(diretorio_atual, nome_arquivo_csv)

    with open(caminho_arquivo_csv, 'w', newline='', encoding='utf-8') as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv)

        # Escrevendo as linhas "Carteira do Dia" e "Carteira Teórica"
        escritor_csv.writerow([carteira_dia])
        escritor_csv.writerow([carteira_teorica])
        escritor_csv.writerow([])  # Linha em branco para separar

        # Escrevendo os nomes das colunas
        escritor_csv.writerow(colunas_tabela)

        # Escrevendo os dados das tabelas
        for dado in dados_pregao:
            escritor_csv.writerow(dado)

        # Escrevendo os dados dinâmicos do rodapé em duas linhas distintas
        escritor_csv.writerow([])  # Linha em branco para separar
        for chave, valores in dados_rodape.items():
            escritor_csv.writerow([chave] + (valores if isinstance(valores, list) else [valores]))

    print(f'Dados salvos com sucesso em {caminho_arquivo_csv}')

    # Converter o arquivo CSV para Parquet e enviar para o S3
    with open(caminho_arquivo_csv, 'r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        rows = list(csv_reader)
        fieldnames = csv_reader.fieldnames

        # Converter para Parquet
        table = pa.Table.from_pylist(rows, schema=pa.schema([(field, pa.string()) for field in fieldnames]))
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)

        # Upload para o bucket S3
        try:
            s3 = boto3.client('s3')
            bucket_name = 'raw-bucket-bovespa'
            data_atual = datetime.now().strftime("%Y-%m-%d")
            s3.upload_fileobj(parquet_buffer, bucket_name, f'{data_atual}/{nome_arquivo_csv.replace(".csv", ".parquet")}')
            print(f'Dados |{nome_arquivo_csv.replace(".csv", ".parquet")}| carregados com sucesso no bucket {bucket_name}')
        except Exception as e:
            print(f"Erro ao carregar dados no S3: {str(e)}")
