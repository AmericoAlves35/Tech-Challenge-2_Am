import csv
import os
import time
import boto3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime

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

try:
    # Acessando a primeira página
    pagina = 1
    url = base_url.format(pagina)
    driver.get(url)

    # Esperando até 10 segundos para o elemento de tabela ser carregado
    wait = WebDriverWait(driver, 10)
    element = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))

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
                if len(colunas) >= 5:  # Verifica se há pelo menos 5 colunas (Código, Ação, Tipo, Qtde. Teórica, Part. (%))
                    codigo = colunas[0].text.strip()
                    acao = colunas[1].text.strip()
                    tipo = colunas[2].text.strip()
                    qtde_teoria = colunas[3].text.strip()
                    part_percentual = colunas[4].text.strip().replace(',', '.')  # Substitui vírgulas por pontos
                    dados_pregao.append([codigo, acao, tipo, qtde_teoria, part_percentual])

        # Capturando valores dinâmicos do rodapé (apenas na primeira página)
        if pagina == 1:
            rodape_element = driver.find_element(By.XPATH, '//tfoot')
            linhas_rodape = rodape_element.find_elements(By.TAG_NAME, 'tr')

            # Primeira linha do rodapé (Quantidade Teórica Total)
            colunas_primeira_linha = linhas_rodape[0].find_elements(By.TAG_NAME, 'td')
            quantidade_total = colunas_primeira_linha[1].text.strip()
            part_percentual = colunas_primeira_linha[2].text.strip().replace(',', '.')
            dados_rodape['Quantidade Teórica Total'] = [quantidade_total, part_percentual]

            # Segunda linha do rodapé (Redutor)
            colunas_segunda_linha = linhas_rodape[1].find_elements(By.TAG_NAME, 'td')
            redutor = colunas_segunda_linha[1].text.strip()
            dados_rodape['Redutor'] = redutor

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
    nome_arquivo = 'dados_pregao_b3.csv'
    caminho_arquivo = os.path.join(diretorio_atual, nome_arquivo)

    with open(caminho_arquivo, 'w', newline='', encoding='utf-8') as arquivo_csv:
        escritor_csv = csv.writer(arquivo_csv)

        # Escrevendo os dados das tabelas
        escritor_csv.writerow(['Código', 'Ação', 'Tipo', 'Qtde. Teórica', 'Part. (%)'])
        for dado in dados_pregao:
            escritor_csv.writerow(dado)

        # Escrevendo os dados dinâmicos do rodapé em duas linhas distintas
        escritor_csv.writerow([])  # Linha em branco para separar
        for chave, valores in dados_rodape.items():
            escritor_csv.writerow([chave] + valores if isinstance(valores, list) else [chave, valores])

    print(f'Dados salvos com sucesso em {caminho_arquivo}')

    # Upload para o bucket S3
    try:
        s3 = boto3.client('s3')
        bucket_name = 'raw-bucket-bovespa'
        data_atual = datetime.now().strftime("%Y-%m-%d")
        s3.upload_file(caminho_arquivo, bucket_name, f'{data_atual}/dados_pregao_b3.csv')
        print(f'Dados carregados com sucesso no bucket {bucket_name}')
    except Exception as e:
        print(f"Erro ao carregar dados no S3: {str(e)}")
