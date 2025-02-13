# %%
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import zipfile
import shutil
import tempfile

# URL base do site da Receita Federal
base_url = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"

# Data que você quer acessar
data_link = "2025-01/"

# Caminho base para salvar os arquivos
base_path = r"C:\Users\kore\Documents\etlreceita"

# URL completa da página da data
url = urljoin(base_url, data_link)

# Pasta da data
data_folder = os.path.join(base_path, data_link.replace("/", ""))

# Pasta Dimensao
dimensao_folder = os.path.join(data_folder, "Dimensao")

def criar_pastas(dimensao_folder, data_folder):
    """Cria as pastas se não existirem."""
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)
        print(f"Pasta {data_folder} criada.")
    else:
        print(f"Pasta {data_folder} já existe.")

    if not os.path.exists(dimensao_folder):
        os.makedirs(dimensao_folder)
        print(f"Pasta {dimensao_folder} criada.")
    else:
        print(f"Pasta {dimensao_folder} já existe.")

def processar_arquivos(url, data_folder, dimensao_folder):
    """Processa os arquivos ZIP (baixa, extrai e renomeia) das pastas data_folder e dimensao_folder."""
    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    # Listas de arquivos para baixar
    arquivos_para_baixar_dimensao = ['Motivos.zip', 'Municipios.zip', 'Naturezas.zip', 'Paises.zip', 'Qualificacoes.zip', 'Cnaes.zip']
    arquivos_para_baixar_data = [f'{tipo}{i}.zip' for tipo in ['Empresas', 'Estabelecimentos', 'Socios'] for i in range(10)]

    # Processa os arquivos da pasta Dimensao
    print("\nProcessando arquivos na pasta Dimensao...")
    processar_pasta(url, dimensao_folder, arquivos_para_baixar_dimensao, soup)

    # Processa os arquivos da pasta data_folder
    print("\nProcessando arquivos na pasta da data...")
    processar_pasta(url, data_folder, arquivos_para_baixar_data, soup)

def processar_pasta(url, folder_path, arquivos_para_baixar, soup):
    """Processa os arquivos ZIP (baixa, extrai e renomeia) em uma pasta específica."""
    # Lista os arquivos já existentes na pasta
    arquivos_existentes = os.listdir(folder_path)

    # Encontra todos os links
    zip_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.zip')]

    for zip_link in zip_links:
        filename_zip = os.path.basename(zip_link)

        if filename_zip in arquivos_para_baixar:
            # Caminho completo para o arquivo zip
            zip_filepath = os.path.join(folder_path, filename_zip)

            if filename_zip in arquivos_existentes:
                print(f"Arquivo {filename_zip} já existe em {folder_path}. Extraindo e renomeando...")
                extrair_renomear(zip_filepath, folder_path)
            else:
                # Constrói a URL completa do arquivo
                file_url = urljoin(url, zip_link)

                # Baixa o arquivo zip
                print(f"Baixando {filename_zip}...")
                file_response = requests.get(file_url)
                file_response.raise_for_status()

                # Salva o arquivo zip
                with open(zip_filepath, 'wb') as f:
                    f.write(file_response.content)
                print(f"{filename_zip} salvo em {zip_filepath}")

                # Extrai e renomeia o arquivo
                extrair_renomear(zip_filepath, folder_path)

def extrair_renomear(zip_filepath, folder_path):
    """Extrai o arquivo ZIP para uma pasta temporária, renomeia o arquivo e move para a pasta final."""
    filename_zip = os.path.basename(zip_filepath)
    filename_sem_ext = os.path.splitext(filename_zip)[0]

    # Cria uma pasta temporária
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)
            print(f"{filename_zip} extraído para {temp_dir}")

            # Localiza o arquivo extraído (sem extensão definida)
            for filename in os.listdir(temp_dir):
                filepath = os.path.join(temp_dir, filename)
                if os.path.isfile(filepath) and not filename.endswith('.zip'):
                    # Renomeia para o nome final desejado
                    csv_filename = filename_sem_ext + '.csv'
                    novo_nome_completo = os.path.join(temp_dir, csv_filename)
                    os.rename(filepath, novo_nome_completo)
                    print(f"Arquivo renomeado para {csv_filename} na pasta temporária")

                    # Move o arquivo para a pasta final
                    destino_final = os.path.join(folder_path, csv_filename)
                    shutil.move(novo_nome_completo, destino_final)
                    print(f"Arquivo movido para {destino_final}")

                    break  # Assume que só há um arquivo por ZIP

            # Remove o arquivo .zip
            # os.remove(zip_filepath)
            # print(f"Arquivo zip {filename_zip} removido")

        except zipfile.BadZipFile as e:
            print(f"Erro ao extrair {filename_zip}: {e}")
        except FileNotFoundError as e:
            print(f"Erro ao renomear {filename_zip}: {e}")
        except Exception as e:
            print(f"Erro inesperado ao processar {filename_zip}: {e}")

# 1. Criar as pastas
criar_pastas(dimensao_folder, data_folder)

# 2. Processar os arquivos
processar_arquivos(url, data_folder, dimensao_folder)

print("Download, extração e organização concluídos!")


# %%
import dask.dataframe as dd
import os

# Caminho da pasta contendo os arquivos CSV
pasta_csv = r"C:\Users\kore\Documents\etlreceita\2025-01"

def csv_para_parquet_com_dask(pasta_csv):
    """Converte arquivos CSV em Parquet usando Dask."""
    # Lista todos os arquivos na pasta
    arquivos = os.listdir(pasta_csv)

    # Filtra apenas os arquivos CSV de Empresas, Estabelecimentos e Socios
    arquivos_csv = [f for f in arquivos if f.endswith('.csv') and any(f.startswith(tipo) for tipo in ['Estabelecimentos', 'Socios'])]

    for arquivo_csv in arquivos_csv:
        # Constrói o caminho completo para o arquivo CSV
        caminho_csv = os.path.join(pasta_csv, arquivo_csv)

        # Cria o DataFrame Dask a partir do CSV, ignorando linhas malformadas
        df = dd.read_csv(caminho_csv, delimiter=',', quotechar='"', encoding='latin1', on_bad_lines='skip')

        # Constrói o caminho para o arquivo Parquet
        nome_parquet = arquivo_csv.replace('.csv', '-pq.parquet')
        caminho_parquet = os.path.join(pasta_csv, nome_parquet)

        # Salva o DataFrame como arquivo Parquet
        try:
            df.to_parquet(caminho_parquet, write_index=False)
            print(f"Arquivo {arquivo_csv} convertido para {nome_parquet}")
        except Exception as e:
            print(f"Erro ao converter {arquivo_csv} para Parquet: {e}")

if __name__ == "__main__":
    csv_para_parquet_com_dask(pasta_csv)
    print("Conversão de CSV para Parquet concluída!")


# %%
import os
import pandas as pd
import csv

# Caminho da pasta contendo os arquivos CSV
pasta_csv = r"C:\Users\kore\Documents\etlreceita\2025-01"

def csv_para_parquet(pasta_csv):
    """Converte arquivos CSV em Parquet e renomeia."""
    # Lista todos os arquivos na pasta
    arquivos = os.listdir(pasta_csv)

    # Filtra apenas os arquivos CSV de Empresas, Estabelecimentos e Socios
    arquivos_csv = [f for f in arquivos if f.endswith('.csv') and any(f.startswith(tipo) for tipo in ['Estabelecimentos', 'Socios'])]

    for arquivo_csv in arquivos_csv:
        # Constrói o caminho completo para o arquivo CSV
        caminho_csv = os.path.join(pasta_csv, arquivo_csv)

        # Lista para armazenar os DataFrames parciais
        lista_dfs = []

        # Abre o arquivo CSV e lê as linhas em partes menores
        with open(caminho_csv, 'r', encoding='latin1') as csvfile:
            leitor_csv = csv.reader(csvfile, delimiter=',', quotechar='"')
            
            # Tamanho do chunk (número de linhas a serem lidas por vez)
            chunk_size = 10000

            # Itera sobre os chunks
            chunk = []
            for i, linha in enumerate(leitor_csv):
                # Adiciona a linha ao chunk atual
                chunk.append(linha)

                # Se o chunk atingir o tamanho máximo, cria um DataFrame parcial
                if (i + 1) % chunk_size == 0:
                    # Cria o DataFrame a partir do chunk
                    df_chunk = pd.DataFrame(chunk)
                    lista_dfs.append(df_chunk)
                    
                    # Limpa o chunk para a próxima iteração
                    chunk = []

            # Processa o último chunk (se houver)
            if chunk:
                df_chunk = pd.DataFrame(chunk)
                lista_dfs.append(df_chunk)

        # Concatena todos os DataFrames parciais em um único DataFrame
        df = pd.concat(lista_dfs, ignore_index=True)

        # Constrói o caminho para o arquivo Parquet
        nome_parquet = arquivo_csv.replace('.csv', '-pq.parquet')
        caminho_parquet = os.path.join(pasta_csv, nome_parquet)

        # Salva o DataFrame como arquivo Parquet
        try:
            df.to_parquet(caminho_parquet, index=False)
            print(f"Arquivo {arquivo_csv} convertido para {nome_parquet}")
        except Exception as e:
            print(f"Erro ao converter {arquivo_csv} para Parquet: {e}")

if __name__ == "__main__":
    csv_para_parquet(pasta_csv)
    print("Conversão de CSV para Parquet concluída!")

# %%
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
import pyarrow.parquet as pq
import pyarrow
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# Iniciar sessão Spark
spark = SparkSession.builder \
    .appName("Reclame Aqui Dados") \
    .getOrCreate()

# URL base da Receita Federal
base_url = "https://arquivos.receitafederal.gov.br/cnpj/dados_abertos_cnpj/"

# Iniciar o navegador Edge usando selenium
def iniciar_navegador():
    options = webdriver.EdgeOptions()
    options.add_argument('--headless')  # Executa o navegador em modo headless (sem interface gráfica)
    try:
        driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)
    except Exception as e:
        print(f"Erro ao iniciar o navegador em modo headless: {e}. Iniciando no modo normal.")
        options = webdriver.EdgeOptions()
        driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)
    return driver

# Função para listar diretórios disponíveis na URL base
def listar_diretorios_base():
    driver = iniciar_navegador()
    driver.get(base_url)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    driver.quit()

    links = soup.find_all('a')
    diretorios = [link.get('href') for link in links if link.get('href') and link.get('href').startswith('20')]
    return diretorios

# Função para baixar e renomear arquivos ZIP de um diretório específico
def baixar_arquivo_zip(diretorio, nome_arquivo_original, nome_arquivo_local):
    url = f"{base_url}{diretorio}{nome_arquivo_original}"
    response = requests.get(url)
    if response.status_code == 200:
        # Diretório para salvar os arquivos
        local_dir = r"C:\Users\kore\Documents\etlreceita"
        
        # Criar diretório local, se não existir
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        
        # Caminho completo do arquivo local
        caminho_arquivo = os.path.join(local_dir, nome_arquivo_local)
        
        # Salvar o arquivo
        with open(caminho_arquivo, 'wb') as file:
            file.write(response.content)
        print(f"Arquivo {nome_arquivo_local} baixado com sucesso no diretório {local_dir}!")
    else:
        print(f"Erro ao acessar a URL: {response.status_code}")

# Função para extrair arquivos ZIP
def extrair_arquivo_zip(diretorio, nome_arquivo_local):
    local_dir = r"C:\Users\kore\Documents\etlreceita"
    caminho_arquivo = os.path.join(local_dir, nome_arquivo_local)
    extrair_para = os.path.join(local_dir, diretorio.strip('/'))
    
    # Extrair o arquivo ZIP
    try:
        with zipfile.ZipFile(caminho_arquivo, 'r') as zip_ref:
            zip_ref.extractall(extrair_para)
        print(f"Arquivo {nome_arquivo_local} extraído com sucesso para o diretório {extrair_para}!")
    except Exception as e:
        print(f"Erro ao extrair o arquivo {nome_arquivo_local}: {e}")

# Função para extrair arquivos ZIP e converter CSV para Parquet
def extrair_zip_converter_parquet(zip_path, output_dir, prefixo_nome):
    """Extrai arquivos ZIP, converte CSVs para Parquet e salva no diretório de saída."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_name in zip_ref.namelist():
                if file_name.endswith('.csv'):
                    csv_path = os.path.join(output_dir, file_name)
                    # Extrai o arquivo CSV
                    zip_ref.extract(file_name, output_dir)
                    # Converte CSV para Parquet
                    nome_base = os.path.splitext(file_name)[0]  # Remove a extensão .csv
                    parquet_nome = f"{prefixo_nome}_{nome_base}.parquet"  # Adiciona o prefixo e a extensão .parquet
                    parquet_path = os.path.join(output_dir, parquet_nome)
                    try:
                        df = pd.read_csv(csv_path, encoding='latin1', sep=';')  # Tenta 'latin1' ou 'utf-8'
                        table = pyarrow.Table.from_pandas(df)
                        pq.write_table(table, parquet_path)
                        print(f"Convertido '{file_name}' para Parquet em: {parquet_path}")
                    except Exception as e:
                        print(f"Erro ao converter CSV para Parquet: {e}")
                    finally:
                        # Remove o arquivo CSV após a conversão
                        os.remove(csv_path)
    except Exception as e:
        print(f"Erro ao extrair o arquivo ZIP: {e}")

# **INÍCIO: Adição do código para baixar arquivos de dimensão (AJUSTADO)**
# Diretório para salvar os arquivos de dimensão
local_dir_dimensao = r"C:\Users\kore\Documents\etlreceita\Dimensao"

# Criar diretório 'Dimensao', se não existir
if not os.path.exists(local_dir_dimensao):
    os.makedirs(local_dir_dimensao)

# Lista de arquivos de dimensão a serem baixados
arquivos_para_baixar = [
    'Cnaes.zip',
    'Motivos.zip',
    'Municipios.zip',
    'Naturezas.zip',
    'Paises.zip',
    'Qualificacoes.zip'
]

# Diretórios desejados
diretorios_desejados = ["2025-01/"]

# Função para baixar arquivos
for diretorio in diretorios_desejados:
    # Criar subdiretório para o diretório de data dentro de local_dir_dimensao
    diretorio_saida_dimensao = os.path.join(local_dir_dimensao, diretorio.strip('/'))
    if not os.path.exists(diretorio_saida_dimensao):
        os.makedirs(diretorio_saida_dimensao)
    
    prefixo_diretorio = diretorio.strip('/').replace('-', '')  # Remove a barra e os hífens
    for arquivo in arquivos_para_baixar:
        url = f'{base_url}{diretorio}{arquivo}'
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # Lança uma exceção para status de erro HTTP
            caminho_arquivo = os.path.join(local_dir_dimensao, arquivo)
            with open(caminho_arquivo, 'wb') as file:
                file.write(response.content)
            print(f'Arquivo {arquivo} baixado com sucesso no diretório {local_dir_dimensao}!')

            # Extrair o arquivo após o download para o subdiretório
            # extrair_zip_converter_parquet(caminho_arquivo, diretorio_saida_dimensao, prefixo_diretorio.lower())
            extrair_arquivo_zip(diretorio, arquivo)

        except requests.exceptions.RequestException as e:
            print(f'Erro ao baixar o arquivo {arquivo}: {e}')
# **FIM: Adição do código para baixar arquivos de dimensão (AJUSTADO)**

# Diretório local para salvar os arquivos
local_dir = r"C:\Users\kore\Documents\etlreceita"

# Listas de arquivos a serem baixados
tipos_arquivos = ["Empresas", "Estabelecimentos", "Socios"]
numeros_arquivos = range(10)  # de 0 a 9

# Listar diretórios e baixar arquivos desejados
diretorios = listar_diretorios_base()

for diretorio in diretorios_desejados:
    # Criar subdiretório para o diretório de data
    diretorio_saida = os.path.join(local_dir, diretorio.strip('/'))
    if not os.path.exists(diretorio_saida):
        os.makedirs(diretorio_saida)

    prefixo_diretorio = diretorio.strip('/').replace('-', '')  # Remove a barra e os hífens
    for tipo_arquivo in tipos_arquivos:
        for numero_arquivo in numeros_arquivos:
            nome_arquivo_zip = f"{tipo_arquivo}{numero_arquivo}-{diretorio.strip('/')}.zip"
            nome_arquivo_local = f"{tipo_arquivo}{numero_arquivo}-{diretorio.strip('/')}.zip"
            caminho_arquivo_zip = os.path.join(local_dir, nome_arquivo_local)
            url_arquivo = f"{base_url}{diretorio}{tipo_arquivo}{numero_arquivo}.zip"
            
            # Defina o prefixo para o nome do arquivo Parquet
            prefixo_nome = f"{prefixo_diretorio.lower()}_{tipo_arquivo[:4].lower()}"  # ex: 202305_empr

            if diretorio in diretorios:
                if os.path.exists(caminho_arquivo_zip):
                    print(f"O arquivo {nome_arquivo_local} já existe no diretório {local_dir}. Pulando o download.")
                    # extrair_zip_converter_parquet(caminho_arquivo_zip, diretorio_saida, prefixo_nome)
                    extrair_arquivo_zip(diretorio, nome_arquivo_local)
                else:
                    try:
                        response = requests.get(url_arquivo, timeout=10)
                        response.raise_for_status()
                        with open(caminho_arquivo_zip, 'wb') as file:
                            file.write(response.content)
                        print(f"Arquivo {nome_arquivo_local} baixado com sucesso no diretório {local_dir}!")
                        # extrair_zip_converter_parquet(caminho_arquivo_zip, diretorio_saida, prefixo_nome)
                        extrair_arquivo_zip(diretorio, nome_arquivo_local)
                    except requests.exceptions.RequestException as e:
                        print(f"Erro ao baixar o arquivo {nome_arquivo_zip}: {e}")
            else:
                print(f"Diretório {diretorio} não encontrado.")

# %%
# Diretório onde os arquivos CSV estão localizados
base_dir = r"C:\Users\kore\Documents\etlreceita"

# Função para encontrar o caminho do arquivo que contém a palavra-chave
def encontrar_arquivo(diretorio, palavra_chave):
    for arquivo in os.listdir(diretorio):
        if palavra_chave in arquivo:
            return os.path.join(diretorio, arquivo)
    return None

# Carregar arquivos CSV usando Spark e nomear as colunas
#arquivo_empre_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "EMPRE")
#if arquivo_empre_2024_12:
#    colunas_empre = ["cnpj_emp", "razao_emp", "nat_jur_emp", "qual_resp_emp", "cap_soc_emp", "port_emp", "ent_fed_resp"]
#    df_empre_2024_12 = spark.read.csv(arquivo_empre_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_empre)
#else:
#    print("Arquivo com 'EMPRE' não encontrado em 2023-05.")

#arquivo_esta_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "ESTA")
#if arquivo_esta_2024_12:
#    colunas_esta = ["cnpj_esta", "cnpj_2", "cnpj_3", "matriz_filial", "fantasia", "sit_cad", "data_sit_cad",
#                    "mot_sit_cad", "nome_cid_ext", "pais", "dat_ini_at", "cnae", "cnae_2", "tip_log",
#                    "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd", "tel1",
#                    "ddd2", "tel2", "dddfax", "fax", "email", "sit_esp", "dat_sit_exp"]
#    df_esta_2024_12 = spark.read.csv(arquivo_esta_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_esta)
#else:
#    print("Arquivo com 'ESTA' não encontrado em 2023-05.")

#arquivo_socios_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "SOC")
#if arquivo_socios_2024_12:
#    colunas_socios = ["cnpj_soc", "id_soc", "nome_socio", "cpf_cnpj", "qual_soc", "dat_ent", "pais", "rep_leg",
#                      "nome_rep", "qual_rep", "faixa_eta"]
#    df_socios_2024_12 = spark.read.csv(arquivo_socios_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_socios)
#else:
#    print("Arquivo com 'SOC' não encontrado em 2023-05.")

arquivo_empre_2025_01 = encontrar_arquivo(os.path.join(base_dir, "2025-01"), "EMPRE")
if arquivo_empre_2025_01:
    colunas_empre = ["cnpj_emp", "razao_emp", "nat_jur_emp", "qual_resp_emp", "cap_soc_emp", "port_emp", "ent_fed_resp"]
    df_empre_2025_01 = spark.read.csv(arquivo_empre_2025_01, header=False, sep=';', inferSchema=True).toDF(*colunas_empre)
else:
    print("Arquivo com 'EMPRE' não encontrado em 2025-01.")

arquivo_esta_2025_01 = encontrar_arquivo(os.path.join(base_dir, "2025-01"), "ESTA")
if arquivo_esta_2025_01:
    colunas_esta = ["cnpj", "cnpj_2", "cnpj_3", "matriz_filial", "fantasia", "sit_cad", "data_sit_cad",
                    "mot_sit_cad", "nome_cid_ext", "pais", "dat_ini_at", "cnae", "cnae_2", "tip_log",
                    "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd", "tel1",
                    "ddd2", "tel2", "dddfax", "fax", "email", "sit_esp", "dat_sit_exp"]
    df_esta_2025_01 = spark.read.csv(arquivo_esta_2025_01, header=False, sep=';', inferSchema=True).toDF(*colunas_esta)
else:
    print("Arquivo com 'ESTA' não encontrado em 2025-01.")

arquivo_socios_2025_01 = encontrar_arquivo(os.path.join(base_dir, "2025-01"), "SOC")
if arquivo_socios_2025_01:
    colunas_socios = ["cnpj_soc", "id_soc", "nome_socio", "cpf_cnpj", "qual_soc", "dat_ent", "pais", "rep_leg",
                      "nome_rep", "qual_rep", "faixa_eta"]
    df_socios_2025_01 = spark.read.csv(arquivo_socios_2025_01, header=False, sep=';', inferSchema=True).toDF(*colunas_socios)
else:
    print("Arquivo com 'SOC' não encontrado em 2025-01.")

# %%
# Mostrar esquemas e as primeiras 10 linhas dos DataFrames, se eles existirem
#if 'df_empre_2024_12' in locals():
#    df_empre_2024_12.printSchema()
#    df_empre_2024_12.show(10)

#if 'df_esta_2024_12' in locals():
#    df_esta_2024_12.printSchema()
#    df_esta_2024_12.show(10)

if 'df_empre_2025_01' in locals():
    df_empre_2025_01.printSchema()
    df_empre_2025_01.show(10)

if 'df_esta_2025_01' in locals():
    df_esta_2025_01.printSchema()
    df_esta_2025_01.show(10)

#if 'df_socios_2024_12' in locals():
#    df_socios_2024_12.printSchema()
#    df_socios_2024_12.show(10)

if 'df_socios_2025_01' in locals():
    df_socios_2025_01.printSchema()
    df_socios_2025_01.show(10)

# %%
# Remover duplicatas de todos os DataFrames
#df_empre_2024_12 = df_empre_2024_12.dropDuplicates()
#df_esta_2024_12 = df_esta_2024_12.dropDuplicates()
#df_socios_2024_12 = df_socios_2024_12.dropDuplicates()
df_empre_2025_01 = df_empre_2025_01.dropDuplicates()
df_esta_2025_01 = df_esta_2025_01.dropDuplicates()
df_socios_2025_01 = df_socios_2025_01.dropDuplicates()

# Verificar o número de linhas após a remoção das duplicatas
#linhas_empre_2024_12 = df_empre_2024_12.count()
#linhas_esta_2024_12 = df_esta_2024_12.count()
#linhas_socios_2024_12 = df_socios_2024_12.count()
#linhas_empre_2025_01 = df_empre_2025_01.count()
#linhas_esta_2025_01 = df_esta_2025_01.count()
#linhas_socios_2025_01 = df_socios_2025_01.count()

#print(f"Número de linhas em df_empre_2024_12 após remoção de duplicatas: {linhas_empre_2024_12}")
#print(f"Número de linhas em df_esta_2024_12 após remoção de duplicatas: {linhas_esta_2024_12}")
#print(f"Número de linhas em df_socios_2024_12 após remoção de duplicatas: {linhas_socios_2024_12}")
#print(f"Número de linhas em df_empre_2025_01 após remoção de duplicatas: {linhas_empre_2025_01}")
#print(f"Número de linhas em df_esta_2025_01 após remoção de duplicatas: {linhas_esta_2025_01}")
#print(f"Número de linhas em df_socios_2025_01 após remoção de duplicatas: {linhas_socios_2025_01}")

# %%
from pyspark.sql.functions import col, substring

# Contar o número de linhas em cada DataFrame
#linhas_empre_2024_12 = df_empre_2024_12.count()
#linhas_esta_2024_12 = df_esta_2024_12.count()
#linhas_socios_2024_12 = df_socios_2024_12.count()
linhas_empre_2025_01 = df_empre_2025_01.count()
linhas_esta_2025_01 = df_esta_2025_01.count()
linhas_socios_2025_01 = df_socios_2025_01.count()

#print(f"Número de linhas em df_empre_2023_05: {linhas_empre_2024_12}")
#print(f"Número de linhas em df_esta_2023_05: {linhas_esta_2024_12}")
#print(f"Número de linhas em df_socios_2023_05: {linhas_socios_2024_12}")
print(f"Número de linhas em df_empre_2025_01: {linhas_empre_2025_01}")
print(f"Número de linhas em df_esta_2025_01: {linhas_esta_2025_01}")
print(f"Número de linhas em df_socios_2025_01: {linhas_socios_2025_01}")

# Fazer o inner join entre as tabelas empre e esta pelo campo razao_emp e fantasia
#df_join_2024_12 = df_empre_2024_12.join(df_esta_2024_12, df_empre_2024_12["razao_emp"] == df_esta_2024_12["fantasia"], how="inner")
df_join_2025_01 = df_empre_2025_01.join(df_esta_2025_01, df_empre_2025_01["razao_emp"] == df_esta_2025_01["fantasia"], how="inner")

# Fazer o inner join com a tabela de sócios, utilizando os primeiros 7 dígitos de 'esta' = cnpj e 'soc' = cnpj_soc
#df_join_socios_2024_12 = df_join_2024_12.join(df_socios_2024_12,
#                                              substring(df_join_2024_12["cnpj"], 1, 7) == substring(df_socios_2024_12["cnpj_soc"], 1, 7),
#                                              how="left")

df_join_socios_2025_01 = df_join_2025_01.join(df_socios_2025_01,
                                              substring(df_join_2025_01["cnpj"], 1, 7) == substring(df_socios_2025_01["cnpj_soc"], 1, 7),
                                              how="left")

# Contar o número de linhas resultantes dos joins
#linhas_join_2024_12 = df_join_socios_2024_12.count()
linhas_join_2025_01 = df_join_socios_2025_01.count()

#print(f"Número de linhas resultantes do join em 2024-12: {linhas_join_2024_12}")
print(f"Número de linhas resultantes do join em 2025-01: {linhas_join_2025_01}")

# Mostrar as primeiras 50 linhas do resultado do join
#print("Join result for 2024-12:")
#df_join_socios_2024_12.show(50)

print("Join result for 2025-01:")
df_join_socios_2025_01.show(50)

# %%
# Salvar os DataFrames resultantes dos joins em arquivos Excel
#output_dir = r"C:\Users\kore\Documents\etlreceita"

# Converter os DataFrames para pandas e salvar em Excel
#df_join_socios_2024_12_pd = df_join_socios_2024_12.toPandas()
#df_join_socios_2025_01_pd = df_join_socios_2025_01.toPandas()

#df_join_socios_2024_12_pd.to_excel(os.path.join(output_dir, "join_socios_2024_12.xlsx"), index=False)
#df_join_socios_2025_01_pd.to_excel(os.path.join(output_dir, "join_socios_2025_01.xlsx"), index=False)

#print(f"Arquivos Excel salvos em {output_dir}")


