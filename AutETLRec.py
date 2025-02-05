# %%
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import zipfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# Configurar JAVA_HOME dentro do script Python
#os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-11'
#os.environ['PATH'] = os.environ['JAVA_HOME'] + r'\bin;' + os.environ['PATH']

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
    with zipfile.ZipFile(caminho_arquivo, 'r') as zip_ref:
        zip_ref.extractall(extrair_para)
    print(f"Arquivo {nome_arquivo_local} extraído com sucesso para o diretório {extrair_para}!")

# Listar diretórios e baixar arquivos desejados
diretorios = listar_diretorios_base()
diretorios_desejados = ["2023-05/", "2025-01/"]

for diretorio in diretorios_desejados:
    # Diretório local para salvar os arquivos
    local_dir = r"C:\Users\kore\Documents\etlreceita"
    nome_arquivo_empresas = f"Empresas1-{diretorio.strip('/')}.zip"
    nome_arquivo_estabelecimentos = f"Estabelecimentos1-{diretorio.strip('/')}.zip"
    nome_arquivo_socios = f"Socios-{diretorio.strip('/')}.zip"
    caminho_arquivo_empresas = os.path.join(local_dir, nome_arquivo_empresas)
    caminho_arquivo_estabelecimentos = os.path.join(local_dir, nome_arquivo_estabelecimentos)
    caminho_arquivo_socios = os.path.join(local_dir, nome_arquivo_socios)

    if diretorio in diretorios:
        # Verificar se os arquivos já existem
        if os.path.exists(caminho_arquivo_empresas):
            print(f"O arquivo {nome_arquivo_empresas} já existe no diretório {local_dir}. Pulando o download.")
            extrair_arquivo_zip(diretorio, nome_arquivo_empresas)
        else:
            baixar_arquivo_zip(diretorio, "Empresas1.zip", nome_arquivo_empresas)
            extrair_arquivo_zip(diretorio, nome_arquivo_empresas)

        if os.path.exists(caminho_arquivo_estabelecimentos):
            print(f"O arquivo {nome_arquivo_estabelecimentos} já existe no diretório {local_dir}. Pulando o download.")
            extrair_arquivo_zip(diretorio, nome_arquivo_estabelecimentos)
        else:
            baixar_arquivo_zip(diretorio, "Estabelecimentos1.zip", nome_arquivo_estabelecimentos)
            extrair_arquivo_zip(diretorio, nome_arquivo_estabelecimentos)
        
        if os.path.exists(caminho_arquivo_socios):
            print(f"O arquivo {nome_arquivo_socios} já existe no diretório {local_dir}. Pulando o download.")
            extrair_arquivo_zip(diretorio, nome_arquivo_socios)
        else:
            baixar_arquivo_zip(diretorio, "Socios1.zip", nome_arquivo_socios)
            extrair_arquivo_zip(diretorio, nome_arquivo_socios)
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
arquivo_empre_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "EMPRE")
if arquivo_empre_2024_12:
    colunas_empre = ["cnpj_emp", "razao_emp", "nat_jur_emp", "qual_resp_emp", "cap_soc_emp", "port_emp", "ent_fed_resp"]
    df_empre_2024_12 = spark.read.csv(arquivo_empre_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_empre)
else:
    print("Arquivo com 'EMPRE' não encontrado em 2023-05.")

arquivo_esta_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "ESTA")
if arquivo_esta_2024_12:
    colunas_esta = ["cnpj", "cnpj_2", "cnpj_3", "matriz_filial", "fantasia", "sit_cad", "data_sit_cad",
                    "mot_sit_cad", "nome_cid_ext", "pais", "dat_ini_at", "cnae", "cnae_2", "tip_log",
                    "logradouro", "numero", "complemento", "bairro", "cep", "uf", "municipio", "ddd", "tel1",
                    "ddd2", "tel2", "dddfax", "fax", "email", "sit_esp", "dat_sit_exp"]
    df_esta_2024_12 = spark.read.csv(arquivo_esta_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_esta)
else:
    print("Arquivo com 'ESTA' não encontrado em 2023-05.")

arquivo_socios_2024_12 = encontrar_arquivo(os.path.join(base_dir, "2023-05"), "SOC")
if arquivo_socios_2024_12:
    colunas_socios = ["cnpj_soc", "id_soc", "nome_socio", "cpf_cnpj", "qual_soc", "dat_ent", "pais", "rep_leg",
                      "nome_rep", "qual_rep", "faixa_eta"]
    df_socios_2024_12 = spark.read.csv(arquivo_socios_2024_12, header=False, sep=';', inferSchema=True).toDF(*colunas_socios)
else:
    print("Arquivo com 'SOC' não encontrado em 2023-05.")

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
if 'df_empre_2024_12' in locals():
    df_empre_2024_12.printSchema()
    df_empre_2024_12.show(10)

if 'df_esta_2024_12' in locals():
    df_esta_2024_12.printSchema()
    df_esta_2024_12.show(10)

if 'df_empre_2025_01' in locals():
    df_empre_2025_01.printSchema()
    df_empre_2025_01.show(10)

if 'df_esta_2025_01' in locals():
    df_esta_2025_01.printSchema()
    df_esta_2025_01.show(10)
    
if 'df_socios_2024_12' in locals():
    df_socios_2024_12.printSchema()
    df_socios_2024_12.show(10)

if 'df_socios_2025_01' in locals():
    df_socios_2025_01.printSchema()
    df_socios_2025_01.show(10)

# %%
# Remover duplicatas de todos os DataFrames
df_empre_2024_12 = df_empre_2024_12.dropDuplicates()
df_esta_2024_12 = df_esta_2024_12.dropDuplicates()
df_socios_2024_12 = df_socios_2024_12.dropDuplicates()
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
#linhas_empre_2025_01 = df_empre_2025_01.count()
#linhas_esta_2025_01 = df_esta_2025_01.count()
#linhas_socios_2025_01 = df_socios_2025_01.count()

#print(f"Número de linhas em df_empre_2024_12: {linhas_empre_2024_12}")
#print(f"Número de linhas em df_esta_2024_12: {linhas_esta_2024_12}")
#print(f"Número de linhas em df_socios_2024_12: {linhas_socios_2024_12}")
#print(f"Número de linhas em df_empre_2025_01: {linhas_empre_2025_01}")
#print(f"Número de linhas em df_esta_2025_01: {linhas_esta_2025_01}")
#print(f"Número de linhas em df_socios_2025_01: {linhas_socios_2025_01}")

# Fazer o inner join entre as tabelas empre e esta pelo campo razao_emp e fantasia
df_join_2024_12 = df_empre_2024_12.join(df_esta_2024_12, df_empre_2024_12["razao_emp"] == df_esta_2024_12["fantasia"], how="inner")
df_join_2025_01 = df_empre_2025_01.join(df_esta_2025_01, df_empre_2025_01["razao_emp"] == df_esta_2025_01["fantasia"], how="inner")

# Fazer o inner join com a tabela de sócios, utilizando os primeiros 7 dígitos de 'esta' = cnpj e 'soc' = cnpj_soc
df_join_socios_2024_12 = df_join_2024_12.join(df_socios_2024_12, 
                                              substring(df_join_2024_12["cnpj"], 1, 7) == substring(df_socios_2024_12["cnpj_soc"], 1, 7), 
                                              how="left")

df_join_socios_2025_01 = df_join_2025_01.join(df_socios_2025_01, 
                                              substring(df_join_2025_01["cnpj"], 1, 7) == substring(df_socios_2025_01["cnpj_soc"], 1, 7), 
                                              how="left")

# Contar o número de linhas resultantes dos joins
linhas_join_2024_12 = df_join_socios_2024_12.count()
linhas_join_2025_01 = df_join_socios_2025_01.count()

print(f"Número de linhas resultantes do join em 2024-12: {linhas_join_2024_12}")
print(f"Número de linhas resultantes do join em 2025-01: {linhas_join_2025_01}")

# Mostrar as primeiras 50 linhas do resultado do join
print("Join result for 2024-12:")
df_join_socios_2024_12.show(50)

print("Join result for 2025-01:")
df_join_socios_2025_01.show(50)

# %%
# Salvar os DataFrames resultantes dos joins em arquivos Excel
output_dir = r"C:\Users\kore\Documents\etlreceita"

# Converter os DataFrames para pandas e salvar em Excel
df_join_socios_2024_12_pd = df_join_socios_2024_12.toPandas()
#df_join_socios_2025_01_pd = df_join_socios_2025_01.toPandas()

df_join_socios_2024_12_pd.to_excel(os.path.join(output_dir, "join_socios_2024_12.xlsx"), index=False)
#df_join_socios_2025_01_pd.to_excel(os.path.join(output_dir, "join_socios_2025_01.xlsx"), index=False)

print(f"Arquivos Excel salvos em {output_dir}")


