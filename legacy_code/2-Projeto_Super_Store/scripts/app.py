
import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
from google.cloud import bigquery
from pandas_gbq import to_gbq
import re
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_data(file_path):
    """Carrega os dados do arquivo CSV."""
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Dados carregados com sucesso de {file_path}. Total de {len(df)} registros.")
        return df
    except FileNotFoundError:
        logging.error(f"Erro: O arquivo {file_path} não foi encontrado.")
        return None
    except Exception as e:
        logging.error(f"Erro ao carregar dados de {file_path}: {e}")
        return None

def clean_data(df):
    """Realiza a limpeza e padronização dos dados."""
    if df is None: return None

    logging.info("Iniciando limpeza e padronização dos dados...")

    # Padronizar nomes de colunas
    df.columns = df.columns.str.lower()

    # Tratar nulos e duplicatas (conforme análise original, não há nulos ou duplicatas significativas)
    # No entanto, é bom manter a verificação para futuras execuções
    null_values = df.isnull().sum()
    if null_values.sum() > 0:
        logging.warning(f"Valores nulos encontrados: {null_values[null_values > 0].to_dict()}")
    
    duplicated_rows = df.duplicated().sum()
    if duplicated_rows > 0:
        logging.warning(f"Registros duplicados encontrados: {duplicated_rows}")
        df.drop_duplicates(inplace=True)
        logging.info(f"{duplicated_rows} registros duplicados removidos.")

    # Variáveis categóricas
    cat_cols = df.select_dtypes(include='object').columns
    for col in cat_cols:
        df[col] = df[col].astype(str).str.strip().str.lower() # Convert to string before strip/lower
        unique_categories = df[col].unique()
        if len(unique_categories) < 20:
            logging.debug(f"Categorias únicas para {col}: {unique_categories}")

    # Variáveis numéricas
    num_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in num_cols:
        # Coerção para numérico, tratando erros
        df[col] = pd.to_numeric(df[col], errors='coerce')
        if df[col].isnull().any():
            logging.warning(f"Valores não numéricos em '{col}' foram convertidos para NaN.")
            df.dropna(subset=[col], inplace=True) # Remover linhas com NaN resultantes da coerção

    # Garantir tipos de dados corretos
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')
    df['customer_id'] = df['customer_id'].astype(str)
    df['order_id'] = df['order_id'].astype(str)

    df.dropna(subset=['order_date', 'ship_date'], inplace=True) # Remover linhas onde a conversão de data falhou

    logging.info("Limpeza e padronização dos dados concluídas.")
    return df

def extract_multinational_data(wiki_url):
    """Extrai dados de supermercados multinacionais da Wikipedia."""
    logging.info(f"Extraindo dados da Wikipedia de: {wiki_url}")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        req = Request(wiki_url, headers=headers)
        with urlopen(req) as wiki_page:
            soup = BeautifulSoup(wiki_page, 'html.parser')

        tabela = soup.find('table', class_='wikitable')
        if not tabela:
            logging.error("Tabela 'wikitable' não encontrada na página da Wikipedia.")
            return None

        dados = []
        linhas = tabela.find_all('tr')

        cabecalho = [th.text.strip() for th in linhas[0].find_all('th')]

        for linha in linhas[1:]:
            colunas = linha.find_all(['td', 'th'])
            if len(colunas) > 0:
                linha_dados = [coluna.text.strip() for coluna in colunas]
                dados.append(linha_dados)

        df_multinacional = pd.DataFrame(dados, columns=cabecalho)
        df_multinacional.columns = df_multinacional.columns.str.lower()
        df_multinacional = df_multinacional.drop(columns=['map'], errors='ignore')

        multinacional_rename_columns = {
            "served countries (besides the headquarters)" : "countries",
            "number of locations": "locations",
            "number of employees": "employees"
        }
        df_multinacional = df_multinacional.rename(columns=multinacional_rename_columns)

        def extract_number(value):
            if pd.isna(value) or str(value).strip() == '':
                return None
            numbers = re.sub(r'[^d]', '', str(value))
            if numbers:
                try:
                    return int(numbers)
                except ValueError:
                    return None
            return None

        df_multinacional['locations'] = df_multinacional['locations'].apply(extract_number)
        df_multinacional['employees'] = df_multinacional['employees'].apply(extract_number)
        df_multinacional['countries_count'] = df_multinacional['countries'].apply(
            lambda x: len([country.strip() for country in str(x).split(",") if country.strip() and country.strip() != "nan"]) if pd.notna(x) and str(x).strip() != "" else 1
        )
        logging.info("Extração de dados da Wikipedia concluída.")
        return df_multinacional
    except Exception as e:
        logging.error(f"Erro ao extrair dados da Wikipedia: {e}")
        return None

def create_dimension_tables(df):
    """Cria as tabelas de dimensão a partir do DataFrame principal."""
    if df is None: return None, None, None, None, None

    logging.info("Criando tabelas de dimensão...")

    dim_tempo = df[['order_date']].drop_duplicates().copy()
    dim_tempo['date_id'] = dim_tempo['order_date'].dt.strftime('%Y%m%d').astype(int)
    dim_tempo['year'] = dim_tempo['order_date'].dt.year
    dim_tempo['weeknum'] = dim_tempo['order_date'].dt.isocalendar().week.astype(int) # Ensure int type
    dim_tempo = dim_tempo[['date_id', 'order_date', 'year', 'weeknum']]

    dim_localizacao = df[['region', 'country', 'state', 'city', 'market', 'market2']].drop_duplicates().copy()
    dim_localizacao['location_id'] = range(1, len(dim_localizacao) + 1)
    dim_localizacao = dim_localizacao[['location_id', 'region', 'country', 'state', 'city', 'market', 'market2']]

    dim_envio = df[['order_id', 'ship_date', 'ship_mode', 'shipping_cost']].drop_duplicates().copy()
    # Ensure order_id is string for merge consistency
    dim_envio['order_id'] = dim_envio['order_id'].astype(str)

    dim_cliente = df[['customer_id', 'customer_name', 'segment']].drop_duplicates().copy()
    dim_cliente['customer_id'] = dim_cliente['customer_id'].astype(str)

    dim_produto = df[['product_id', 'product_name', 'category', 'sub_category']].drop_duplicates().copy()
    dim_produto['product_id'] = dim_produto['product_id'].astype(str)

    logging.info("Tabelas de dimensão criadas.")
    return dim_tempo, dim_localizacao, dim_envio, dim_cliente, dim_produto

def create_fact_table(df, dim_localizacao):
    """Cria a tabela de fatos de vendas."""
    if df is None or dim_localizacao is None: return None

    logging.info("Criando tabela de fatos...")

    fato_vendas = df[['row_id', 'order_id', 'customer_id', 'product_id',
                       'order_date', 'region', 'country', 'state', 'city', 'market', 'market2',
                       'sales', 'profit', 'quantity', 'discount', 'order_priority']].copy()

    fato_vendas['date_id'] = fato_vendas['order_date'].dt.strftime('%Y%m%d').astype(int)

    # Ensure join columns are of consistent types
    fato_vendas['region'] = fato_vendas['region'].astype(str)
    fato_vendas['country'] = fato_vendas['country'].astype(str)
    fato_vendas['state'] = fato_vendas['state'].astype(str)
    fato_vendas['city'] = fato_vendas['city'].astype(str)
    fato_vendas['market'] = fato_vendas['market'].astype(str)
    fato_vendas['market2'] = fato_vendas['market2'].astype(str)

    dim_localizacao_join = dim_localizacao[['location_id', 'region', 'country', 'state', 'city', 'market', 'market2']].copy()
    dim_localizacao_join['region'] = dim_localizacao_join['region'].astype(str)
    dim_localizacao_join['country'] = dim_localizacao_join['country'].astype(str)
    dim_localizacao_join['state'] = dim_localizacao_join['state'].astype(str)
    dim_localizacao_join['city'] = dim_localizacao_join['city'].astype(str)
    dim_localizacao_join['market'] = dim_localizacao_join['market'].astype(str)
    dim_localizacao_join['market2'] = dim_localizacao_join['market2'].astype(str)

    fato_vendas = fato_vendas.merge(
        dim_localizacao_join,
        on=['region', 'country', 'state', 'city', 'market', 'market2'],
        how='left'
    )

    fato_vendas = fato_vendas[[
        'row_id', 'order_id', 'customer_id', 'product_id',
        'date_id', 'location_id', 'order_priority',
        'sales', 'profit', 'quantity', 'discount'
    ]]
    logging.info("Tabela de fatos criada.")
    return fato_vendas

def upload_to_bigquery(dataframe, table_name, project_id, dataset_name, if_exists='replace'):
    """Faz o upload de um DataFrame para o BigQuery."""
    if dataframe is None or dataframe.empty: 
        logging.warning(f"DataFrame para {table_name} está vazio ou é None. Pulando upload.")
        return

    full_table_name = f"{dataset_name}.{table_name}"
    try:
        logging.info(f"Iniciando upload para {full_table_name}...")
        dataframe.to_gbq(
            destination_table=full_table_name,
            project_id=project_id,
            if_exists=if_exists,
            progress_bar=False
        )
        logging.info(f"✓ Upload concluído: {full_table_name} ({len(dataframe)} registros)")
    except Exception as e:
        logging.error(f"✗ Erro ao fazer upload de {full_table_name}: {e}")
        raise

def main():
    load_dotenv()

    # Configurações
    csv_file_path = os.getenv("CSV_FILE_PATH", 'data\superstore.csv')
    wiki_url = os.getenv("WIKI_URL", "https://en.wikipedia.org/wiki/List_of_supermarket_chains")
    project_id = os.getenv("PROJECT_ID")
    dataset_name = os.getenv("DATASET_NAME", "superstore")
    dataset_wiki_name = os.getenv("DATASET_WIKI_NAME", "dataset_wiki")

    if not project_id:
        logging.error("PROJECT_ID não está configurado no .env ou como variável de ambiente.")
        return

    # Autenticação BigQuery (garantir que GOOGLE_APPLICATION_CREDENTIALS esteja configurado)
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        logging.error("GOOGLE_APPLICATION_CREDENTIALS não está configurada no .env ou como variável de ambiente.")
        return
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

    # 1. Extração e Limpeza
    df_superstore = load_data(csv_file_path)
    df_superstore_cleaned = clean_data(df_superstore)
    if df_superstore_cleaned is None: return

    df_multinacional = extract_multinational_data(wiki_url)
    if df_multinacional is None: return

    # 2. Criação de Tabelas de Dimensão e Fato
    dim_tempo, dim_localizacao, dim_envio, dim_cliente, dim_produto = create_dimension_tables(df_superstore_cleaned)
    if any(d is None for d in [dim_tempo, dim_localizacao, dim_envio, dim_cliente, dim_produto]): return

    fato_vendas = create_fact_table(df_superstore_cleaned, dim_localizacao)
    if fato_vendas is None: return

    # Adicionar company_id à dim_company
    dim_company = df_multinacional.copy()
    dim_company['company_id'] = range(1, len(dim_company) + 1)
    dim_company = dim_company[[
        'company_id', 'company', 'headquarters', 'countries', 'countries_count', 'locations', 'employees'
    ]]

    # 3. Carga no BigQuery
    logging.info("Iniciando carga de dados para o BigQuery...")
    try:
        upload_to_bigquery(fato_vendas, "fato_vendas", project_id, dataset_name)
        upload_to_bigquery(dim_tempo, "dim_tempo", project_id, dataset_name)
        upload_to_bigquery(dim_localizacao, "dim_localizacao", project_id, dataset_name)
        upload_to_bigquery(dim_envio, "dim_envio", project_id, dataset_name)
        upload_to_bigquery(dim_cliente, "dim_cliente", project_id, dataset_name)
        upload_to_bigquery(dim_produto, "dim_produto", project_id, dataset_name)
        upload_to_bigquery(dim_company, "dim_company", project_id, dataset_wiki_name)
        logging.info("Todos os uploads para o BigQuery foram concluídos com sucesso!")
    except Exception as e:
        logging.error(f"Um erro ocorreu durante o upload para o BigQuery: {e}")

if __name__ == "__main__":
    main()

