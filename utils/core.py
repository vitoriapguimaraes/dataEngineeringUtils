import pandas as pd
import sqlite3

from urllib.request import urlopen, Request
from bs4 import BeautifulSoup

import re


def clean_data(df):
    """Realiza a limpeza e padronização dos dados."""
    if df is None:
        return None

    # Padronizar nomes de colunas
    df.columns = df.columns.str.lower()

    # Remover duplicatas
    df.drop_duplicates(inplace=True)

    # Variáveis categóricas
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        df[col] = df[col].astype(str).str.strip().str.lower()

    # Variáveis numéricas
    num_cols = df.select_dtypes(include=["int64", "float64"]).columns
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if df[col].isnull().any():
            df.dropna(subset=[col], inplace=True)

    # Garantir tipos de dados e datas
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")
    df["customer_id"] = df["customer_id"].astype(str)
    df["order_id"] = df["order_id"].astype(str)

    df.dropna(subset=["order_date", "ship_date"], inplace=True)

    return df


def extract_multinational_data(wiki_url):
    """Extrai dados de supermercados multinacionais da Wikipedia."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        req = Request(wiki_url, headers=headers)
        with urlopen(req) as wiki_page:
            soup = BeautifulSoup(wiki_page, "html.parser")

        tabela = soup.find("table", class_="wikitable")
        if not tabela:
            return None, "Tabela não encontrada"

        dados = []
        linhas = tabela.find_all("tr")
        cabecalho = [th.text.strip() for th in linhas[0].find_all("th")]

        for linha in linhas[1:]:
            colunas = linha.find_all(["td", "th"])
            if len(colunas) > 0:
                linha_dados = [coluna.text.strip() for coluna in colunas]
                dados.append(linha_dados)

        df = pd.DataFrame(dados, columns=cabecalho)

        # Limpezas específicas
        df.columns = df.columns.str.lower()
        df = df.drop(columns=["map"], errors="ignore")

        rename_map = {
            "served countries (besides the headquarters)": "countries",
            "number of locations": "locations",
            "number of employees": "employees",
        }
        df = df.rename(columns=rename_map)

        def extract_number(value):
            if pd.isna(value) or str(value).strip() == "":
                return None
            numbers = re.sub(r"[^\d]", "", str(value))
            return int(numbers) if numbers else None

        df["locations"] = df["locations"].apply(extract_number)
        df["employees"] = df["employees"].apply(extract_number)

        return df, "Sucesso"

    except Exception as e:
        return None, str(e)


def create_star_schema(df):
    """Cria as tabelas de dimensão e fato."""
    if df is None:
        return {}

    # Dimensão Tempo
    dim_tempo = df[["order_date"]].drop_duplicates().copy()
    dim_tempo["date_id"] = dim_tempo["order_date"].dt.strftime("%Y%m%d").astype(int)
    dim_tempo["year"] = dim_tempo["order_date"].dt.year
    dim_tempo["weeknum"] = dim_tempo["order_date"].dt.isocalendar().week.astype(int)
    dim_tempo = dim_tempo[["date_id", "order_date", "year", "weeknum"]]

    # Dimensão Localização
    dim_localizacao = (
        df[["region", "country", "state", "city", "market", "market2"]]
        .drop_duplicates()
        .copy()
    )
    dim_localizacao["location_id"] = range(1, len(dim_localizacao) + 1)

    # Dimensão Envio
    dim_envio = (
        df[["order_id", "ship_date", "ship_mode", "shipping_cost"]]
        .drop_duplicates()
        .copy()
    )
    dim_envio["order_id"] = dim_envio["order_id"].astype(str)

    # Dimensão Cliente
    dim_cliente = (
        df[["customer_id", "customer_name", "segment"]].drop_duplicates().copy()
    )
    dim_cliente["customer_id"] = dim_cliente["customer_id"].astype(str)

    # Dimensão Produto
    dim_produto = (
        df[["product_id", "product_name", "category", "sub_category"]]
        .drop_duplicates()
        .copy()
    )
    dim_produto["product_id"] = dim_produto["product_id"].astype(str)

    # Tabela Fato
    fato = df.copy()
    fato["date_id"] = fato["order_date"].dt.strftime("%Y%m%d").astype(int)

    # Merge para pegar Location ID (necessário garantir tipos str)
    join_cols = ["region", "country", "state", "city", "market", "market2"]
    for col in join_cols:
        fato[col] = fato[col].astype(str)
        dim_localizacao[col] = dim_localizacao[col].astype(str)

    fato = fato.merge(dim_localizacao, on=join_cols, how="left")

    fato = fato[
        [
            "row_id",
            "order_id",
            "customer_id",
            "product_id",
            "date_id",
            "location_id",
            "order_priority",
            "sales",
            "profit",
            "quantity",
            "discount",
        ]
    ]

    return {
        "dim_tempo": dim_tempo,
        "dim_localizacao": dim_localizacao,
        "dim_envio": dim_envio,
        "dim_cliente": dim_cliente,
        "dim_produto": dim_produto,
        "fato_vendas": fato,
    }


def run_food_production_etl(df, db_path):
    """Executa o pipeline de dados de produção de alimentos."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Schema
        cursor.execute("DROP TABLE IF EXISTS producao")
        cursor.execute(
            """CREATE TABLE producao (
                        produto TEXT,
                        quantidade INTEGER,
                        preco_medio REAL,
                        receita_total INTEGER,
                        margem_lucro REAL
                    )"""
        )

        processed_count = 0
        rows_dropped = 0

        c_prod = "produto"
        c_qty = "quantidade_produzida_kgs"
        c_price = "valor_venda_medio"
        c_rev = "receita_total"

        for index, row in df.iterrows():
            try:
                qtd = int(row[c_qty])
            except ValueError:
                continue

            if qtd > 10:
                receita_raw = str(row[c_rev])
                try:
                    receita_clean = int(round(float(receita_raw.replace(".", "")), 0))
                except (ValueError, AttributeError):
                    receita_clean = 0

                try:
                    preco = float(row[c_price])
                    margem = round((receita_clean / qtd) - preco, 2)
                except (ValueError, ZeroDivisionError, TypeError):
                    margem = 0.0

                cursor.execute(
                    "INSERT INTO producao (produto, quantidade, preco_medio, receita_total, margem_lucro) VALUES (?, ?, ?, ?, ?)",
                    (str(row[c_prod]), qtd, preco, receita_clean, margem),
                )
                processed_count += 1
            else:
                rows_dropped += 1

        conn.commit()
        return processed_count, rows_dropped

    finally:
        conn.close()
