import streamlit as st
import pandas as pd
import sqlite3

from utils.ui import setup_sidebar, add_back_to_top
from utils.load_file import load_data

st.set_page_config(page_title="Estudos de Fluxo", page_icon="⚙️", layout="wide")

setup_sidebar()
add_back_to_top()

st.title("Estudos de Fluxo")

st.markdown(
    """
Este laboratório demonstra um pipeline simples de **Extração, Transformação e Carga (ETL)**.
O objetivo é processar dados de produção de alimentos, aplicar regras de negócio e persistir em um banco SQL.
"""
)

# --- CONFIGURAÇÃO ---
DB_FILE = "Pipeline_Studies/db.db"
CSV_FILE = "Pipeline_Studies/producao_alimentos.csv"

# --- INTERFACE ---

col1, col2 = st.columns([1, 1])

with col1:
    st.subheader("1. 📂 Origem (Raw Data)")

    # Opção de Upload
    uploaded_file = st.file_uploader("Carregar CSV de Produção", type=["csv"])

    # Lógica de Carregamento
    if uploaded_file is not None:
        file_to_load = uploaded_file
        st.info("Usando arquivo enviado pelo usuário.")
    else:
        file_to_load = CSV_FILE
        st.info("Usando arquivo de exemplo padrão.")

    df_raw, msg = load_data(file_to_load)

    if df_raw is not None:
        st.dataframe(df_raw, height=250)
        st.info(f"Registros encontrados: {len(df_raw)}")
    else:
        st.error(msg)

with col2:
    st.subheader("2. 📜 Lógica do Pipeline")
    st.code(
        """
# Regras de Negócio:
1. Filtrar quantidade > 10.
2. Remover '.' do campo 'receita_total' (sanitize).
3. Calcular Margem de Lucro: (Receita / Qtd) - Preço Médio.
4. Salvar em SQLite.
    """,
        language="python",
    )

st.markdown("---")

# --- EXECUÇÃO ---
st.subheader("3. ⚙️ Execução do Pipeline")

if st.button("🚀 Executar Pipeline", type="primary"):
    with st.status("Executando pipeline...", expanded=True) as status:
        try:
            # 1. Conexão
            st.write("🔌 Conectando ao SQLite...")
            conn = sqlite3.connect(DB_FILE)
            cursor = conn.cursor()

            # 2. Schema
            st.write("🛠️ Recriando tabela 'producao'...")
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

            # 3. Processamento
            st.write("🔄 Processando registros...")
            processed_count = 0

            # Simular leitura linha a linha como no script original (mas usando o DF carregado)
            # Para fidelidade ao script original, faremos iteração
            for index, row in df_raw.iterrows():
                qtd = int(row["quantidade"])

                # Regra 1: Quantidade > 10
                if qtd > 10:
                    # Regra 2: Sanitize Receita
                    receita_raw = str(row["receita_total"])
                    receita_clean = int(round(float(receita_raw.replace(".", "")), 0))

                    # Regra 3: Margem
                    preco_medio = float(row["preco_medio"])
                    margem = round((receita_clean / qtd) - preco_medio, 2)

                    # Carga
                    cursor.execute(
                        "INSERT INTO producao (produto, quantidade, preco_medio, receita_total, margem_lucro) VALUES (?, ?, ?, ?, ?)",
                        (row["produto"], qtd, preco_medio, receita_clean, margem),
                    )
                    processed_count += 1

            conn.commit()
            conn.close()

            status.update(
                label="✅ Pipeline Concluído!", state="complete", expanded=False
            )
            st.success(
                f"Sucesso! {processed_count} registros processados e inseridos no banco."
            )

            # 4. Resultado (Ler do banco para provar)
            st.subheader("4. 🏁 Destino (SQL Data)")
            conn = sqlite3.connect(DB_FILE)
            df_result = pd.read_sql("SELECT * FROM producao", conn)
            st.dataframe(df_result, use_container_width=True)
            conn.close()

        except Exception as e:
            st.error(f"Erro na execução: {e}")
            status.update(label="❌ Falha no Pipeline", state="error")
