import streamlit as st
import pandas as pd
import sqlite3

from utils.ui import setup_sidebar, add_back_to_top
from utils.load_file import load_data
from utils.core import run_food_production_etl
from utils.paths import DATA_DIR

st.set_page_config(page_title="Estudos de Fluxo", page_icon="⚙️", layout="wide")

setup_sidebar()
add_back_to_top()

st.title("Estudos de Fluxo")

# --- CONFIGURAÇÃO ---
DB_FILE = str(DATA_DIR / "estudos_de_fluxos.db")
CSV_FILE = str(DATA_DIR / "producao_alimentos.csv")

# --- INTERFACE ---

tabs = st.tabs(["Cenário e Dados", "Pipeline e Resultados"])

with tabs[0]:
    st.subheader("O Cenário")
    st.markdown(
        """
        Você é um Engenheiro de Dados em uma indústria de alimentos. O sistema legado de vendas exporta relatórios diários em CSV, mas com dois problemas crônicos:
        1.  **Lixo nos Dados**: Registros com quantidades insignificantes (<= 10 kg) que não deveriam estar lá.
        2.  **Formatação Errada**: O campo de receita vem com pontos (`.`) separando milhares, o que quebra a conversão numérica em alguns sistemas.
        """
    )
    st.markdown(
        "**Objetivo**: Construir um pipeline que limpe esses dados automaticamente e calcule a margem de lucro real."
    )

    st.subheader("Análise da Qualidade dos Dados (Raw)")

    # Carregamento Fixo
    df_raw, msg = load_data(CSV_FILE)

    if df_raw is not None:
        st.caption(
            "Abaixo, visualizamos os dados originais. As cores indicam onde o pipeline atuará:"
        )
        st.caption("🟥 **Fundo Vermelho**: Linhas que serão removidas (Qtd <= 10 kg).")
        st.caption(
            "🟨 **Texto Laranja**: Valores de receita que precisam de sanitização (remover pontos)."
        )

        # Colunas Fixas do Dataset
        col_qty = "quantidade_produzida_kgs"
        col_rev = "receita_total"

        def highlight_showcase(row):
            styles = [""] * len(row)

            # Regra 1: Quantidade <= 10
            try:
                if float(row[col_qty]) <= 10:
                    return ["background-color: #ffcdd2"] * len(row)
            except Exception:
                pass

            # Regra 2: Receita com Ponto
            try:
                val_rev = str(row[col_rev])
                if "." in val_rev:
                    idx = row.index.get_loc(col_rev)
                    styles[idx] = (
                        "color: #e65100; font-weight: bold; background-color: #fff3e0"
                    )
            except Exception:
                pass

            return styles

        st.dataframe(
            df_raw.style.apply(highlight_showcase, axis=1),
            use_container_width=True,
            height=400,
        )
        st.markdown(f"**Total de Registros Brutos**: {len(df_raw)}")

    else:
        st.error(f"Erro ao carregar dataset de demonstração: {msg}")


with tabs[1]:
    st.subheader("Execução do Pipeline")

    st.markdown(
        """
        O pipeline aplica as seguintes transformações:
        1.  **Filtro**: Ignora linhas com `quantidade <= 10`.
        2.  **Sanatização**: Remove pontos da coluna `receita_total` e converte para Inteiro.
        3.  **Enriquecimento**: Calcula `Margem de Lucro = (Receita / Qtd) - Preço Médio`.
        4.  **Carga**: Salva o resultado limpo no banco SQLite.
        """
    )

    if st.button("Rodar Pipeline de Limpeza", type="primary"):
        if df_raw is None:
            st.stop()

        with st.status("Processando dados...", expanded=True) as status:
            try:
                # Execução do Pipeline via função Core
                st.write("🔌 Conectando e Processando...")

                processed_count, rows_dropped = run_food_production_etl(df_raw, DB_FILE)

                status.update(
                    label="✅ Pipeline Concluído!", state="complete", expanded=True
                )

                # Métricas de Sucesso
                c1, c2, c3 = st.columns(3)
                c1.metric("Registros Processados", processed_count)
                c2.metric("Registros Removidos (Lixo)", rows_dropped)
                c3.metric("Qualidade Final", "100%")

                st.success("Dados limpos armazenados com sucesso no SQLite.")

                # 4. Resultado
                st.markdown("#### Dados Finais)")
                conn = sqlite3.connect(DB_FILE)
                df_result = pd.read_sql("SELECT * FROM producao", conn)
                st.dataframe(df_result, use_container_width=True)
                conn.close()

            except Exception as e:
                st.error(f"Erro na execução: {e}")
                status.update(label="❌ Falha no Pipeline", state="error")
