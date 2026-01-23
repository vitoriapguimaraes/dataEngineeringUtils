import streamlit as st

from utils.ui import setup_sidebar, add_back_to_top

st.set_page_config(
    page_title="Data Engineering Utilities", page_icon="⚙️", layout="wide"
)

add_back_to_top()
setup_sidebar()

st.title("Data Engineering & Utilities Portfolio")

st.info(
    "Acesse os estudos de arquitetura de dados, processos ETL e cloud computing, na lista abaixo ou na barra lateral"
)

st.page_link(
    "pages/1-Estudos_de_Fluxo.py",
    label="Estudos de Fluxo",
    icon="⛓️",
    use_container_width=True,
)

st.page_link(
    "pages/2-Projeto_Super_Store.py",
    label="Projeto Super Store",
    icon="🛒",
    use_container_width=True,
)

st.markdown("---")

st.subheader("Ferramentas & Tecnologias")
st.code(
    "Python 3.10+ | Google BigQuery | SQL | Pandas | SQLite | Streamlit | BeautifulSoup4"
)

st.subheader("Competências Demonstradas")
c1, c2 = st.columns(2)

with c1:
    st.markdown(
        """
        **Engenharia de Dados (Core)**
        -   Construção de Pipelines ETL e ELT Robustos.
        -   Modelagem Dimensional (Star Schema) para BI.
        -   Ingestão de Dados Híbrida (Arquivos Locais + Web).
        -   Arquitetura de Dados em Nuvem (GCP).
        """
    )

with c2:
    st.markdown(
        """
        **Qualidade & Governança**
        -   Data Wrangling e Limpeza de Dados Legados.
        -   Tratamento de Anomalias e Outliers.
        -   Padronização e *Schema Enforcement*.
        -   Automação de Fluxos de Dados.
        """
    )
