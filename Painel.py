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
    icon="⚙️",
    use_container_width=True,
)

st.markdown("---")

st.subheader("Ferramentas Utilizadas")
st.info("Python | Pandas |....")

st.subheader("Competências Desenvolvidas")
st.markdown(
    """
    - ...
    """
)
