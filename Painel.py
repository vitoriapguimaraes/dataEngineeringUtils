import streamlit as st


# Configuração da Página
st.set_page_config(
    page_title="Data Engineering Utilities",
    page_icon="⚙️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estilização CSS Customizada
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #424242;
        margin-bottom: 1rem;
    }
    .card {
        background-color: #f9f9f9;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
</style>
""",
    unsafe_allow_html=True,
)

# Cabeçalho Principal
st.markdown(
    '<div class="main-header">⚙️ Data Engineering & Utilities Portfolio</div>',
    unsafe_allow_html=True,
)
st.markdown("---")

# Introdução
st.markdown(
    """
### 🚀 Bem-vindo ao meu laboratório de Engenharia de Dados!

Este portfólio demonstra minha evolução e competências em **Engenharia de Dados**, focando em:
- **Arquitetura de Dados**: Pipelines robustos e escaláveis.
- **ETL/ELT**: Processos de Extração, Transformação e Carga.
- **Data Quality**: Garantia de integridade e consistência.
- **Cloud Computing**: Integração com Google BigQuery e soluções em nuvem.

Navegue pelas páginas no menu lateral para explorar os projetos interativos.
"""
)

st.markdown("---")

# Resumo dos Projetos
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
    <div class="card">
        <h3>🧪 Pipeline Lab</h3>
        <p><strong>Foco:</strong> Fundamentos & Lógica</p>
        <p>Um ambiente interativo para experimentar com pipelines de dados simples, limpeza e formatação usando SQLite.</p>
        <p><em>Tecnologias: Python, SQLite, CSV.</em></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col2:
    st.markdown(
        """
    <div class="card">
        <h3>⛓️ ETL Pipelines</h3>
        <p><strong>Foco:</strong> Scripts Robustos & Web Scraping</p>
        <p>Execução completa de ETL: Coleta de dados da Web (Wikipedia), tratamento avançado e modelagem Dimensional (Star Schema).</p>
        <p><em>Tecnologias: Pandas, BeautifulSoup, BigQuery.</em></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

with col3:
    st.markdown(
        """
    <div class="card">
        <h3>🛒 Super Store Case</h3>
        <p><strong>Foco:</strong> Arquitetura & Documentação</p>
        <p>Um estudo de caso completo de migração e estruturação de dados de varejo para nuvem, com documentação técnica detalhada.</p>
        <p><em>Tecnologias: Cloud Architecture, Data Governance.</em></p>
    </div>
    """,
        unsafe_allow_html=True,
    )

st.markdown("---")
st.markdown("Desenvolvido por Vitória Guimarães")
