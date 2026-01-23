import streamlit as st

from utils.paths import DATA_DIR
from utils.ui import setup_sidebar, add_back_to_top
from utils.load_file import load_data
from utils.core import (
    clean_data,
    extract_multinational_data,
    create_star_schema,
)

st.set_page_config(page_title="Projeto Super Store", page_icon="🛒", layout="wide")

setup_sidebar()
add_back_to_top()

st.title("🛒 Projeto Super Store: Modern Data Stack na Prática")

CSV_PATH = str(DATA_DIR / "superstore.csv")
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_supermarket_chains"

tabs = st.tabs(["Relatório do Projeto", "Demo Interativa"])

with tabs[0]:
    # --- CABEÇALHO: O PROBLEMA ---
    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("O Desafio de Negócio")
        st.markdown(
            """
            A **Global Superstore** é uma gigante do varejo com operações em múltiplos continentes.
            No entanto, a equipe de analytics enfrentava problemas críticos:
            1.  **Silos de Dados**: Vendas em CSVs desconectados do ERP.
            2.  **Cegueira de Mercado**: Falta de dados sobre competidores locais para análise de share.
            3.  **Processos Manuais**: Relatórios levavam dias para serem consolidados.
            """
        )
        st.markdown(
            """
            **A Solução Proposta**:
            Uma arquitetura de dados escalável na nuvem (ELT), integrando dados internos e externos em um Data Warehouse centralizado.
            """
        )

    with c2:
        st.info(
            "**Impacto Gerado**: Redução de 90% no tempo de fechamento de relatórios e visão 360º competitiva."
        )

        st.success(
            """
            **Stack Tecnológico:**
            *   🐍 **Python** (Pandas, BeautifulSoup)
            *   🗄️ **SQL** (Modelagem)
            *   ☁️ **Google BigQuery** (Data Warehouse)
            """
        )

    # --- ARQUITETURA DO PIPELINE ---
    st.subheader("Arquitetura e Metodologia")

    with st.expander("🟦 Etapa 1: Extração e Enriquecimento", expanded=True):
        st.markdown(
            """
            *   **Dados Internos**: Ingestão de arquivos CSV exportados do ERP legado.
            *   **Dados Externos**: *Web Scraping* da lista de multinacionais da Wikipedia para gerar a dimensão `dim_company`.
            """
        )

    with st.expander("🟨 Etapa 2: Transformação (Data Quality)", expanded=True):
        st.markdown(
            """
            Processo de limpeza realizado em Python/Pandas:
            1.  **Standardization**: Conversão de colunas para *snake_case*.
            2.  **Cleaning**: Remoção de espaços em branco em variáveis categóricas.
            3.  **Typos**: Correção manual de categorias com base em frequência.
            4.  **Outliers**: Análise estatística de vendas e lucro (identificados 5k+ outliers).
            """
        )

    with st.expander("🟧 Etapa 3: Modelagem Dimensional (Star Schema)", expanded=True):
        st.markdown("O modelo final foi desenhado para otimizar consultas de BI.")
        st.markdown("**Tabela Fato**: `fato_vendas` (Granularidade: Item do Pedido).")
        st.markdown(
            """
            **Dimensões**:
            *   `dim_tempo`: Calendário fiscal.
            *   `dim_produto`: Hierarquia (Categoria > Sub > Produto).
            *   `dim_cliente`: Visão única do cliente (CRM).
            *   `dim_localizacao`: Geo-referência.
            *   `dim_company`: Dados de mercado (Externo).
            """
        )

    # --- DESTAQUES ---
    st.subheader("Destaques do Projeto")
    st.markdown(
        """
        🌟 **Avaliação Técnica:**
        > *"O projeto apresenta uma execução exemplar... A modelagem em Star Schema foi corretamente projetada e a lógica de atualização incremental demonstra um nível avançado de engenharia."*
    """
    )
    st.markdown(
        """
        **Pontos Fortes Identificados:**
        *   ✅ **Pipeline Híbrido**: Fusão eficaz de CSV local + Web Scraping.
        *   ✅ **Documentação**: Rastreabilidade completa das regras de negócio.
        *   ✅ **Preparado para Escala**: Lógica de *Upsert/Merge* já desenhada para produção.
        """
    )

    # --- ROADMAP ---
    st.subheader("Próximos Passos (Roadmap)")
    st.markdown(
        """
        1.  **Orquestração**: Migrar a execução para **Apache Airflow** (Cloud Composer).
        2.  **Governança**: Implementar *Data Contracts* e testes automatizados (Great Expectations).
        3.  **Observabilidade**: Adicionar alertas de falha via Slack/Email.
        4.  **CI/CD**: Esteira de deploy automatizada para scripts de ETL.
        """
    )

with tabs[1]:
    st.subheader("Demonstração Interativa do Pipeline")
    st.caption("Experimente o fluxo de dados real executado em memória.")

    # Sub-tabs para o fluxo técnico
    subtab_extract, subtab_transform, subtab_model = st.tabs(
        [
            "1. Ingestão (Extract)",
            "2. Tratamento (Transform)",
            "3. Modelagem (Star Schema)",
        ]
    )

    # State Init
    if "df_raw" not in st.session_state:
        st.session_state.df_raw = None
    if "df_clean" not in st.session_state:
        st.session_state.df_clean = None
    if "schema" not in st.session_state:
        st.session_state.schema = None
    if "df_wiki" not in st.session_state:
        st.session_state.df_wiki = None

    # --- 2.1 EXTRACT ---
    with subtab_extract:

        st.subheader("Fonte A: Vendas Internas (CSV)")
        st.caption("Simulação da extração do ERP (40k+ linhas).")
        df_raw, msg = load_data(CSV_PATH)
        if df_raw is not None:
            st.session_state.df_raw = df_raw
            st.dataframe(df_raw.head(), use_container_width=True)
            st.success(f"✅ Extraído com sucesso: {len(df_raw):,} registros.")
        else:
            st.error(msg)

        st.subheader("Fonte B: Padrões de Mercado (Web)")
        st.caption(f"Scraping em tempo real de: {WIKI_URL}")
        if st.button("🔄 Executar Scraping"):
            with st.spinner("Acessando Wikipedia..."):
                df_wiki, msg = extract_multinational_data(WIKI_URL)
                if df_wiki is not None:
                    st.session_state.df_wiki = df_wiki
                    st.dataframe(df_wiki.head(), use_container_width=True)
                    st.success(
                        f"✅ Enriquecimento: {len(df_wiki)} multinacionais identificadas."
                    )
                else:
                    st.error(msg)
        elif st.session_state.df_wiki is not None:
            st.dataframe(st.session_state.df_wiki.head(), use_container_width=True)
            st.success("Dados carregados da memória.")

    # --- 2.2 TRANSFORM ---
    with subtab_transform:
        st.subheader("Engine de Tratamento de Dados")
        if st.session_state.df_raw is not None:
            st.markdown("**Regras de Qualidade Aplicadas:**")
            st.code(
                """
1. Sanitização de Nomes de Coluna (Standardization)
2. Deduplicação de Registros (Idempotência)
3. Tipagem Forte (Schema Enforcement)
4. Tratamento de Nulos (Data Quality)""",
                language="markdown",
            )

            if st.button("▶️ Rodar Pipeline de Limpeza"):
                df_clean = clean_data(st.session_state.df_raw.copy())
                st.session_state.df_clean = df_clean
                st.success(f"Dados processados! Linhas válidas: {len(df_clean)}")

            if st.session_state.df_clean is not None:
                st.dataframe(st.session_state.df_clean.head(), use_container_width=True)
        else:
            st.warning("Aguardando Ingestão dos dados na etapa anterior.")

    # --- 2.3 MODEL ---
    with subtab_model:
        st.subheader("Modelagem Dimensional (Analytics Ready)")

        col = st.columns(2)

        col[0].markdown(
            "Transformação para **Star Schema** para otimização de performance no Power BI."
        )

        col[1].markdown(
            """
            **Entidades Geradas:**
            -   🔵 **Fato Vendas**: Transacional
            -   🟡 **Dim Tempo**: Calendário
            -   🟡 **Dim Produto**: Catálogo
            -   🟡 **Dim Cliente**: CRM
            -   🟡 **Dim Local**: Geo
            """
        )

        if st.session_state.df_clean is not None:
            if col[0].button("🔨 Construir Modelo Dimensional"):
                schema = create_star_schema(st.session_state.df_clean)
                st.session_state.schema = schema
                col[0].success("Tabelas geradas em memória!")
        else:
            col[0].warning(
                "⚠️ Por favor, execute as etapas 1 (Ingestão) e 2 (Tratamento) antes de prosseguir."
            )

        if st.session_state.schema:
            table_list = list(st.session_state.schema.keys())

            # Prioridade de Seleção: fato_vendas
            default_index = 0
            if "fato_vendas" in table_list:
                default_index = table_list.index("fato_vendas")

            sel_table = st.selectbox(
                "Inspecionar Tabela Resultante (Gold Layer):",
                table_list,
                index=default_index,
            )
            st.dataframe(
                st.session_state.schema[sel_table].head(100),
                use_container_width=True,
            )
        elif st.session_state.df_clean is not None:
            col[0].info("Execute a modelagem para visualizar os dados.")
