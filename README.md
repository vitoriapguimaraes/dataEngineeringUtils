# Engenharia de Dados e Utilitários

> **A espinha dorsal dos dados.**  
> Um portfólio prático de Engenharia de Dados, focado na construção de pipelines robustos, modelagem dimensional e arquitetura em nuvem.

![Demonstração do Sistema](https://github.com/vitoriapguimaraes/dataEngineeringUtils/blob/main/demo/navigation.gif)

## Objetivo

Centralizar demonstrativos técnicos de **Engenharia de Dados**, provando competência na transformação de dados brutos e desestruturados em ativos analíticos confiáveis (Analytical Ready).
O foco aqui é a "cozinha" dos dados:

- **ETL & ELT Pipelines**: Extração, Carga e Transformação.
- **Data Quality**: Limpeza, deduplicação e _schema enforcement_.
- **Modelagem Dimensional**: Criação de Star Schemas para otimização de OLAP.
- **Modern Data Stack**: Integração com Cloud (Google BigQuery) e Web Scraping.

## Projetos e Funcionalidades

Navegue pelo menu lateral do aplicativo para explorar os seguintes módulos:

| Módulo                                   | Descrição e Funcionalidades                                                                               |
| :--------------------------------------- | :-------------------------------------------------------------------------------------------------------- |
| **⛓️ Estudos de Fluxo (Data Wrangling)** | Pipeline de limpeza automatizado com Python/Pandas e carga em SQLite.                                     |
| **🛒 Projeto Super Store (Flagship)**    | Arquitetura **End-to-End** no GCP. Ingestão de vendas (ERP) + Scraping de competidores (Web) -> BigQuery. |

## Tecnologias Utilizadas

- **Linguagem**: Python 3.10+
- **Orquestração & Dados**: Pandas, NumPy
- **Banco de Dados**: SQLite (Local), Google BigQuery (Cloud)
- **Ingestão Web**: BeautifulSoup4, Urllib
- **Frontend de Data Apps**: Streamlit

## Como Executar

Siga os passos abaixo para rodar a aplicação localmente e interagir com os pipelines:

1. **Acesse o diretório do projeto**

   ```bash
   git clone https://github.com/vitoriapguimaraes/dataEngineeringUtils.git
   cd dataEngineeringUtils
   ```

2. **Instale as dependências**
   Recomenda-se usar um ambiente virtual (`venv` ou `conda`).

   ```bash
   pip install -e .
   ```

   _Ou instale via requirements se disponível:_ `pip install -r requirements.txt`

3. **Execute o Dashboard**

   ```bash
   streamlit run Painel.py
   ```

4. **Acesse no navegador**
   O app abrirá automaticamente em: `http://localhost:8501`

## Estrutura de Diretórios

```dash
dataEngineeringUtils/
├── data/                # Datasets de exemplo (CSVs brutos)
├── pages/               # Páginas do Portfólio
│   ├── 1-Estudos_de_Fluxo.py       # Projeto 1: Wrangling
│   └── 2-Projeto_Super_Store.py    # Projeto 2: BigQuery & ETL
├── utils/               # Módulos reutilizáveis (Core Engine)
│   ├── core.py          # Lógica pesada de ETL e Modelagem
│   ├── load_file.py     # Ingestão de arquivos
│   └── ui.py            # Componentes visuais
├── Painel.py            # Home Page
└── README.md            # Documentação deste repositório
```

## Status

✅ Concluído

## Mais Sobre Mim

Acesse os arquivos disponíveis na [Pasta Documentos](https://github.com/vitoriapguimaraes/vitoriapguimaraes/tree/main/DOCUMENTOS) para mais informações sobre minhas qualificações e certificações.
