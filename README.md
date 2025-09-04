```markdown
# Análise de Indicadores de Saúde Materno-Infantil na Baixada Santista com Databricks

## Visão Geral do Projeto

Este projeto demonstra a construção de um pipeline de dados a partir da camada silver na plataforma Databricks para analisar indicadores de saúde materno-infantil. Utilizando dados públicos do
**DATASUS SINASC (Sistema de Informações sobre Nascidos Vivos)**, o pipeline transforma dados brutos em um _data mart_ analítico e confiável, pronto para gerar insights estratégicos sobre a saúde na região da Baixada Santista, SP.

O fluxo de trabalho segue as melhores práticas da **Arquitetura Medalhão (Bronze, Silver, Gold)**, garantindo governança, qualidade e rastreabilidade dos dados em todas as etapas do processo.

## Objetivos

- **Engenharia de Dados:** Implementar um pipeline ELT (Extração, Carga e Transformação) robusto e reproduzível utilizando PySpark e Spark SQL.
- **Qualidade de Dados:** Integrar testes de qualidade automatizados na camada Silver para garantir a integridade e plausibilidade dos dados.
- **Modelagem Analítica:** Construir uma camada Gold (`mart_nascidos_vivos`) com métricas e dimensões de negócio para otimizar análises e relatórios.
- **Análise e Storytelling:** Extrair insights a partir dos dados tratados para responder a perguntas estratégicas, como:
  - Qual a evolução anual dos nascimentos com baixo peso na região?
  - Existe variabilidade significativa nos indicadores entre os diferentes municípios?
  - Qual a relação entre a qualidade do pré-natal e o tipo de parto, e essa relação é estatisticamente significativa?

## Tecnologias Utilizadas

- **Plataforma de Dados:** Databricks Free Edition
- **Linguagens:** PySpark e Spark SQL
- **Bibliotecas de Análise:** Pandas, Plotly, Seaborn, Matplotlib, SciPy
- **Versionamento:** Git e GitHub

## Arquitetura de Dados: Medalhão

O pipeline foi estruturado em três camadas lógicas para garantir um fluxo de dados progressivamente mais limpo e agregado.

![delta lake] (/imagens/delta%20lake.png)


### 🥉 Camada Bronze

**Objetivo:** Ingerir os dados brutos do SINASC em seu formato original, servindo como a fonte única da verdade.

- **Fonte:** Arquivo Parquet (`sinasc_baixada.parquet`)
- **Ações:**
  - Leitura do arquivo Parquet.
  - Padronização técnica dos nomes das colunas (para `snake_case`).
  - Gravação da tabela `bronze.sinasc_baixada` em formato Delta Lake.

### 🥈 Camada Silver

**Objetivo:** Limpar, validar, enriquecer e focar os dados no escopo da análise.

- **Ações:**
  - **Filtragem Geográfica:** Seleção de registros apenas dos 9 municípios da Baixada Santista.
  - **Seleção de Colunas:** Manutenção apenas das variáveis de interesse para a análise.
  - **Tratamento de Nulos:** Imputação de valores faltantes utilizando a mediana para colunas numéricas (`IDADEMAE`, `PESO`) e a moda para categóricas.
  - **Testes de Qualidade:** Validações automatizadas para garantir a integridade dos dados (ex: peso plausível, consultas não negativas). O pipeline é interrompido se um teste falha.
  - **Padronização Final:** Conversão de todos os nomes de colunas para o padrão `snake_case`.
- **Tabela Resultante:** `silver.sinasc_baixada_santista_tratada`

### 🥇 Camada Gold

**Objetivo:** Criar um _data mart_ agregado e otimizado para consumo final por analistas e ferramentas de BI.

- **Ações:**
  - Criação de dimensões analíticas (ex: `idade_mae_faixa_etaria`, `nascido_no_municipio_residencia`).
  - Agregação dos dados por múltiplas dimensões (`ano`, `município`, `faixa_etaria`, etc.).
  - Cálculo de métricas de negócio aditivas (ex: `total_nascidos_vivos`, `total_nascidos_baixo_peso`, `total_maes_adolescentes`).
- **Tabela Resultante:** `gold.mart_nascidos_vivos`

## Análise e Principais Insights

A camada Gold permitiu a criação de visualizações detalhadas e a validação de hipóteses, gerando os seguintes insights para a Baixada Santista:

1.  **Variabilidade entre Municípios:** Embora a taxa regional de baixo peso ao nascer seja relativamente estável, existe uma variação significativa entre as cidades. Municípios como [Ex: Peruíbe] apresentam taxas consistentemente mais altas, indicando a necessidade de investigações e ações de saúde focadas.

2.  **Relação entre Pré-Natal e Tipo de Parto:** A análise demonstrou uma forte associação estatística (validada por um Teste Qui-Quadrado com p-valor < 0.05) entre a qualidade do pré-natal e o tipo de parto. Gestantes com acompanhamento adequado (7+ consultas) tendem a ter uma proporção menor de partos cesáreos em comparação com aquelas com acompanhamento inadequado.

![Cobertura de Pré-Natal por Tipo de Parto](image_d3f1c6.png)

## Estrutura do Repositório
```

/
├── README.md
└── notebooks/
├── 00-configuracoes/
│ └── 00_setup_ambiente.sql
├── 01-bronze/
│ └── 01_ingestao_sinasc.py
├── 02-silver/
│ └── 02_tratamento_sinasc.py
├── 03-gold/
│ └── 03_gold_mart_nascidos_vivos.sql
└── 04-analises/
└── 04_analise_exploratoria_sinasc.py

```

## Como Reproduzir

1.  **Configuração do Ambiente:** Execute o notebook `00-configuracoes/00_setup_ambiente.sql` para criar os schemas `bronze`, `silver` e `gold`.
2.  **Upload dos Dados:** Faça o upload do arquivo `sinasc_baixada.parquet` para o Volume no Databricks no caminho `/Volumes/datasus_eng_analytics/bronze/raw/`.
3.  **Execução do Pipeline:** Execute os notebooks na ordem numérica:
    - `01_ingestao_sinasc.py` para popular a camada Bronze.
    - `02_tratamento_sinasc.py` para criar a tabela limpa na camada Silver.
    - `03_gold_mart_nascidos_vivos.sql` para criar o data mart analítico.
4.  **Análise dos Resultados:** Execute o notebook `04_analise_exploratoria_sinasc.py` para gerar as visualizações e validar as hipóteses.

## Referências
- Documentação e notas técnicas do DATASUS e da Secretaria de Estado da Saúde de São Paulo.
```
