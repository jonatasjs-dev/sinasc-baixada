# Databricks notebook source
# MAGIC %md
# MAGIC ### 1: Preparação do Ambiente e Carga dos Dados

# COMMAND ----------

# Importando bibliotecas para análise e visualização
import pandas as pd
import plotly.express as px
from scipy.stats import chi2_contingency
import numpy as np

# Define um padrão visual para os gráficos
px.defaults.template = "plotly_dark"

# Carrega a tabela Gold para um DataFrame Spark
gold_df_spark = spark.table("datasus.gold.mart_nascidos_vivos")

# Converte para Pandas para facilitar a manipulação e plotagem
pdf = gold_df_spark.toPandas()

print("Dados da camada Gold carregados em um DataFrame pandas.")
pdf.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Análise - % de Baixo Peso e Muito Baixo Peso
# MAGIC Pergunta do Desafio: Qual o percentual anual de nascidos vivos com baixo peso (< 2.500g) e muito baixo peso (< 1.500g) nos municípios selecionados?

# COMMAND ----------

# Agrupando os dados por ano para obter os totais anuais
analise_peso_anual = pdf.groupby('ano_nascimento').agg(
    total_nascidos_vivos=('total_nascidos_vivos', 'sum'),
    total_nascidos_baixo_peso=('total_nascidos_baixo_peso', 'sum'),
    total_nascidos_muito_baixo_peso=('total_nascidos_muito_baixo_peso', 'sum')
).reset_index()

# Calculando os percentuais
analise_peso_anual['perc_baixo_peso'] = (analise_peso_anual['total_nascidos_baixo_peso'] / analise_peso_anual['total_nascidos_vivos']) * 100
analise_peso_anual['perc_muito_baixo_peso'] = (analise_peso_anual['total_nascidos_muito_baixo_peso'] / analise_peso_anual['total_nascidos_vivos']) * 100

# Criando o gráfico de barras
fig_peso = px.bar(
    analise_peso_anual,
    x='ano_nascimento',
    y=['perc_baixo_peso', 'perc_muito_baixo_peso'],
    title='Percentual Anual de Nascidos com Baixo e Muito Baixo Peso na Baixada Santista',
    labels={'ano_nascimento': 'Ano de Nascimento', 'value': 'Percentual (%)', 'variable': 'Categoria de Peso'},
    barmode='group' # Agrupa as barras lado a lado
)

fig_peso.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1 - Distribuição Anual das Taxas de Baixo Peso ao Nascer por 1.000 Nascidos Vivos

# COMMAND ----------

# Importando as funções necessárias
from pyspark.sql.functions import col, year, sum as _sum, when, lit

# 1. Carregar os dados da camada Silver (já limpos e tratados)
df_silver = spark.table("datasus.silver.sinasc_baixada_santista_tratada")

# 2. Calcular as taxas anuais por município
df_taxas_municipio = df_silver.groupBy(
    year("dtnasc").alias("ano"),
    "mun_resid"
).agg(
    # Contagem total de nascimentos no grupo
    _sum(lit(1)).alias("total_nascimentos"),
    # Contagem de nascidos com baixo peso (< 2500g)
    _sum(when(col("peso") < 2500, 1).otherwise(0)).alias("total_baixo_peso"),
    # Contagem de nascidos com muito baixo peso (< 1500g)
    _sum(when(col("peso") < 1500, 1).otherwise(0)).alias("total_muito_baixo_peso")
).withColumn(
    # Calcula a taxa por 1.000 nascidos vivos
    "taxa_baixo_peso",
    (col("total_baixo_peso") / col("total_nascimentos")) * 1000
).withColumn(
    "taxa_muito_baixo_peso",
    (col("total_muito_baixo_peso") / col("total_nascimentos")) * 1000
)

# 3. Converter para pandas e "derreter" (melt) o DataFrame para o formato longo
# O formato longo é ideal para o Seaborn, com uma coluna para a categoria e outra para o valor.
pdf_taxas = df_taxas_municipio.select("ano", "mun_resid", "taxa_baixo_peso", "taxa_muito_baixo_peso").toPandas()

pdf_melted = pdf_taxas.melt(
    id_vars=['ano', 'mun_resid'],
    value_vars=['taxa_baixo_peso', 'taxa_muito_baixo_peso'],
    var_name='categoria_peso',
    value_name='taxa_por_mil'
)

# Ajusta os nomes das categorias para o gráfico
pdf_melted['categoria_peso'] = pdf_melted['categoria_peso'].replace({
    'taxa_baixo_peso': 'Baixo Peso (<2500g)',
    'taxa_muito_baixo_peso': 'Muito Baixo Peso (<1500g)'
})


print("Dados preparados para visualização:")
display(pdf_melted.head())

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Definindo uma paleta de cores para as categorias
palette_colors = {
    "Baixo Peso (<2500g)": "skyblue",
    "Muito Baixo Peso (<1500g)": "salmon"
}

# Criando a figura e os eixos
fig, ax = plt.subplots(figsize=(14, 9))

# Camada Única: Boxplot agrupado (geom_boxplot com hue)
sns.boxplot(
    data=pdf_melted,
    x='ano',
    y='taxa_por_mil',
    hue='categoria_peso', # A mágica acontece aqui!
    palette=palette_colors,
    showfliers=False, # Opcional: esconde outliers para uma visão mais limpa da distribuição principal
    ax=ax
)

# Adicionando os pontos individuais com jitter para ver a dispersão real
sns.stripplot(
    data=pdf_melted,
    x='ano',
    y='taxa_por_mil',
    hue='categoria_peso',
    palette=palette_colors,
    jitter=True,
    dodge=True, # Separa os pontos de acordo com o hue
    alpha=0.6,
    ax=ax
)

# Configurando títulos e rótulos
ax.set_title(
    "Distribuição Anual das Taxas de Baixo Peso ao Nascer por 1.000 Nascidos Vivos",
    fontsize=18, weight='bold', pad=20
)
fig.suptitle(
    "Municípios da Baixada Santista",
    y=0.92, fontsize=12
)
ax.set_xlabel("Ano de Nascimento", fontsize=12, weight='bold')
ax.set_ylabel("Taxa por 1.000 Nascidos Vivos", fontsize=12, weight='bold')

# Legenda
handles, labels = ax.get_legend_handles_labels()
# Mostra apenas as duas primeiras legendas para evitar duplicatas do stripplot
ax.legend(handles[0:2], labels[0:2], title="Categoria de Peso", loc='upper left')

# Ajustes finais
plt.tight_layout(rect=[0, 0, 1, 0.95])
sns.despine()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2 - Distribuição Anual das Taxas de Baixo Peso ao Nascer por Município

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Criando a figura e os eixos
fig, ax = plt.subplots(figsize=(16, 10)) # Aumentei um pouco a figura para a legenda caber bem

# --- CAMADA 1: BOXPLOT (sem alterações) ---
# Os boxplots cinzas ainda representam a distribuição geral de cada categoria de peso.
sns.boxplot(
    data=pdf_melted,
    x='ano',
    y='taxa_por_mil',
    hue='categoria_peso',
    palette={"Baixo Peso (<2500g)": "lightgray", "Muito Baixo Peso (<1500g)": "darkgray"},
    showfliers=False,
    ax=ax
)

# --- CAMADA 2: PONTOS COM JITTER (alteração principal aqui) ---
# Agora, o parâmetro 'hue' do stripplot será 'mun_resid'
sns.stripplot(
    data=pdf_melted,
    x='ano',
    y='taxa_por_mil',
    hue='mun_resid', # ALTERAÇÃO PRINCIPAL: colorir por município
    palette='tab10',  # Uma paleta com cores distintas para os municípios
    jitter=True,
    dodge=True,     # Mantém a separação entre 'Baixo Peso' e 'Muito Baixo Peso'
    alpha=0.9,
    size=8,
    ax=ax
)

# --- Configuração de Títulos e Rótulos (sem alterações) ---
ax.set_title(
    "Distribuição Anual das Taxas de Baixo Peso ao Nascer por Município",
    fontsize=20, weight='bold', pad=20
)
fig.suptitle(
    "Cada ponto representa a taxa de um município no ano. Baixada Santista.",
    y=0.93, fontsize=14
)
ax.set_xlabel("Ano de Nascimento", fontsize=14, weight='bold')
ax.set_ylabel("Taxa por 1.000 Nascidos Vivos", fontsize=14, weight='bold')

# --- LEGENDA (ajustada para os municípios) ---
handles, labels = ax.get_legend_handles_labels()
# A legenda agora terá muitas entradas (municípios + categorias). Vamos separar.
# Boxplot (categoria de peso)
box_legend = ax.legend(handles[0:2], labels[0:2], title="Categoria de Peso (Box Plot)", loc='upper left')
# Adiciona a legenda dos boxplots de volta ao gráfico
ax.add_artist(box_legend)

# Pontos (municípios) - posicionada fora do gráfico para não sobrepor
points_legend = ax.legend(handles[2:], labels[2:], title="Município", bbox_to_anchor=(1.02, 1), loc='upper left')


# --- Ajustes Finais ---
plt.tight_layout(rect=[0, 0, 0.85, 0.95]) # Ajusta o layout para a legenda externa caber
sns.despine()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Pré-Natal por Local de Nascimento

# COMMAND ----------

# Importando as funções necessárias
from pyspark.sql.functions import col, year, sum as _sum, when, lit

# 1. Carregar os dados da camada Silver
df_silver = spark.table("datasus.silver.sinasc_baixada_santista_tratada")

# 2. Filtrar para tipos de parto de interesse para ter uma comparação limpa
df_partos_filtrados = df_silver.filter(col("parto").isin("Cesáreo", "Vaginal"))

# 3. Calcular o percentual de pré-natal adequado por ano, município e tipo de parto
df_prenatal_agregado = df_partos_filtrados.groupBy(
    year("dtnasc").alias("ano"),
    "mun_resid",
    "parto"
).agg(
    # Contagem total de nascimentos no grupo
    _sum(lit(1)).alias("total_nascimentos"),
    # Contagem de nascimentos com pré-natal adequado
    _sum(when(col("consultas") == '7 ou mais vezes', 1).otherwise(0)).alias("total_prenatal_adequado")
).withColumn(
    # Calcula o percentual
    "perc_prenatal_adequado",
    # Adiciona uma verificação para evitar divisão por zero
    when(col("total_nascimentos") > 0, (col("total_prenatal_adequado") / col("total_nascimentos")) * 100)
    .otherwise(0)
)

# 4. Converter para Pandas para a visualização
pdf_prenatal = df_prenatal_agregado.select("ano", "mun_resid", "parto", "perc_prenatal_adequado").toPandas()

print("Dados preparados para visualização:")
display(pdf_prenatal.head())

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Criando a figura e os eixos
fig, ax = plt.subplots(figsize=(16, 10))

# --- CAMADA 1: BOXPLOT (agrupado por tipo de parto) ---
# O 'hue' aqui será o 'parto', criando dois boxplots por ano.
sns.boxplot(
    data=pdf_prenatal,
    x='ano',
    y='perc_prenatal_adequado',
    hue='parto', # ALTERAÇÃO: Agrupa os boxplots por tipo de parto
    palette={"Cesáreo": "lightblue", "Vaginal": "darkgray"},
    showfliers=False,
    ax=ax
)

# --- CAMADA 2: PONTOS COM JITTER (coloridos por município) ---
# O 'hue' aqui continua sendo 'mun_resid' para colorir os pontos.
sns.stripplot(
    data=pdf_prenatal,
    x='ano',
    y='perc_prenatal_adequado',
    hue='mun_resid', # Colore os pontos por município
    palette='tab10',
    jitter=True,
    dodge=True, # Separa os pontos de acordo com o hue do boxplot (parto)
    alpha=0.9,
    size=8,
    ax=ax
)

# --- Configuração de Títulos e Rótulos ---
ax.set_title(
    "Cobertura de Pré-Natal Adequado (7+) por Tipo de Parto",
    fontsize=20, weight='bold', pad=20
)
fig.suptitle(
    "Cada ponto representa a taxa de um município no ano. Baixada Santista.",
    y=0.93, fontsize=14
)
ax.set_xlabel("Ano de Nascimento", fontsize=14, weight='bold')
ax.set_ylabel("Percentual de Nascimentos com Pré-Natal Adequado (%)", fontsize=14, weight='bold')

# --- Legendas ---
handles, labels = ax.get_legend_handles_labels()

# Legenda para os Box Plots (Tipo de Parto)
box_legend = ax.legend(handles[0:2], labels[0:2], title="Tipo de Parto (Box Plot)", loc='upper left')
ax.add_artist(box_legend)

# Legenda para os Pontos (Municípios), posicionada fora do gráfico
points_legend = ax.legend(handles[2:], labels[2:], title="Município", bbox_to_anchor=(1.02, 1), loc='upper left')

# --- Ajustes Finais ---
plt.tight_layout(rect=[0, 0, 0.85, 0.95]) # Ajusta o layout para a legenda externa
sns.despine()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4 Pré-Natal Adequado por Tipo de Parto

# COMMAND ----------

# Importando as funções necessárias
from pyspark.sql.functions import col, year, sum as _sum, when, lit

# 1. Carregar os dados da camada Silver
df_silver = spark.table("datasus.silver.sinasc_baixada_santista_tratada")

# 2. Criar a nova dimensão de análise
df_com_dimensao_local = df_silver.withColumn(
    "nascido_no_municipio_residencia",
    when(col("mun_resid") == col("mun_nasc"), "Sim").otherwise("Não")
)

# 3. Calcular o percentual de pré-natal adequado pela nova dimensão
df_local_agregado = df_com_dimensao_local.groupBy(
    year("dtnasc").alias("ano"),
    "mun_resid",
    "nascido_no_municipio_residencia"
).agg(
    _sum(lit(1)).alias("total_nascimentos"),
    _sum(when(col("consultas") == '7 ou mais vezes', 1).otherwise(0)).alias("total_prenatal_adequado")
).withColumn(
    "perc_prenatal_adequado",
    when(col("total_nascimentos") > 0, (col("total_prenatal_adequado") / col("total_nascimentos")) * 100)
    .otherwise(0)
)

# 4. Converter para Pandas para a visualização
pdf_local_prenatal = df_local_agregado.select("ano", "mun_resid", "nascido_no_municipio_residencia", "perc_prenatal_adequado").toPandas()

print("Dados preparados para visualização:")
display(pdf_local_prenatal.head())

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Criando a figura e os eixos
fig, ax = plt.subplots(figsize=(16, 10))

# --- CAMADA 1: BOXPLOT (agrupado pela nova dimensão) ---
sns.boxplot(
    data=pdf_local_prenatal,
    x='ano',
    y='perc_prenatal_adequado',
    hue='nascido_no_municipio_residencia', # ALTERAÇÃO: Agrupa por local de nascimento
    palette={"Sim": "lightblue", "Não": "darkgray"},
    showfliers=False,
    ax=ax
)

# --- CAMADA 2: PONTOS COM JITTER (coloridos por município) ---
sns.stripplot(
    data=pdf_local_prenatal,
    x='ano',
    y='perc_prenatal_adequado',
    hue='mun_resid', # Pontos ainda coloridos por município
    palette='tab10',
    jitter=True,
    dodge=True, # Separa os pontos de acordo com o hue do boxplot
    alpha=0.9,
    size=8,
    ax=ax
)

# --- Configuração de Títulos e Rótulos ---
ax.set_title(
    "Cobertura de Pré-Natal Adequado por Local de Nascimento",
    fontsize=20, weight='bold', pad=20
)
fig.suptitle(
    "Comparativo entre partos ocorridos no município de residência da mãe vs. fora. Baixada Santista.",
    y=0.93, fontsize=14
)
ax.set_xlabel("Ano de Nascimento", fontsize=14, weight='bold')
ax.set_ylabel("Percentual de Nascimentos com Pré-Natal Adequado (%)", fontsize=14, weight='bold')

# --- Legendas ---
handles, labels = ax.get_legend_handles_labels()

# Legenda para os Box Plots (Local de Nascimento)
box_legend = ax.legend(handles[0:2], labels[0:2], title="Nascido no Município de Residência", loc='upper left')
ax.add_artist(box_legend)

# Legenda para os Pontos (Municípios), posicionada fora do gráfico
points_legend = ax.legend(handles[2:], labels[2:], title="Município de Residência", bbox_to_anchor=(1.02, 1), loc='upper left')

# --- Ajustes Finais ---
plt.tight_layout(rect=[0, 0, 0.85, 0.95])
sns.despine()
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Teste de Hipótese

# COMMAND ----------

# Célula para o Teste de Hipótese
# 1. Importar as bibliotecas necessárias
import pandas as pd
import numpy as np
from scipy.stats import chi2_contingency
from pyspark.sql.functions import col

# 2. Carregar os dados da camada Silver
# Para este teste, precisamos das contagens brutas, então partimos da tabela Silver.
# Selecionamos apenas as colunas de interesse para otimizar a performance.
silver_df = spark.table("datasus.silver.sinasc_baixada_santista_tratada")
pdf_teste = silver_df.select("parto", "consultas").toPandas()

# 3. Preparar as variáveis categóricas para o teste
# Filtra para manter apenas os tipos de parto mais comuns e relevantes para a hipótese
pdf_teste = pdf_teste[pdf_teste['parto'].isin(['Vaginal', 'Cesáreo'])]

# Cria a categoria binária para a adequação do pré-natal, conforme o desafio
pdf_teste['prenatal_adequado'] = np.where(pdf_teste['consultas'] == '7 ou mais vezes', 
                                          'Adequado (7+)', 
                                          'Inadequado (<7)')

# 4. Criar a Tabela de Contingência
# Esta tabela cruzará as contagens de cada categoria
tabela_contingencia = pd.crosstab(pdf_teste['prenatal_adequado'], pdf_teste['parto'])

print("--- Tabela de Contingência (Valores Observados) ---")
print(tabela_contingencia)
print("-" * 50)

# 5. Executar o Teste Qui-Quadrado
chi2_stat, p_valor, graus_liberdade, expected_freq = chi2_contingency(tabela_contingencia)

# 6. Apresentar e Interpretar os Resultados
print("\n--- Resultados do Teste Qui-Quadrado ---")
print(f"Estatística Qui-Quadrado: {chi2_stat:.4f}")
print(f"P-valor: {p_valor:.4f}")
print(f"Graus de Liberdade: {graus_liberdade}")
print("\nHipóteses:")
print(" H₀ (Nula): A adequação do pré-natal e o tipo de parto SÃO independentes.")
print(" H₁ (Alternativa): A adequação do pré-natal e o tipo de parto NÃO SÃO independentes.")
print("-" * 50)

# Nível de significância
alpha = 0.05

if p_valor < alpha:
    print(f"\nConclusão: Como o p-valor ({p_valor:.4f}) é menor que {alpha}, rejeitamos a Hipótese Nula.")
    print("Há uma evidência estatística forte para afirmar que existe uma associação significativa entre a adequação do pré-natal e o tipo de parto.")
else:
    print(f"\nConclusão: Como o p-valor ({p_valor:.4f}) é maior ou igual a {alpha}, não podemos rejeitar a Hipótese Nula.")
    print("Não há evidência estatística suficiente para afirmar que existe uma associação entre a adequação do pré-natal e o tipo de parto.")