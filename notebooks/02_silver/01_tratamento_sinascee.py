# Databricks notebook source
# MAGIC %md
# MAGIC ### Camada Silver - Limpeza, Enriquecimento e Filtragem
# MAGIC
# MAGIC **Objetivo:** Converter os dados brutos da camada Bronze em um conjunto de dados confiável, limpo e focado no nosso escopo de análise (a Baixada Santista).
# MAGIC
# MAGIC   * **Onde:** Criar o notebook `/Workspace/saude-materno-infantil/notebooks/02-silver/01_tratamento_sinasc.py`.
# MAGIC
# MAGIC   * **Resumo do Fluxo da Camada Silver**
# MAGIC
# MAGIC     1.  **Leitura** da camada Bronze.
# MAGIC     2.  **Filtragem** Geográfica.
# MAGIC     3.  **Seleção** de Colunas.
# MAGIC     4.  **Análise e Remoção** de Nulos (\>30%).
# MAGIC     5.  **Imputação** de Nulos (Mediana/Moda).
# MAGIC     6.  **Padronização** de Nomes de Colunas.
# MAGIC     7.  **Gravação** da tabela final.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1: Leitura da Tabela Bronze e Importações

# COMMAND ----------

from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import IntegerType, StringType

# Lendo a tabela da camada Bronze que criamos no Passo 1
df_silver = spark.table("datasus.bronze.sinasc_baixada")

print("Leitura da tabela Bronze concluída.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2: Filtragem Geográfica - Baixada Santista
# MAGIC
# MAGIC Alinhamento de escopo: limita o conjunto de dados aos 9 municípios da Baixada Santista, com base no **município de residência da mãe.**

# COMMAND ----------

# Lista dos códigos IBGE sem o dígito verificador (6 dígitos) para os municípios da Baixada Santista
codigos_baixada_santista = [
    "350635", # Bertioga
    "351350", # Cubatão
    "351870", # Guarujá
    "352210", # Itanhaém
    "353110", # Mongaguá
    "353760", # Peruíbe
    "354100", # Praia Grande
    "354850", # Santos
    "355100"  # São Vicente
]

# Filtra o DataFrame, mantendo apenas os registros onde o 'codmunres' está na nossa lista
df_filtrado = df_silver.filter(col("CODMUNRES").isin(codigos_baixada_santista))

print(f"Dados filtrados para os {len(codigos_baixada_santista)} municípios da Baixada Santista.")

print(f"Total de registros após o filtro: {df_filtrado.count()}")

# COMMAND ----------

# validando o filtro
df_filtrado.groupBy("MUN_RESID").count().orderBy("count", ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3: Seleção e Agrupamento Inicial de Colunas
# MAGIC
# MAGIC Traze apenas as colunas relevantes para a análise melhora a performance e a clareza do pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Agrupamento Inicial das variáveis
# MAGIC
# MAGIC ##### **Localização**
# MAGIC - **CODESTAB:** Código do estabelecimento de saúde.
# MAGIC - **UNIDADE_SAUDE:** Nome da unidade de saúde.
# MAGIC - **CODMUNNASC:** Código do município de nascimento.
# MAGIC - **MUN_NASC:** Nome do município de nascimento.
# MAGIC - **LOCNASC:** Local de nascimento.
# MAGIC - **CODMUNRES:** Código do município de residência.
# MAGIC - **MUN_RESID:** Nome do município de residência.
# MAGIC - **CODPAISRES:** Código do país de residência.
# MAGIC
# MAGIC ##### **Mãe**
# MAGIC - **IDADEMAE:** Idade da mãe.
# MAGIC - **DTNASCMAE:** Data de nascimento da mãe.
# MAGIC - **RACACORMAE:** Raça/cor da mãe.
# MAGIC - **ESTCIVMAE:** Estado civil da mãe.
# MAGIC - **ESCMAE:** Escolaridade da mãe.
# MAGIC - **CODOCUPMAE:** Código da ocupação da mãe.
# MAGIC - **DTULTMENST:** Data da última menstruação.
# MAGIC - **QTDFILVIVO:** Quantidade de filhos vivos.
# MAGIC - **QTDFILMORT:** Quantidade de filhos mortos.
# MAGIC - **QTDGESTANT:** Quantidade de gestações anteriores.
# MAGIC - **QTDPARTNOR:** Quantidade de partos normais anteriores.
# MAGIC - **QTDPARTCES:** Quantidade de cesarianas anteriores.
# MAGIC - **TPMETESTIM:** Tipo de método de estimativa.
# MAGIC - **IDADEPAI:** Idade do pai.
# MAGIC
# MAGIC ##### **Pré-natal**
# MAGIC - **CONSPRENAT:** Número de Consulta pré-natal.
# MAGIC - **CONSULTAS:** consultas pré-natal (categorias).
# MAGIC - **MESPRENAT:** Mês do início do pré-natal.
# MAGIC
# MAGIC ##### **Condições do Nascimento**
# MAGIC - **SEMAGESTAC:** Semanas de gestação.
# MAGIC - **GESTACAO:** Tipo de gestação.
# MAGIC - **GRAVIDEZ:** Tipo de gravidez.
# MAGIC - **IDANOMAL:** Identificação de anomalias.
# MAGIC - **PARTO:** Tipo de parto.
# MAGIC - **PARIDADE:** Paridade.
# MAGIC - **DTNASC:** Data de nascimento.
# MAGIC - **HORANASC:** Hora de nascimento.
# MAGIC - **SEXO:** Sexo do recém-nascido.
# MAGIC - **APGAR1:** Apgar no 1º minuto.
# MAGIC - **APGAR5:** Apgar no 5º minuto.
# MAGIC - **RACACOR:** Raça/cor do recém-nascido.
# MAGIC - **PESO:** Peso ao nascer.
# MAGIC - **DTCADASTRO:** Data de cadastro.
# MAGIC - **TPROBSON:** Tipo de Robson.

# COMMAND ----------

# Listas de colunas agrupadas por contexto para facilitar a organização

colunas_localizacao_inicial = ["UNIDADE_SAUDE", "MUN_RESID", "MUN_NASC"]
colunas_mae_inicial = ["IDADEMAE","RACACORMAE","ESCMAE"]
colunas_prenatal_inicial = ["CONSULTAS"]
colunas_nascimento_inicial  = ["DTNASC", "GESTACAO", "PARTO", "PESO", "SEXO", "IDANOMAL"]

# Juntando todas as listas em uma única lista de colunas selecionadas
colunas_selecionadas = colunas_localizacao_inicial + colunas_mae_inicial + colunas_prenatal_inicial + colunas_nascimento_inicial

# Selecionando apenas as colunas de interesse do DataFrame original
df_selecionado = df_filtrado.select(colunas_selecionadas)

print("Colunas de interesse selecionadas.")
display(df_selecionado.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4: Análise e Remoção de Colunas com Mais de 30% de Nulos

# COMMAND ----------

import pandas as pd

pdf = df_selecionado.toPandas()

pdf_perc_null = pdf.isnull().sum()/len(pdf)*100

pdf_perc_null

# COMMAND ----------

# MAGIC %md
# MAGIC #### ou

# COMMAND ----------

# 1. Análise de Nulos
total_rows = df_selecionado.count()

# Calcula a contagem de nulos para cada coluna e o percentual
null_counts = df_selecionado.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_selecionado.columns
]).first().asDict()

percent_nulls = {c: (null_counts[c] / total_rows * 100) for c in null_counts}

# 2. Identificação e Remoção
threshold = 30.0
colunas_para_remover = [c for c, p in percent_nulls.items() if p > threshold]

df_sem_nulos_excessivos = df_selecionado.drop(*colunas_para_remover)

display(print(f"Total de registros: {total_rows}"))
display(print(f"Percentual de nulos: {percent_nulls}"))
display(print(f"Colunas removidas por excesso de nulos (> {threshold}%): {colunas_para_remover}"))


# COMMAND ----------

# MAGIC %md
# MAGIC Nenhuma das colunas selecionadas inicialmente possuí um percentual de nulos superior a 3,7%

# COMMAND ----------

pdf_perc_null[pdf_perc_null > 0].index

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 5: Tratamento de Nulos (Imputação com Mediana e Moda)
# MAGIC
# MAGIC Tratamento de campos nulos.
# MAGIC
# MAGIC - mediana para as colunas numéricas;
# MAGIC - moda para as categóricas;

# COMMAND ----------

pdf.info()

# COMMAND ----------

# DataFrame da célula anterior (após tipagem)

colunas_categoricas = pdf_perc_null[pdf_perc_null > 0].index

# 2. Calcular Moda para Colunas Categóricas
modas = {}
# Excluímos colunas de código (como IDs de município) da imputação pela moda, pois pode gerar dados incorretos.
colunas_categoricas_para_imputar = [c for c in colunas_categoricas if c not in ["codmunnasc", "codmunres"]]

for col_categorica in colunas_categoricas_para_imputar:
    if col_categorica in df_selecionado.columns:
        # A moda é o valor com a maior contagem. Agrupamos, contamos, ordenamos e pegamos o primeiro.
        moda_valor = df_selecionado.groupBy(col_categorica).count().orderBy(col("count"), ascending=False).first()[0]
        modas[col_categorica] = moda_valor

print("\nModas calculadas para colunas categóricas:")
print(modas)

# Aplica a imputação com a moda nas colunas categóricas
df_final_imputado = df_selecionado.na.fill(modas)

print("\nImputação de valores nulos concluída.")

# COMMAND ----------

# Calcula a contagem de nulos para cada coluna e o percentual
total_rows = df_final_imputado.count()

null_counts = df_final_imputado.select([
    count(when(col(c).isNull(), c)).alias(c) for c in df_final_imputado.columns
]).first().asDict()

percent_nulls = {c: (null_counts[c] / total_rows * 100) for c in null_counts}

# Exibe o percentual de nulos para cada coluna
percent_nulls_df = spark.createDataFrame(percent_nulls.items(), ["Coluna", "Percentual de Nulos"])
display(percent_nulls_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6: Testes de Qualidade de Dados (Data Quality Checks)
# MAGIC
# MAGIC **Objetivo:** Validar a integridade e a plausibilidade dos dados *antes* de salvá-los na camada Silver. Se um teste falhar, o pipeline deve parar para evitar a propagação de dados incorretos.

# COMMAND ----------

# DataFrame da etapa anterior (após a imputação de nulos)
df_para_validar = df_final_imputado

# --- Função Reutilizável para Testes de Qualidade ---
def executar_teste_qualidade(df, condicao, nome_teste):
  """
  Executa um teste em um DataFrame. Se encontrar registros que violam a condição,
  falha o processo levantando uma exceção.
  """
  registros_invalidos = df.filter(condicao)
  contagem_invalidos = registros_invalidos.count()
  
  if contagem_invalidos > 0:
    print(f"ERRO: Teste de qualidade '{nome_teste}' falhou.")
    print(f"Encontrados {contagem_invalidos} registros inválidos.")
    display(registros_invalidos.limit(10))
    raise Exception(f"Teste de qualidade '{nome_teste}' falhou com {contagem_invalidos} registros.")
  else:
    print(f"SUCESSO: Teste de qualidade '{nome_teste}' passou.")

# --- Execução dos Testes Definidos no Desafio ---

# Teste 1: Peso ao nascer deve estar em um intervalo biologicamente plausível
print("Executando Teste 1: Validade do Peso...")
executar_teste_qualidade(
  df=df_para_validar,
  condicao=(col("PESO") <= 200) | (col("PESO") >= 8000),
  nome_teste="peso_plausivel"
)

# Teste 2: Número de consultas de pré-natal não pode ser negativo
# Nota: Assumindo que a coluna CONSULTAS já foi tratada e é numérica.
print("\nExecutando Teste 2: Consultas de Pré-Natal não negativas...")
executar_teste_qualidade(
  df=df_para_validar,
  condicao=col("CONSULTAS") < 0,
  nome_teste="consultas_nao_negativas"
)

# --- DataFrame final validado ---
df_validado = df_para_validar

# COMMAND ----------

# MAGIC %md
# MAGIC Tratamento dos erros (adicionar regra para automatizar o processo)

# COMMAND ----------

df_registros_validos = df_final_imputado.filter(
    (col("PESO") > 200) & (col("PESO") < 8000)
)
display(df_registros_validos.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7: Padronização Final dos Nomes das Colunas (`snake_case`)
# MAGIC
# MAGIC **Objetivo:** Converter todos os nomes de colunas para o padrão `snake_case` (minúsculas com underscores) para garantir consistência e facilidade de uso nas camadas seguintes.

# COMMAND ----------

# DataFrame da etapa anterior (após a imputação de nulos)
df_para_renomear = df_registros_validos

# Cria uma lista com os nomes de colunas atuais
colunas_atuais = df_para_renomear.columns

# Cria um novo DataFrame renomeando cada coluna para sua versão em minúsculas
# Para o seu caso (ex: 'UNIDADE_SAUDE'), apenas converter para minúsculas já resolve.
df_silver_final = df_para_renomear
for nome_col in colunas_atuais:
    df_silver_final = df_silver_final.withColumnRenamed(nome_col, nome_col.lower())

print("Nomes das colunas padronizados para o padrão snake_case (minúsculas).")
df_silver_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8: Gravação Final na Tabela Silver
# MAGIC
# MAGIC **Objetivo:** Converter todos os nomes de colunas para o padrão `snake_case` (minúsculas com underscores) para garantir consistência e facilidade de uso nas camadas seguintes.

# COMMAND ----------

# Define o nome da tabela de destino na camada Silver
tabela_destino_silver = "datasus.silver.sinasc_baixada_santista_tratada"

# Grava o DataFrame final como uma tabela Delta
df_silver_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(tabela_destino_silver)

print(f"Dados tratados e filtrados salvos com sucesso na tabela: {tabela_destino_silver}")