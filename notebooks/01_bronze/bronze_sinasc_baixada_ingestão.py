# Databricks notebook source
# MAGIC %md
# MAGIC ## Exploração inicial

# COMMAND ----------

# MAGIC %md
# MAGIC Leitura do arquivo parquet diretamente do volume 

# COMMAND ----------

df = spark.read.format("parquet").load("/Volumes/datasus/bronze/raw")

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(df.describe())

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Após observação os campos já se encontram com o tipo de dado adequado

# COMMAND ----------


df.count(), len(df.columns)


# COMMAND ----------


# Grava dados do df na tabela delta da camada bronze
df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("datasus.bronze.sinasc_baixada")