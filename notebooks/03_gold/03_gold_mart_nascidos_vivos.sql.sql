-- Databricks notebook source
-- SELECT * FROM datasus.silver.sinasc_baixada_santista_tratada VERSION AS OF 5 LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TABLE datasus.gold.mart_nascidos_vivos
USING DELTA
COMMENT "Data mart consolidado com indicadores de saúde materno-infantil para a Baixada Santista."
AS

WITH silver_com_dimensoes AS (
  -- 1. Cria as dimensões (faixas e categorias) a partir dos dados da Silver
  SELECT
    -- Dimensões de Tempo e Local
    YEAR(dtnasc) AS ano_nascimento,
    mun_resid AS municipio_residencia,

    -- Nascimento no Município de Residência da Mãe
    CASE
      WHEN mun_resid = mun_nasc THEN 'sim'
      ELSE 'não'
    END AS nascido_no_municipio_de_residencia_da_mae,
    
    -- Dimensões da Mãe
    CASE
      WHEN idademae < 20 THEN 'Menor de 20 anos'
      WHEN idademae BETWEEN 20 AND 34 THEN '20 a 34 anos'
      ELSE '35 anos ou mais'
    END AS idade_mae_faixa_etaria,
    
    -- Dimensões do Nascimento e Pré-Natal
    parto AS tipo_parto,
    consultas AS consultas_prenatal_faixa,
    
    -- Campos base para cálculo das métricas
    peso,
    idademae,
    consultas,
    parto
  FROM
    datasus.silver.sinasc_baixada_santista_tratada -- Usando a tabela Silver padronizada
)

-- 2. Agrupa pelas dimensões e calcula as métricas aditivas
SELECT
  -- Chaves de Agrupamento (Dimensões)
  ano_nascimento,
  municipio_residencia,
  nascido_no_municipio_de_residencia_da_mae,
  idade_mae_faixa_etaria,
  tipo_parto,
  consultas_prenatal_faixa,
  
  -- Métricas Aditivas (Fatos)
  COUNT(1) AS total_nascidos_vivos,
  SUM(CASE WHEN peso < 2500 THEN 1 ELSE 0 END) AS total_nascidos_baixo_peso,
  SUM(CASE WHEN peso < 1500 THEN 1 ELSE 0 END) AS total_nascidos_muito_baixo_peso,
  SUM(CASE WHEN idademae < 20 THEN 1 ELSE 0 END) AS total_maes_adolescentes,
  SUM(CASE WHEN parto = 'Cesáreo' THEN 1 ELSE 0 END) AS total_partos_cesarea,
  SUM(CASE WHEN consultas = '7 ou mais vezes' THEN 1 ELSE 0 END) AS total_com_7_mais_cons_prenatal
  
FROM
  silver_com_dimensoes
GROUP BY
  ano_nascimento,
  municipio_residencia,
  nascido_no_municipio_de_residencia_da_mae,
  idade_mae_faixa_etaria,
  tipo_parto,
  consultas_prenatal_faixa