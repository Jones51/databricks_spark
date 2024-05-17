# Databricks notebook source
dados = [
("Grimaldo","Oliveira","Brasileira","Professor","M",3000),
("Ana ","Santos","Portuguesa","Atriz","F",4000),
("Roberto","Carlos","Brasileira","Analista","M",4000),
("Maria","Santanna","Italiana","Dentista","F",6000),
("Jeane","Andrade","Inglesa","Medica","F",7000)]

colunas=["Primeiro_Nome","Ultimo_nome","Nacionalidade","Trabalho","Genero","Salario"]

datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

# COMMAND ----------

# DBTITLE 1,Splitting files
datafparquet.write.partitionBy("Nacionalidade","salario").mode("overwrite").parquet("/FileStore/tables/parquet/pessoal.parquet")


# COMMAND ----------

# DBTITLE 1,Showing only the data  "Nacionalidade=Brasileira"
datafnacional=spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira")
datafnacional.show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Using SQL to read the parquet files
spark.sql("CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path \"/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira\")")
spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Oliveira'").show()

# COMMAND ----------


