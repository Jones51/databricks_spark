# Databricks notebook source
# DBTITLE 1,Creating a Dataframe
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

# DBTITLE 1,Creating the Parquet file
datafparquet.write.parquet("/FileStore/tables/parquet/pessoal.parquet")


# COMMAND ----------

# DBTITLE 1,Updating the file
datafparquet.write.mode('overwrite').parquet('/FileStore/tables/parquet/pessoal.parquet')


# COMMAND ----------

# DBTITLE 1,Once Parquet file created, now reading it back as a dataframe for data manipulation
datafleitura= spark.read.parquet("/FileStore/tables/parquet/pessoal.parquet")
datafleitura.show()


# COMMAND ----------

# DBTITLE 1,With the dataframe, reading it as SQL for ease manipulation
datafleitura.createOrReplaceTempView("Tabela_Parquet")
ResultSQL = spark.sql("select * from Tabela_Parquet where salario >= 6000 ")
ResultSQL.show()

# COMMAND ----------


