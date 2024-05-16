-- Databricks notebook source
-- DBTITLE 1,Upload data in Python
-- MAGIC %python
-- MAGIC food = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=',').load('/FileStore/tables/onlinefoods.csv')
-- MAGIC display(food)

-- COMMAND ----------

-- DBTITLE 1,Upload data in Scala
-- MAGIC %scala
-- MAGIC val food_scala = spark.read.format("csv")
-- MAGIC  .option("header", "true")
-- MAGIC  .option("inferSchema", "true")
-- MAGIC  .option("delimiter", ",")
-- MAGIC  .load("/FileStore/tables/onlinefoods.csv")
-- MAGIC display(food_scala)

-- COMMAND ----------

-- DBTITLE 1,Creating a view with the dataframe data
-- MAGIC %scala
-- MAGIC food_scala.createOrReplaceTempView("data_foods")

-- COMMAND ----------

-- DBTITLE 1,Selecting the data from the new view
select * from data_foods

-- COMMAND ----------


