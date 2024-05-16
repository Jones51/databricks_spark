-- Databricks notebook source
-- DBTITLE 1,Games Dataset Visualization
select
console,
sum(score) as totalScore 
from games
where score >= 7
group by console
order by totalScore desc
limit 10

-- COMMAND ----------

-- DBTITLE 1,Data Analysis - Percentage of single people by Occupation and Gender
select
Gender,
Occupation,
count(`Marital Status`) as countStatus
from onlinefoods_2_csv
where `Marital Status` = 'Single'
group by Gender, Occupation
order by countStatus desc
limit 10

-- COMMAND ----------


