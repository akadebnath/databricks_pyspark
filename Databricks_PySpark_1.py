# Databricks notebook source
import pyspark
from pyspark.sql import functions as F

# COMMAND ----------


data1 = [["1000", "Microsoft", "01-Nov-2024"],
		["1000", "Microsoft", "02-Nov-2024"],
		["1000", "Microsoft", "03-Nov-2024"],
		["1000", "Microsoft", "04-Nov-2024"],
		["1000", "Microsoft", "05-Nov-2024"],
		["2000", "Apple", "01-Nov-2024"],
		["2000", "Apple", "02-Nov-2024"],
		["2000", "Apple", "03-Nov-2024"],
		["2000", "Apple", "04-Nov-2024"],
		["2000", "Apple", "05-Nov-2024"]]

columns1 = ['CompCode', 'Company', 'Date'] 

dataframe1 = spark.createDataFrame(data1, columns1) 

data2 = [["1000", "01-Nov-2024", 1000], 
         ["1000", "02-Nov-2024", 900], 
         ["1000", "03-Nov-2024", 1100], 
         ["1000", "04-Nov-2024", 8900], 
         ["1000", "01-Nov-2024", 4000], 
         ["2000", "03-Nov-2024", 26000], 
         ["2000", "04-Nov-2024", 27000], 
         ["2000", "05-Nov-2024", 21000]]

columns2 = ['CompCode', 'Date', 'StockPrice'] 

dataframe2 = spark.createDataFrame(data2, columns2) 



# COMMAND ----------

# MAGIC %md
# MAGIC ##Inner join on two dataframes

# COMMAND ----------


# inner join on two dataframes 
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"inner").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Show all from company dataframe

# COMMAND ----------

dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"leftouter").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Take only not-matching data

# COMMAND ----------

dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"leftouter").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).filter(dataframe2.StockPrice.isNull()).show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Alternative approach for non-matching rows

# COMMAND ----------

dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='left_anti').show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Shows only matching rows only from the left table

# COMMAND ----------

dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='left_semi').show() 

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregate to get average stock price

# COMMAND ----------

dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='inner') \
            .withColumn("CompanyCode", dataframe1["CompCode"]) \
            .withColumn("CompanyName", dataframe1["Company"]) \
            .groupBy('CompanyCode', 'CompanyName') \
            .avg('StockPrice') \
            .withColumnRenamed("avg(StockPrice)", 'AvgStockPrice') \
            .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Aggregate to get max, min, count together

# COMMAND ----------

from pyspark.sql.functions import max, min, count
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='inner') \
            .withColumn("CompanyCode", dataframe1["CompCode"]) \
            .withColumn("CompanyName", dataframe1["Company"]) \
            .groupBy('CompanyCode', 'CompanyName') \
            .agg(
                min('StockPrice').alias('MinStockPrice'),
                max('StockPrice').alias('MaxStockPrice'),
                count('StockPrice').alias('CntStockPrice')
            ) \
            .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create tables from dataframes

# COMMAND ----------

dataframe1.createOrReplaceTempView("table_company")
dataframe2.createOrReplaceTempView("table_stock")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_company

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_stock
