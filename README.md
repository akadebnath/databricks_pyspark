<h2 align="center">A beginners guide to get started with PySpark in Databricks</h2>

<h3>Import minimum required library</h3>

<pre>
import pyspark
from pyspark.sql import functions as F
</pre>

<h3>Create Comany dataframe from company data</h3>

<pre>
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
</pre>

<h3>Create Stocks dataframe from stocks data</h3>

<pre>
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
</pre>

<h3>Inner join on two dataframes</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"inner").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).show() 
</pre>

<h3>Show all from company dataframe</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"leftouter").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).show() 
</pre>

<h3>Take only not-matching data</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			"leftouter").select(dataframe1.CompCode, dataframe1.Company, dataframe1.Date, dataframe2.StockPrice).filter(dataframe2.StockPrice.isNull()).show() 
</pre>

<h3>Alternative approach for non-matching rows</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='left_anti').show() 
</pre>

<h3>Shows only matching rows only from the left table</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='left_semi').show() 
</pre>

<h3>Aggregate to get average stock price</h3>

<pre>
dataframe1.join(dataframe2, 
			(dataframe1.CompCode == dataframe2.CompCode) & (dataframe1.Date == dataframe2.Date), 
			how='inner') \
            .withColumn("CompanyCode", dataframe1["CompCode"]) \
            .withColumn("CompanyName", dataframe1["Company"]) \
            .groupBy('CompanyCode', 'CompanyName') \
            .avg('StockPrice') \
            .withColumnRenamed("avg(StockPrice)", 'AvgStockPrice') \
            .show()
</pre>

<h3>Aggregate to get max, min, count together</h3>

<pre>
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
</pre>

<h3>Create tables from dataframes</h3>

<pre>
dataframe1.createOrReplaceTempView("table_company")
dataframe2.createOrReplaceTempView("table_stock")
</pre>

<pre>
select * from table_company
</pre>

<pre>
select * from table_stock
</pre>
