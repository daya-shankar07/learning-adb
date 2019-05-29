// Databricks notebook source
// MAGIC 
// MAGIC %python
// MAGIC salesDF = spark.read.format('csv').options(header='true', inferschema='true').load("/mnt/salesdata2/*.csv")
// MAGIC salesDF.write.mode("append").parquet("/mnt/salesdata2/parquet/sales")
// MAGIC salesDF.printSchema
// MAGIC 
// MAGIC print("Done")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC salesDF1 = spark.read.format('parquet').options(header='true', inferschema='true').load("/mnt/salesdata2/parquet/sales")
// MAGIC salesDF1.printSchema
// MAGIC salesDF1.show(20, False)
// MAGIC 
// MAGIC display(salesDF1)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC sales = spark.read.parquet('/mnt/salesdata2/parquet/sales')
// MAGIC sales.createOrReplaceTempView('sales')
// MAGIC 
// MAGIC out1 = spark.sql("SELECT * FROM sales where orderId between 1 and 20")
// MAGIC out1.show