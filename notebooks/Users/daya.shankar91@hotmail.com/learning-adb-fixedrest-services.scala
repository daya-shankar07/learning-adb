// Databricks notebook source
import scalaj.http._
import org.apache.dsext.spark.datasource.rest.RestDataSource


// COMMAND ----------

val sodauri = "https://soda.demo.socrata.com/resource/6yvf-kk3n.json"
val sodainput1 = ("Nevada", "nn")
val sodainput2 = ("Northern California", "pr")
val sodainput3 = ("Virgin Islands region", "pr")
val sodainputRdd = sc.parallelize(Seq(sodainput1, sodainput2, sodainput3))
val sodainputKey1 = "region"
val sodainputKey2 = "source"
val sodaDf = sodainputRdd.toDF(sodainputKey1, sodainputKey2)

sodaDf.createOrReplaceTempView("sodainputtbl")

// COMMAND ----------

sodaDf.createOrReplaceTempView("sodainputtbl")

val parmg = Map("url" -> sodauri, "input" -> "sodainputtbl", "method" -> "GET", "readTimeout" -> "10000", "connectionTimeout" -> "2000", "partitions" -> "10")
val sodasDf = spark.read.format("org.apache.dsext.spark.datasource.rest.RestDataSource").options(parmg).load()

sodasDf.printSchema 

sodasDf.createOrReplaceTempView("sodastbl")


// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC Select * from sodastbl