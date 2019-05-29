// Databricks notebook source
// DBTITLE 1,Notebook 2 for Volume Mounting Exercise
// MAGIC %python
// MAGIC configs = {
// MAGIC 			"fs.azure.account.auth.type": "OAuth",
// MAGIC 			"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
// MAGIC 			"fs.azure.account.oauth2.client.id": "0507f55a-c7f6-4ac6-8fa9-e82aa75ac602",
// MAGIC 			"fs.azure.account.oauth2.client.secret": "].5DKJviOh8s4X.OJvBGCr_nEZhPpuH5",
// MAGIC 			"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9b17fa45-daab-4589-8669-aebbd2b72c36/oauth2/token",
// MAGIC 			"fs.azure.createRemoteFileSystemDuringInitialization": "true"
// MAGIC 		}

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.mount(
// MAGIC source = "abfss://data@dakushwaday3.dfs.core.windows.net/",
// MAGIC mount_point = "/mnt/salesdata5",
// MAGIC extra_configs = configs)

// COMMAND ----------

val fileName = "/mnt/salesdata5/ratings-c.csv"
val data = sc.textFile(fileName).mapPartitionsWithIndex(
  (index, iterator) => {
    if(index == 0) {
      iterator.drop(1)
    }
    
    iterator
  }).map(line => {
    val splitted = line.split(",")
  
    (splitted(2).toFloat, 1)
  })

val pdata = data.reduceByKey((value1, value2) => value1 + value2).sortBy(_._2).collect

pdata.reverse.foreach(println)
