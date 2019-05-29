// Databricks notebook source
val userName = dbutils.secrets.get(scope = "adbtrainingscope", key = "my-key")

print(userName)

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dakushwaday3.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dakushwaday3.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dakushwaday3.dfs.core.windows.net", "0507f55a-c7f6-4ac6-8fa9-e82aa75ac602")
spark.conf.set("fs.azure.account.oauth2.client.secret.dakushwaday3.dfs.core.windows.net", "].5DKJviOh8s4X.OJvBGCr_nEZhPpuH5")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dakushwaday3.dfs.core.windows.net", "https://login.microsoftonline.com/9b17fa45-daab-4589-8669-aebbd2b72c36/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://data@dakushwaday3.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %sh 
// MAGIC 
// MAGIC wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://data@dakushwaday3.dfs.core.windows.net/")

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS radio_sample_data;
// MAGIC CREATE TABLE radio_sample_data
// MAGIC USING json
// MAGIC OPTIONS (
// MAGIC  path  "abfss://data@dakushwaday3.dfs.core.windows.net/small_radio_json.json"
// MAGIC )

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from radio_sample_data