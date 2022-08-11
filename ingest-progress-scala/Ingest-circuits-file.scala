// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

// COMMAND ----------

val storage_account = "lmman724store"
val client_id            = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-app-id")
val tenant_id            = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-tenant-id")
val client_secret        = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-secret")

// COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.lmman724store.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.lmman724store.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.lmman724store.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.lmman724store.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.lmman724store.dfs.core.windows.net","https://login.microsoftonline.com/client_secret/oauth2/token")

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/training-formular-databricks")

// COMMAND ----------

val circuits_df = spark.read
.option("header", "true")
.csv("dbfs:/mnt/lmman724store/training-formular-databricks/raw-data/circuits.csv")

circuits_df.show

// COMMAND ----------

circuits_df.printSchema

// COMMAND ----------

val schema_circuits = StructType(Array(
    StructField("circuiId", IntegerType, false),
    StructField("circuitRef", StringType, true),
    StructField("name", StringType, true),
    StructField("country", StringType, true),
    StructField("location", StringType, true),
    StructField("lat", DoubleType, true),
    StructField("lng", DoubleType, true),
    StructField("alt", DoubleType, true),
    StructField("url", StringType, true)
))

// COMMAND ----------

var circuits_df_schema = spark.read
.option("header", "true")
.schema(schema_circuits)
.csv("dbfs:/mnt/lmman724store/training-formular-databricks/raw-data/circuits.csv")

// COMMAND ----------

circuits_df_schema.printSchema()

// COMMAND ----------

circuits_df_schema.show

// COMMAND ----------

val circuits_df_select = circuits_df_schema.select(col("circuiId"),col("circuitRef"),col("name"),col("location"),col("lat"),col("lng"),col("alt"))

// COMMAND ----------

val circuits_df_rename = circuits_df_select.withColumnRenamed("circuitId","circuit_id")
                    .withColumnRenamed("circuitRef","circuit_ref")
                    .withColumnRenamed("lat","latitude")
                    .withColumnRenamed("lng","longtitude")
                    .withColumnRenamed("alt","altitude")

// COMMAND ----------

val circuits_df_results = circuits_df_rename.withColumn("ingestion_date",current_timestamp())

// COMMAND ----------

circuits_df_results.show()

// COMMAND ----------

circuits_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/training-formular-databricks/ingested-data/circuits")

// COMMAND ----------


