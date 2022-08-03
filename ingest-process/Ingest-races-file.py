# Databricks notebook source
import pyspark
from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

storage_account_name = "lmman724store"
client_id            = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-client-id")
tenant_id            = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-tenant-id")
client_secret        = dbutils.secrets.get(scope="lmman724-scope", key="databricks-app-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/raw-data")

# COMMAND ----------

# circuits_df = spark.read\
# .option("header", True)\
# .csv("dbfs:/mnt/lmman724store/raw-data/circuits.csv")

# COMMAND ----------

# circuits_df.show()
# circuits_df.printSchema()

# COMMAND ----------

schema_circuits = StructType(fields =[StructField("circuitId",IntegerType(), False ),
                                      StructField("circuitRef",StringType(), True ),
                                      StructField("name",StringType(), True ),
                                      StructField("location",StringType(), True ),
                                      StructField("country",StringType(), True ),
                                      StructField("lat",DoubleType(), True ),
                                      StructField("lng",DoubleType(), True ),
                                      StructField("alt",IntegerType(), True ),
                                      StructField("url",StringType(), True ),
])

# COMMAND ----------

circuits_df = spark.read\
.option("header", True)\
.schema(schema_circuits)\
.csv("dbfs:/mnt/lmman724store/raw-data/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

circuits_df.printSchema()

# COMMAND ----------

circuits_df_rename = circuits_df.withColumnRenamed("circuitId","circuit_id")\
                    .withColumnRenamed("circuitRef","circuit_ref")\
                    .withColumnRenamed("lat","latitude")\
                    .withColumnRenamed("lng","longtitude")\
                    .withColumnRenamed("alt","altitude")
circuits_df_rename.printSchema()

# COMMAND ----------

circuits_df_results = circuits_df_rename.withColumn("ingestion_date",current_timestamp() )


circuits_df_results.show()

# COMMAND ----------

circuits_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/circuits")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


