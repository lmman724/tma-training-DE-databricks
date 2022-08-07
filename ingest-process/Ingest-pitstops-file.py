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

schema_pit_stops = StructType(fields =[StructField("circuitId",IntegerType(), False ),
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

pit_stops_df = spark.read\
.option("header", True)\
.schema(schema_circuits)\
.csv("dbfs:/mnt/lmman724store/raw-data/circuits.csv")

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

pit_stops_df.show()

# COMMAND ----------
pit_stops_df.printSchema()

# COMMAND ----------

pit_stops_df_rename = pit_stops_df.withColumnRenamed("raceId","circuit_id")\
                    .withColumnRenamed("driverId","circuit_ref")
pit_stops_df_rename.printSchema()

# COMMAND ----------

pit_stops_df_rusults = pit_stops_df_rename.withColumn("ingestion_date",current_timestamp() )


pit_stops_df_rusults.show()

# COMMAND ----------

pit_stops_df_rusults.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/pit_stops")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


