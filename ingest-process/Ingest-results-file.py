# Databricks notebook source
from tkinter.tix import COLUMN
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

schema_results = StructType(fields =[StructField("resultId",IntegerType(), False ),
                                      StructField("raceId",IntegerType(), True ),
                                      StructField("driverId",IntegerType(), True ),
                                      StructField("constructorId",IntegerType(), True ),
                                      StructField("number",IntegerType(), True ),
                                      StructField("grid",IntegerType(), True ),
                                      StructField("positionText",IntegerType(), True ),
                                      StructField("positionOrder",IntegerType(), True ),
                                      StructField("points",IntegerType(), True ),
                                      StructField("laps",IntegerType(), True ),
                                      StructField("time",StringType(), True ),
                                      StructField("milliseconds",IntegerType(), True ),
                                      StructField("fastestLap",IntegerType(), True ),
                                      StructField("rank",IntegerType(), True ),
                                      StructField("fastestLapTime",StringType(), True ),
                                      StructField("fastestLapSpeed",DoubleType(), True ),
                                      StructField("statusId",IntegerType(), True ),
    ])
# COMMAND ----------

results_df = spark.read\
.option("header", True)\
.schema(schema_results)\
.json("dbfs:/mnt/lmman724store/raw-data/results.json")

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

results_df.show()

# COMMAND ----------

results_df = results_df.drop(col("statusId"))

results_df.printSchema()

# COMMAND ----------

results_df_rename = results_df.withColumnRenamed("resultId","result_id")\
                    .withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("positionText","position_text")\
                    .withColumnRenamed("fastestLap","fastest_lap")\
                    .withColumnRenamed("positionOrder","position_order")\
                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")
results_df_rename.printSchema()

# COMMAND ----------

results_df_results = results_df_rename.withColumn("ingestion_date",current_timestamp() )


results_df_results.show()

# COMMAND ----------

results_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/results")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


