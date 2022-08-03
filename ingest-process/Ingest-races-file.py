# Databricks notebook source
import pyspark
from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType, TimestampType
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

# races_df = spark.read\
# .option("header", True)\
# .csv("dbfs:/mnt/lmman724store/raw-data/races.csv")

# COMMAND ----------

# races_df.show()
# races_df.printSchema()

# COMMAND ----------

schema_races = StructType(fields =[StructField("raceId",IntegerType(), False ),
                                      StructField("year",IntegerType(), True ),
                                      StructField("round",IntegerType(), True ),
                                      StructField("circuitId",IntegerType(), True ),
                                      StructField("name",StringType(), True ),
                                      StructField("date",DateType(), True ),
                                      StructField("time",TimestampType(), True ),
                                      StructField("url",StringType(), True )
])

# COMMAND ----------

racces_df = spark.read\
.option("header", True)\
.schema(schema_races)\
.csv("dbfs:/mnt/lmman724store/raw-data/races.csv")

# COMMAND ----------

racces_df.printSchema()

# COMMAND ----------

racces_df.show()

# COMMAND ----------

racces_df = racces_df.select(col("raceId"),col("year"),col("round"),col("circuitId"),col("name"),col("date"),col("time"))

racces_df.printSchema()

# COMMAND ----------

racces_df_rename = racces_df.withColumnRenamed("raceId","race_id")\
                    .withColumnRenamed("year","race_year")\
                    .withColumnRenamed("circuitId","circuit_id")\
                    .withColumnRenamed("date","race_timeslamp")
racces_df_rename.printSchema()

# COMMAND ----------

racces_df_results = racces_df_rename.withColumn("ingestion_date",current_timestamp() )


racces_df_results.show()

# COMMAND ----------

racces_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/races")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


