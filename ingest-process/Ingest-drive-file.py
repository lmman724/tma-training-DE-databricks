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

schema_drives = StructType(fields =[StructField("driverId",IntegerType(), False ),
                                      StructField("driverRef",StringType(), True ),
                                      StructField("number",IntegerType(), True ),
                                      StructField("code",StringType(), True ),
                                      StructField("name",StringType(), True ),
                                      StructField("dob",StringType(), True ),
                                      StructField("nationality",StringType(), True ),
                                      StructField("url",StringType(), True ),
])

# COMMAND ----------

drivers_df = spark.read\
.option("header", True)\
.schema(schema_drives)\
.json("dbfs:/mnt/lmman724store/raw-data/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df.show()

# COMMAND ----------

drivers_df = drivers_df.select(col("driverId"),col("driverRef"),col("number"),col("code"),col("name"),col("dob"),col("nationality"))

drivers_df.printSchema()

# COMMAND ----------

drivers_df_rename = drivers_df.withColumnRenamed("driverId","driver_id")\
                    .withColumnRenamed("driverRef","driver_ref")

drivers_df_rename.printSchema()

# COMMAND ----------

drivers_df_results = drivers_df_rename.withColumn("ingestion_date",current_timestamp())/
                        



drivers_df_results.show()

# COMMAND ----------

drivers_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/circuits")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


