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

schema_contructors= StructType(fields =[StructField("constructorId",IntegerType(), False ),
                                      StructField("constructorRef",StringType(), True ),
                                      StructField("name",StringType(), True ),
                                      StructField("nationality",StringType(), True ),
                                      StructField("url",StringType(), True )
])

# COMMAND ----------

constructors_df = spark.read\
.option("header", True)\
.schema(schema_contructors)\
.json("dbfs:/mnt/lmman724store/raw-data/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

constructors_df = circuits_df.select(col("constructorId"),col("constructorRef"),col("name"),col("nationality"))

constructors_df.printSchema()

# COMMAND ----------

constructors_df_rename = constructors_df.withColumnRenamed("constructorId","constructor_id")\
                    .withColumnRenamed("constructorRef","constructor_ref")
constructors_df_rename.printSchema()

# COMMAND ----------

constructors_df_results = constructors_df_rename.withColumn("ingestion_date",current_timestamp() )


constructors_df_results.show()

# COMMAND ----------

constructors_df_results.write.mode("overwrite").parquet("/mnt/lmman724store/processed-data/constructors")

# COMMAND ----------

dbutils.fs.ls("/mnt/lmman724store/processed-data")

# COMMAND ----------


