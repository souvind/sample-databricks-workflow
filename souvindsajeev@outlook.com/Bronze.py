# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

myschema = StructType(
    [
        StructField("Index", IntegerType(), True),
        StructField("Customer_Id", StringType(), True),
        StructField("First_Name", StringType(), True),
        StructField("Last_Name", StringType(), True),
        StructField("Company", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Phone_1", StringType(), True),
        StructField("Phone_2", StringType(), True),
        StructField("Email", StringType(), True),
        StructField("Subscription_Date", DateType(), True),
        StructField("Website", StringType(), True),
    ]
)

# COMMAND ----------

df = spark.read.format("csv").option("header",True).schema(myschema).load("/Volumes/test_catalog/test_schema/test_volume/customers-1000.csv")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS test_catalog.test_schema.Customer_Table (
# MAGIC     Index INT,
# MAGIC     Customer_Id STRING,
# MAGIC     First_Name STRING,
# MAGIC     Last_Name STRING,
# MAGIC     Company STRING,
# MAGIC     City STRING,
# MAGIC     Country STRING,
# MAGIC     Phone_1 STRING,
# MAGIC     Phone_2 STRING,
# MAGIC     Email STRING,
# MAGIC     Subscription_Date DATE,
# MAGIC     Website STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", True)\
    .saveAsTable("test_catalog.test_schema.Customer_Table")
