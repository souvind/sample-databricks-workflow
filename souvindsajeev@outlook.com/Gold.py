# Databricks notebook source
# MAGIC %run "/Workspace/Users/souvindsajeev@outlook.com/Silver"

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

window_spec = Window.partitionBy("country").orderBy(desc("Subscription_date"))
df_diff = df.withColumn("rank", dense_rank().over(window_spec))
display(df_diff)
