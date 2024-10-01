# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

spark.sql("USE working_1.nyc_airbnb")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define the schema (optional but recommended for better performance)
schema = StructType([
  StructField("id", IntegerType(), True),
  StructField("name", StringType(), True),
  StructField("host_id", IntegerType(), True),
  StructField("host_name", StringType(), True),
  StructField("neighbourhood_group", StringType(), True),
  StructField("neighbourhood", StringType(), True),
  StructField("latitude", DoubleType(), True),
  StructField("longitude", DoubleType(), True),
  StructField("room_type", StringType(), True),
  StructField("price", IntegerType(), True),
  StructField("minimum_nights", IntegerType(), True),
  StructField("number_of_reviews", IntegerType(), True),
  StructField("last_review", StringType(), True),
  StructField("reviews_per_month", DoubleType(), True),
  StructField("calculated_host_listings_count", IntegerType(), True),
  StructField("availability_365", IntegerType(), True)
])


# COMMAND ----------

# Source directory for Auto Loader
source_path = "dbfs:/FileStore/tables/AB_NYC_2019/"

# Configure Auto Loader to read CSV files
df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("header", "true") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .schema(schema) \
  .load(source_path)

display(df)

# COMMAND ----------

# Database and table name
database_name = "nyc_airbnb"
bronze_table_name = "bronze_AB_NYC_2019"

# Full table path including the database name
bronze_full_table_name = f"{database_name}.{bronze_table_name}"
# Create the Bronze table if it doesn't exist
spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_full_table_name} USING DELTA")
# Specify the checkpoint location
checkpoint_location = "dbfs:/FileStore/checkpoints/"
# Write the streaming DataFrame to the Bronze Delta table in the specified database
query = df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", checkpoint_location) \
  .option("mergeSchema", "true") \
  .toTable(bronze_full_table_name)

# Start the stream
query.awaitTermination()
