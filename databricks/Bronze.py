# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Bronze table
# MAGIC
# MAGIC This notebook will check for new files uploading into folder, and save data to Bronze table

# COMMAND ----------

import logging

# COMMAND ----------

# Create a logger
logger = logging.getLogger("bronze_logger")
logger.setLevel(logging.ERROR)

# Create a file handler and set the log level
file_handler = logging.FileHandler("error_bronze.log")
file_handler.setLevel(logging.ERROR)

# Create a formatter and add it to the file handler
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

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

try:

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
except Exception as e:
  # Log the error message
  logger.error(str(e))
