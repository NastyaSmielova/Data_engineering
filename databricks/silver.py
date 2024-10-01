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


bronze_table_name = "bronze_AB_NYC_2019"
database_name = "nyc_airbnb"

# Full table path including the database name
bronze_full_table_name = f"{database_name}.{bronze_table_name}"
silver_table_name = "silver_AB_NYC_2019"

# Full table path including the database name
silver_full_table_name = f"{database_name}.{silver_table_name}"

# Create the Silver Delta Table with the specified schema
spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_full_table_name} (id INT, name STRING, host_id INT, host_name STRING, neighbourhood_group STRING,  neighbourhood STRING, latitude DOUBLE, longitude DOUBLE,  room_type STRING,  price INT NOT NULL, minimum_nights INT NOT NULL, number_of_reviews INT,   last_review DATE, reviews_per_month DOUBLE, calculated_host_listings_count INT,  availability_365 INT NOT NULL) USING DELTA")


# COMMAND ----------

from pyspark.sql.functions import to_date, lit, min


# COMMAND ----------


spark.sql(f"CREATE TABLE IF NOT EXISTS {bronze_full_table_name} USING DELTA")
# Read the streaming data from the Bronze Delta Table
bronze_df = spark.readStream.format("delta").table(bronze_full_table_name)

# Handle prices lover or equal to 0
silver_df = bronze_df.filter("price > 0")

# Handle missing values in reviews_per_month by setting them to 0
silver_df = silver_df.na.fill(0, subset=["reviews_per_month"])

# Drop rows with missing values in latitude or longitude columns
silver_df = silver_df.dropna(subset=["latitude", "longitude"])

silver_df = silver_df.na.fill("2010-01-01", subset=["last_review"])
# First, convert the 'last_review' column to a date type (if it's not already)
silver_df = silver_df.withColumn("last_review", to_date("last_review", "yyyy-MM-dd"))


# Specify the checkpoint location
checkpoint_location = "dbfs:/FileStore/checkpoints_2/"
# Ensure the checkpoint directory is cleared before starting the streaming query
dbutils.fs.rm(checkpoint_location, recurse=True)
# Write the transformed streaming data to the Silver Delta Table
silver_query = silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location) \
    .option("mergeSchema", "true") \
    .toTable(silver_full_table_name)

# Start the streaming query
silver_query.awaitTermination()
