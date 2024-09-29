from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, DateType, IntegerType, FloatType
from pyspark.sql.functions import *
from delta.tables import *


def get_spark_session():
    return (SparkSession.builder.appName('airbnb').config("spark.eventLog.enabled", "true")\
            .config("spark.eventLog.dir", "data").config('spark.sql.catalogImplementation', 'hive')
            .enableHiveSupport().master('local').getOrCreate())


def get_data_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("host_id", IntegerType(), True),
        StructField("host_name", StringType(), True),
        StructField("neighbourhood_group", StringType(), True),
        StructField("neighbourhood", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("room_type", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("minimum_nights", IntegerType(), True),
        StructField("number_of_reviews", IntegerType(), True),
        StructField("last_review", StringType(), True),
        StructField("reviews_per_month", FloatType(), True),
        StructField("calculated_host_listings_count", IntegerType(), True),
        StructField("availability_365", IntegerType(), True),
    ])


def transform_data(data):
    data = data.filter(data.price > 0)
    data = data.na.fill(value=0, subset=["reviews_per_month"])
    data = data.na.fill(value='2010-01-01', subset=["last_review"])

    data = data.na.drop(subset=["latitude", "longitude"])
    data = data.dropDuplicates(['host_id', 'name'])
    data = data.withColumn('price_category', when(data.price < 80, 'budget').
                           when(data.price < 150, 'mid-range').
                           otherwise('luxury'))
    data = data.withColumn('price_per_review', data.price/ data.number_of_reviews)
    return data


def process_sql_queries(session):
    sql_1 = session.sql("SELECT neighbourhood_group, COUNT(*) AS listings "
                        "FROM airbnb "
                        "GROUP BY neighbourhood_group "
                        "ORDER BY listings DESC")

    sql_2 = session.sql("SELECT name, MAX(price) as price "
                        "FROM airbnb "
                        "GROUP BY name "
                        "ORDER BY price DESC "
                        "LIMIT 10")

    sql_3 = session.sql("SELECT neighbourhood_group, room_type, AVG(price) as avg_price "
                        "FROM airbnb "
                        "GROUP BY neighbourhood_group, room_type ")

    sql_1_query = sql_1.writeStream \
        .outputMode("complete") \
        .format("console") \
        .queryName('SQL Query 1: Listings by Neighborhood Group') \
        .start()

    sql_2_query = sql_2.writeStream \
        .outputMode("complete") \
        .queryName('SQL Query 2: Top 10 Most Expensive Listings') \
        .format("console") \
        .start()

    sql_3_query = sql_3.writeStream \
        .outputMode("complete") \
        .queryName('SQL Query 3: Average Price by Room Type') \
        .format("console") \
        .start()
    # tt = session.sql("SELECT COUNT(DISTINCT host_name) AS listings "
    #                "FROM airbnb")
    # sql_4_query = tt.writeStream \
    #     .outputMode("complete") \
    #     .queryName('unique') \
    #     .format("console") \
    #     .start()
    # sql_1_query.awaitTermination(10)
    # sql_2_query.awaitTermination(10)
    sql_3_query.awaitTermination()


def mergeDF(initial, batchID):
    print('here')
    print(f'{batchID}. {initial.count()}')


def update_spark_log_level(spark, log_level='info'):
    spark.sparkContext.setLogLevel(log_level)
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger("my custom Log Level")
    return logger


def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel(logLevel='error')

    logger = update_spark_log_level(spark, 'error')
    logger.info('you log message')
    spark.conf.set("spark.sql.streaming.schemaInference", True)
    data_schema = get_data_schema()

    df_sink = spark.readStream \
        .option("header", True) \
        .schema(data_schema) \
        .option("maxFilesPerTrigger", 1) \
        .option("checkpointLocation", "data/checkpoint") \
        .csv('data/raw')\
        .withColumn("FileName", input_file_name().name())
    print(df_sink.isStreaming)
    print(df_sink.summary())

    data = transform_data(df_sink)
    data.createOrReplaceTempView("airbnb")
    filename = data.select('FileName').dropDuplicates()
    filename.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    data.drop('FileName')

    query = data.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    # # .foreachBatch(mergeDF)\ for merging current batch

    process_sql_queries(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()



