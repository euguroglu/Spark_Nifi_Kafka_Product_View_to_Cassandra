from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum, approx_count_distinct, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Tumbling Window Stream Active Users") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 2) \
        .getOrCreate()

#Describe schema (productid will be enough to find viewed category in the last 5 minute)
    schema = StructType([
    StructField("messageid", StringType()),
    StructField("userid", StringType()),
    StructField("properties", StructType([
        StructField("productid", StringType())
    ])),
])
#Read data from kafka topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "api") \
        .option("startingOffsets", "earliest") \
        .load()
#Data in kafka topic have key-value format, from_json is used to deserialize json value from string
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
#Checking schema if everything is correct
    value_df.printSchema()
#Explode dataframe to remove sub-structures
    explode_df = value_df.selectExpr("value.messageid", "value.userid", "value.properties.productid")
#Checking schema if everything is correct
    explode_df.printSchema()
#Set timeParserPolicy=Legacy to parse timestamp in given format
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#Convert string type to timestamp
    transformed_df = explode_df.select("messageid", "userid", "productid")

#Checcking schema if everything is correct
    transformed_df.printSchema()

    kafka_target_df = transformed_df.selectExpr("messageid as key",
                                                 "to_json(struct(*)) as value")
    kafka_target_df.printSchema()
#Write spark stream to console or csv sink

    kafka_target_df.printSchema()

    nifi_query = kafka_target_df \
            .writeStream \
            .queryName("User Api Writer") \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "apiconsume") \
            .outputMode("append") \
            .option("checkpointLocation", "chk-point-dir") \
            .start()

    nifi_query.awaitTermination()
