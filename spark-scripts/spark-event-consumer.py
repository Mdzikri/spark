import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("furniture", StringType(), True),
        StructField("color", StringType(), True),
        StructField("price", IntegerType(), True),
        StructField("ts", LongType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("DibimbingStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "dataeng-kafka:9092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "latest")
    .load()
)

df_finish = (
    stream_df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

df_finish = df_finish.withColumn("event_time", to_timestamp(col("ts") / 1000))

# PostgreSQL configuration
postgres_host = "dataeng-postgres"
postgres_db = "warehouse"
postgres_user = "user"
postgres_password = "password"
jdbc_url = f"jdbc:postgresql://{postgres_host}/{postgres_db}"
jdbc_properties = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver",
    "stringtype": "unspecified",
}

# Define a function to write the dataframe to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write.jdbc(url=jdbc_url, table="public.real_time_data2", mode="append", properties=jdbc_properties)

# Start the streaming query and write to PostgreSQL
query = (
    df_finish.writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("append")
    .start()
)

# Start the streaming query to show data in console
query_console = (
    df_finish.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
query_console.awaitTermination()