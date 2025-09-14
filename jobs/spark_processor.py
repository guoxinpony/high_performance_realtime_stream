from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_BROKERS = "kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092"
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'


# excute spark-shell --version in docker spark master/worker container, 
# check spark(3.5.1) and scala version(2.12), 
# for spark.jars.packages, org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
# the connector between spark and kafka: spark.readStream.format("kafka")
spark = SparkSession.builder \
    .appName("FinancialTransactionProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .config("spark.sql.shuffle.partitions", 5) \
    .config("spark.sql.streaming.stateStore.stateStoreDir", STATES_DIR) \
    .getOrCreate()


spark.sparkContext.setLogLevel("WARN")

'''
data format in kafka topic financial_transactions:

{
    "transactionId": "192ed43d-30b7-4455-b555-95931ce940b0",
    "userId": "user_6",
    "amount": 58035.52,
    "transactionTime": 1757643752,
    "merchantId": "merchant_3",
    "transactionType": "purchase",
    "location": "location_33",
    "paymentMethod": "credit_card",
    "isInternational": "True",
    "currency": "GBP"
}

'''
# pred-efine of schema
transaction_schema = StructType([
    StructField("transactionId", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("merchantId", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("transactionTime", LongType(), True),
    StructField("transactionType", StringType(), True),
    StructField("location", StringType(), True),
    StructField("paymentMethod", StringType(), True),
    StructField("isInternational", StringType(), True),
    StructField("currency", StringType(), True),
])


# read from kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", SOURCE_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()



'''
Deserialize: kafka bytes -> string -> json 

format: 
  offset:
  partition:
  timestamp:
  key:
  value: generated transaction data in JSON format
'''
transactions_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), transaction_schema).alias("data")) \
    .select("data.*")


# group by merchant, calculate count and toal txn amount for each
aggregated_df = transactions_df.groupBy("merchantId") \
    .agg(
        sum("amount").alias("totalAmount"),
        count("*").alias("transactionCount")
    ) 


# write result back to kafka topic AGGREGATES_TOPIC
aggregation_query = aggregated_df \
  .withColumn("key", col("merchantId").cast("string")) \
  .withColumn(
      "value", 
      to_json(
          struct(
              col("merchantId"),
              col("totalAmount"),
              col("transactionCount")
          )
      )
  ).selectExpr("key", "value") \
  .writeStream \
  .format("kafka") \
  .outputMode("update") \
  .option('kafka.bootstrap.servers', KAFKA_BROKERS) \
  .option("topic", AGGREGATES_TOPIC) \
  .option("checkpointLocation", f'{CHECKPOINT_DIR}/aggregates') \
  .start().awaitTermination()





