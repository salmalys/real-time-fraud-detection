from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import os

os.makedirs("output/parquet_alerts", exist_ok=True)
os.makedirs("output/checkpoint_parquet", exist_ok=True)
os.makedirs("output/checkpoint_kafka", exist_ok=True)

# 1. Création SparkSession avec Kafka
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
    .getOrCreate()
 
# N'affiche que les avertissements et les erreurs
spark.sparkContext.setLogLevel("WARN")
 
# 2. Schéma du JSON Kafka dans Producer
schema = StructType() \
    .add("user_id", StringType()) \
    .add("transaction_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("timestamp", StringType()) \
    .add("location", StringType()) \
    .add("method", StringType()) \
    .add("merchant_id", StringType())
 
# 3. Lecture du stream depuis Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
 
# 4. Parse les valeurs JSON, convertit chaque message Kafka en JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))
 
# 5. Ajoute un watermark pour gérer les données en retard
df_watermarked = df_parsed.withWatermark("timestamp", "5 minutes")
 
# RÈGLE 1 : montant > 49 000
high_value_tx = df_watermarked.filter("amount > 49000") \
    .withColumn("fraud_type", lit("HIGH_VALUE"))
 
# RÈGLE 2 : >= 3 transactions par user en < 5 min (glissante)
suspicious_volume = df_watermarked \
    .groupBy(
        window("timestamp", "5 minutes", "10 seconds"),
        col("user_id")
    ) \
    .count() \
    .filter("count >= 3") \
    .withColumn("fraud_type", lit("HIGH_FREQUENCY")) \
    .select("user_id", "fraud_type", "window")
 
# RÈGLE 3 : plusieurs pays différents en < 5 min (basé sur location)
geo_anomaly = df_watermarked \
    .groupBy(window("timestamp", "5 minutes"), col("user_id")) \
    .agg(
        collect_set("location").alias("locations"),
        collect_list("transaction_id").alias("related_transactions")
    ) \
    .filter(size("locations") > 1) \
    .withColumn("fraud_type", lit("GEO_SWITCH")) \
    .select("user_id", "fraud_type", "window", "related_transactions")
 
# REGLE 4 : si un utilisateur utilise au moins deux devises différentes en moins de 5 minutes
currency_switch = df_watermarked \
    .groupBy(
        window("timestamp", "5 minutes"),
        col("user_id")
    ) \
    .agg(approx_count_distinct("currency").alias("currency_count")) \
    .filter("currency_count > 1") \
    .withColumn("fraud_type", lit("CURRENCY_SWITCH")) \
    .selectExpr("user_id", "'' as transaction_id", "window.start as timestamp", "fraud_type")
 
# REGLE 5 : Si plusieurs petits montant (<30) dans plusieurs marchands (>=3) différents ont été fait en un court instant (<2m30)
carousel_fraud = df_watermarked \
    .filter("amount < 30") \
    .groupBy(
        window("timestamp", "3 minutes", "30 seconds"),
        col("user_id")
    ) \
    .agg(approx_count_distinct("merchant_id").alias("distinct_merchants")) \
    .filter("distinct_merchants >= 3") \
    .withColumn("fraud_type", lit("CAROUSEL_FRAUD")) \
    .selectExpr("user_id", "'' as transaction_id", "window.start as timestamp", "fraud_type")

 
# Union des alertes reunies dans un dataframe
alerts = high_value_tx.select("user_id", "transaction_id", "timestamp", "fraud_type") \
    .unionByName(suspicious_volume.selectExpr("user_id", "'' as transaction_id", "window.start as timestamp", "fraud_type")) \
    .unionByName(geo_anomaly.selectExpr("user_id", "'' as transaction_id", "window.start as timestamp", "fraud_type")) \
    .unionByName(currency_switch) \
    .unionByName(carousel_fraud)

# (1) Écriture sur la console
query_console = alerts.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# (2) Écriture dans un fichier Parquet
query_parquet = alerts.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/output/parquet_alerts/") \
    .option("checkpointLocation", "/app/output/checkpoint_parquet/") \
    .start()

# (3) Écriture dans un topic Kafka "fraud-alerts"
alerts_for_kafka = alerts.withColumn(
    "value", to_json(struct("user_id", "transaction_id", "timestamp", "fraud_type"))
).selectExpr("CAST(user_id AS STRING) AS key", "CAST(value AS STRING) AS value")

query_kafka = alerts_for_kafka.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", "/app/output/checkpoint_kafka/") \
    .start()

# Attendre la fin des 3 streams
query_console.awaitTermination()
query_parquet.awaitTermination()
query_kafka.awaitTermination()
