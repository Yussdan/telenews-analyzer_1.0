import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, length, window
from pyspark.sql.types import IntegerType, BooleanType, StringType, StructType, StructField, TimestampType

# Настройки
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "telegram_messages")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "telegram_news")
CHECKPOINT_LOCATION = "/tmp/checkpoints"

# Схема для сообщений
message_schema = StructType([
    StructField("message_id", IntegerType()),
    StructField("channel_id", IntegerType()),
    StructField("channel_name", StringType()),
    StructField("date", TimestampType()),
    StructField("text", StringType()),
    StructField("has_media", BooleanType()),
    StructField("views", IntegerType()),
    StructField("forwards", IntegerType()),
    StructField("processed", BooleanType()),
    StructField("created_at", TimestampType())
])

def create_spark_session():
    """Создает и настраивает Spark сессию."""
    return SparkSession.builder \
        .appName("TelegramNewsProcessor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                                      "org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3,"
                                      "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .getOrCreate()

def read_from_kafka(spark):
    """Читает данные из Kafka."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

def process_data(df):
    """Обрабатывает входящие данные."""
    # Парсинг JSON
    parsed_df = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), message_schema).alias("data")
    ).select("data.*")

    # Обработка данных
    return parsed_df \
        .withColumn("processing_time", current_timestamp()) \
        .withColumn("text_length", length(col("text"))) \
        .withColumn("has_media_int", col("has_media").cast("integer"))

def write_to_elasticsearch(df):
    """Записывает данные в Elasticsearch."""
    return df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .outputMode("append") \
        .option("es.nodes", ELASTICSEARCH_HOST) \
        .option("es.port", ELASTICSEARCH_PORT) \
        .option("es.resource", "telegram_news/_doc") \
        .option("es.mapping.id", "message_id") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/es") \
        .start()

def write_to_mongodb(df, collection):
    """Записывает данные в MongoDB."""
    def write_batch(batch_df):
        batch_df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", MONGODB_URI) \
            .option("database", MONGODB_DB) \
            .option("collection", collection) \
            .save()

    return df.writeStream \
        .foreachBatch(write_batch) \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_LOCATION}/{collection}") \
        .start()

def calculate_aggregations(df):
    """Вычисляет агрегации по данным."""
    return df \
        .withWatermark("date", "10 minutes") \
        .groupBy(
            window(col("date"), "5 minutes"),
            col("channel_name")
        ) \
        .count()

def main():
    # Инициализация Spark
    spark = create_spark_session()
    
    try:
        # Чтение из Kafka
        kafka_df = read_from_kafka(spark)
        
        # Обработка данных
        processed_df = process_data(kafka_df)
        
        # Запись в Elasticsearch
        es_query = write_to_elasticsearch(processed_df)
        
        # Запись в MongoDB
        mongo_query = write_to_mongodb(processed_df, "messages")
        
        # Агрегации для дашборда
        aggregations_df = calculate_aggregations(processed_df)
        aggregations_query = write_to_mongodb(aggregations_df, "message_counts") \
            .outputMode("complete")
        
        # Ожидание завершения
        es_query.awaitTermination()
        mongo_query.awaitTermination()
        aggregations_query.awaitTermination()
        
    except Exception as e:
        print(f"Ошибка в работе приложения: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
