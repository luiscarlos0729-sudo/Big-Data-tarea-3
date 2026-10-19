"""
Tarea 3 - Big Data (202016911)
Consumidor de Spark Streaming - procesa en tiempo real los pedidos que llegan
al topic de Kafka "pedidos_ecommerce" y calcula estadisticas por ventana de tiempo.
Adaptado del ejemplo de sensores del Anexo 3 (spark_streaming_consumer.py).
Ejecutar en la VM (Hadoop+Spark+Kafka ya instalados, ver Anexo 3):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_pedidos_consumer.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum as _sum, count as _count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
spark = (
    SparkSession.builder
    .appName("PedidosEcommerceStreaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
schema = StructType([
    StructField("InvoiceNo", StringType()),
    StructField("StockCode", StringType()),
    StructField("Description", StringType()),
    StructField("Quantity", IntegerType()),
    StructField("UnitPrice", FloatType()),
    StructField("Country", StringType()),
    StructField("timestamp", LongType()),
])
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "pedidos_ecommerce")
    .load()
)
parsed_df = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("TotalPedido", col("Quantity") * col("UnitPrice"))
)
parsed_df = parsed_df.withColumn(
    "event_time", (col("timestamp")).cast("timestamp")
)
windowed_stats = (
    parsed_df
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(
        _count("*").alias("num_pedidos"),
        _sum("TotalPedido").alias("ingreso_ventana"),
    )
)
query = (
    windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)
query.awaitTermination()
