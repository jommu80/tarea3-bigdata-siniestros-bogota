from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, current_timestamp

spark = SparkSession.builder.appName("StreamingSiniestroBogota").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


datos = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "siniestros-bogota") \
    .option("startingOffsets", "latest") \
    .load()

mensajes = datos.selectExpr("CAST(value AS STRING) AS mensaje")

resultado = mensajes.select(
    split(col("mensaje"), ",").getItem(0).alias("localidad"),
    split(col("mensaje"), ",").getItem(1).alias("gravedad"),
    split(col("mensaje"), ",").getItem(2).alias("cantidad"),
    current_timestamp().alias("fecha_procesamiento")
)

consulta = resultado.writeStream.format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

consulta.awaitTermination()