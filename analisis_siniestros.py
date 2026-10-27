from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("siniestros").getOrCreate()

df = spark.read.csv(
    "hdfs://localhost:9000/datos/accidentes/siniestros.csv",
    header=True,
    inferSchema=True,
)
df = df.dropDuplicates()
df = df.dropna()
print("=== REGISTROS DESPUES DE LIMPIAR ===")
print(df.count())
df.printSchema()
print(df.count())
df.select("ANO_OCURRENCIA_ACC", "GRAVEDAD", "CLASE_ACC", "LOCALIDAD").show(5, truncate=False)

print("=== SINIESTROS POR LOCALIDAD ===")
df.groupBy("LOCALIDAD").count().orderBy("count", ascending=False).show(10, truncate=False)

print("=== SINIESTROS POR GRAVEDAD ===")
df.groupBy("GRAVEDAD").count().orderBy("count", ascending=False).show(10, truncate=False)
resultado = df.groupBy("GRAVEDAD").count()
resultado.coalesce(1).write.mode("overwrite").option("header", True).csv("/home/jhonathan/resultados_gravedad")
spark.stop()