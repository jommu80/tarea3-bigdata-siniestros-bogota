# Análisis de siniestros viales de Bogotá

## Descripción
Proyecto académico de Big Data que utiliza Python, Apache Spark y Apache Kafka para analizar siniestros viales de Bogotá.

## Objetivo
Procesar datos históricos, limpiarlos y analizar los siniestros por gravedad y localidad.

## Archivos del proyecto
- `analisis_siniestros.py`: procesamiento por lotes (Batch).
- `streaming_siniestros.py`: procesamiento en tiempo real con Kafka y Spark Streaming.

## Tecnologías utilizadas
- Python
- Apache Spark
- Apache Kafka
- Hadoop y HDFS

## Fuente de datos
Datos abiertos de siniestros viales de Bogotá D. C.

https://datosabiertos.bogota.gov.co/dataset/historico-siniestros-bogota-d-c

## Procesamiento en tiempo real
Se envía un registro simulado mediante Kafka y se verifica que Spark Streaming lo reciba y procese correctamente.
