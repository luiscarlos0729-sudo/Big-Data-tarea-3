# Big Data · Tarea 3 · Procesamiento de datos con Apache Spark

Luis Carlos Benedetti Hernández · UNAD · Curso Big Data 202016911A_2204 · Grupo 202016911_30

Análisis por lotes y en tiempo real del dataset **Online Retail** (UCI Machine Learning Repository): 541.909 transacciones de una tienda en línea del Reino Unido, de diciembre de 2010 a diciembre de 2011, en 38 países.

## Contenido

| Archivo | Qué hace |
|---|---|
| `batch_analysis.py` | Análisis por lotes con Spark: limpieza, ventas por país, productos top, tendencia mensual, clientes top (DataFrames), ventas por país con RDD (`map` / `reduceByKey`) y con Spark SQL. |
| `producer.py` | Productor Kafka: simula pedidos con productos reales del dataset y los envía al topic `pedidos_ecommerce`. |
| `rc2.py` | Consumidor con Spark Structured Streaming: lee el topic y calcula número de pedidos e ingreso por ventana de 1 minuto. |

## Requisitos

- Linux (probado en la VM `hadoop-node1`, VirtualBox).
- Python 3 y Apache Spark 3.5.3 (`spark-submit` en el PATH).
- Apache Kafka (para la parte en tiempo real).
- Librería `kafka-python` para el productor: `pip install kafka-python`.
- Dataset: `batch_analysis.py` descarga solo el Online Retail desde https://archive.ics.uci.edu/dataset/352/online+retail y lo convierte a `online_retail.csv` la primera vez (necesita internet).
- Nota: con Python 3.13 o superior, Spark 3.5 no puede ejecutar funciones Python sobre RDD; en ese caso el script lo avisa y hace el mismo cálculo con Spark SQL (la VM usa Python 3.14).

## Cómo ejecutar

### 1. Análisis por lotes

```bash
spark-submit batch_analysis.py
```

Al final el script deja la Spark UI (`http://<IP de la máquina>:4040`) abierta 60 segundos y se cierra solo.

### 2. Tiempo real (Kafka + Spark Streaming)

```bash
# 1) Verificar que Kafka esté activo
pgrep -f kafka

# 2) Arrancar el consumidor en segundo plano
python3 rc2.py &

# 3) Enviar pedidos simulados
python3 producer.py
```

En la consola aparecen los micro-lotes (`Batch: N`) con las columnas `window`, `num_pedidos` e `ingreso_ventana`. Se detiene con `Ctrl+C`.

## Videos

- Video 1 · Análisis por lotes: [enlace pendiente]
- Video 2 · Kafka y Spark Streaming: https://youtu.be/qzbCUZ6MaKo
