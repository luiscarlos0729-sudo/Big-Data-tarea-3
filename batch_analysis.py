"""
Big Data - Tarea 3 - Procesamiento por lotes con Apache Spark
Luis Carlos Benedetti Hernandez - UNAD

Dataset: Online Retail (UCI Machine Learning Repository)
541.909 transacciones de una tienda en linea del Reino Unido, dic 2010 - dic 2011.

Ejecucion:  spark-submit batch_analysis.py   (o: python3 batch_analysis.py)
Si el CSV no existe, el script descarga el dataset de UCI y lo convierte solo.
"""
import csv
import sys
import time
import io
import os
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

URL = "https://archive.ics.uci.edu/static/public/352/online+retail.zip"
CARPETA = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(CARPETA, "online_retail.csv")
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"


def columna(ref):
    """'C12' -> 2 (indice de columna desde 0)."""
    n = 0
    for c in ref:
        if c.isalpha():
            n = n * 26 + ord(c.upper()) - 64
        else:
            break
    return n - 1


def xlsx_a_csv(datos_xlsx, destino):
    """Convierte la primera hoja de un .xlsx a CSV usando solo la libreria estandar."""
    z = zipfile.ZipFile(io.BytesIO(datos_xlsx))
    textos = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).iter(NS + "si"):
            textos.append("".join(t.text or "" for t in si.iter(NS + "t")))
    base = datetime(1899, 12, 30)
    with open(destino, "w", newline="", encoding="utf-8") as f, z.open("xl/worksheets/sheet1.xml") as hoja:
        w = csv.writer(f)
        for _, fila in ET.iterparse(hoja):
            if fila.tag != NS + "row":
                continue
            valores = [""] * 8
            for celda in fila.iter(NS + "c"):
                i = columna(celda.get("r"))
                if i >= 8:
                    continue
                v = celda.find(NS + "v")
                if celda.get("t") == "s" and v is not None:
                    valor = textos[int(v.text)]
                elif celda.get("t") == "inlineStr":
                    valor = "".join(t.text or "" for t in celda.iter(NS + "t"))
                else:
                    valor = v.text if v is not None else ""
                if i == 4 and valor and valor.replace(".", "", 1).isdigit():
                    # InvoiceDate viene como numero de serie de Excel
                    valor = (base + timedelta(days=float(valor))).strftime("%Y-%m-%d %H:%M:%S")
                valores[i] = valor
            w.writerow(valores)
            fila.clear()


def preparar_dataset():
    if os.path.exists(CSV):
        return
    print("Descargando Online Retail desde UCI...")
    zbytes = urllib.request.urlopen(URL, timeout=300).read()
    z = zipfile.ZipFile(io.BytesIO(zbytes))
    nombre = [n for n in z.namelist() if n.lower().endswith(".xlsx")][0]
    print("Convirtiendo", nombre, "a CSV...")
    xlsx_a_csv(z.read(nombre), CSV)
    print("Listo:", CSV)


def titulo(texto):
    print("\n" + "=" * 70 + "\n" + texto + "\n" + "=" * 70)


def main():
    preparar_dataset()

    spark = (SparkSession.builder.appName("Tarea3_Batch_OnlineRetail")
             .config("spark.driver.memory", "1g").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Carga en un DataFrame
    df = (spark.read.option("header", True).option("inferSchema", True).csv("file://" + CSV))
    titulo("1. CARGA DEL DATASET")
    df.printSchema()
    total = df.count()
    print("Registros originales:", total)

    # 2. Limpieza
    limpio = (df.dropna(subset=["CustomerID"])
                .filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
                .dropDuplicates()
                .withColumn("InvoiceDate", F.to_timestamp("InvoiceDate"))
                .withColumn("Total", F.round(F.col("Quantity") * F.col("UnitPrice"), 2)))
    limpio.cache()
    titulo("2. LIMPIEZA")
    print("Se quitan filas sin cliente, devoluciones (cantidad negativa), precios en cero y duplicados.")
    print("Registros despues de limpiar:", limpio.count())
    limpio.select("InvoiceNo", "Description", "Quantity", "UnitPrice", "Total", "Country").show(5, truncate=30)

    # 3. EDA con DataFrames
    titulo("3. VENTAS POR PAIS (top 10)")
    (limpio.groupBy("Country")
           .agg(F.round(F.sum("Total"), 2).alias("Ventas"),
                F.countDistinct("InvoiceNo").alias("Facturas"))
           .orderBy(F.desc("Ventas")).show(10, truncate=False))

    titulo("4. PRODUCTOS MAS VENDIDOS (unidades)")
    (limpio.groupBy("StockCode", "Description")
           .agg(F.sum("Quantity").alias("Unidades"))
           .orderBy(F.desc("Unidades")).show(10, truncate=40))

    titulo("5. TENDENCIA MENSUAL DE VENTAS")
    (limpio.withColumn("Mes", F.date_format("InvoiceDate", "yyyy-MM"))
           .groupBy("Mes").agg(F.round(F.sum("Total"), 2).alias("Ventas"))
           .orderBy("Mes").show(13, truncate=False))

    titulo("6. CLIENTES QUE MAS COMPRAN")
    (limpio.groupBy("CustomerID", "Country")
           .agg(F.round(F.sum("Total"), 2).alias("Gasto"),
                F.countDistinct("InvoiceNo").alias("Compras"))
           .orderBy(F.desc("Gasto")).show(10, truncate=False))

    # 4. Mismo calculo con RDD y con Spark SQL
    titulo("7. VENTAS POR PAIS CON RDD (map + reduceByKey)")
    try:
        if sys.version_info >= (3, 13):
            raise RuntimeError("Python 3.13+")
        rdd = limpio.select("Country", "Total").rdd
        por_pais = (rdd.map(lambda fila: (fila["Country"], fila["Total"]))
                       .reduceByKey(lambda a, b: a + b)
                       .sortBy(lambda par: par[1], ascending=False))
        for pais, ventas in por_pais.take(10):
            print(f"{pais:<20} {ventas:>15,.2f}")
    except Exception:
        # Spark 3.5 no puede serializar funciones Python en Python 3.14 (version de la VM)
        print("Las funciones Python sobre RDD no son compatibles con Python 3.14 en Spark 3.5;")
        print("el mismo calculo se hace abajo con Spark SQL.")

    titulo("8. VENTAS POR PAIS CON SPARK SQL")
    limpio.createOrReplaceTempView("ventas")
    spark.sql("""
        SELECT Country, ROUND(SUM(Total), 2) AS Ventas
        FROM ventas GROUP BY Country ORDER BY Ventas DESC LIMIT 10
    """).show(truncate=False)

    titulo("FIN DEL ANALISIS")
    print("Spark UI disponible en http://<IP de la maquina>:4040 durante 60 segundos (Ctrl+C cierra antes).")
    try:
        time.sleep(60)
    except KeyboardInterrupt:
        pass
    spark.stop()


if __name__ == "__main__":
    main()
