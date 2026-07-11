from pyspark.sql import SparkSession
import os

# 1. Inicio de SparkSession
spark = SparkSession.builder \
    .appName("Olist - Fase 1 - Ingesta") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("SparkSession iniciado correctamente.\n")

# 2. Ruta de los datos
DATA_PATH = r"C:\Users\Usuario1\Desktop\5to Ciclo\EFSRT IV\Proyecto\olist\data"

# 3. Carga de archivos CSV (9)
archivos = {
    "orders":         "olist_orders_dataset.csv",
    "order_items":    "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews":  "olist_order_reviews_dataset.csv",
    "customers":      "olist_customers_dataset.csv",
    "products":       "olist_products_dataset.csv",
    "sellers":        "olist_sellers_dataset.csv",
    "category_names": "product_category_name_translation.csv",
    "geolocation":    "olist_geolocation_dataset.csv",
}

dataframes = {}

for nombre, archivo in archivos.items():
    ruta = os.path.join(DATA_PATH, archivo)
    df = spark.read.csv(ruta, header=True, inferSchema=True)
    dataframes[nombre] = df
    print(f"{nombre:20s} -> {df.count():>7,} registros | {len(df.columns)}. columnas")

# 4. Reporte de calidad inicial
print("\n" + "="*55)
print("REPORTE DE CALIDAD — TABLA: orders")
print("="*55)

orders = dataframes["orders"]
total = orders.count()

print(f"\nTotal de registros: {total:,}")
print(f"\nEstados de pedido únicos:")
orders.groupBy("order_status").count().orderBy("count", ascending=False).show()

print(f"\nValores nulos por columna:")
from pyspark.sql.functions import col, sum as spark_sum, isnan, when

nulos = orders.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in orders.columns
])
nulos.show(truncate=False)

print("\nFase 1 completada. Dataset Olist cargado exitosamente.")
spark.stop()