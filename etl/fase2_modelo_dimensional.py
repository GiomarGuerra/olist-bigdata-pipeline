from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, dayofmonth,
    dayofweek, quarter, when, isnan, count, lit,
    monotonically_increasing_id
)
import os

# ── Configuración Hadoop para Windows ───────────────────────────────
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

# ── 1. Iniciar SparkSession ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Olist - Fase 2 Modelo Dimensional") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("SparkSession iniciada\n")

DATA_PATH = r"C:\Users\Usuario1\Desktop\5to Ciclo\EFSRT IV\Proyecto\olist\data"
MODELS_PATH = r"C:\Users\Usuario1\Desktop\5to Ciclo\EFSRT IV\Proyecto\olist\models"

# ── 2. Cargar fuentes necesarias ─────────────────────────────────────
print("Cargando fuentes...")

orders = spark.read.csv(
    os.path.join(DATA_PATH, "olist_orders_dataset.csv"),
    header=True, inferSchema=True)

order_items = spark.read.csv(
    os.path.join(DATA_PATH, "olist_order_items_dataset.csv"),
    header=True, inferSchema=True)

order_payments = spark.read.csv(
    os.path.join(DATA_PATH, "olist_order_payments_dataset.csv"),
    header=True, inferSchema=True)

customers = spark.read.csv(
    os.path.join(DATA_PATH, "olist_customers_dataset.csv"),
    header=True, inferSchema=True)

products = spark.read.csv(
    os.path.join(DATA_PATH, "olist_products_dataset.csv"),
    header=True, inferSchema=True)

sellers = spark.read.csv(
    os.path.join(DATA_PATH, "olist_sellers_dataset.csv"),
    header=True, inferSchema=True)

category_names = spark.read.csv(
    os.path.join(DATA_PATH, "product_category_name_translation.csv"),
    header=True, inferSchema=True)

print("Fuentes cargadas\n")

# ── 3. DIM_CLIENTE ───────────────────────────────────────────────────
print("Construyendo DIM_CLIENTE...")

dim_cliente = customers.select(
    col("customer_id").alias("cliente_id"),
    col("customer_unique_id").alias("cliente_unico_id"),
    col("customer_city").alias("ciudad"),
    col("customer_state").alias("estado"),
    col("customer_zip_code_prefix").alias("codigo_postal")
).dropDuplicates(["cliente_id"])

print(f"   → {dim_cliente.count():,} clientes únicos")

# ── 4. DIM_PRODUCTO ──────────────────────────────────────────────────
print("Construyendo DIM_PRODUCTO...")

dim_producto = products.join(
    category_names,
    products.product_category_name == category_names.product_category_name,
    "left"
).select(
    col("product_id").alias("producto_id"),
    col("product_category_name_english").alias("categoria"),
    col("product_weight_g").alias("peso_gramos"),
    col("product_length_cm").alias("largo_cm"),
    col("product_height_cm").alias("alto_cm"),
    col("product_width_cm").alias("ancho_cm")
).fillna({"categoria": "sin_categoria"})

print(f"   → {dim_producto.count():,} productos únicos")

# ── 5. DIM_VENDEDOR ──────────────────────────────────────────────────
print("Construyendo DIM_VENDEDOR...")

dim_vendedor = sellers.select(
    col("seller_id").alias("vendedor_id"),
    col("seller_city").alias("ciudad_vendedor"),
    col("seller_state").alias("estado_vendedor"),
    col("seller_zip_code_prefix").alias("cp_vendedor")
)

print(f"   → {dim_vendedor.count():,} vendedores únicos")

# ── 6. DIM_TIEMPO ────────────────────────────────────────────────────
print("Construyendo DIM_TIEMPO...")

dim_tiempo = orders.select(
    to_timestamp(col("order_purchase_timestamp")).alias("fecha_completa")
).dropDuplicates().filter(col("fecha_completa").isNotNull()).select(
    col("fecha_completa"),
    year(col("fecha_completa")).alias("anio"),
    month(col("fecha_completa")).alias("mes"),
    dayofmonth(col("fecha_completa")).alias("dia"),
    quarter(col("fecha_completa")).alias("trimestre"),
    dayofweek(col("fecha_completa")).alias("dia_semana"),
    when(dayofweek(col("fecha_completa")).isin([1, 7]), "Fin de semana")
    .otherwise("Día hábil").alias("tipo_dia")
)

print(f"   → {dim_tiempo.count():,} fechas únicas")

# ── 7. FACT_VENTAS ───────────────────────────────────────────────────
print("Construyendo FACT_VENTAS...")

# Pagos agregados por orden
pagos_agg = order_payments.groupBy("order_id").agg(
    {"payment_value": "sum", "payment_installments": "max"}
).withColumnRenamed("sum(payment_value)", "monto_total") \
 .withColumnRenamed("max(payment_installments)", "cuotas")

# Join central
fact_ventas = order_items \
    .join(orders, "order_id", "inner") \
    .join(pagos_agg, "order_id", "left") \
    .select(
        col("order_id").alias("orden_id"),
        col("customer_id").alias("cliente_id"),
        col("product_id").alias("producto_id"),
        col("seller_id").alias("vendedor_id"),
        to_timestamp(col("order_purchase_timestamp")).alias("fecha_compra"),
        col("price").alias("precio_unitario"),
        col("freight_value").alias("costo_envio"),
        col("monto_total"),
        col("cuotas"),
        col("order_status").alias("estado_orden"),
        (col("price") + col("freight_value")).alias("total_item")
    ).filter(col("estado_orden") == "delivered")

total_fact = fact_ventas.count()
print(f"   → {total_fact:,} transacciones en la tabla de hechos")

# ── 8. Guardar en formato Parquet ────────────────────────────────────
print("\nGuardando modelo dimensional en formato Parquet...")

dim_cliente.write.mode("overwrite").parquet(os.path.join(MODELS_PATH, "dim_cliente"))
dim_producto.write.mode("overwrite").parquet(os.path.join(MODELS_PATH, "dim_producto"))
dim_vendedor.write.mode("overwrite").parquet(os.path.join(MODELS_PATH, "dim_vendedor"))
dim_tiempo.write.mode("overwrite").parquet(os.path.join(MODELS_PATH, "dim_tiempo"))
fact_ventas.write.mode("overwrite").parquet(os.path.join(MODELS_PATH, "fact_ventas"))

print("\nModelo dimensional guardado exitosamente")
print("\nRESUMEN DEL ESQUEMA ESTRELLA:")
print(f"   DIM_CLIENTE   : {dim_cliente.count():>7,} registros")
print(f"   DIM_PRODUCTO  : {dim_producto.count():>7,} registros")
print(f"   DIM_VENDEDOR  : {dim_vendedor.count():>7,} registros")
print(f"   DIM_TIEMPO    : {dim_tiempo.count():>7,} registros")
print(f"   FACT_VENTAS   : {total_fact:>7,} registros")
print("\nFase 2 completada. Esquema estrella listo.")

spark.stop()