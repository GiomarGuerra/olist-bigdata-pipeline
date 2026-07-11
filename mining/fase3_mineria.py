from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, round as spark_round,
    desc, month, year
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import os

# ── Configuración Hadoop para Windows ───────────────────────────────
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += r";C:\hadoop\bin"

# ── 1. Iniciar SparkSession ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Olist - Fase 3 Mineria de Datos") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
    .config("spark.local.dir", "C:/tmp/spark-temp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("SparkSession iniciada\n")

MODELS_PATH = r"C:\Users\Usuario1\Desktop\5to Ciclo\EFSRT IV\Proyecto\olist\models"
MINING_PATH = r"C:\Users\Usuario1\Desktop\5to Ciclo\EFSRT IV\Proyecto\olist\mining"

# ── 2. Cargar modelo dimensional ─────────────────────────────────────
print("Cargando modelo dimensional...")
fact    = spark.read.parquet(os.path.join(MODELS_PATH, "fact_ventas"))
dim_cli = spark.read.parquet(os.path.join(MODELS_PATH, "dim_cliente"))
dim_pro = spark.read.parquet(os.path.join(MODELS_PATH, "dim_producto"))
dim_tie = spark.read.parquet(os.path.join(MODELS_PATH, "dim_tiempo"))
print("Modelo cargado\n")

# ════════════════════════════════════════════════════════════════════
# PATRÓN 1 — TOP 10 CATEGORÍAS MÁS RENTABLES
# ════════════════════════════════════════════════════════════════════
print("=" * 55)
print("PATRÓN 1: TOP 10 CATEGORÍAS MÁS RENTABLES")
print("=" * 55)

top_categorias = fact \
    .join(dim_pro, "producto_id", "left") \
    .groupBy("categoria") \
    .agg(
        count("orden_id").alias("total_ventas"),
        spark_round(spark_sum("precio_unitario"), 2).alias("ingreso_total"),
        spark_round(avg("precio_unitario"), 2).alias("ticket_promedio")
    ) \
    .orderBy(desc("ingreso_total")) \
    .limit(10)

top_categorias.show(truncate=False)

# Guardar para Power BI
top_categorias.toPandas().to_csv(
    os.path.join(MINING_PATH, "top_categorias.csv"),
    index=False, encoding="utf-8"
)
print("top_categorias.csv guardado\n")

# ════════════════════════════════════════════════════════════════════
# PATRÓN 2 — TENDENCIA DE VENTAS POR MES Y AÑO
# ════════════════════════════════════════════════════════════════════
print("=" * 55)
print("PATRÓN 2: TENDENCIA DE VENTAS POR MES/AÑO")
print("=" * 55)

tendencia = fact \
    .join(dim_tie, fact.fecha_compra == dim_tie.fecha_completa, "left") \
    .groupBy("anio", "mes") \
    .agg(
        count("orden_id").alias("total_pedidos"),
        spark_round(spark_sum("precio_unitario"), 2).alias("ingreso_mensual"),
        spark_round(avg("precio_unitario"), 2).alias("ticket_promedio")
    ) \
    .orderBy("anio", "mes")

tendencia.show(30, truncate=False)

tendencia.toPandas().to_csv(
    os.path.join(MINING_PATH, "tendencia_ventas.csv"),
    index=False, encoding="utf-8"
)
print("tendencia_ventas.csv guardado\n")

# ════════════════════════════════════════════════════════════════════
# PATRÓN 3 — SEGMENTACIÓN DE CLIENTES CON K-MEANS
# ════════════════════════════════════════════════════════════════════
print("=" * 55)
print("PATRÓN 3: SEGMENTACIÓN DE CLIENTES — K-MEANS")
print("=" * 55)

# Construir perfil RFM simplificado por cliente
perfil_cliente = fact \
    .groupBy("cliente_id") \
    .agg(
        count("orden_id").alias("frecuencia"),
        spark_round(spark_sum("precio_unitario"), 2).alias("valor_total"),
        spark_round(avg("precio_unitario"), 2).alias("ticket_promedio")
    ) \
    .filter(col("frecuencia") > 0)

print(f"Clientes con perfil: {perfil_cliente.count():,}")

# Preparar features para KMeans
assembler = VectorAssembler(
    inputCols=["frecuencia", "valor_total", "ticket_promedio"],
    outputCol="features_raw"
)

scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withMean=True,
    withStd=True
)

data_assembled = assembler.transform(perfil_cliente)

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[assembler, scaler])

# Nota: re-ensamblamos correctamente
data_vec = assembler.transform(perfil_cliente)
scaler_model = scaler.fit(data_vec)
data_scaled = scaler_model.transform(data_vec)

# Entrenar KMeans con 3 segmentos
kmeans = KMeans(k=3, seed=42, featuresCol="features", predictionCol="segmento")
modelo_kmeans = kmeans.fit(data_scaled)

# Asignar segmentos
clientes_segmentados = modelo_kmeans.transform(data_scaled)

# Evaluar
evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="segmento")
silhouette = evaluator.evaluate(clientes_segmentados)
print(f"\nSilhouette Score: {silhouette:.4f} (mientras más cerca de 1, mejor)")

# Resumen por segmento
print("\nResumen por segmento:")
clientes_segmentados.groupBy("segmento") \
    .agg(
        count("cliente_id").alias("cantidad_clientes"),
        spark_round(avg("frecuencia"), 2).alias("freq_promedio"),
        spark_round(avg("valor_total"), 2).alias("valor_promedio"),
        spark_round(avg("ticket_promedio"), 2).alias("ticket_promedio")
    ) \
    .orderBy("segmento") \
    .show()

# Guardar para Power BI
clientes_segmentados.select(
    "cliente_id", "frecuencia", "valor_total", "ticket_promedio", "segmento"
).toPandas().to_csv(
    os.path.join(MINING_PATH, "segmentacion_clientes.csv"),
    index=False, encoding="utf-8"
)
print("segmentacion_clientes.csv guardado\n")

print("Fase 3 completada. 3 patrones identificados y exportados.")
print(f"\nArchivos en: {MINING_PATH}")
print("   → top_categorias.csv")
print("   → tendencia_ventas.csv")
print("   → segmentacion_clientes.csv")

spark.stop()