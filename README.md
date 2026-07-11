# 📊 Pipeline Big Data para Análisis de Ventas y Minería de Datos — Caso Olist

> Pipeline end-to-end de Big Data construido con **PySpark**, **modelado dimensional** y **Power BI**, sobre el dataset real de e-commerce brasileño Olist (+100,000 transacciones).

![Status](https://img.shields.io/badge/status-completed-success)
![PySpark](https://img.shields.io/badge/PySpark-4.1.2-orange)
![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)

---

## 🎯 Resumen del Proyecto

Este proyecto implementa una arquitectura completa de procesamiento de Big Data: desde la **ingesta** de datos crudos hasta la **visualización** de resultados accionables para el negocio. El objetivo es demostrar cómo una organización puede transformar datos transaccionales en conocimiento de negocio real, reduciendo el tiempo de análisis de días a minutos.

**Dataset:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle) — más de 99,000 pedidos reales entre 2016 y 2018.

---

## 🏗️ Arquitectura del Pipeline

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌───────────────────┐     ┌─────────────┐
│   INGESTA   │ --> │TRANSFORMACIÓN│ --> │MODELO DIMENSIONAL│ --> │ MINERÍA DE DATOS  │ --> │VISUALIZACIÓN│
│  (9 CSV)    │     │ (PySpark ETL)│     │(Esquema Estrella)│     │(K-Means, Patrones)│     │ (Power BI)  │
└─────────────┘     └──────────────┘     └──────────────────┘     └───────────────────┘     └─────────────┘
```

| Fase | Herramienta | Resultado |
|------|-------------|-----------|
| 1. Ingesta | PySpark | 99,441 pedidos auditados, calidad de datos validada |
| 2. Modelo Dimensional | PySpark + Parquet | Esquema estrella: 4 dimensiones + 1 tabla de hechos (110,197 registros) |
| 3. Minería de Datos | PySpark MLlib (K-Means) | 3 patrones de negocio identificados |
| 4. Visualización | Power BI | Dashboard interactivo de 2 páginas |

---

## 📈 Resultados Destacados

- **Silhouette Score de 0.92** en la segmentación de clientes mediante K-Means — separación excepcionalmente clara entre los 3 segmentos identificados.
- **3 segmentos de cliente** descubiertos: Premium (alto valor, baja frecuencia), Recurrente (alta frecuencia, ticket bajo) y Ocasional (94.8% de la base).
- **Top 10 categorías** de producto identificadas por rentabilidad, liderado por *health_beauty* con S/1.23M en ingresos.
- Pipeline capaz de procesar **110,197 transacciones reales** end-to-end en un entorno local, sin infraestructura cloud.

---

## 🗂️ Estructura del Repositorio

```
olist-bigdata-pipeline/
├── data/                     # Dataset Olist (CSV originales — no incluido por tamaño, ver Kaggle)
├── etl/
│   ├── fase1_ingesta.py      # Carga y auditoría de calidad de los 9 CSV
│   └── fase2_modelo_dimensional.py   # Construcción del esquema estrella (Parquet)
├── mining/
│   └── fase3_mineria.py      # Minería de datos: Top categorías, tendencia, K-Means
├── models/                    # Salida del modelo dimensional en Parquet
├── dashboard/
│   └── Dashboard_Olist_BigData.pbix  # Dashboard interactivo en Power BI
├── docs/
│   ├── Informe_Final.docx
│   └── Manual_Usuario_Guia_Instalacion.docx
└── README.md
```

---

## ⚙️ Stack Tecnológico

- **Procesamiento distribuido:** Apache Spark (PySpark) 4.1.2
- **Lenguaje:** Python 3.10+
- **Modelado de datos:** Esquema estrella, almacenamiento en formato Parquet
- **Machine Learning:** K-Means clustering con StandardScaler (PySpark MLlib)
- **Visualización:** Power BI Desktop
- **Entorno:** Java 17 (Eclipse Temurin), Windows + Hadoop winutils

---

## 🚀 Cómo Ejecutar el Proyecto

### Requisitos previos
```bash
# Java 17+
java -version

# Instalar dependencias de Python
pip install pyspark jupyter pandas matplotlib seaborn
```

### Ejecución del pipeline
```bash
# Fase 1: Ingesta y auditoría de datos
cd etl
python fase1_ingesta.py

# Fase 2: Construcción del modelo dimensional
python fase2_modelo_dimensional.py

# Fase 3: Minería de datos
cd ../mining
python fase3_mineria.py
```

Luego, abrir `dashboard/Dashboard_Olist_BigData.pbix` con Power BI Desktop para visualizar los resultados.

> 📘 Guía de instalación detallada (incluyendo configuración de Hadoop en Windows) disponible en `docs/Manual_Usuario_Guia_Instalacion.docx`.

---

## 📊 Vista del Dashboard

**Página 1 — Resumen Ejecutivo:** KPIs de ingresos totales, pedidos y ticket promedio, evolución mensual de ventas y Top 10 categorías más rentables.

**Página 2 — Segmentación de Clientes:** Visualización de los 3 segmentos de cliente identificados mediante K-Means, con tabla resumen de métricas por segmento.

*(Capturas disponibles en `docs/` y en el informe técnico completo)*

---

## 🧠 Decisiones de Arquitectura

- **PySpark sobre Pandas:** elegido por su capacidad de procesamiento distribuido, escalable a volúmenes que excederían la memoria de una sola máquina.
- **Esquema estrella sobre tabla plana:** permite análisis multidimensional eficiente y es el estándar de la industria para Business Intelligence (Kimball methodology).
- **Parquet sobre CSV:** formato columnar comprimido, óptimo para cargas analíticas de Big Data.
- **K-Means con StandardScaler:** la estandarización previa es crítica para que variables de distinta escala (frecuencia vs. valor monetario) no distorsionen el clustering.

---

## 👤 Autor

**Giomar Guerra** — Estudiante de Arquitectura de Datos Empresariales, Cibertec  
🔗 [LinkedIn](https://www.linkedin.com/in/data-giomar)

Proyecto desarrollado como Experiencia Formativa en Situación Real de Trabajo (EFSRT IV) en colaboración con:
- Jonathan Delgado — Data Engineer
- Bryan Avalos — Data Analyst / BI Developer

---

## 📄 Licencia

Este proyecto tiene fines educativos y de portafolio profesional. El dataset utilizado pertenece a Olist y está disponible públicamente en Kaggle bajo su licencia correspondiente.
