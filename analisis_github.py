
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date, desc
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 1. Inicializar Spark
spark = SparkSession.builder.appName("AnalisisGitHub").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el conjunto de datos
df = spark.read.json("datos/*.json.gz")

print("Conjunto original")
df.printSchema()

print("Total de registros:", df.count())

# 3. Limpieza y transformacion
datos = df.select(
    col("id"),
    col("type"),
    col("repo.name").alias("repositorio"),
    col("created_at")
)

datos = datos.dropDuplicates(["id"])
datos = datos.dropna(
    subset=["type", "repositorio", "created_at"]
)

datos = datos.withColumn(
    "fecha", to_date(col("created_at"))
)

datos.cache()

print("Registros validos:", datos.count())

# 4. Analisis exploratorio

# Cantidad de eventos por tipo
eventos = datos.groupBy("type").agg(count("*").alias("total")).orderBy(desc("total"))

print("Eventos por tipo")
eventos.show(30, truncate=False)

# Repositorios con mayor actividad
repositorios = datos.groupBy("repositorio").agg(count("*").alias("total")).orderBy(desc("total"))

print("Top 10 repositorios con mayor actividad")
repositorios.show(10, truncate=False)

# Actividad por dia
actividad = datos.groupBy("fecha").agg(count("*").alias("total")).orderBy("fecha")

print("Actividad diaria")
actividad.show()

# 5. Visualizacion

top_eventos = eventos.limit(10).toPandas()

plt.figure(figsize=(11, 6))

plt.bar(
    top_eventos["type"],
    top_eventos["total"]
)

plt.title("Tipos de eventos mas frecuentes")
plt.xlabel("Tipo de evento")
plt.ylabel("Cantidad de eventos")
plt.xticks(rotation=45, ha="right")
plt.tight_layout()

plt.savefig("eventos_github.png")

# Liberar recursos
datos.unpersist()
spark.stop()
