from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# Crear sesión Spark
spark = SparkSession.builder.appName("BatchBeneficiarios").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Leer datos desde HDFS
df = spark.read.csv("hdfs://localhost:9000/user/hadoop/ckxe-iu4k.csv", header=True, inferSchema=True)

# Mostrar datos
print("Muestra de Datos")
df.show(5)

# Limpieza
df = df.dropna()
df = df.dropDuplicates()

# Transformación
df = df.withColumn("Año", year(col("FechaInscripcionBeneficiario")))

# Análisis
print("Beneficiarios por genero")
df.groupBy("Genero").count().show()

print("Beneficiarios por tipo de población")
df.groupBy("TipoPoblacion").count().show()

print("Beneficiarios por año")
df.groupBy("Año").count().show()
