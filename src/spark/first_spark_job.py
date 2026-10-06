from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = (
    SparkSession.builder
    .appName("ClinicalBigData")
    .master("local[*]")
    .getOrCreate()
)

print("Spark version:", spark.version)

data = [
    ("P001", "Diabetes", 58),
    ("P002", "Hypertension", 64),
    ("P003", "Diabetes", 72),
    ("P004", "Asthma", 35),
    ("P005", "Hypertension", 69),
]

columns = ["patient_id", "diagnosis", "age"]

df = spark.createDataFrame(data, columns)

print("\n=== INPUT DATA ===")
df.show()

result = (
    df.groupBy("diagnosis")
      .agg(
          count("*").alias("patient_count"),
          avg("age").alias("average_age")
      )
      .orderBy(col("patient_count").desc())
)

print("\n=== SPARK RESULT ===")
result.show()

input("Spark UI is available. Press Enter to stop Spark...")
spark.stop()
