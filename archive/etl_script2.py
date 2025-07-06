# STEP 1: Read CSV from DBFS (or local path if testing in notebook)
df = spark.read.csv("/Volumes/workspace/pyspark_practice_schema/pyspark_lab/test4.csv", header=True, inferSchema=True)
df.show()
df.printSchema()

# STEP 2: Simple transformation (e.g., trim column names and drop nulls)
from pyspark.sql.functions import col, trim

df_clean = df.select([trim(col(c)).alias(c.strip()) for c in df.columns]).dropna()

# STEP 3: Set PostgreSQL connection details
# postgres_url = "jdbc:postgresql://192.168.0.1:5432/sqlpractice"
# postgres_properties = {
#     "user": "postgres",
#     "password": "postgres",
#     "driver": "org.postgresql.Driver"
# }
df.write.csv("/Volumes/workspace/pyspark_practice_schema/pyspark_lab/final.csv", header=True, mode="overwrite")

import psycopg2

conn = psycopg2.connect(
    dbname="sqlpractice",
    user="postgres",
    password="postgres",
    host="192.168.0.1",
    port="5432"
)
cur = conn.cursor()

copy_sql = "COPY sample FROM '/Volumes/workspace/pyspark_practice_schema/pyspark_lab/final.csv' CSV header;"
cur.execute(copy_sql)
conn.commit()

cur.close()
conn.close()

# STEP 4: Write to PostgreSQL table (e.g., vendors_performance)
# df.write.jdbc(
#     url=postgres_url,
#     table="sample",
#     mode="overwrite",  # or "append"
#     properties=postgres_properties
# )
