# STEP 1: Read CSV from DBFS (or local path if testing in notebook)
df = spark.read.csv("/Volumes/workspace/pyspark_practice_schema/pyspark_lab/test4.csv", header=True, inferSchema=True)
df.show()
df.printSchema()

# STEP 2: Simple transformation (e.g., trim column names and drop nulls)
from pyspark.sql.functions import col, trim

df_clean = df.select([trim(col(c)).alias(c.strip()) for c in df.columns]).dropna()

# STEP 3: Save DataFrame as CSV to S3
s3_path = "s3://your-bucket-name/path/to/save/test4_clean.csv"
df_clean.write.csv(s3_path, header=True, mode="overwrite")

# STEP 4: Use COPY INTO command in PostgreSQL to load data from S3
import psycopg2

conn = psycopg2.connect(
    dbname="sqlpractice",
    user="postgres",
    password="postgres",
    host="192.168.0.1",
    port="5432"
)
cur = conn.cursor()

copy_sql = """
COPY sample FROM 's3://your-bucket-name/path/to/save/test4_clean.csv'
CREDENTIALS 'aws_access_key_id=YOUR_ACCESS_KEY;aws_secret_access_key=YOUR_SECRET_KEY'
CSV HEADER;
"""
cur.execute(copy_sql)
conn.commit()

cur.close()
conn.close()