from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev, lag
from pyspark.sql.window import Window

# Step 1: Spark session
spark = SparkSession.builder.appName("CryptoAnalysis").getOrCreate()

# Step 2: Load historical data CSV
df = spark.read.csv("crypto_prices.csv", header=True, inferSchema=True)

# Step 3: Data Cleaning
df = df.withColumn("date", col("date").cast("date"))
df.createOrReplaceTempView("crypto")

# Step 4: Calculate Simple Moving Average (SMA) for 2-day window
window_spec = Window.partitionBy("symbol").orderBy("date").rowsBetween(-1, 0)  # 2-day SMA
df = df.withColumn("SMA_2", avg("close").over(window_spec))

# Step 5: Calculate Daily Returns
df = df.withColumn("daily_return", (col("close") - lag("close", 1).over(Window.partitionBy("symbol").orderBy("date")))/lag("close", 1).over(Window.partitionBy("symbol").orderBy("date")))

# Step 6: Calculate Volatility (Standard Deviation over 2-day window)
df = df.withColumn("volatility_2", stddev("daily_return").over(window_spec))

# Step 7: Store processed data in local "data lake" folder
df.write.mode("overwrite").parquet("processed_crypto_data")

# Step 8: Query trends using SQL
df = df.fillna(0)

df.createOrReplaceTempView("processed_crypto")


trend = spark.sql("""
    SELECT symbol, date, close, SMA_2, daily_return, volatility_2
    FROM processed_crypto
    ORDER BY symbol, date
""")

trend.show()
