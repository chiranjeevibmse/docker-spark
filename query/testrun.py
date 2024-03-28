from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum, to_timestamp

# Create SparkSession
spark = SparkSession.builder \
    .appName("TotalRevenue2010") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.option("header", "true").csv("/opt/data/retail_data.csv")

# Convert 'InvoiceDate' column to timestamp
df_with_timestamp = df.withColumn("InvoiceDate", to_timestamp(df["InvoiceDate"], "dd-MM-yyyy HH:mm"))

# Filter records for the year 2010
df_2010 = df_with_timestamp.filter(year(df_with_timestamp["InvoiceDate"]) == 2010)

# Debugging: Print count of records for the year 2010
print("Number of records in the year 2010:", df_2010.count())

# Aggregate total revenue
total_revenue = df_2010.agg(sum("Price").alias("TotalRevenue")).collect()[0]["TotalRevenue"]

# Print the result
print("Total revenue received in the year 2010:", total_revenue)

# Stop SparkSession
spark.stop()
