from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum, to_timestamp

# Create SparkSession
spark = SparkSession.builder \
    .appName("ItemSalesVolume") \
    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.option("header", "true").csv("/opt/data/retail_data.csv")

# Convert 'InvoiceDate' column to timestamp
df_with_timestamp = df.withColumn("InvoiceDate", to_timestamp(df["InvoiceDate"], "dd-MM-yyyy HH:mm"))

# Filter records for the year 2010
df_2010 = df_with_timestamp.filter(year(df_with_timestamp["InvoiceDate"]) == 2010)

# Group by 'StockCode' and aggregate total sales volume
item_sales_volume = df_2010.groupBy("StockCode").agg(sum("Quantity").alias("TotalSalesVolume"))

# Sort the result by 'StockCode' in ascending order
item_sales_volume_sorted = item_sales_volume.sort("StockCode")

# Collect the result
result = item_sales_volume_sorted.collect()

# Print the result
print("StockCode\tTotalSalesVolume")
for row in result:
    print(row["StockCode"], "\t\t", row["TotalSalesVolume"])

# Stop SparkSession
spark.stop()
