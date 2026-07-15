# Start Spark Session

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, count, when, round, \
to_date, upper, first, coalesce, desc, month, avg, date_format, lit, ceil, lower, \
concat, row_number, year, dayofmonth, radians, sin, cos, asin, sqrt
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("apply_modeling").getOrCreate()

# Load the data

df_retail_weather_station = spark.read.parquet(
    "hdfs://namenode:9000/silver/df_retail_weather_station"
)

df_city_best_station = spark.read.parquet(
    "hdfs://namenode:9000/silver/df_city_best_station"
)


# 1. Create Dim_Date
# Extract unique dates and add calendar attributes
df_dim_date = df_retail_weather_station.select("capture_date").distinct() \
    .withColumn("date_id", date_format(col("capture_date"), "yyyyMMdd").cast("int")) \
    .withColumn("year", year("capture_date")) \
    .withColumn("month", month("capture_date")) \
    .withColumn("day", dayofmonth("capture_date")) \
    .withColumn("day_name", date_format("capture_date", "E")) \
    .withColumn("is_weekend", when(date_format("capture_date", "E").isin("Sat", "Sun"), 1).otherwise(0)) \
    .withColumnRenamed("capture_date", "full_date")

print("=== Dim_Date ===")
df_dim_date.show(5, truncate=False)

# 2. Create Dim_Product
# Generate surrogate keys for unique products
w_prod = Window.orderBy("product_name", "category_name")
df_dim_product = df_retail_weather_station.select("product_name", "category_name", "unit", "price_unit_gbp").distinct() \
    .withColumn("product_id", concat(lit("prod_"), (row_number().over(w_prod) + 100)))

print("=== Dim_Product ===")
df_dim_product.show(5, truncate=False)

# 3. Create Dim_Supermarket
# Unique Supermarket
w_market = Window.orderBy("supermarket_name")
df_dim_supermarket = df_retail_weather_station.select("supermarket_name").distinct() \
    .withColumn("supermarket_id", concat(lit("market_"), (row_number().over(w_market) + 100)))

print("=== Dim_Supermarket ===")
df_dim_supermarket.show(5, truncate=False)

# 4. Create Dim_Station
df_dim_station = df_city_best_station

print("=== Dim_Station ===")
df_dim_station.show(5, truncate=False)

# 5. Create Fact_Sales
# Join dimensions back to the main data to replace strings with IDs
df_fact_sales = df_retail_weather_station.alias("f") \
    .join(df_dim_product.alias("p"),
          (col("f.product_name") == col("p.product_name")) &
          (col("f.category_name") == col("p.category_name")), "inner") \
    .join(df_dim_supermarket.alias("sm"),
          (col("f.supermarket_name") == col("sm.supermarket_name")), "inner") \
    .join(df_dim_station.alias("st"),
          (col("f.station_id") == col("st.station_id")), "inner") \
    .join(df_dim_date.alias("d"),
          col("f.capture_date") == col("d.full_date"), "inner") \
    .select(
        col("f.transc_id"),
        col("d.date_id"),
        col("p.product_id"),
        col("sm.supermarket_id"),
        col("st.station_id"),
        col("f.quantity"),
        col("f.price_gbp"),
        col("f.price_unit_gbp"),
        col("f.avg_temp"),
        col("f.max_temp"),
        col("f.min_temp"),
        col("f.precipitation"),
        col("f.temp_category"),
        col("f.rain_category")
    )

print("=== Fact_Sales ===")
df_fact_sales.show(5)

# Snowflake connection parameters
sfOptions = {
    "sfURL": "QVEGDRD-NP46171.snowflakecomputing.com",
    "sfAccount": "QVEGDRD-NP46171",
    "sfUser": "spark_user",
    "sfPassword": "spark_password",
    "sfDatabase": "sales_weather_db",
    "sfSchema": "gold",
    "sfWarehouse": "sales_weather_wh"
}

SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"

df_dim_date.write \
    .format(SNOWFLAKE_SOURCE) \
    .options(**sfOptions) \
    .option("dbtable", "DIM_DATE") \
    .mode("overwrite") \
    .save()

df_dim_product.write \
    .format(SNOWFLAKE_SOURCE) \
    .options(**sfOptions) \
    .option("dbtable", "DIM_PRODUCT") \
    .mode("overwrite") \
    .save()

df_dim_supermarket.write \
    .format(SNOWFLAKE_SOURCE) \
    .options(**sfOptions) \
    .option("dbtable", "DIM_SUPERMARKET") \
    .mode("overwrite") \
    .save()

df_dim_station.write \
    .format(SNOWFLAKE_SOURCE) \
    .options(**sfOptions) \
    .option("dbtable", "DIM_STATION") \
    .mode("overwrite") \
    .save()

df_fact_sales.write \
    .format(SNOWFLAKE_SOURCE) \
    .options(**sfOptions) \
    .option("dbtable", "FACT_SALES") \
    .mode("overwrite") \
    .save()