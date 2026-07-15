# Start Spark Session

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, trim, count, when, round, \
to_date, upper, first, coalesce, desc, month, avg, date_format, lit, ceil, lower, \
concat, row_number, year, dayofmonth, radians, sin, cos, asin, sqrt
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("project_transformations").getOrCreate()

# Load the data

df_cities=spark.read.csv("/staging_zone/nifi/weather_sales_data/gb.csv", header=True, inferSchema=True).select("city", "lat", "lng")
df_cities.show(5)

schema_cols = ["Station_ID", "Date", "Element", "Value", "M_FLAG", "Q_FLAG", "S_FLAG", "OBS_TIME"]
df_weather = spark.read.csv("/staging_zone/nifi/weather_sales_data/2024.csv", header=False, inferSchema=True).toDF(*schema_cols)
df_weather.show(5)

# Load 'stations' (Fixed-width parsing)
# ID:1-11, Lat:13-20, Lon:22-30, Elev:32-37, State:39-40, Name:42-71
df_stations = spark.read.text("/staging_zone/nifi/weather_sales_data/ghcnd-stations.txt").select(
    trim(substring(col("value"), 1, 11)).alias("station_id"),
    trim(substring(col("value"), 13, 8)).cast("float").alias("latitude"),
    trim(substring(col("value"), 22, 9)).cast("float").alias("longitude"),
    trim(substring(col("value"), 32, 6)).cast("float").alias("elevation"),
    trim(substring(col("value"), 39, 2)).alias("state"),
    trim(substring(col("value"), 42, 30)).alias("name")
)

df_stations.show(5)

# It's actually a csv file not parquet as the name implies. Keep it as I wrote
df_retail = spark.read.parquet("/staging_zone/nifi/weather_sales_data/uk_retail_with_city.parquet")
df_retail.show(5)

# Basic EDA
# Nulls and Duplicates

def analyze_quality(df):
    n_rows = df.count()

    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]) \
      .union(df.select([round(count(when(col(c).isNull(), c)) / n_rows * 100, 2).alias(c) for c in df.columns])) \
      .show()
    duplicates = df.count() - df.distinct().count()
    print(f"Total Duplicates: {duplicates}\n")


analyze_quality(df_weather)
analyze_quality(df_stations)
analyze_quality(df_retail)


# Data Cleaning
# Remove Nulls

# Drop columns with excessive nulls (>90%)
cols_to_drop = ["M_FLAG", "Q_FLAG", "OBS_TIME"]
df_weather = df_weather.drop(*cols_to_drop)

analyze_quality(df_weather)

# Remove duplicates and drop rows with any null values (since nulls are < 0.02%)
df_retail = df_retail.dropDuplicates().dropna()

analyze_quality(df_retail)

# Transformations
# Clean & Pivot Weather Data

df_weather = df_weather.withColumn("Date", to_date(col("Date").cast("string"), "yyyyMMdd"))

df_weather_clean = df_weather.groupBy("Station_ID", "Date") \
    .pivot("Element", ["TMAX", "TMIN", "PRCP", "TAVG"]) \
    .avg("Value") \
    .withColumn("TMAX", col("TMAX") / 10.0) \
    .withColumn("TMIN", col("TMIN") / 10.0) \
    .withColumn("TAVG", col("TAVG") / 10.0) \
    .withColumn("PRCP", col("PRCP") / 10.0) \
    .na.fill(0, ["PRCP"])


df_weather_clean = df_weather_clean.withColumn(
    "TAVG",
    coalesce(col("TAVG"), round((col("TMAX") + col("TMIN")) / 2, 1))
)

# Transform Retail: Calculate Quantity & Date Dimensions

df_retail_enriched = df_retail.withColumn("quantity", ceil(col("price_gbp") / col("price_unit_gbp"))) \
    .withColumn("day_name", date_format("capture_date", "E")) \
    .withColumn("is_weekend", when(date_format("capture_date", "E").isin("Sat", "Sun"), 1).otherwise(0))

# City -> Station
#filter the data according to the range of lat and lon of UK

df_stations = df_stations.where(
    (col("latitude").between(49.8, 61.1)) &
    (col("longitude").between(-8.7, 2.0))
)

non_uk_count = (
    df_stations
    .filter(~col("station_id").startswith("UK"))  # station_id does NOT start with "UK"
    .count()
)
#it's just only the 11 records shown in the table above
#since that the station_id must start with UK so we drop any record doesn't match that

df_stations = df_stations.filter(col("station_id").startswith("UK"))

df_cities = (
    df_cities
    .withColumnRenamed("lat", "city_lat")
    .withColumnRenamed("lng", "city_lng")
)

# Cities from gb.csv
df_cities_clean = (
    df_cities
    .withColumn("city_clean", lower(trim(col("city"))))
)

# Retail cities
df_retail_cities = (
    df_retail_enriched
    .withColumn("city_clean", lower(trim(col("city"))))
    .select("city_clean")
    .distinct()
)

df_cities_filtered = (
    df_cities_clean
    .join(df_retail_cities, on="city_clean", how="inner")
    .select("*")
)

print("Cities in retail & gb.csv:", df_cities_filtered.count())


from pyspark.sql.functions import radians, sin, cos, asin, sqrt
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col


# Approximate radius of Earth in kilometers (used in the Haversine formula)
R = 6371.0  # km

# -----------------------------------------------------------------------------
# 1) Create all possible station–city pairs
# -----------------------------------------------------------------------------
# Cross join every station with every city. This is only feasible because
# df_cities_filtered (GB cities) is small. The result will have:
#   rows = (#stations) * (#cities)
# Each row will represent:
#   one (station, city) candidate pair whose distance we will compute.
# -----------------------------------------------------------------------------
st_city = df_stations.crossJoin(df_cities_filtered)

# -----------------------------------------------------------------------------
# 2) Compute distance from each station to each city using Haversine formula
# -----------------------------------------------------------------------------
# Haversine formula computes great-circle distance between two points on a
# sphere given their latitudes and longitudes (in radians).
#
# lat1, lon1: station coordinates
# lat2, lon2: city coordinates
# -----------------------------------------------------------------------------
st_city = (
    st_city
    # Convert degrees to radians for trigonometric functions
    .withColumn("lat1", radians(col("latitude")))
    .withColumn("lon1", radians(col("longitude")))
    .withColumn("lat2", radians(col("city_lat")))
    .withColumn("lon2", radians(col("city_lng")))

    # Differences in coordinates
    .withColumn("dlat", col("lat2") - col("lat1"))
    .withColumn("dlon", col("lon2") - col("lon1"))

    # Haversine "a" term: part of the formula capturing chord length squared
    .withColumn(
        "a",
        sin(col("dlat") / 2) ** 2
        + cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon") / 2) ** 2
    )

    # Angular distance in radians
    .withColumn("c", 2 * asin(sqrt(col("a"))))

    # Final distance in kilometers
    .withColumn("distance_km", R * col("c"))
)

# -----------------------------------------------------------------------------
# 3) For each station, choose its nearest city
# -----------------------------------------------------------------------------
# We now have many rows per station_id (one per candidate city). We want only
# the closest city for each station, i.e., the row with minimal distance_km.
#
# Window spec:
#   - partitionBy("station_id"): group rows by station
#   - orderBy(distance_km): sort candidate cities from nearest to farthest
# -----------------------------------------------------------------------------
w = Window.partitionBy("station_id").orderBy(col("distance_km").asc())

stations_with_city = (
    st_city
    # Rank candidate cities per station by distance (1 = closest)
    .withColumn("rn", row_number().over(w))

    # Keep only the closest city per station
    .filter(col("rn") == 1)

    # Select a clean set of columns for downstream use
    .select(
        "station_id",
        "latitude",
        "longitude",
        "city",          # city from df_cities_filtered
        "distance_km",
    )

    # Rename 'city' to 'nearest_city' to clarify its meaning
    .withColumnRenamed("city", "nearest_city")
)

# Preview a few rows to validate mapping: each station has exactly one nearest_city
stations_with_city.show(20, truncate=False)



# Retail: normalize city, ensure capture_date is DateType, add id column
df_retail_enriched = (
    df_retail_enriched
    .withColumn("city_clean", lower(trim(col("city"))))
    .withColumn("capture_date", to_date("capture_date"))
    .withColumn("transc_id", concat(lit("transc_"), (row_number().over(Window.orderBy(col("capture_date"))) + 100)))
)

# Stations_with_city: normalize city the same way
stations_with_city = (
    stations_with_city
    .withColumn("city_clean", lower(trim(col("nearest_city"))))
)

# Transform Weather: Categorize Temperature & Precipitation

df_weather_enriched = df_weather_clean.withColumn(
    "temp_category",
    when(col("TAVG") >= 20, "Hot")
    .when(col("TAVG") <= 10, "Cold")
    .otherwise("Mild")
).withColumn(
    "rain_category",
    when(col("PRCP") > 0, "Rainy").otherwise("Dry")
)


df_weather_enriched = df_weather_enriched.withColumn(
    "date",
    to_date("Date")
)

# pick closest station per city using distance_km
w_city = Window.partitionBy("city_clean").orderBy(col("distance_km"))

df_city_best_station = (
    stations_with_city
    .withColumn("rn", row_number().over(w_city))
    .filter(col("rn") == 1)
    .drop("rn", "distance_km")
)

# df_city_best_station: one row per city, with the best station_id
# join weather with best station per city

df_weather_with_city = (
    df_weather_enriched.alias("w")
    .join(
        df_city_best_station.alias("cs"),
        col("w.station_id") == col("cs.station_id"),
        "inner",
    )
    .select(
        col("cs.nearest_city").alias("city_clean"),
        col("w.date"),
        col("w.station_id"),
        col("w.TAVG").alias("avg_temp"),
        col("w.TMAX").alias("max_temp"),
        col("w.TMIN").alias("min_temp"),
        col("w.PRCP").alias("precipitation"),
        col("w.temp_category"),
        col("w.rain_category"),
    )
)

df_weather_with_city.show(5, truncate=False)

# Normalize keys on BOTH dataframes
df_weather_with_city = (
    df_weather_with_city
    .withColumn("city_key", lower(col("city_clean")))
    .withColumn("date_key", to_date(col("date")))
)

df_retail_enriched = (
    df_retail_enriched
    .withColumn("city_key", lower(col("city_clean")))
    .withColumn("date_key", to_date(col("capture_date")))
)

#  Join on the *normalized* keys
df_retail_weather_station = (
    df_retail_enriched.alias("r")
    .join(
        df_weather_with_city.alias("ws"),
        on=["city_key", "date_key"],   # equivalent to the two-column condition
        how="inner",
    )
    .select(
        col("r.*"),
        col("ws.station_id"),
        col("ws.avg_temp"),
        col("ws.max_temp"),
        col("ws.min_temp"),
        col("ws.precipitation"),
        col("ws.temp_category"),
        col("ws.rain_category"),
    )
     .drop("city_key", "date_key")
)

df_retail_weather_station.show(5, truncate=False)



# Retail + weather joined
df_retail_weather_station.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/silver/df_retail_weather_station")

# Station ↔ nearest city mapping
df_city_best_station.write \
    .mode("overwrite") \
    .parquet("hdfs://namenode:9000/silver/df_city_best_station")