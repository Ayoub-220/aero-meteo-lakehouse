from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format

# 1. Initialisation de Spark connecté à LocalStack
spark = SparkSession.builder \
    .appName("BronzeToSilver_Lakehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
    .getOrCreate()

print("Lecture de la couche Bronze...")
df_flights = spark.read.option("multiline", "true").json("s3a://bronze/raw_flights/*.json")
df_weather = spark.read.option("multiline", "true").json("s3a://bronze/raw_weather/*.json")

# 2. Flattening :
df_flights_clean = df_flights.select(
    col("flight_date"),
    col("flight_status"),
    col("airline.name").alias("airline"),
    col("flight.iata").alias("flight_number"),
    col("departure.icao").alias("dep_icao"),
    date_format(col("departure.estimated").cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("dep_estimated"),
    col("departure.delay").alias("dep_delay"),
    col("arrival.icao").alias("arr_icao"),
    date_format(col("arrival.estimated").cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("arr_estimated"),
    col("arrival.delay").alias("arr_delay")
).filter((col("arr_icao").isNotNull()))

df_weather_clean = df_weather.select(
    col("icao_code"),
    date_format(col("fetch_time").cast("timestamp"), "yyyy-MM-dd HH:mm:ss").alias("fetch_time"),
    col("weather_raw.current_weather.temperature").alias("temperature"),
    col("weather_raw.current_weather.windspeed").alias("windspeed")
)

# 3. Jointure : on enrichit les vols avec la météo de la destination
df_silver = df_flights_clean.join(
    df_weather_clean,
    df_flights_clean.arr_icao == df_weather_clean.icao_code,
    "left"
).drop("icao_code")

df_silver = df_silver.dropDuplicates(["flight_date", "flight_number"]) # Déduplication des vols pour éviter les doublons dans la couche Silver

# 4. Écriture en Silver au format Parquet (Orienté colonne et compressé)
silver_path = "s3a://silver/enriched_flights/"
df_silver.write.mode("overwrite").parquet(silver_path)
print(f"Couche Silver générée avec succès dans {silver_path}")