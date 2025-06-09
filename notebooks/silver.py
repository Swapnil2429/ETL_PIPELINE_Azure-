from datetime import datetime
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import TimestampType, DoubleType

# 📅 Use system date
start_date = datetime.today().strftime("%Y-%m-%d")

# 📂 Input & output containers
bronze_adls = "abfss://extractbronze@etlprojectbi.dfs.core.windows.net"
silver_adls = "abfss://transformsilver@etlprojectbi.dfs.core.windows.net"

# 📁 Read from Bronze
input_path = f"{bronze_adls}/{start_date}_opensky_data.json"
df = spark.read.option("multiline", "true").json(input_path)

# 🧹 Clean and transform
df_transformed = (
    df.selectExpr(
        "icao24 as aircraft_id",
        "callsign",
        "origin_country",
        "time_position",
        "last_contact",
        "longitude",
        "latitude",
        "baro_altitude as altitude",
        "velocity",
        "true_track as heading"
    )
    # 🛠 Clean non-numeric or invalid values
    .withColumn("longitude", when(col("longitude").cast(DoubleType()).isNotNull(), col("longitude").cast(DoubleType())).otherwise(lit(0.0)))
    .withColumn("latitude", when(col("latitude").cast(DoubleType()).isNotNull(), col("latitude").cast(DoubleType())).otherwise(lit(0.0)))
    .withColumn("altitude", when(col("altitude").cast(DoubleType()).isNotNull(), col("altitude").cast(DoubleType())).otherwise(lit(0.0)))
    .withColumn("velocity", when(col("velocity").cast(DoubleType()).isNotNull(), col("velocity").cast(DoubleType())).otherwise(lit(None)))
    .withColumn("heading", when(col("heading").cast(DoubleType()).isNotNull(), col("heading").cast(DoubleType())).otherwise(lit(None)))
    .withColumn("time", col("time_position").cast(TimestampType()))
    .limit(50)
)

# 💾 Save to Silver container (as-is)
silver_output_path = f"{silver_adls}/opensky_flight_silver/"
df_transformed.coalesce(1).write.mode("overwrite").parquet(silver_output_path)

print(f"✅ Transformed data written to: {silver_output_path}")
dbutils.notebook.exit(silver_output_path)
