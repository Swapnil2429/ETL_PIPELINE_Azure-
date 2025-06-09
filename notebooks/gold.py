from datetime import datetime
from pyspark.sql.functions import when, col, lit
from pyspark.sql.types import DoubleType, StringType

# 📅 Use today’s date
start_date = datetime.today().strftime("%Y-%m-%d")

# 📂 Silver input path
silver_adls = "abfss://transformsilver@etlprojectbi.dfs.core.windows.net"
silver_path = f"{silver_adls}/opensky_flight_silver/"

# 🗄 Azure SQL DB connection config (🔐 replace with actual values)
jdbc_hostname = "openskydemodb-server.database.windows.net"
jdbc_port = 1433
jdbc_database = "OpenSkyDemoDB"
jdbc_username = "openskyadmin"        # 🔒 Replace with your SQL login
jdbc_password = "Ambibuzz@24"        # 🔒 Replace with your SQL password

# JDBC connection string
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"
connection_properties = {
    "user": jdbc_username,
    "password": jdbc_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 📥 Load data from Silver (Parquet)
df = spark.read.parquet(silver_path)

# 🛠 Transform to match schema of dbo.OpenskyFlights
df_gold = (
    df.withColumn("country_code", lit(None).cast(StringType()))  # ✅ FIXED to avoid void error
      .withColumn("altitude_class", when(col("altitude") < 1000, "LOW")
                                     .when((col("altitude") >= 1000) & (col("altitude") < 10000), "MEDIUM")
                                     .otherwise("HIGH"))
      .select(
          "aircraft_id",
          "callsign",
          "origin_country",
          "time",
          "last_contact",
          "longitude",
          "latitude",
          "altitude",
          "velocity",
          "heading",
          "country_code",
          "altitude_class"
      )
)

# 📝 Target Azure SQL table
table_name = "dbo.OpenskyFlights"

# 💾 Write to Azure SQL
df_gold.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)

print(f"✅ Gold data written to Azure SQL table: {table_name}")
dbutils.notebook.exit(f"gold_written_to_sql::{table_name}")
