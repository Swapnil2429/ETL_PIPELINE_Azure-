
# COMMAND ----------
import json, requests
from datetime import datetime

# 📅 Step 1: Set today's date and define ADLS path
start_date = datetime.today().strftime("%Y-%m-%d")
bronze_adls = "abfss://extractbronze@etlprojectbi.dfs.core.windows.net"
output_path = f"{bronze_adls}/opensky/{start_date}"  # ✅ safer folder path

print(f"📅 Start Date: {start_date}")
print(f"📁 Will save to: {output_path}")

# 🔐 Step 2: Authenticate with OpenSky API
client_id = "swapnilk-api-client"
client_secret = "ghV7NIGwBgOkHj6kptWUppc627xhNxOf"

token_resp = requests.post(
    "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token",
    data={
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }
)
token_resp.raise_for_status()
access_token = token_resp.json()["access_token"]

# 🌍 Step 3: Fetch OpenSky live flight data
headers = {"Authorization": f"Bearer {access_token}"}
resp = requests.get("https://opensky-network.org/api/states/all", headers=headers)
resp.raise_for_status()
states = resp.json().get("states", [])
print(f"✈️ Total flight records received: {len(states)}")

# 🧱 Step 4: Process only first 50 rows
columns = [
    "icao24", "callsign", "origin_country", "time_position",
    "last_contact", "longitude", "latitude", "baro_altitude",
    "velocity", "true_track"
]
rows = [dict(zip(columns, row[:len(columns)])) for row in states[:50]]

# 💡 Step 5: Convert to Spark DataFrame via RDD
json_rdd = spark.sparkContext.parallelize([json.dumps(row) for row in rows])
df = spark.read.json(json_rdd)

# 💾 Step 6: Write to ADLS as JSON folder
df.coalesce(1).write.mode("overwrite").json(output_path)

# ✅ Step 7: Exit for ADF pipeline chaining
print(f"✅ Successfully written to: {output_path}")
dbutils.notebook.exit(output_path)
