import pandas as pd
import requests
import io
import psycopg2
from psycopg2 import sql
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# ======================================================
# 1. Load environment variables
# ======================================================
load_dotenv()

KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")

KOBO_CSV_URL = (
    "https://kf.kobotoolbox.org/api/v2/assets/"
    "a7tiPL7KShZK3SQfuag8it/export-settings/"
    "esZEyRmqR9UCmsaQBS6bTXV/data.csv"
)

PG_HOST = os.getenv("PG_HOST")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_PORT = os.getenv("PG_PORT")

SCHEMA_NAME = "women_survey"
TABLE_NAME = "women_participation_energy"

# ======================================================
# 2. Fetch data from Kobo Toolbox
# ======================================================
print("📥 Fetching data from Kobo Toolbox...")

response = requests.get(
    KOBO_CSV_URL,
    auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD)
)

if response.status_code != 200:
    raise Exception("❌ Failed to fetch data from Kobo Toolbox")

print("✅ Data fetched successfully")

df = pd.read_csv(
    io.StringIO(response.text),
    sep=";",
    on_bad_lines="skip"
)

# ======================================================
# 3. Clean & normalize column names
# ======================================================
print("🧹 Cleaning data...")

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace('"', '')
    .str.replace(" ", "_")
    .str.replace("&", "and")
    .str.replace("-", "_")
    .str.replace("?", "", regex=False)
    .str.replace(".", "", regex=False)
    .str.replace("!", "", regex=False)
)

print("📊 Columns found:")
print(df.columns.tolist())

# ======================================================
# 4. Rename Kobo columns → SQL-friendly names
# ======================================================
COLUMN_MAPPING = {
    "start": "start_time",
    "end": "end_time",
    "nationality": "nationality",
    "region": "region",
    "school_name": "school_name",
    "level_of_study": "level_of_study",
    "gender": "gender",
    "age": "age",
    "gps_coordinates": "gps_coordinates",
    "name_of_organisation": "name_of_organization",
    "your_current_position_in_the_organization": "current_position",
    "years_of_work_experience": "years_of_experience",
    "which_specific_energy_domain_does_your_organization_concentrate_on": "energy_domain",
    "i_give_consent_for_my_information_to_be_collected_and_used_for_research_purposes": "consent",
}

df = df.rename(columns=COLUMN_MAPPING)

# Convert timestamps
df["start_time"] = pd.to_datetime(df.get("start_time"), errors="coerce")
df["end_time"] = pd.to_datetime(df.get("end_time"), errors="coerce")

# ======================================================
# 5. PostgreSQL connection
# ======================================================
print("🔌 Connecting to PostgreSQL...")

conn = psycopg2.connect(
    host=PG_HOST,
    database=PG_DATABASE,
    user=PG_USER,
    password=PG_PASSWORD,
    port=PG_PORT
)

cur = conn.cursor()

# ======================================================
# 6. Create schema & table
# ======================================================
print("🗄️ Creating schema and table...")

cur.execute(
    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
    .format(sql.Identifier(SCHEMA_NAME))
)

cur.execute(
    sql.SQL("DROP TABLE IF EXISTS {}.{}")
    .format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))
)

cur.execute(
    sql.SQL("""
        CREATE TABLE {}.{} (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            nationality TEXT,
            region TEXT,
            school_name TEXT,
            level_of_study TEXT,
            gender TEXT,
            age TEXT,
            gps_coordinates TEXT,
            name_of_organization TEXT,
            current_position TEXT,
            years_of_experience TEXT,
            energy_domain TEXT,
            consent TEXT
        );
    """).format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME)
    )
)

# ======================================================
# 7. Insert data
# ======================================================
print("📤 Inserting data into PostgreSQL...")

insert_query = sql.SQL("""
    INSERT INTO {}.{} (
        start_time, end_time, nationality, region, school_name,
        level_of_study, gender, age, gps_coordinates,
        name_of_organization, current_position,
        years_of_experience, energy_domain, consent
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""").format(
    sql.Identifier(SCHEMA_NAME),
    sql.Identifier(TABLE_NAME)
)

for _, row in df.iterrows():
    cur.execute(
        insert_query,
        (
            row.get("start_time"),
            row.get("end_time"),
            row.get("nationality"),
            row.get("region"),
            row.get("school_name"),
            row.get("level_of_study"),
            row.get("gender"),
            row.get("age"),
            row.get("gps_coordinates"),
            row.get("name_of_organization"),
            row.get("current_position"),
            row.get("years_of_experience"),
            row.get("energy_domain"),
            row.get("consent"),
        )
    )

conn.commit()
cur.close()
conn.close()

print("🎉 Pipeline completed successfully!")

# else:
 
#  print
