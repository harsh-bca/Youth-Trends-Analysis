import pandas as pd
import mysql.connector
from mysql.connector import Error
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os

# 🔐 Load environment variables from .env file
load_dotenv()

# 🔹 Google Sheets connection
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    r"C:\Users\Richa Tripathi\Downloads\august-valor-454820-n2-ce6c983f3c35.json", scope
)
client = gspread.authorize(credentials)

# 🔹 Open the spreadsheet
sheet_id = "1ySZR0KZ234r-A5U3tw12gBEsp5DhPTebJNRoyKqoHr8"
spreadsheet = client.open_by_key(sheet_id)
worksheet = spreadsheet.worksheet("Form Responses 1")

# 🔹 Load data into DataFrame
data = worksheet.get_all_records()
df = pd.DataFrame(data)
print(f"✅ Total rows fetched from Google Sheet: {len(df)}")

# 🔹 Normalize column names
df.columns = df.columns.str.replace("\n", " ").str.strip().str.lower()

# 🔹 Column renaming
column_mapping = {
    "timestamp": "timestamp",
    "your current place of living": "current_place",
    "date_of_birth": "date_of_birth",
    "is your mental health okay??": "mental_health",
    "your gender": "gender",
    "occupation type": "occupation",
    "how much time do you spend on social media daily": "social_media_time",
    "do you believe in traditional marriage or modern relationships ?": "relationship_preference",
    "do you prefer saving money or spending on experiences?": "money_habit",
    "have you invested in stocks, crypto or mutual funds?": "investment_status",
    "contact info": "contact_info",
    "email address": "email",
}

# 🔹 Detect and rename dynamic column
for col in df.columns:
    if "how important is financial independence for you" in col:
        df.rename(columns={col: "financial_importance"}, inplace=True)

df.rename(columns=column_mapping, inplace=True)
print("✅ Final DataFrame Columns:", df.columns.tolist())

# 🔹 Data cleaning
df["financial_importance"] = pd.to_numeric(df["financial_importance"], errors="coerce").fillna(0).astype(int)
df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce").dt.date
# Replace empty strings with NaN
df["contact_info"].replace("", pd.NA, inplace=True)

# Fill missing contact_info with unique IDs based on row index
df["contact_info"] = df["contact_info"].fillna(
    pd.Series([f"unknown_contact_{i}" for i in range(len(df))], index=df.index)
)

# 🔹 Filter out rows with missing critical fields
initial_len = len(df)
df = df.dropna(subset=["timestamp", "current_place"])
print(f"✅ Rows after dropping empty contact_info, timestamp, or current_place: {len(df)} (Dropped {initial_len - len(df)})")

# ✅ MySQL Insertion with hidden credentials
conn = None  # ✅ Define it before the try block
try:
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        use_pure=True
    )

    if conn.is_connected():
        cursor = conn.cursor()
        inserted_rows = 0

        for _, row in df.iterrows():
            cursor.execute(
                "SELECT COUNT(*) FROM youth_survey WHERE contact_info = %s AND timestamp = %s",
                (row["contact_info"], row["timestamp"])
            )
            exists = cursor.fetchone()[0]

            if exists == 0:
                cursor.execute("""
                    INSERT INTO youth_survey (
                        timestamp, current_place, date_of_birth, mental_health, gender,
                        occupation, social_media_time, financial_importance, 
                        relationship_preference, money_habit, investment_status, 
                        email, contact_info
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row["timestamp"],
                    row["current_place"],
                    row["date_of_birth"],
                    row["mental_health"],
                    row["gender"],
                    row["occupation"],
                    row["social_media_time"],
                    row["financial_importance"],
                    row["relationship_preference"],
                    row["money_habit"],
                    row["investment_status"],
                    row.get("email", None),
                    row["contact_info"]
                ))
                inserted_rows += 1

        conn.commit()
        print(f"✅ Data inserted successfully! Rows inserted: {inserted_rows}")

except Error as e:
    print(f"❌ MySQL Error: {e}")

finally:
    if conn and conn.is_connected():
        cursor.close()
        conn.close()
