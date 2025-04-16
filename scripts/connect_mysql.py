import pymysql
import pandas as pd
import mysql.connector

# ✅ Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="@icandoit69#",
    database="major_project"
)

# ✅ Create a cursor
cursor = conn.cursor()

# ✅ Now you can execute queries using `cursor.execute(...)`


# 1) Connect to MySQL
connection = pymysql.connect(
    host='localhost',        # or your server's host
    user='root',             # your MySQL username
    password='@icandoit69#',
    db='major_project'       # name of the DB containing your table
)

try:
    # 2) Use pandas read_sql_query with an *open connection*
    df = pd.read_sql_query("SELECT * FROM youth_survey", connection)
    print(df.head())
finally:
    connection.close()
print(df[['date_of_birth', 'financial_importance']].head())


print(f"✅ Total rows in DataFrame: {len(df)}")  # Debug: Total fetched
