import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Define the scope
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials
credentials = ServiceAccountCredentials.from_json_keyfile_name(
    r"C:\Users\Richa Tripathi\Downloads\august-valor-454820-n2-ce6c983f3c35.json", scope
)
# Authenticate Google Sheets
client = gspread.authorize(credentials)

# Open Google Sheet (Change the name accordingly)
spreadsheet = client.open("YOUTH SURVEY 2025").worksheet("Form Responses 1")
# Read data into Pandas DataFrame
data = spreadsheet.get_all_records()
df = pd.DataFrame(data)

# Debugging: Print column names to check if 'date_of_birth' exists
print("Columns in DataFrame:", df.columns)

# Ensure column exists before conversion
if 'date_of_birth' in df.columns:
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce', dayfirst=True)
else:
    print("Column 'date_of_birth' not found in Google Sheets data.")

# Display first few rows
print(df.head())
print(f"✅ Final rows before insertion: {df.shape[0]}")


