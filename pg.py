# import pandas as pd

# MD5_FILE = "md5.csv"
# PGMD5_FILE = "pgmd5.csv"
# OUTPUT_FILE = "missing_md5.csv"

# # Read files
# df_md5 = pd.read_csv(MD5_FILE, dtype=str)
# df_pgmd5 = pd.read_csv(PGMD5_FILE, dtype=str)

# # Clean MD5 values
# df_md5["md5"] = df_md5["md5"].str.strip()
# df_pgmd5["md5"] = df_pgmd5["md5"].str.strip()

# # Create set of MD5s already present in pgmd5.csv
# existing_md5 = set(df_pgmd5["md5"].dropna())

# # Keep rows from md5.csv whose MD5 is NOT in pgmd5.csv
# missing = df_md5[
#     ~df_md5["md5"].isin(existing_md5)
# ]

# # Keep only category and md5
# missing = missing[["category", "md5"]]

# # Save
# missing.to_csv(OUTPUT_FILE, index=False)

# print(f"Total rows in md5.csv: {len(df_md5)}")
# print(f"Total MD5s in pgmd5.csv: {len(existing_md5)}")
# print(f"Missing rows: {len(missing)}")
# print(f"Saved to: {OUTPUT_FILE}")


import requests

DEFAULT_QDRANT_URL = "http://37.27.101.243:6333"

response = requests.get(f"{DEFAULT_QDRANT_URL}/collections")
response.raise_for_status()

collections = response.json()["result"]["collections"]

for collection in collections:
    print(collection["name"])


