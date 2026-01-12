import mysql.connector

# ==========================
# CONFIG
# ==========================
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "medical2",
    "database": "macpractice",
    "port": 3309
}

SEARCH_VALUE = "67017"   # value to search
USE_LIKE = False                   # True for partial match

# ==========================
# CONNECT
# ==========================
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor(dictionary=True)

# ==========================
# GET ALL STRING COLUMNS
# ==========================
cursor.execute("""
    SELECT TABLE_NAME, COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = %s
      AND DATA_TYPE IN ('int')
""", (DB_CONFIG["database"],))

columns = cursor.fetchall()

print(f"🔍 Scanning {len(columns)} columns...\n")

# ==========================
# SEARCH EACH COLUMN
# ==========================
matches = []

for col in columns:
    table = col["TABLE_NAME"]
    column = col["COLUMN_NAME"]

    try:
        if USE_LIKE:
            query = f"""
                SELECT 1 FROM `{table}`
                WHERE `{column}` LIKE %s
                LIMIT 1
            """
            value = f"%{SEARCH_VALUE}%"
        else:
            query = f"""
                SELECT 1 FROM `{table}`
                WHERE `{column}` = %s
                LIMIT 1
            """
            value = SEARCH_VALUE

        cursor.execute(query, (value,))
        result = cursor.fetchone()

        if result:
            matches.append((table, column))
            print(f"✅ FOUND → {table}.{column}")

    except Exception as e:
        # Skip columns that error out (permissions, encoding issues)
        print(f"⚠️ SkIPPED {table}.{column} ({e})")

# ==========================
# SUMMARY
# ==========================
print("\n========== SUMMARY ==========")
if matches:
    for t, c in matches:
        print(f"✔ {t}.{c}")
else:
    print("❌ Value not found")

cursor.close()
conn.close()
