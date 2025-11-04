# verificar_tablas.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 VERIFICANDO TABLAS EN LA BASE DE DATOS...")

cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
""")

tablas = cur.fetchall()
print(f"\n📋 Tablas encontradas: {len(tablas)}\n")

for tabla in tablas:
    # Contar registros en cada tabla
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tabla[0]}")
        count = cur.fetchone()[0]
        print(f"  ✅ {tabla[0]:<30} ({count:,} registros)")
    except Exception as e:
        print(f"  ❌ {tabla[0]:<30} (Error: {e})")

cur.close()
conn.close()
