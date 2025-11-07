"""
Ver estructura de tabla items_factura
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("=" * 80)
print("🔍 ESTRUCTURA DE TABLA items_factura")
print("=" * 80)

cur.execute("""
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'items_factura'
ORDER BY ordinal_position
""")

columnas = cur.fetchall()

print("\nColumnas disponibles:")
for col in columnas:
    nombre, tipo, nullable = col
    null_info = "NULL" if nullable == "YES" else "NOT NULL"
    print(f"   • {nombre:30} {tipo:20} {null_info}")

print("\n" + "=" * 80)

cur.close()
conn.close()
