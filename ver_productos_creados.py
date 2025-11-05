# ver_productos_creados.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 PRODUCTOS CREADOS HOY\n")

# Canónicos creados hoy
cur.execute("""
    SELECT id, nombre_oficial, ean_principal, fecha_creacion
    FROM productos_canonicos
    WHERE DATE(fecha_creacion) = CURRENT_DATE
    ORDER BY id DESC
    LIMIT 10
""")

canonicos = cur.fetchall()
print(f"📦 Productos canónicos creados hoy: {len(canonicos)}\n")

for c in canonicos:
    print(f"ID: {c[0]} | {c[1]} | EAN: {c[2]} | {c[3]}")

# Variantes creadas hoy
cur.execute("""
    SELECT id, nombre_en_recibo, codigo, establecimiento, producto_canonico_id
    FROM productos_variantes
    WHERE DATE(primera_vez_visto) = CURRENT_DATE
    ORDER BY id DESC
    LIMIT 10
""")

variantes = cur.fetchall()
print(f"\n📦 Variantes creadas hoy: {len(variantes)}\n")

for v in variantes:
    print(f"ID: {v[0]} | {v[1][:30]} | Código: {v[2]} | {v[3]} | Canónico: {v[4]}")

# Maestros creados hoy
cur.execute("""
    SELECT id, nombre_normalizado, codigo_ean, producto_canonico_id
    FROM productos_maestros
    WHERE DATE(primera_vez_reportado) = CURRENT_DATE
    ORDER BY id DESC
    LIMIT 10
""")

maestros = cur.fetchall()
print(f"\n📦 Maestros creados hoy: {len(maestros)}\n")

for m in maestros:
    print(f"ID: {m[0]} | {m[1][:30]} | EAN: {m[2]} | Canónico: {m[3]}")

cur.close()
conn.close()
