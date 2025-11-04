# diagnostico_factura_23.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 DIAGNÓSTICO FACTURA #23\n")

# Ver columnas de items_factura
print("📋 COLUMNAS EN TABLA ITEMS_FACTURA:")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'items_factura'
    ORDER BY ordinal_position
""")
for col in cur.fetchall():
    print(f"   {col[0]}: {col[1]}")

# Ver TODOS los items
print(f"\n\n📦 ITEMS EN FACTURA #23:")
cur.execute("""
    SELECT * FROM items_factura
    WHERE factura_id = 23
    ORDER BY id
""")

# Obtener nombres de columnas
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'items_factura'
    ORDER BY ordinal_position
""")
columnas_items = [c[0] for c in cur.fetchall()]

items = cur.fetchall()
print(f"   Total items: {len(items)}\n")

for item in items:
    print(f"   Item #{item[0]}:")
    for i, col in enumerate(columnas_items):
        print(f"      {col}: {item[i]}")
    print()

# Ver si hay raw_ocr_text en facturas
print("\n📝 TEXTO OCR DE LA FACTURA:")
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'facturas' AND column_name LIKE '%ocr%'
""")
columnas_ocr = cur.fetchall()
if columnas_ocr:
    print(f"   Columnas OCR encontradas: {columnas_ocr}")
    for col in columnas_ocr:
        cur.execute(f"SELECT {col[0]} FROM facturas WHERE id = 23")
        texto = cur.fetchone()[0]
        if texto:
            print(f"\n   {col[0]} (primeros 500 chars):")
            print(f"   {texto[:500]}")
else:
    print("   ⚠️ No hay columnas OCR en la tabla facturas")

# Ver processing_job
print(f"\n\n🔧 PROCESSING JOB:")
cur.execute("""
    SELECT * FROM processing_jobs WHERE factura_id = 23
""")

job = cur.fetchone()
if job:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'processing_jobs'
        ORDER BY ordinal_position
    """)
    job_cols = [c[0] for c in cur.fetchall()]

    for i, col in enumerate(job_cols):
        print(f"   {col}: {job[i]}")
else:
    print("   ⚠️ NO HAY PROCESSING JOB para esta factura")

cur.close()
conn.close()
