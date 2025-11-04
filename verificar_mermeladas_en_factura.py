# verificar_mermeladas_en_factura.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 VERIFICANDO MERMELADAS EN ITEMS_FACTURA...")

# Buscar las mermeladas en items_factura
cur.execute("""
    SELECT
        i.id,
        i.nombre_leido,
        i.codigo_leido,
        i.producto_maestro_id,
        i.precio_pagado,
        i.cantidad,
        pm.nombre_normalizado,
        pm.codigo_ean
    FROM items_factura i
    LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
    WHERE i.factura_id = 22
      AND (
          LOWER(i.nombre_leido) LIKE '%mermelada%'
          OR LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
      )
""")

mermeladas = cur.fetchall()

if mermeladas:
    print(f"\n✅ {len(mermeladas)} MERMELADAS encontradas en items_factura:\n")
    for m in mermeladas:
        print(f"Item ID: {m[0]}")
        print(f"  Nombre OCR: {m[1]}")
        print(f"  Código OCR: {m[2]}")
        print(f"  Producto Maestro ID: {m[3]}")
        print(f"  Nombre en maestro: {m[6]}")
        print(f"  EAN: {m[7]}")
        print(f"  Precio: ${m[4]:,}")
        print(f"  Cantidad: {m[5]}")
        print()
else:
    print("\n❌ NO se encontraron mermeladas en items_factura")

cur.close()
conn.close()
