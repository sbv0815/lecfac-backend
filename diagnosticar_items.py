# diagnosticar_items.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 DIAGNÓSTICO FACTURA #32\n")

# Ver items
cur.execute("""
    SELECT id, nombre_leido, precio_pagado, cantidad,
           codigo_leido, producto_canonico_id, variante_id, producto_maestro_id
    FROM items_factura
    WHERE factura_id = 32
""")

items = cur.fetchall()

print(f"📦 Items encontrados: {len(items)}\n")

for item in items:
    print(f"Item ID: {item[0]}")
    print(f"  Nombre: {item[1]}")
    print(f"  Precio: ${item[2]:,}")
    print(f"  Cantidad: {item[3]}")
    print(f"  Código: {item[4]}")
    print(f"  Canónico ID: {item[5]} {'✅' if item[5] else '❌'}")
    print(f"  Variante ID: {item[6]} {'✅' if item[6] else '❌'}")
    print(f"  Maestro ID: {item[7]} {'✅' if item[7] else '❌'}")
    print()

cur.close()
conn.close()
