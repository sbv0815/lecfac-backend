# buscar_logs_procesamiento.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

# Ver la factura 33
cur.execute("""
    SELECT id, nombre_leido, codigo_leido,
           producto_canonico_id, variante_id, producto_maestro_id,
           precio_pagado, cantidad
    FROM items_factura
    WHERE factura_id = 33
""")

print("ITEMS DE FACTURA #33:\n")
for row in cur.fetchall():
    print(f"Item {row[0]}: {row[1]}")
    print(f"  Código: {row[2]}")
    print(f"  Canónico: {row[3]} | Variante: {row[4]} | Maestro: {row[5]}")
    print(f"  Precio: ${row[6]:,} x {row[7]}")
    print()

# Ver si existen productos con esos códigos
cur.execute("""
    SELECT codigo, establecimiento, producto_canonico_id
    FROM productos_variantes
    WHERE codigo IN ('1183777', '2322571')
    ORDER BY id DESC
    LIMIT 5
""")

print("\nVARIANTES CON ESOS CÓDIGOS:")
for row in cur.fetchall():
    print(f"Código: {row[0]} | {row[1]} | Canónico: {row[2]}")

cur.close()
conn.close()
