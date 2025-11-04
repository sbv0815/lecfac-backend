# investigar_curuba.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 INVESTIGANDO: ¿De dónde viene CURUBA?\n")
print("="*70)

# 1. Buscar en productos_maestros
print("\n📦 PRODUCTOS_MAESTROS:")
cur.execute("""
    SELECT id, codigo_ean, nombre_normalizado, marca, categoria,
           producto_canonico_id, total_reportes
    FROM productos_maestros
    WHERE codigo_ean = '24030427' OR nombre_normalizado ILIKE '%curuba%'
""")
for row in cur.fetchall():
    print(f"   ID: {row[0]}")
    print(f"   EAN: {row[1]}")
    print(f"   Nombre: {row[2]}")
    print(f"   Marca: {row[3]}")
    print(f"   Categoría: {row[4]}")
    print(f"   Canónico ID: {row[5]}")
    print(f"   Total reportes: {row[6]}")
    print()

# 2. Buscar en productos_canonicos
print("\n🎯 PRODUCTOS_CANÓNICOS:")
cur.execute("""
    SELECT id, nombre_oficial, marca, categoria, ean_principal, total_variantes
    FROM productos_canonicos
    WHERE ean_principal = '24030427' OR nombre_oficial ILIKE '%curuba%'
""")
for row in cur.fetchall():
    print(f"   ID: {row[0]}")
    print(f"   Nombre: {row[1]}")
    print(f"   Marca: {row[2]}")
    print(f"   Categoría: {row[3]}")
    print(f"   EAN: {row[4]}")
    print(f"   Variantes: {row[5]}")
    print()

# 3. Buscar en productos_variantes
print("\n🔄 PRODUCTOS_VARIANTES:")
cur.execute("""
    SELECT id, producto_canonico_id, codigo, tipo_codigo, nombre_en_recibo,
           establecimiento, veces_reportado
    FROM productos_variantes
    WHERE codigo = '24030427' OR nombre_en_recibo ILIKE '%curuba%'
""")
for row in cur.fetchall():
    print(f"   ID: {row[0]}")
    print(f"   Canónico ID: {row[1]}")
    print(f"   Código: {row[2]}")
    print(f"   Tipo: {row[3]}")
    print(f"   Nombre: {row[4]}")
    print(f"   Establecimiento: {row[5]}")
    print(f"   Reportes: {row[6]}")
    print()

# 4. Buscar en items_factura de factura 25
print("\n📄 ITEMS_FACTURA (FACTURA #25):")
cur.execute("""
    SELECT id, producto_maestro_id, producto_canonico_id, variante_id,
           codigo_leido, nombre_leido, precio_pagado, cantidad
    FROM items_factura
    WHERE factura_id = 25
    ORDER BY id
""")
items = cur.fetchall()
print(f"   Total items en factura 25: {len(items)}\n")

for item in items:
    print(f"   Item #{item[0]}:")
    print(f"      Maestro: {item[1]}")
    print(f"      Canónico: {item[2]}")
    print(f"      Variante: {item[3]}")
    print(f"      Código leído: {item[4]}")
    print(f"      Nombre: {item[5]}")
    print(f"      Precio: ${item[6]:,}")
    print(f"      Cantidad: {item[7]}")
    print()

# 5. Buscar curuba específicamente en items
print("\n🔍 ¿CURUBA ESTÁ EN ITEMS_FACTURA 25?")
cur.execute("""
    SELECT COUNT(*)
    FROM items_factura
    WHERE factura_id = 25 AND (
        codigo_leido = '24030427'
        OR nombre_leido ILIKE '%curuba%'
    )
""")
count = cur.fetchone()[0]
print(f"   {'✅ SÍ' if count > 0 else '❌ NO'} - {count} items con Curuba")

# 6. Ver de dónde vino al inventario
print("\n📦 ¿CURUBA EN INVENTARIO USUARIO 3?")
cur.execute("""
    SELECT id, producto_maestro_id, cantidad_actual, precio_ultima_compra,
           fecha_ultima_compra, ultima_factura_id, establecimiento
    FROM inventario_usuario
    WHERE usuario_id = 3 AND (
        producto_maestro_id IN (
            SELECT id FROM productos_maestros
            WHERE codigo_ean = '24030427' OR nombre_normalizado ILIKE '%curuba%'
        )
    )
""")
inv = cur.fetchone()
if inv:
    print(f"   ✅ SÍ está en inventario:")
    print(f"      ID inventario: {inv[0]}")
    print(f"      Producto maestro: {inv[1]}")
    print(f"      Cantidad: {inv[2]}")
    print(f"      Precio: ${inv[3]:,}")
    print(f"      Última compra: {inv[4]}")
    print(f"      Última factura: {inv[5]}")
    print(f"      Establecimiento: {inv[6]}")
else:
    print(f"   ❌ NO está en inventario")

cur.close()
conn.close()

print("\n" + "="*70)
