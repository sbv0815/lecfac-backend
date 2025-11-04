# verificar_items_factura22.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 VERIFICANDO ITEMS DE LA FACTURA 22...")

# Ver todos los items de la factura
cur.execute("""
    SELECT
        id,
        nombre_leido,
        codigo_leido,
        producto_maestro_id,
        precio_pagado,
        cantidad
    FROM items_factura
    WHERE factura_id = 22
    ORDER BY id
""")

items = cur.fetchall()

print(f"\n📦 Total items en factura 22: {len(items)}\n")

# Buscar las mermeladas
mermeladas = []
for item in items:
    nombre = item[1].lower()
    if 'mermelada' in nombre or 'fresa' in nombre or 'mora' in nombre:
        mermeladas.append(item)
        print(f"🍓 MERMELADA ENCONTRADA:")
        print(f"   ID: {item[0]}")
        print(f"   Nombre OCR: {item[1]}")
        print(f"   Código: {item[2]}")
        print(f"   Producto Maestro ID: {item[3]}")
        print(f"   Precio: ${item[4]:,}")
        print()

if not mermeladas:
    print("❌ NO se encontraron mermeladas en los items de la factura 22")
    print("\n🚨 ESTO SIGNIFICA que las mermeladas en el inventario NO vienen de esta factura")

print("\n" + "="*60)
print("📊 RESUMEN")
print("="*60)
print(f"Items en factura: {len(items)}")
print(f"Mermeladas encontradas: {len(mermeladas)}")

# Verificar de dónde vienen las mermeladas del inventario
print("\n🔍 Buscando origen de las mermeladas en inventario...")

cur.execute("""
    SELECT
        inv.id,
        pm.nombre_normalizado,
        pm.codigo_ean,
        inv.ultima_factura_id,
        inv.numero_compras,
        inv.fecha_ultima_compra
    FROM inventario_usuario inv
    JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
    WHERE inv.usuario_id = 2
      AND LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
""")

mermeladas_inv = cur.fetchall()

if mermeladas_inv:
    print(f"\n🍓 {len(mermeladas_inv)} mermeladas en inventario:")
    for m in mermeladas_inv:
        print(f"\n   Inventario ID: {m[0]}")
        print(f"   Nombre: {m[1]}")
        print(f"   EAN: {m[2]}")
        print(f"   Última factura: {m[3]}")
        print(f"   Número de compras: {m[4]}")
        print(f"   Última compra: {m[5]}")

cur.close()
conn.close()
