# verificar_productos_maestros_incorrectos.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 VERIFICANDO PRODUCTOS MAESTROS SOSPECHOSOS...\n")

# Verificar el producto "Mermelada Mo"
print("="*60)
print("1. Producto: Mermelada Mo (ID: 287)")
print("="*60)

cur.execute("""
    SELECT
        id,
        codigo_ean,
        nombre_normalizado,
        marca,
        categoria,
        total_reportes,
        primera_vez_reportado
    FROM productos_maestros
    WHERE id = 287
""")

prod1 = cur.fetchone()
if prod1:
    print(f"ID: {prod1[0]}")
    print(f"EAN: {prod1[1]}")
    print(f"Nombre: {prod1[2]}")
    print(f"Marca: {prod1[3]}")
    print(f"Categoría: {prod1[4]}")
    print(f"Reportes: {prod1[5]}")
    print(f"Primera vez: {prod1[6]}")

# Ver en qué facturas aparece
print("\n📄 Facturas donde aparece este producto:")
cur.execute("""
    SELECT
        f.id,
        f.usuario_id,
        f.establecimiento,
        i.nombre_leido,
        i.codigo_leido,
        i.precio_pagado
    FROM items_factura i
    JOIN facturas f ON i.factura_id = f.id
    WHERE i.producto_maestro_id = 287
    ORDER BY f.id
""")

for item in cur.fetchall():
    print(f"  • Factura #{item[0]} (Usuario {item[1]}, {item[2]})")
    print(f"    Nombre OCR: {item[3]}")
    print(f"    Código OCR: {item[4]}")
    print(f"    Precio: ${item[5]:,}")

# Verificar el producto "Mermelada Fresa"
print("\n" + "="*60)
print("2. Producto: Mermelada Fresa (ID: 322)")
print("="*60)

cur.execute("""
    SELECT
        id,
        codigo_ean,
        nombre_normalizado,
        marca,
        categoria,
        total_reportes,
        primera_vez_reportado
    FROM productos_maestros
    WHERE id = 322
""")

prod2 = cur.fetchone()
if prod2:
    print(f"ID: {prod2[0]}")
    print(f"EAN: {prod2[1]}")
    print(f"Nombre: {prod2[2]}")
    print(f"Marca: {prod2[3]}")
    print(f"Categoría: {prod2[4]}")
    print(f"Reportes: {prod2[5]}")
    print(f"Primera vez: {prod2[6]}")

# Ver en qué facturas aparece
print("\n📄 Facturas donde aparece este producto:")
cur.execute("""
    SELECT
        f.id,
        f.usuario_id,
        f.establecimiento,
        i.nombre_leido,
        i.codigo_leido,
        i.precio_pagado
    FROM items_factura i
    JOIN facturas f ON i.factura_id = f.id
    WHERE i.producto_maestro_id = 322
    ORDER BY f.id
""")

for item in cur.fetchall():
    print(f"  • Factura #{item[0]} (Usuario {item[1]}, {item[2]})")
    print(f"    Nombre OCR: {item[3]}")
    print(f"    Código OCR: {item[4]}")
    print(f"    Precio: ${item[5]:,}")

# Buscar si existen otros productos con los códigos que leyó el OCR
print("\n" + "="*60)
print("3. ¿Existen productos con los códigos que leyó el OCR?")
print("="*60)

print("\n🔍 Buscando EAN: 7702299304782 (Chocolate BI)")
cur.execute("""
    SELECT id, codigo_ean, nombre_normalizado
    FROM productos_maestros
    WHERE codigo_ean = '7702299304782'
""")

chocolate = cur.fetchone()
if chocolate:
    print(f"  ✅ EXISTE: ID {chocolate[0]} - {chocolate[2]}")
else:
    print(f"  ❌ NO EXISTE en productos_maestros")

print("\n🔍 Buscando EAN: 7703260002563 (Bimbojaldres)")
cur.execute("""
    SELECT id, codigo_ean, nombre_normalizado
    FROM productos_maestros
    WHERE codigo_ean = '7703260002563'
""")

bimbo = cur.fetchone()
if bimbo:
    print(f"  ✅ EXISTE: ID {bimbo[0]} - {bimbo[2]}")
else:
    print(f"  ❌ NO EXISTE en productos_maestros")

cur.close()
conn.close()

print("\n" + "="*60)
print("📊 CONCLUSIÓN")
print("="*60)
print("El ProductResolver está haciendo MATCHING INCORRECTO")
print("Está asignando productos equivocados a los códigos leídos")
