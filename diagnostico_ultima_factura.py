"""
🔍 DIAGNÓSTICO: Última Factura Procesada
==========================================
Ver qué productos se guardaron realmente en la base de datos
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Conectar a PostgreSQL
conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cursor = conn.cursor()

print("="*80)
print("🔍 DIAGNÓSTICO: ÚLTIMA FACTURA PROCESADA")
print("="*80)

# 1. Ver última factura procesada
print("\n📄 ÚLTIMA FACTURA:")
cursor.execute("""
    SELECT
        id,
        establecimiento_id,
        fecha_factura,
        total_factura,
        fecha_cargue,
        estado
    FROM facturas
    ORDER BY fecha_cargue DESC
    LIMIT 1
""")

factura = cursor.fetchone()
if not factura:
    print("❌ No hay facturas en la base de datos")
    exit()

factura_id = factura[0]
print(f"   ID: {factura_id}")
print(f"   Establecimiento: {factura[1]}")
print(f"   Fecha: {factura[2]}")
print(f"   Total: ${factura[3]:,}" if factura[3] else "   Total: No registrado")
print(f"   Estado: {factura[5]}")

# 2. Ver productos de esa factura
print(f"\n📦 PRODUCTOS DE LA FACTURA {factura_id}:")
cursor.execute("""
    SELECT
        ite.id,
        ite.producto_maestro_id,
        pm.codigo_ean,
        pm.nombre_normalizado,
        ite.precio_pagado,
        ite.cantidad,
        (ite.precio_pagado * ite.cantidad) as subtotal
    FROM items_factura ite
    LEFT JOIN productos_maestros pm ON ite.producto_maestro_id = pm.id
    WHERE ite.factura_id = %s
    ORDER BY ite.id
""", (factura_id,))

productos = cursor.fetchall()
print(f"\n   Total productos guardados: {len(productos)}")
print("="*80)

for i, prod in enumerate(productos, 1):
    print(f"\n{i}. ID: {prod[0]} | Maestro ID: {prod[1]}")
    print(f"   EAN: {prod[2] or 'SIN EAN'}")
    print(f"   Nombre: {prod[3]}")
    print(f"   Precio: ${prod[4]:,} x {prod[5]} = ${prod[6]:,}")

# 3. Ver productos maestros recién creados
print("\n" + "="*80)
print("📊 PRODUCTOS MAESTROS RECIENTES (últimos 30):")
cursor.execute("""
    SELECT
        id,
        codigo_ean,
        nombre_normalizado,
        marca,
        categoria,
        total_reportes,
        precio_promedio_global,
        primera_vez_reportado
    FROM productos_maestros
    ORDER BY primera_vez_reportado DESC
    LIMIT 30
""")

maestros = cursor.fetchall()
for prod in maestros:
    print(f"\n   ID: {prod[0]}")
    print(f"   EAN: {prod[1] or 'SIN EAN'}")
    print(f"   Nombre: {prod[2]}")
    print(f"   Marca: {prod[3] or 'Sin marca'}")
    print(f"   Categoría: {prod[4] or 'Sin categoría'}")
    print(f"   Reportes: {prod[5]} | Precio prom: ${prod[6]:,}")
    print(f"   Creado: {prod[7]}")

# 4. Detectar posibles problemas
print("\n" + "="*80)
print("⚠️ DETECCIÓN DE PROBLEMAS:")
print("="*80)

# Productos con nombres sospechosos
cursor.execute("""
    SELECT
        id,
        nombre_normalizado
    FROM productos_maestros
    WHERE
        LENGTH(nombre_normalizado) < 5
        OR nombre_normalizado ILIKE '%ahorra%'
        OR nombre_normalizado ILIKE '%precio%'
        OR nombre_normalizado ILIKE '%final%'
        OR nombre_normalizado ILIKE '%descuento%'
        OR nombre_normalizado ILIKE '%promocion%'
        OR nombre_normalizado ILIKE '%espaci%'
    ORDER BY primera_vez_reportado DESC
    LIMIT 20
""")

basura = cursor.fetchall()
if basura:
    print(f"\n🗑️ PRODUCTOS BASURA DETECTADOS ({len(basura)}):")
    for prod in basura:
        print(f"   ID {prod[0]}: {prod[1]}")
else:
    print("\n✅ No se detectó texto basura")

# Productos duplicados por nombre similar
cursor.execute("""
    SELECT
        nombre_normalizado,
        COUNT(*) as cantidad,
        STRING_AGG(id::text, ', ') as ids
    FROM productos_maestros
    WHERE LENGTH(nombre_normalizado) > 5
    GROUP BY LOWER(TRIM(nombre_normalizado))
    HAVING COUNT(*) > 1
    ORDER BY cantidad DESC
    LIMIT 10
""")

duplicados = cursor.fetchall()
if duplicados:
    print(f"\n⚠️ POSIBLES DUPLICADOS DETECTADOS:")
    for dup in duplicados:
        print(f"   '{dup[0]}' aparece {dup[1]} veces - IDs: {dup[2]}")
else:
    print("\n✅ No se detectaron duplicados obvios")

# 5. Ver códigos PLU guardados
print("\n" + "="*80)
print("🏪 CÓDIGOS PLU GUARDADOS:")
cursor.execute("""
    SELECT
        ce.id,
        ce.codigo_local,
        ce.tipo_codigo,
        ce.veces_visto,
        e.nombre_normalizado as establecimiento,
        pm.nombre_normalizado
    FROM codigos_establecimiento ce
    JOIN establecimientos e ON ce.establecimiento_id = e.id
    JOIN productos_maestros pm ON ce.producto_maestro_id = pm.id
    ORDER BY ce.primera_vez_visto DESC
    LIMIT 20
""")

plus = cursor.fetchall()
if plus:
    for plu in plus:
        print(f"\n   PLU: {plu[1]} ({plu[2]})")
        print(f"   Establecimiento: {plu[4]}")
        print(f"   Producto: {plu[5]}")
        print(f"   Visto: {plu[3]} veces")
else:
    print("\n   ℹ️ No hay códigos PLU guardados")

cursor.close()
conn.close()

print("\n" + "="*80)
print("✅ DIAGNÓSTICO COMPLETADO")
print("="*80)
