#!/usr/bin/env python3
"""
DIAGNÓSTICO COMPLETO - Ver todos los items de factura #2
"""

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = input("📎 DATABASE_URL: ").strip()

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor(cursor_factory=RealDictCursor)

print("\n" + "=" * 90)
print("📊 DIAGNÓSTICO COMPLETO - FACTURA #2")
print("=" * 90)

# Ver TODOS los items
cursor.execute("""
    SELECT
        id,
        nombre_leido,
        precio_pagado,
        cantidad,
        precio_pagado * cantidad as subtotal
    FROM items_factura
    WHERE factura_id = 2
    ORDER BY id
""")

items = cursor.fetchall()

print(f"\n📦 TODOS LOS ITEMS ({len(items)} productos):")
print("-" * 90)

total_calculado = 0
precios_bajos = 0  # < 100
precios_medios = 0  # 100-10000
precios_altos = 0  # > 10000

for item in items:
    precio = item['precio_pagado']
    subtotal = item['subtotal']
    total_calculado += subtotal

    # Clasificar
    if precio < 100:
        categoria = "❌ MUY BAJO"
        precios_bajos += 1
    elif precio < 10000:
        categoria = "⚠️ BAJO"
        precios_medios += 1
    else:
        categoria = "✅ OK"
        precios_altos += 1

    print(f"{item['id']:4d} | {item['nombre_leido'][:30]:30} | ${precio:>10,.0f} x{item['cantidad']} = ${subtotal:>12,.0f} {categoria}")

print("-" * 90)
print(f"\n📊 RESUMEN:")
print(f"   Total calculado: ${total_calculado:,.0f}")
print(f"   Total en BD: $2,842")
print(f"   Total correcto esperado: $284,220")
print(f"\n   Precios MUY BAJOS (< $100): {precios_bajos}")
print(f"   Precios BAJOS ($100-$10k): {precios_medios}")
print(f"   Precios OK (> $10k): {precios_altos}")

# Análisis
print(f"\n🔍 ANÁLISIS:")
if precios_bajos > 0:
    print(f"   ⚠️ Hay {precios_bajos} items con precios < $100 (deben multiplicarse x100)")
if precios_altos > 0:
    print(f"   ✅ Hay {precios_altos} items con precios normales (ya están bien)")

# Sugerencia
if precios_bajos > 0 and precios_altos == 0:
    print(f"\n✅ ACCIÓN: Multiplicar TODOS los items x100")
elif precios_bajos > 0 and precios_altos > 0:
    print(f"\n⚠️ ACCIÓN: Solo multiplicar items con precio < $100")
elif precios_altos > 0 and precios_bajos == 0:
    print(f"\n✅ Los precios de items están correctos")
    print(f"   Solo falta corregir total_factura de $2,842 a $284,220")

cursor.close()
conn.close()
