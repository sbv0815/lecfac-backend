#!/usr/bin/env python3
"""
CORRECCIÓN FINAL - FACTURA #2
Multiplicar precios por 100
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys

print("=" * 70)
print("🔧 CORRECCIÓN DE PRECIOS - FACTURA #2")
print("=" * 70)

DATABASE_URL = input("\n📎 DATABASE_URL: ").strip()

if not DATABASE_URL or not DATABASE_URL.startswith("postgres"):
    print("❌ ERROR: URL inválida")
    sys.exit(1)

print("\n📡 Conectando...")
try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    print("✅ Conexión exitosa")
except Exception as e:
    print(f"❌ ERROR: {e}")
    sys.exit(1)

# ==========================================
# DIAGNÓSTICO
# ==========================================
print("\n📊 DIAGNÓSTICO ACTUAL:")
print("-" * 70)

try:
    cursor.execute("""
        SELECT
            f.id,
            f.establecimiento,
            f.total_factura,
            f.usuario_id,
            COUNT(i.id) as num_items,
            SUM(i.precio_pagado * i.cantidad) as suma_items
        FROM facturas f
        LEFT JOIN items_factura i ON i.factura_id = f.id
        WHERE f.id = 2
        GROUP BY f.id, f.establecimiento, f.total_factura, f.usuario_id
    """)

    factura = cursor.fetchone()

    if not factura:
        print("❌ No se encontró la factura #2")
        sys.exit(1)

    print(f"\n📄 FACTURA #2:")
    print(f"   Establecimiento: {factura['establecimiento']}")
    print(f"   Usuario ID: {factura['usuario_id']}")
    print(f"   Total actual: ${factura['total_factura']:,.2f}")
    print(f"   Suma items: ${factura['suma_items']:,.2f}")
    print(f"   Número items: {factura['num_items']}")

    # Ver ejemplos de items
    cursor.execute("""
        SELECT id, nombre_leido, precio_pagado, cantidad
        FROM items_factura
        WHERE factura_id = 2
        ORDER BY id
        LIMIT 5
    """)

    items = cursor.fetchall()
    print(f"\n📦 EJEMPLOS DE ITEMS (primeros 5):")
    for item in items:
        print(f"   #{item['id']:4d} | {item['nombre_leido'][:35]:35} | ${item['precio_pagado']:>10,.2f} x{item['cantidad']}")

    # Análisis
    total_actual = float(factura['total_factura'])
    total_correcto = 284220.0
    factor = total_correcto / total_actual if total_actual > 0 else 0

    print(f"\n🔍 ANÁLISIS:")
    print(f"   Total actual: ${total_actual:,.2f}")
    print(f"   Total correcto: ${total_correcto:,.0f}")
    print(f"   Factor necesario: x{factor:.2f}")

    if abs(factor - 100) < 10:
        print(f"\n✅ Necesita multiplicarse por 100")
    elif abs(factor - 1) < 0.1:
        print(f"\n✅ Ya está correcto")
        sys.exit(0)
    else:
        print(f"\n⚠️ Factor inesperado: {factor:.2f}")
        respuesta = input("   ¿Continuar? (SI/NO): ")
        if respuesta != "SI":
            sys.exit(0)

    usuario_id = factura['usuario_id']

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==========================================
# CONFIRMACIÓN
# ==========================================
print("\n" + "=" * 70)
print("⚠️  CORRECCIÓN")
print("=" * 70)
print("\n¿Qué se va a hacer?")
print(f"1. Multiplicar precios de items por 100")
print(f"2. Actualizar total_factura a $284,220")
print(f"3. Corregir inventario del usuario #{usuario_id}")
print(f"4. Corregir productos_maestros relacionados")
print()

confirmacion = input("¿Continuar? (escribe SI): ").strip()

if confirmacion != "SI":
    print("❌ Cancelado")
    sys.exit(0)

# ==========================================
# CORRECCIÓN
# ==========================================
print("\n🔧 Ejecutando corrección...")
print("-" * 70)

try:
    # 1. Corregir items_factura
    print("\n1️⃣ Corrigiendo items_factura...")
    cursor.execute("""
        UPDATE items_factura
        SET precio_pagado = precio_pagado * 100
        WHERE factura_id = 2
    """)
    items_corr = cursor.rowcount
    print(f"   ✅ {items_corr} items corregidos")

    # 2. Corregir total_factura
    print("\n2️⃣ Corrigiendo total_factura...")
    cursor.execute("""
        UPDATE facturas
        SET total_factura = 284220
        WHERE id = 2
    """)
    print(f"   ✅ Total actualizado a $284,220")

    # 3. Corregir inventario
    print(f"\n3️⃣ Corrigiendo inventario del usuario #{usuario_id}...")
    cursor.execute("""
        UPDATE inventario_usuarios
        SET ultimo_precio = ultimo_precio * 100
        WHERE usuario_id = %s
        AND ultimo_precio < 1000
    """, (usuario_id,))
    inv_corr = cursor.rowcount
    print(f"   ✅ {inv_corr} items de inventario corregidos")

    # 4. Corregir productos_maestros
    print("\n4️⃣ Corrigiendo productos_maestros...")
    cursor.execute("""
        UPDATE productos_maestros
        SET precio_promedio_global = precio_promedio_global * 100
        WHERE id IN (
            SELECT DISTINCT producto_maestro_id
            FROM items_factura
            WHERE factura_id = 2
            AND producto_maestro_id IS NOT NULL
        )
        AND precio_promedio_global < 10000
    """)
    prod_corr = cursor.rowcount
    print(f"   ✅ {prod_corr} productos corregidos")

    # COMMIT
    print("\n💾 Guardando cambios...")
    conn.commit()
    print("   ✅ COMMIT exitoso")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\n🔄 Revertiendo...")
    conn.rollback()
    print("   ✅ ROLLBACK exitoso")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ==========================================
# VERIFICACIÓN
# ==========================================
print("\n" + "=" * 70)
print("📊 VERIFICACIÓN FINAL")
print("-" * 70)

try:
    cursor.execute("""
        SELECT
            f.id,
            f.total_factura,
            SUM(i.precio_pagado * i.cantidad) as suma_items,
            f.total_factura - SUM(i.precio_pagado * i.cantidad) as diferencia,
            COUNT(i.id) as num_items
        FROM facturas f
        JOIN items_factura i ON i.factura_id = f.id
        WHERE f.id = 2
        GROUP BY f.id, f.total_factura
    """)

    resultado = cursor.fetchone()

    print(f"\n✅ FACTURA #2 CORREGIDA:")
    print(f"   Total factura: ${resultado['total_factura']:,}")
    print(f"   Suma items: ${resultado['suma_items']:,}")
    print(f"   Diferencia: ${resultado['diferencia']:,}")
    print(f"   Número items: {resultado['num_items']}")

    if abs(resultado['diferencia']) < 100:
        print(f"\n🎉 ¡CORRECCIÓN EXITOSA!")
    else:
        print(f"\n⚠️ Diferencia: ${resultado['diferencia']:,}")

    # Ejemplos corregidos
    cursor.execute("""
        SELECT nombre_leido, precio_pagado, cantidad
        FROM items_factura
        WHERE factura_id = 2
        ORDER BY id
        LIMIT 5
    """)

    print(f"\n📦 EJEMPLOS CORREGIDOS:")
    for item in cursor.fetchall():
        print(f"   • {item['nombre_leido'][:35]:35} | ${item['precio_pagado']:>8,}")

except Exception as e:
    print(f"❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

cursor.close()
conn.close()

print("\n" + "=" * 70)
print("✅ PROCESO COMPLETADO")
print("=" * 70)
print("\nRefresh el dashboard para ver los cambios.")
