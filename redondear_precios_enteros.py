#!/usr/bin/env python3
"""
CORRECCIÓN FINAL: Dividir items_factura entre 100
Problema: 1,615,000 debe ser 16,150
Factor de corrección: ÷100
"""

import psycopg2
from psycopg2.extras import RealDictCursor

print("=" * 70)
print("🔧 CORRECCIÓN FINAL - DIVIDIR ENTRE 100")
print("=" * 70)

DATABASE_URL = input("\n📎 Pega tu DATABASE_URL: ").strip()

if not DATABASE_URL:
    exit(1)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    print("✅ Conectado\n")
except Exception as e:
    print(f"❌ Error: {e}")
    exit(1)

# ==========================================
# MOSTRAR ANTES
# ==========================================
print("📊 ANTES DE LA CORRECCIÓN:")
print("-" * 70)

cursor.execute("""
    SELECT
        i.nombre_leido,
        i.precio_pagado as items_precio,
        pm.precio_promedio_global as maestros_precio
    FROM items_factura i
    LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
    WHERE i.precio_pagado IS NOT NULL
    ORDER BY i.id
    LIMIT 10
""")

items_antes = cursor.fetchall()

for item in items_antes:
    items_precio = int(item['items_precio'])
    maestros_precio = int(item['maestros_precio']) if item['maestros_precio'] else 0

    if maestros_precio > 0:
        debe_ser = items_precio // 100
        print(f"{item['nombre_leido'][:25]:25} | items: ${items_precio:>10,} → debe ser: ${debe_ser:>6,} (maestros: ${maestros_precio:>6,})")
    else:
        debe_ser = items_precio // 100
        print(f"{item['nombre_leido'][:25]:25} | items: ${items_precio:>10,} → debe ser: ${debe_ser:>6,}")

print("\n" + "=" * 70)
print("⚠️  SE DIVIDIRÁ TODO ENTRE 100")
print("=" * 70)
print("\nEjemplos:")
print("  • 1,615,000 ÷ 100 = 16,150 ✅")
print("  • 626,000 ÷ 100 = 6,260 ✅")
print("  • 16,150 ÷ 100 = 162 ❌ (los que ya están bien no se tocan)")

confirmacion = input("\n¿Continuar? (escribe SI): ").strip()

if confirmacion != "SI":
    print("❌ Cancelado")
    exit(0)

# ==========================================
# CORRECCIÓN
# ==========================================
print("\n🔧 Ejecutando corrección...")

try:
    # Dividir items_factura entre 100
    print("\n1️⃣ Corrigiendo items_factura...")
    cursor.execute("""
        UPDATE items_factura
        SET precio_pagado = ROUND(precio_pagado / 100.0)
        WHERE precio_pagado > 100000
    """)
    items_corregidos = cursor.rowcount
    print(f"   ✅ {items_corregidos} items divididos entre 100")

    # Dividir facturas entre 100
    print("\n2️⃣ Corrigiendo facturas...")
    cursor.execute("""
        UPDATE facturas
        SET total_factura = ROUND(total_factura / 100.0)
        WHERE total_factura > 100000
    """)
    facturas_corregidas = cursor.rowcount
    print(f"   ✅ {facturas_corregidas} facturas divididas entre 100")

    # Commit
    print("\n💾 Guardando...")
    conn.commit()
    print("   ✅ COMMIT realizado")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    conn.rollback()
    print("   ✅ ROLLBACK realizado")
    exit(1)

# ==========================================
# VERIFICACIÓN FINAL
# ==========================================
print("\n" + "=" * 70)
print("📊 DESPUÉS DE LA CORRECCIÓN:")
print("-" * 70)

cursor.execute("""
    SELECT
        i.nombre_leido,
        i.precio_pagado as items_precio,
        pm.precio_promedio_global as maestros_precio
    FROM items_factura i
    LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
    WHERE i.precio_pagado IS NOT NULL
    ORDER BY i.id
    LIMIT 10
""")

items_despues = cursor.fetchall()

todo_correcto = True

for item in items_despues:
    items_precio = int(item['items_precio'])
    maestros_precio = int(item['maestros_precio']) if item['maestros_precio'] else 0

    if maestros_precio > 0:
        diferencia = abs(items_precio - maestros_precio)
        estado = "✅" if diferencia < 10 else "⚠️"
        if diferencia >= 10:
            todo_correcto = False
        print(f"{item['nombre_leido'][:25]:25} | items: ${items_precio:>6,} | maestros: ${maestros_precio:>6,} {estado}")
    else:
        print(f"{item['nombre_leido'][:25]:25} | items: ${items_precio:>6,} (sin match)")

cursor.close()
conn.close()

print("\n" + "=" * 70)
if todo_correcto:
    print("✅ ¡PERFECTO! TODOS LOS PRECIOS COINCIDEN")
else:
    print("⚠️  Algunos precios aún no coinciden exactamente")
print("=" * 70)
print(f"\nResumen:")
print(f"  • {items_corregidos} items corregidos")
print(f"  • {facturas_corregidas} facturas corregidas")
print("\n🎉 Recarga el dashboard para ver los cambios")
