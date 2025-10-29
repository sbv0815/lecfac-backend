#!/usr/bin/env python3
"""
Script para corregir precios en items_factura
Los precios están multiplicados por 10,000
Necesitan dividirse entre 10,000 para volver a centavos
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Configuración de conexión
DATABASE_URL = os.environ.get("DATABASE_URL")

if not DATABASE_URL:
    print("❌ ERROR: No se encontró DATABASE_URL en las variables de entorno")
    exit(1)

print("=" * 60)
print("🔧 CORRECCIÓN DE PRECIOS EN items_factura")
print("=" * 60)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    print("\n📊 ANÁLISIS DE DATOS ACTUALES:")
    print("-" * 60)

    # 1. Verificar items_factura
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(precio_pagado) as minimo,
            MAX(precio_pagado) as maximo,
            AVG(precio_pagado) as promedio
        FROM items_factura
        WHERE precio_pagado IS NOT NULL
    """)

    stats_items = cursor.fetchone()
    print(f"\n📦 ITEMS DE FACTURA (ANTES):")
    print(f"   Total items: {stats_items['total']}")
    print(f"   Precio mínimo: ${stats_items['minimo']:,.0f}")
    print(f"   Precio máximo: ${stats_items['maximo']:,.0f}")
    print(f"   Precio promedio: ${stats_items['promedio']:,.0f}")

    # 2. Verificar facturas
    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(total_factura) as minimo,
            MAX(total_factura) as maximo,
            AVG(total_factura) as promedio
        FROM facturas
        WHERE total_factura IS NOT NULL
    """)

    stats_facturas = cursor.fetchone()
    print(f"\n📄 FACTURAS (ANTES):")
    print(f"   Total facturas: {stats_facturas['total']}")
    print(f"   Total mínimo: ${stats_facturas['minimo']:,.0f}")
    print(f"   Total máximo: ${stats_facturas['maximo']:,.0f}")
    print(f"   Total promedio: ${stats_facturas['promedio']:,.0f}")

    # 3. Muestra de precios problemáticos
    cursor.execute("""
        SELECT
            i.id,
            i.nombre_leido,
            i.precio_pagado,
            i.factura_id
        FROM items_factura i
        WHERE i.precio_pagado > 1000000
        ORDER BY i.precio_pagado DESC
        LIMIT 10
    """)

    items_malos = cursor.fetchall()
    if items_malos:
        print(f"\n⚠️  ITEMS CON PRECIOS INCORRECTOS (ejemplos):")
        for item in items_malos:
            precio_correcto = round(item['precio_pagado'] / 10000)
            print(f"   • {item['nombre_leido'][:30]:30} | ${item['precio_pagado']:>12,} → ${precio_correcto:>6,}")

    # Detectar si necesita corrección
    promedio_item = float(stats_items['promedio'] or 0)

    NECESITA_CORRECCION = promedio_item > 100000

    if not NECESITA_CORRECCION:
        print("\n✅ Los precios parecen estar correctos (no necesitan corrección)")
        print(f"   Promedio item: ${promedio_item:,.0f}")
        cursor.close()
        conn.close()
        exit(0)

    print("\n⚠️  LOS PRECIOS NECESITAN CORRECCIÓN")
    print(f"   Los precios están multiplicados por 10,000")
    print(f"   Se dividirán entre 10,000 para volver a centavos")

    respuesta = input("\n⏸️  ¿Continuar con la corrección? (escribe SI): ")

    if respuesta.upper() != "SI":
        print("❌ Corrección cancelada")
        cursor.close()
        conn.close()
        exit(0)

    print("\n🔧 INICIANDO CORRECCIÓN...")
    print("-" * 60)

    # CORRECCIÓN 1: Items de factura
    print("\n1️⃣ Corrigiendo items_factura.precio_pagado...")
    cursor.execute("""
        UPDATE items_factura
        SET precio_pagado = ROUND(precio_pagado / 10000.0)
        WHERE precio_pagado > 100000
    """)
    items_corregidos = cursor.rowcount
    print(f"   ✅ {items_corregidos} items corregidos")

    # CORRECCIÓN 2: Facturas (también están mal)
    print("\n2️⃣ Corrigiendo facturas.total_factura...")
    cursor.execute("""
        UPDATE facturas
        SET total_factura = ROUND(total_factura / 10000.0)
        WHERE total_factura > 1000000
    """)
    facturas_corregidas = cursor.rowcount
    print(f"   ✅ {facturas_corregidas} facturas corregidas")

    # CORRECCIÓN 3: Actualizar precio_total en items_factura
    print("\n3️⃣ Recalculando items_factura.precio_total...")
    cursor.execute("""
        UPDATE items_factura
        SET precio_total = ROUND(precio_pagado * cantidad)
        WHERE precio_pagado IS NOT NULL AND cantidad IS NOT NULL
    """)
    totales_actualizados = cursor.rowcount
    print(f"   ✅ {totales_actualizados} totales recalculados")

    # COMMIT
    print("\n💾 Guardando cambios...")
    conn.commit()
    print("   ✅ Cambios guardados en la base de datos")

    # VERIFICACIÓN FINAL
    print("\n" + "=" * 60)
    print("📊 VERIFICACIÓN DESPUÉS DE LA CORRECCIÓN:")
    print("-" * 60)

    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(precio_pagado) as minimo,
            MAX(precio_pagado) as maximo,
            AVG(precio_pagado) as promedio
        FROM items_factura
        WHERE precio_pagado IS NOT NULL
    """)

    stats_items_final = cursor.fetchone()
    print(f"\n📦 ITEMS DE FACTURA (DESPUÉS):")
    print(f"   Total items: {stats_items_final['total']}")
    print(f"   Precio mínimo: ${stats_items_final['minimo']:,.0f}")
    print(f"   Precio máximo: ${stats_items_final['maximo']:,.0f}")
    print(f"   Precio promedio: ${stats_items_final['promedio']:,.0f}")

    cursor.execute("""
        SELECT
            COUNT(*) as total,
            MIN(total_factura) as minimo,
            MAX(total_factura) as maximo,
            AVG(total_factura) as promedio
        FROM facturas
        WHERE total_factura IS NOT NULL
    """)

    stats_facturas_final = cursor.fetchone()
    print(f"\n📄 FACTURAS (DESPUÉS):")
    print(f"   Total facturas: {stats_facturas_final['total']}")
    print(f"   Total mínimo: ${stats_facturas_final['minimo']:,.0f}")
    print(f"   Total máximo: ${stats_facturas_final['maximo']:,.0f}")
    print(f"   Total promedio: ${stats_facturas_final['promedio']:,.0f}")

    # Ejemplos corregidos
    cursor.execute("""
        SELECT
            i.nombre_leido,
            i.precio_pagado,
            f.total_factura
        FROM items_factura i
        JOIN facturas f ON i.factura_id = f.id
        ORDER BY i.precio_pagado DESC
        LIMIT 10
    """)

    items_corregidos_muestra = cursor.fetchall()
    print(f"\n✅ EJEMPLOS DE ITEMS CORREGIDOS:")
    for item in items_corregidos_muestra:
        print(f"   • {item['nombre_leido'][:30]:30} | ${item['precio_pagado']:>6,}")

    print("\n" + "=" * 60)
    print("✅ CORRECCIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 60)
    print(f"\nResumen:")
    print(f"  • {items_corregidos} items corregidos")
    print(f"  • {facturas_corregidas} facturas corregidas")
    print(f"  • {totales_actualizados} totales recalculados")

    cursor.close()
    conn.close()

    print("\n🎉 ¡Todos los precios han sido corregidos!")
    print("   Recarga el dashboard y el editor para ver los cambios.")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    if 'conn' in locals():
        conn.rollback()
        conn.close()
    exit(1)
