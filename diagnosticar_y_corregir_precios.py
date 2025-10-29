#!/usr/bin/env python3
"""
Script para diagnosticar y corregir precios en la base de datos
EJECUTAR DESDE TU PC LOCAL conectándose a Railway
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import sys

print("=" * 70)
print("🔧 DIAGNÓSTICO Y CORRECCIÓN DE PRECIOS - LECFAC")
print("=" * 70)

# ==========================================
# PASO 1: PEDIR DATABASE_URL
# ==========================================
print("\n📋 PASO 1: Conectar a la base de datos")
print("-" * 70)
print("\n¿Dónde obtener el DATABASE_URL?")
print("1. Ve a Railway.app")
print("2. Abre tu proyecto 'lecfac-backend'")
print("3. Click en la base de datos PostgreSQL")
print("4. Busca la variable 'DATABASE_URL' o 'DATABASE_PRIVATE_URL'")
print("5. Copia el valor completo (postgres://...)")
print()

DATABASE_URL = input("📎 Pega aquí tu DATABASE_URL de Railway: ").strip()

if not DATABASE_URL:
    print("❌ ERROR: No ingresaste ninguna URL")
    sys.exit(1)

if not DATABASE_URL.startswith("postgres"):
    print("❌ ERROR: La URL debe empezar con 'postgres://' o 'postgresql://'")
    sys.exit(1)

print("✅ URL recibida correctamente")

# ==========================================
# PASO 2: CONECTAR
# ==========================================
print("\n📡 PASO 2: Conectando a la base de datos...")
print("-" * 70)

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    print("✅ Conexión exitosa a PostgreSQL")
except Exception as e:
    print(f"❌ ERROR de conexión: {e}")
    print("\nVerifica que:")
    print("  • El DATABASE_URL sea correcto")
    print("  • Tu IP tenga acceso a Railway")
    print("  • El servicio de PostgreSQL esté activo")
    sys.exit(1)

# ==========================================
# PASO 3: DIAGNÓSTICO
# ==========================================
print("\n📊 PASO 3: Analizando precios actuales...")
print("-" * 70)

try:
    # Estadísticas de items_factura
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

    print(f"\n📦 ITEMS_FACTURA:")
    print(f"   Total items: {stats_items['total']:,}")
    print(f"   Precio mínimo: ${stats_items['minimo']:,.2f}")
    print(f"   Precio máximo: ${stats_items['maximo']:,.2f}")
    print(f"   Precio promedio: ${stats_items['promedio']:,.2f}")

    # Estadísticas de facturas
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

    print(f"\n📄 FACTURAS:")
    print(f"   Total facturas: {stats_facturas['total']:,}")
    print(f"   Total mínimo: ${stats_facturas['minimo']:,.2f}")
    print(f"   Total máximo: ${stats_facturas['maximo']:,.2f}")
    print(f"   Total promedio: ${stats_facturas['promedio']:,.2f}")

    # Ejemplos de items con precios
    cursor.execute("""
        SELECT
            id,
            nombre_leido,
            precio_pagado,
            fecha_creacion
        FROM items_factura
        WHERE precio_pagado IS NOT NULL
        ORDER BY fecha_creacion DESC
        LIMIT 10
    """)

    items_ejemplo = cursor.fetchall()

    print(f"\n📋 EJEMPLOS DE ITEMS RECIENTES:")
    print("-" * 70)
    for item in items_ejemplo:
        print(f"   #{item['id']:4d} | {item['nombre_leido'][:30]:30} | ${item['precio_pagado']:>12,.0f}")

    # Detectar si necesita corrección
    promedio_items = float(stats_items['promedio'])
    promedio_facturas = float(stats_facturas['promedio'])

    print("\n" + "=" * 70)
    print("🔍 DIAGNÓSTICO:")
    print("-" * 70)

    NECESITA_CORRECCION = promedio_items > 100000 or promedio_facturas > 1000000

    if NECESITA_CORRECCION:
        print("❌ LOS PRECIOS ESTÁN INCORRECTOS")
        print(f"   • Promedio de items: ${promedio_items:,.0f}")
        print(f"   • Promedio de facturas: ${promedio_facturas:,.0f}")
        print("\n💡 Los precios están multiplicados por 10,000")
        print("   Ejemplos:")
        if items_ejemplo:
            ejemplo = items_ejemplo[0]
            precio_actual = ejemplo['precio_pagado']
            precio_correcto = round(precio_actual / 10000)
            print(f"   • {ejemplo['nombre_leido'][:30]}")
            print(f"     Actual: ${precio_actual:,} → Correcto: ${precio_correcto:,}")
    else:
        print("✅ LOS PRECIOS ESTÁN CORRECTOS")
        print(f"   • Promedio de items: ${promedio_items:,.0f}")
        print(f"   • Promedio de facturas: ${promedio_facturas:,.0f}")
        print("\n🎉 No se necesita corrección")
        cursor.close()
        conn.close()
        sys.exit(0)

except Exception as e:
    print(f"❌ ERROR en diagnóstico: {e}")
    import traceback
    traceback.print_exc()
    cursor.close()
    conn.close()
    sys.exit(1)

# ==========================================
# PASO 4: CONFIRMACIÓN
# ==========================================
print("\n" + "=" * 70)
print("⚠️  CORRECCIÓN DE PRECIOS")
print("=" * 70)
print("\n¿Qué se va a hacer?")
print("1. Dividir items_factura.precio_pagado entre 10,000")
print("2. Dividir facturas.total_factura entre 10,000")
print()
print("⚠️  IMPORTANTE: Esta operación modificará la base de datos")
print("   Se puede revertir con ROLLBACK si algo sale mal")
print()

confirmacion = input("¿Continuar con la corrección? (escribe SI en mayúsculas): ").strip()

if confirmacion != "SI":
    print("\n❌ Corrección cancelada por el usuario")
    cursor.close()
    conn.close()
    sys.exit(0)

# ==========================================
# PASO 5: CORRECCIÓN
# ==========================================
print("\n🔧 PASO 5: Ejecutando corrección...")
print("-" * 70)

try:
    # Iniciar transacción
    print("\n1️⃣ Corrigiendo items_factura.precio_pagado...")
    cursor.execute("""
        UPDATE items_factura
        SET precio_pagado = ROUND(precio_pagado / 10000.0)
        WHERE precio_pagado > 100000
    """)
    items_corregidos = cursor.rowcount
    print(f"   ✅ {items_corregidos:,} items corregidos")

    print("\n2️⃣ Corrigiendo facturas.total_factura...")
    cursor.execute("""
        UPDATE facturas
        SET total_factura = ROUND(total_factura / 10000.0)
        WHERE total_factura > 1000000
    """)
    facturas_corregidas = cursor.rowcount
    print(f"   ✅ {facturas_corregidas:,} facturas corregidas")

    # Commit
    print("\n💾 Guardando cambios en la base de datos...")
    conn.commit()
    print("   ✅ Cambios guardados exitosamente (COMMIT)")

except Exception as e:
    print(f"\n❌ ERROR durante la corrección: {e}")
    print("\n🔄 Revirtiendo cambios (ROLLBACK)...")
    conn.rollback()
    print("   ✅ Cambios revertidos, base de datos sin modificar")
    import traceback
    traceback.print_exc()
    cursor.close()
    conn.close()
    sys.exit(1)

# ==========================================
# PASO 6: VERIFICACIÓN FINAL
# ==========================================
print("\n" + "=" * 70)
print("📊 PASO 6: Verificando corrección...")
print("-" * 70)

try:
    # Estadísticas finales de items
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

    print(f"\n📦 ITEMS_FACTURA (DESPUÉS):")
    print(f"   Total items: {stats_items_final['total']:,}")
    print(f"   Precio mínimo: ${stats_items_final['minimo']:,.2f}")
    print(f"   Precio máximo: ${stats_items_final['maximo']:,.2f}")
    print(f"   Precio promedio: ${stats_items_final['promedio']:,.2f}")

    # Estadísticas finales de facturas
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
    print(f"   Total facturas: {stats_facturas_final['total']:,}")
    print(f"   Total mínimo: ${stats_facturas_final['minimo']:,.2f}")
    print(f"   Total máximo: ${stats_facturas_final['maximo']:,.2f}")
    print(f"   Total promedio: ${stats_facturas_final['promedio']:,.2f}")

    # Ejemplos corregidos
    cursor.execute("""
        SELECT
            id,
            nombre_leido,
            precio_pagado
        FROM items_factura
        WHERE precio_pagado IS NOT NULL
        ORDER BY precio_pagado DESC
        LIMIT 10
    """)

    items_corregidos_muestra = cursor.fetchall()

    print(f"\n✅ EJEMPLOS DE ITEMS CORREGIDOS:")
    print("-" * 70)
    for item in items_corregidos_muestra:
        print(f"   #{item['id']:4d} | {item['nombre_leido'][:30]:30} | ${item['precio_pagado']:>6,}")

    cursor.close()
    conn.close()

    print("\n" + "=" * 70)
    print("✅ CORRECCIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 70)
    print(f"\nResumen:")
    print(f"  • {items_corregidos:,} items corregidos")
    print(f"  • {facturas_corregidas:,} facturas corregidas")
    print("\n🎉 ¡Todos los precios han sido corregidos!")
    print("   Recarga tu dashboard para ver los cambios.")

except Exception as e:
    print(f"❌ ERROR en verificación: {e}")
    import traceback
    traceback.print_exc()
    cursor.close()
    conn.close()
    sys.exit(1)
