#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔧 CORRECCIÓN DEFINITIVA - PRECIOS MULTIPLICADOS x10,000
Conecta directamente a PostgreSQL de Railway y corrige los precios
"""

import psycopg
from decimal import Decimal

# 🔗 CONEXIÓN DIRECTA A RAILWAY
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def format_price(price):
    """Formatea precio para mostrar en pesos colombianos"""
    if price is None:
        return "$0"
    return f"${price:,.0f}".replace(",", ".")

def corregir_precios():
    """Corrige todos los precios dividiendo entre 10,000"""

    print("=" * 80)
    print("🔧 CORRECCIÓN DEFINITIVA - PRECIOS LECFAC")
    print("=" * 80)
    print()
    print("⚠️  IMPORTANTE: Este script modificará PERMANENTEMENTE los precios en la BD")
    print("    - Dividirá precios en items_factura entre 10,000")
    print("    - Dividirá total en facturas entre 10,000")
    print("    - NO tocará productos_maestros (ya están correctos)")
    print()

    confirmacion = input("¿Estás seguro de continuar? Escribe 'SI' para proceder: ")

    if confirmacion.strip().upper() != "SI":
        print("❌ Operación cancelada por el usuario")
        return

    print()
    print("🔗 Conectando a Railway...")

    try:
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        # ========================================
        # 1. CORREGIR ITEMS_FACTURA
        # ========================================
        print("=" * 80)
        print("📦 PASO 1: Corrigiendo items_factura")
        print("=" * 80)

        # 1.1 Contar items a corregir
        cursor.execute("""
            SELECT COUNT(*)
            FROM items_factura
            WHERE precio_pagado > 100000
        """)
        items_a_corregir = cursor.fetchone()[0]

        print(f"📊 Items con precio > $100,000: {items_a_corregir}")

        if items_a_corregir == 0:
            print("✅ No hay items que corregir")
        else:
            print(f"🔄 Corrigiendo {items_a_corregir} items...")

            # 1.2 Mostrar algunos ejemplos ANTES
            cursor.execute("""
                SELECT id, nombre_leido, precio_pagado
                FROM items_factura
                WHERE precio_pagado > 100000
                ORDER BY id DESC
                LIMIT 5
            """)
            ejemplos_antes = cursor.fetchall()

            print("\n📋 Ejemplos ANTES de la corrección:")
            for item in ejemplos_antes:
                id_item, nombre, precio = item
                print(f"   ID {id_item}: {nombre} = {format_price(precio)}")

            # 1.3 EJECUTAR CORRECCIÓN
            cursor.execute("""
                UPDATE items_factura
                SET precio_pagado = CAST(precio_pagado / 10000.0 AS INTEGER)
                WHERE precio_pagado > 100000
            """)

            items_corregidos = cursor.rowcount
            conn.commit()

            print(f"\n✅ {items_corregidos} items corregidos")

            # 1.4 Mostrar ejemplos DESPUÉS
            ids_ejemplos = [item[0] for item in ejemplos_antes]
            if ids_ejemplos:
                placeholders = ','.join(['%s'] * len(ids_ejemplos))
                cursor.execute(f"""
                    SELECT id, nombre_leido, precio_pagado
                    FROM items_factura
                    WHERE id IN ({placeholders})
                    ORDER BY id DESC
                """, ids_ejemplos)
                ejemplos_despues = cursor.fetchall()

                print("\n📋 Ejemplos DESPUÉS de la corrección:")
                for item in ejemplos_despues:
                    id_item, nombre, precio = item
                    print(f"   ID {id_item}: {nombre} = {format_price(precio)}")

        # ========================================
        # 2. CORREGIR FACTURAS
        # ========================================
        print("\n" + "=" * 80)
        print("🧾 PASO 2: Corrigiendo facturas")
        print("=" * 80)

        # 2.1 Contar facturas a corregir
        cursor.execute("""
            SELECT COUNT(*)
            FROM facturas
            WHERE total_factura > 1000000
        """)
        facturas_a_corregir = cursor.fetchone()[0]

        print(f"📊 Facturas con total > $1,000,000: {facturas_a_corregir}")

        if facturas_a_corregir == 0:
            print("✅ No hay facturas que corregir")
        else:
            print(f"🔄 Corrigiendo {facturas_a_corregir} facturas...")

            # 2.2 Mostrar facturas ANTES
            cursor.execute("""
                SELECT id, establecimiento, total_factura, fecha_factura
                FROM facturas
                WHERE total_factura > 1000000
                ORDER BY id DESC
            """)
            facturas_antes = cursor.fetchall()

            print("\n📋 Facturas ANTES de la corrección:")
            for factura in facturas_antes:
                id_fac, establecimiento, total, fecha = factura
                print(f"   ID {id_fac}: {establecimiento} = {format_price(total)} ({fecha})")

            # 2.3 EJECUTAR CORRECCIÓN
            cursor.execute("""
                UPDATE facturas
                SET total_factura = CAST(total_factura / 10000.0 AS INTEGER)
                WHERE total_factura > 1000000
            """)

            facturas_corregidas = cursor.rowcount
            conn.commit()

            print(f"\n✅ {facturas_corregidas} facturas corregidas")

            # 2.4 Mostrar facturas DESPUÉS
            ids_facturas = [f[0] for f in facturas_antes]
            if ids_facturas:
                placeholders = ','.join(['%s'] * len(ids_facturas))
                cursor.execute(f"""
                    SELECT id, establecimiento, total_factura, fecha_factura
                    FROM facturas
                    WHERE id IN ({placeholders})
                    ORDER BY id DESC
                """, ids_facturas)
                facturas_despues = cursor.fetchall()

                print("\n📋 Facturas DESPUÉS de la corrección:")
                for factura in facturas_despues:
                    id_fac, establecimiento, total, fecha = factura
                    print(f"   ID {id_fac}: {establecimiento} = {format_price(total)} ({fecha})")

        # ========================================
        # 3. VERIFICAR PRODUCTOS_MAESTROS
        # ========================================
        print("\n" + "=" * 80)
        print("📋 PASO 3: Verificando productos_maestros")
        print("=" * 80)

        cursor.execute("""
            SELECT COUNT(*)
            FROM productos_maestros
            WHERE precio_promedio_global > 1000000
        """)
        productos_problema = cursor.fetchone()[0]

        if productos_problema > 0:
            print(f"⚠️  {productos_problema} productos con precio > $1,000,000")
            print("🔄 Corrigiendo productos_maestros...")

            cursor.execute("""
                UPDATE productos_maestros
                SET precio_promedio_global = CAST(precio_promedio_global / 10000.0 AS INTEGER)
                WHERE precio_promedio_global > 1000000
            """)

            productos_corregidos = cursor.rowcount
            conn.commit()
            print(f"✅ {productos_corregidos} productos corregidos")
        else:
            print("✅ productos_maestros están correctos (no requieren corrección)")

        # ========================================
        # 4. RESUMEN FINAL
        # ========================================
        print("\n" + "=" * 80)
        print("📊 RESUMEN FINAL")
        print("=" * 80)

        # Verificar rangos actuales
        cursor.execute("""
            SELECT
                MIN(precio_pagado) as min_precio,
                MAX(precio_pagado) as max_precio,
                AVG(precio_pagado) as avg_precio
            FROM items_factura
        """)
        stats = cursor.fetchone()

        if stats:
            min_p, max_p, avg_p = stats
            print("\n💰 Precios en items_factura:")
            print(f"   Mínimo: {format_price(min_p)}")
            print(f"   Máximo: {format_price(max_p)}")
            print(f"   Promedio: {format_price(avg_p)}")

            if max_p and max_p < 500000:  # 500 mil es razonable como máximo
                print("   ✅ Rangos de precios ahora son razonables")
            else:
                print(f"   ⚠️  Aún hay precios altos (máx: {format_price(max_p)})")

        cursor.execute("""
            SELECT
                MIN(total_factura) as min_total,
                MAX(total_factura) as max_total,
                AVG(total_factura) as avg_total
            FROM facturas
        """)
        stats_fact = cursor.fetchone()

        if stats_fact:
            min_t, max_t, avg_t = stats_fact
            print("\n🧾 Totales en facturas:")
            print(f"   Mínimo: {format_price(min_t)}")
            print(f"   Máximo: {format_price(max_t)}")
            print(f"   Promedio: {format_price(avg_t)}")

            if max_t and max_t < 500000:
                print("   ✅ Totales de facturas ahora son razonables")
            else:
                print(f"   ⚠️  Aún hay totales altos (máx: {format_price(max_t)})")

        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ CORRECCIÓN COMPLETADA")
        print("=" * 80)
        print()
        print("🎯 SIGUIENTE PASO:")
        print("   Ejecuta: python diagnostico_definitivo.py")
        print("   Para verificar que todo está correcto")
        print()

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()

if __name__ == "__main__":
    corregir_precios()
