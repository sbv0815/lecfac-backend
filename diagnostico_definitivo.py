#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔍 DIAGNÓSTICO DEFINITIVO - PROBLEMA DE PRECIOS LECFAC
Conecta directamente a PostgreSQL de Railway
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

def analizar_problema():
    """Analiza y diagnostica el problema de precios"""

    print("=" * 80)
    print("🔍 DIAGNÓSTICO DEFINITIVO - LECFAC PRECIOS")
    print("=" * 80)
    print(f"🔗 Conectando a Railway...")
    print()

    try:
        # Conectar a PostgreSQL
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa a PostgreSQL Railway\n")

        # ========================================
        # 1. VERIFICAR ITEMS_FACTURA
        # ========================================
        print("=" * 80)
        print("📦 TABLA: items_factura")
        print("=" * 80)

        cursor.execute("""
            SELECT
                id,
                nombre_leido,
                precio_pagado,
                cantidad,
                producto_maestro_id
            FROM items_factura
            ORDER BY id DESC
            LIMIT 10
        """)

        items = cursor.fetchall()

        if items:
            print(f"Mostrando últimos {len(items)} items:\n")
            problema_detectado = False

            for item in items:
                id_item, nombre, precio_pagado, cantidad, producto_maestro_id = item
                print(f"ID: {id_item}")
                print(f"  📝 Producto: {nombre}")
                print(f"  💰 Precio pagado: {format_price(precio_pagado)}")
                print(f"  🔢 Cantidad: {cantidad}")
                print(f"  🔗 Producto maestro ID: {producto_maestro_id}")

                # Detectar si hay problema (precios mayores a 1 millón son sospechosos)
                if precio_pagado and precio_pagado > 1000000:
                    print(f"  ⚠️  PROBLEMA: Precio sospechosamente alto")
                    problema_detectado = True

                print()

            if problema_detectado:
                print("❌ PROBLEMA CONFIRMADO: Hay precios multiplicados por 10,000\n")
        else:
            print("⚠️  No hay items en la tabla\n")

        # ========================================
        # 2. VERIFICAR FACTURAS
        # ========================================
        print("=" * 80)
        print("🧾 TABLA: facturas")
        print("=" * 80)

        cursor.execute("""
            SELECT
                id,
                establecimiento,
                total_factura,
                fecha_factura,
                usuario_id
            FROM facturas
            ORDER BY id DESC
            LIMIT 10
        """)

        facturas = cursor.fetchall()

        if facturas:
            print(f"Mostrando últimas {len(facturas)} facturas:\n")

            for factura in facturas:
                id_fac, establecimiento, total, fecha, usuario_id = factura
                print(f"ID: {id_fac}")
                print(f"  🏪 Establecimiento: {establecimiento}")
                print(f"  💰 Total: {format_price(total)}")
                print(f"  📅 Fecha: {fecha}")
                print(f"  👤 Usuario ID: {usuario_id}")

                if total and total > 1000000:
                    print(f"  ⚠️  PROBLEMA: Total sospechosamente alto para una factura de supermercado")

                print()
        else:
            print("⚠️  No hay facturas en la tabla\n")

        # ========================================
        # 3. VERIFICAR PRODUCTOS_MAESTROS
        # ========================================
        print("=" * 80)
        print("📋 TABLA: productos_maestros")
        print("=" * 80)

        cursor.execute("""
            SELECT
                id,
                nombre_normalizado,
                precio_promedio_global,
                codigo_ean
            FROM productos_maestros
            ORDER BY id DESC
            LIMIT 10
        """)

        productos = cursor.fetchall()

        if productos:
            print(f"Mostrando últimos {len(productos)} productos maestros:\n")

            for prod in productos:
                id_prod, nombre, precio_prom, ean = prod
                print(f"ID: {id_prod}")
                print(f"  📝 Nombre: {nombre}")
                print(f"  💰 Precio promedio: {format_price(precio_prom)}")
                print(f"  🔢 EAN: {ean}")

                if precio_prom and precio_prom > 1000000:
                    print(f"  ⚠️  PROBLEMA: Precio promedio sospechosamente alto")

                print()
        else:
            print("⚠️  No hay productos maestros en la tabla\n")

        # ========================================
        # 4. ESTADÍSTICAS GENERALES
        # ========================================
        print("=" * 80)
        print("📊 ESTADÍSTICAS GENERALES")
        print("=" * 80)

        # Contar registros
        cursor.execute("SELECT COUNT(*) FROM items_factura")
        count_items = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM facturas")
        count_facturas = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        count_productos = cursor.fetchone()[0]

        print(f"📦 Total items_factura: {count_items}")
        print(f"🧾 Total facturas: {count_facturas}")
        print(f"📋 Total productos_maestros: {count_productos}")
        print()

        # Analizar rangos de precios en items_factura
        cursor.execute("""
            SELECT
                MIN(precio_pagado) as min_precio,
                MAX(precio_pagado) as max_precio,
                AVG(precio_pagado) as avg_precio,
                COUNT(*) as total
            FROM items_factura
            WHERE precio_pagado IS NOT NULL
        """)

        stats = cursor.fetchone()
        if stats and stats[3] > 0:
            min_p, max_p, avg_p, total = stats
            print("💰 Rangos de precios en items_factura:")
            print(f"  Mínimo: {format_price(min_p)}")
            print(f"  Máximo: {format_price(max_p)}")
            print(f"  Promedio: {format_price(avg_p)}")
            print()

            # Detectar problema basado en estadísticas
            if max_p and max_p > 10000000:  # 10 millones
                print("❌ CONFIRMADO: Hay precios multiplicados por 10,000")
                print(f"   El precio máximo ({format_price(max_p)}) es irreal para un supermercado\n")
            elif avg_p and avg_p > 1000000:  # 1 millón
                print("❌ CONFIRMADO: Los precios promedio son demasiado altos")
                print(f"   Precio promedio {format_price(avg_p)} indica multiplicación por 10,000\n")

        # Analizar rangos en facturas
        cursor.execute("""
            SELECT
                MIN(total_factura) as min_total,
                MAX(total_factura) as max_total,
                AVG(total_factura) as avg_total
            FROM facturas
            WHERE total_factura IS NOT NULL
        """)

        stats_fact = cursor.fetchone()
        if stats_fact:
            min_t, max_t, avg_t = stats_fact
            print("🧾 Rangos de totales en facturas:")
            print(f"  Mínimo: {format_price(min_t)}")
            print(f"  Máximo: {format_price(max_t)}")
            print(f"  Promedio: {format_price(avg_t)}")
            print()

            if max_t and max_t > 50000000:  # 50 millones
                print("❌ CONFIRMADO: Hay totales de facturas multiplicados por 10,000")
                print(f"   Una factura de supermercado no debería costar {format_price(max_t)}\n")

        # ========================================
        # 5. RECOMENDACIÓN
        # ========================================
        print("=" * 80)
        print("🎯 RECOMENDACIÓN")
        print("=" * 80)

        if count_items > 0 or count_facturas > 0:
            print("✅ Hay datos en la base de datos")
            print()
            print("📋 PASOS A SEGUIR:")
            print()
            print("1️⃣  CORREGIR BASE DE DATOS (urgente)")
            print("   python corregir_precios_bd.py")
            print("   → Dividirá todos los precios entre 10,000")
            print()
            print("2️⃣  CORREGIR OCR (prevenir futuro)")
            print("   → Editar ocr_processor.py")
            print("   → Eliminar comas antes de parsear precios")
            print()
            print("3️⃣  VERIFICAR CORRECCIÓN")
            print("   python diagnostico_definitivo.py")
            print("   → Ejecutar de nuevo para verificar")
        else:
            print("⚠️  Base de datos vacía")
            print("   Solo necesitas corregir ocr_processor.py antes de escanear facturas")

        cursor.close()
        conn.close()

        print()
        print("=" * 80)
        print("✅ Diagnóstico completado")
        print("=" * 80)

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analizar_problema()
