#!/usr/bin/env python3
"""
diagnostico_ocr.py
Diagnostica problemas con el procesamiento OCR
"""

import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def ver_ultima_factura():
    """Muestra datos de la última factura procesada"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Última factura
    cur.execute("""
        SELECT
            f.id,
            f.establecimiento,
            f.establecimiento_id,
            f.fecha_factura,
            f.usuario_id,
            u.email,
            e.nombre_normalizado
        FROM facturas f
        LEFT JOIN usuarios u ON f.usuario_id = u.id
        LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
        ORDER BY f.id DESC
        LIMIT 1
    """)

    factura = cur.fetchone()
    if not factura:
        print("❌ No hay facturas")
        return None

    factura_id = factura[0]

    print("📄 ÚLTIMA FACTURA:")
    print(f"   ID: {factura_id}")
    print(f"   Establecimiento (texto): {factura[1]}")
    print(f"   Establecimiento ID: {factura[2]}")
    print(f"   Establecimiento (tabla): {factura[6]}")
    print(f"   Fecha: {factura[3]}")
    print(f"   Usuario ID: {factura[4]} ({factura[5]})")

    # Items de la factura
    print("\n📦 ITEMS PROCESADOS:")
    cur.execute("""
        SELECT
            i.codigo_leido,
            i.nombre_leido,
            i.precio_pagado,
            i.producto_maestro_id,
            pm.codigo_ean,
            pm.nombre_normalizado,
            pm.es_producto_fresco
        FROM items_factura i
        LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
        WHERE i.factura_id = %s
        ORDER BY i.id
    """, (factura_id,))

    items = cur.fetchall()
    print(f"\nTotal items: {len(items)}")

    for idx, item in enumerate(items[:10], 1):  # Mostrar solo 10
        print(f"\n{idx}. Código leído: '{item[0]}' → EAN guardado: '{item[4]}'")
        print(f"   Nombre OCR: '{item[1]}'")
        print(f"   Nombre catalogo: '{item[5]}'")
        print(f"   Precio: ${item[2]:,}")
        print(f"   Es fresco?: {item[6]}")

        # Detectar si es PLU
        if item[0] and len(item[0]) <= 6:
            print(f"   ⚠️ POSIBLE PLU (código corto: {len(item[0])} dígitos)")

    # Ver PLUs guardados
    print("\n🏷️ PLUs EN productos_por_establecimiento:")
    cur.execute("""
        SELECT
            ppe.codigo_plu,
            ppe.establecimiento_id,
            e.nombre_normalizado,
            ppe.producto_maestro_id
        FROM productos_por_establecimiento ppe
        LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
        ORDER BY ppe.id DESC
        LIMIT 10
    """)

    plus = cur.fetchall()
    if plus:
        for plu in plus:
            print(f"   PLU: {plu[0]} → Establecimiento: {plu[2]} (ID: {plu[1]})")
    else:
        print("   ❌ No hay PLUs guardados")

    cur.close()
    conn.close()
    return factura_id

def ver_inventario_problematico():
    """Busca items problemáticos en el inventario"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n🔍 INVENTARIO PROBLEMÁTICO:")

    # Buscar nombres raros
    cur.execute("""
        SELECT
            iu.id,
            iu.usuario_id,
            iu.producto_maestro_id,
            pm.nombre_normalizado,
            pm.codigo_ean,
            iu.establecimiento,
            iu.establecimiento_id
        FROM inventario_usuario iu
        LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
        WHERE pm.nombre_normalizado LIKE '%marina%'
           OR pm.nombre_normalizado LIKE '%haz d%'
           OR LENGTH(pm.nombre_normalizado) < 5
        ORDER BY iu.id DESC
        LIMIT 10
    """)

    problemas = cur.fetchall()
    if problemas:
        for prob in problemas:
            print(f"\nInventario ID: {prob[0]}")
            print(f"   Producto: '{prob[3]}' (ID: {prob[2]})")
            print(f"   EAN: {prob[4]}")
            print(f"   Establecimiento: {prob[5]} (ID: {prob[6]})")

    cur.close()
    conn.close()

def analizar_codigos():
    """Analiza la distribución de códigos"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n📊 ANÁLISIS DE CÓDIGOS:")

    # Distribución por longitud
    cur.execute("""
        SELECT
            LENGTH(codigo_ean) as longitud,
            COUNT(*) as cantidad
        FROM productos_maestros
        WHERE codigo_ean IS NOT NULL
        GROUP BY LENGTH(codigo_ean)
        ORDER BY longitud
    """)

    print("\nLongitud de códigos EAN:")
    for row in cur.fetchall():
        print(f"   {row[0]} dígitos: {row[1]} productos")

    # Códigos muy cortos (probables PLUs)
    cur.execute("""
        SELECT
            codigo_ean,
            nombre_normalizado,
            es_producto_fresco
        FROM productos_maestros
        WHERE LENGTH(codigo_ean) <= 6
        ORDER BY codigo_ean
        LIMIT 20
    """)

    print("\n⚠️ Códigos sospechosos (muy cortos - probables PLUs):")
    for row in cur.fetchall():
        print(f"   '{row[0]}' → {row[1]} (Fresco: {row[2]})")

    cur.close()
    conn.close()

def main():
    print("=" * 70)
    print("DIAGNÓSTICO OCR - LECFAC")
    print("=" * 70)

    factura_id = ver_ultima_factura()
    ver_inventario_problematico()
    analizar_codigos()

    print("\n" + "=" * 70)
    print("PROBLEMAS DETECTADOS:")
    print("1. PLUs guardados como EAN (códigos cortos)")
    print("2. Establecimiento no detectado correctamente")
    print("3. Nombres de productos mal procesados")
    print("=" * 70)

if __name__ == "__main__":
    main()
