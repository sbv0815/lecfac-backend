#!/usr/bin/env python3
"""
diagnostico_establecimientos.py
Diagnostica por qué aparecen establecimientos incorrectos
"""

import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def ver_todas_las_facturas():
    """Muestra TODAS las facturas con sus establecimientos"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("📋 TODAS LAS FACTURAS EN EL SISTEMA:")
    print("=" * 80)

    cur.execute("""
        SELECT
            f.id,
            f.usuario_id,
            u.email,
            f.establecimiento as texto_ocr,
            f.establecimiento_id,
            e.nombre_normalizado as nombre_tabla,
            f.fecha_factura,
            f.total_factura,
            f.productos_detectados
        FROM facturas f
        LEFT JOIN usuarios u ON f.usuario_id = u.id
        LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
        ORDER BY f.id
    """)

    facturas = cur.fetchall()

    for f in facturas:
        print(f"\nFactura ID: {f[0]}")
        print(f"   Usuario: {f[2]} (ID: {f[1]})")
        print(f"   Establecimiento OCR: '{f[3]}'")
        print(f"   Establecimiento ID: {f[4]}")
        print(f"   Establecimiento tabla: '{f[5]}'")
        print(f"   Fecha: {f[6]}")
        print(f"   Total: ${f[7]:,}" if f[7] else "   Total: N/A")
        print(f"   Productos: {f[8]}")

    cur.close()
    conn.close()

def ver_establecimientos():
    """Muestra todos los establecimientos en la BD"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n\n🏪 ESTABLECIMIENTOS EN LA BASE DE DATOS:")
    print("=" * 60)

    cur.execute("""
        SELECT
            id,
            nombre_normalizado,
            cadena,
            total_facturas_reportadas
        FROM establecimientos
        ORDER BY id
    """)

    for e in cur.fetchall():
        print(f"ID: {e[0]:3} | {e[1]:<30} | Cadena: {e[2]:<20} | Facturas: {e[3] or 0}")

    cur.close()
    conn.close()

def ver_productos_con_plu():
    """Muestra productos que deberían tener PLU"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n\n🏷️ PRODUCTOS CON CÓDIGOS CORTOS (POSIBLES PLUs):")
    print("=" * 80)

    # Ver productos con códigos cortos y en qué facturas/establecimientos aparecen
    cur.execute("""
        SELECT DISTINCT
            pm.codigo_ean,
            pm.nombre_normalizado,
            f.establecimiento,
            f.establecimiento_id,
            e.nombre_normalizado as estab_nombre
        FROM productos_maestros pm
        INNER JOIN items_factura if ON pm.id = if.producto_maestro_id
        INNER JOIN facturas f ON if.factura_id = f.id
        LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
        WHERE LENGTH(pm.codigo_ean) <= 7
        ORDER BY pm.codigo_ean, f.establecimiento
    """)

    ultimo_codigo = None
    for row in cur.fetchall():
        if row[0] != ultimo_codigo:
            print(f"\nCódigo: '{row[0]}' → {row[1]}")
            ultimo_codigo = row[0]
        print(f"   Usado en: {row[2]} (ID: {row[3]}, tabla: {row[4]})")

    cur.close()
    conn.close()

def ver_inventario_problematico():
    """Ve específicamente el problema del inventario"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n\n🔍 INVENTARIO CON PROBLEMAS DE NOMBRES:")
    print("=" * 60)

    cur.execute("""
        SELECT
            iu.id,
            iu.usuario_id,
            iu.producto_maestro_id,
            pm.codigo_ean,
            pm.nombre_normalizado,
            iu.establecimiento,
            iu.establecimiento_id,
            iu.ultima_factura_id
        FROM inventario_usuario iu
        LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
        WHERE pm.nombre_normalizado LIKE '%haz%'
           OR pm.nombre_normalizado LIKE '%marina%'
           OR pm.nombre_normalizado LIKE '%atun%'
        ORDER BY pm.nombre_normalizado
    """)

    for inv in cur.fetchall():
        print(f"\nInventario ID: {inv[0]}")
        print(f"   Producto ID: {inv[2]} - '{inv[4]}'")
        print(f"   Código: {inv[3]}")
        print(f"   Establecimiento: {inv[5]} (ID: {inv[6]})")
        print(f"   Última factura: {inv[7]}")

    cur.close()
    conn.close()

def main():
    ver_todas_las_facturas()
    ver_establecimientos()
    ver_productos_con_plu()
    ver_inventario_problematico()

    print("\n\n" + "="*80)
    print("PROBLEMAS IDENTIFICADOS:")
    print("1. PLUs guardados como EAN - no se puede saber de qué super son")
    print("2. El mismo PLU '1045' podría ser zanahoria en Jumbo pero tomate en Olímpica")
    print("3. Nombres mal leídos por OCR creando duplicados")
    print("4. Posible confusión de establecimiento_id")
    print("="*80)

if __name__ == "__main__":
    main()
