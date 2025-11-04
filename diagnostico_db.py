"""
LECFAC - DIAGNÓSTICO DE BASE DE DATOS
"""

import os

# CONFIGURACIÓN - Pega tu DATABASE_URL aquí
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

# IMPORTS
try:
    import psycopg2
    print("✅ psycopg2 importado correctamente")
except ImportError:
    print("❌ psycopg2 no instalado")
    print("   Ejecuta: pip install psycopg2-binary")
    exit(1)

# FUNCIONES
def conectar():
    """Conecta a la base de datos"""
    try:
        print(f"\n🔗 Conectando a PostgreSQL...")
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ Conexión exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return None


def diagnostico_completo(conn):
    """Ejecuta diagnóstico completo"""
    cursor = conn.cursor()

    # 1. TODAS LAS FACTURAS
    print("\n" + "=" * 80)
    print("📋 TODAS LAS FACTURAS (últimas 20)")
    print("=" * 80)

    cursor.execute("""
        SELECT
            f.id,
            f.usuario_id,
            f.establecimiento,
            f.total_factura,
            f.productos_guardados,
            f.fecha_cargue,
            COUNT(i.id) as items_en_bd
        FROM facturas f
        LEFT JOIN items_factura i ON f.id = i.factura_id
        GROUP BY f.id, f.usuario_id, f.establecimiento, f.total_factura, f.productos_guardados, f.fecha_cargue
        ORDER BY f.fecha_cargue DESC
        LIMIT 20
    """)

    facturas = cursor.fetchall()

    if not facturas:
        print("⚠️ No hay facturas en la base de datos")
    else:
        print(f"\n✅ {len(facturas)} facturas encontradas:\n")
        print(f"{'ID':<6} {'Usuario':<8} {'Establecimiento':<20} {'Total':<12} {'Items':<8} {'Fecha':<20}")
        print("-" * 80)

        for f in facturas:
            factura_id = f[0]
            usuario_id = f[1]
            establecimiento = f[2] or "Sin nombre"
            total = f"{f[3]:,.0f}" if f[3] else "0"
            items = f[6]
            fecha = str(f[5])[:19] if f[5] else "Sin fecha"

            print(f"{factura_id:<6} {usuario_id:<8} {establecimiento:<20} ${total:<11} {items:<8} {fecha:<20}")

    # 2. ITEMS DE ÚLTIMA FACTURA
    if facturas:
        ultima_factura_id = facturas[0][0]
        print("\n" + "=" * 80)
        print(f"📦 ITEMS DE ÚLTIMA FACTURA (#{ultima_factura_id})")
        print("=" * 80)

        cursor.execute("""
            SELECT
                i.id,
                i.nombre_leido,
                i.precio_pagado,
                i.cantidad,
                i.producto_maestro_id,
                pm.nombre_normalizado
            FROM items_factura i
            LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
            WHERE i.factura_id = %s
            ORDER BY i.id
            LIMIT 20
        """, (ultima_factura_id,))

        items = cursor.fetchall()

        if not items:
            print(f"⚠️ No hay items para factura #{ultima_factura_id}")
        else:
            print(f"\n✅ {len(items)} items (mostrando primeros 20):\n")
            print(f"{'ID':<6} {'Nombre OCR':<35} {'Precio':<10} {'PM_ID':<7}")
            print("-" * 70)

            items_sin_pm = 0
            for item in items:
                item_id = item[0]
                nombre_ocr = (item[1] or "Sin nombre")[:34]
                precio = f"${item[2]:,.0f}" if item[2] else "$0"
                pm_id = item[4] or "-"

                if not item[4]:
                    items_sin_pm += 1

                print(f"{item_id:<6} {nombre_ocr:<35} {precio:<10} {pm_id:<7}")

            if items_sin_pm > 0:
                print(f"\n⚠️ PROBLEMA: {items_sin_pm} items SIN producto_maestro_id")

    # 3. INVENTARIO USUARIO 2
    print("\n" + "=" * 80)
    print(f"🏠 INVENTARIO DEL USUARIO #2")
    print("=" * 80)

    cursor.execute("""
        SELECT
            iu.id,
            iu.producto_maestro_id,
            pm.nombre_normalizado,
            iu.cantidad_actual,
            iu.precio_ultima_compra
        FROM inventario_usuario iu
        LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
        WHERE iu.usuario_id = 2
        ORDER BY iu.fecha_ultima_actualizacion DESC
        LIMIT 20
    """)

    inventario = cursor.fetchall()

    if not inventario:
        print(f"⚠️ El usuario #2 NO tiene inventario")
    else:
        print(f"\n✅ {len(inventario)} productos en inventario:\n")
        print(f"{'ID':<6} {'PM_ID':<7} {'Producto':<40} {'Cant':<8}")
        print("-" * 70)

        for inv in inventario:
            inv_id = inv[0]
            pm_id = inv[1] or "-"
            nombre = (inv[2] or "Sin nombre")[:39]
            cantidad = f"{inv[3]:.0f}" if inv[3] else "0"

            print(f"{inv_id:<6} {pm_id:<7} {nombre:<40} {cantidad:<8}")

    # 4. BUSCAR FACTURA DE ARA
    print("\n" + "=" * 80)
    print("🔍 BUSCANDO FACTURA DE ARA ($375,093)")
    print("=" * 80)

    cursor.execute("""
        SELECT
            f.id,
            f.usuario_id,
            f.establecimiento,
            f.total_factura,
            COUNT(i.id) as items_en_bd,
            COUNT(i.producto_maestro_id) as items_con_pm_id
        FROM facturas f
        LEFT JOIN items_factura i ON f.id = i.factura_id
        WHERE f.total_factura BETWEEN 375000 AND 376000
        GROUP BY f.id, f.usuario_id, f.establecimiento, f.total_factura
    """)

    ara = cursor.fetchall()

    if not ara:
        print("❌ NO se encontró la factura de Ara")
    else:
        print(f"\n✅ Factura encontrada:\n")
        for f in ara:
            print(f"Factura ID: {f[0]}")
            print(f"Usuario ID: {f[1]}")
            print(f"Establecimiento: {f[2]}")
            print(f"Total: ${f[3]:,.0f}")
            print(f"Items en BD: {f[4]}")
            print(f"Items con producto_maestro_id: {f[5]}")

    cursor.close()


# MAIN
def main():
    print("\n" + "=" * 80)
    print("🔍 LECFAC - DIAGNÓSTICO DE BASE DE DATOS")
    print("=" * 80)

    conn = conectar()
    if not conn:
        return

    try:
        diagnostico_completo(conn)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("\n✅ Conexión cerrada")


if __name__ == "__main__":
    main()
