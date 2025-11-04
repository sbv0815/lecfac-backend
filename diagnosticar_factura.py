"""
Diagnóstico de Última Factura Escaneada
========================================
"""

import psycopg

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def diagnosticar_ultima_factura():
    """Verifica qué pasó con la última factura del usuario"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        # Usuario Santiago
        usuario_id = 1

        # Obtener última factura
        cursor.execute("""
            SELECT
                id,
                establecimiento,
                total_factura,
                fecha_factura,
                estado,
                estado_validacion,
                productos_detectados,
                productos_guardados,
                fecha_cargue,
                fecha_procesamiento
            FROM facturas
            WHERE usuario_id = %s
            ORDER BY fecha_cargue DESC
            LIMIT 1
        """, (usuario_id,))

        factura = cursor.fetchone()

        if not factura:
            print("❌ No hay facturas para este usuario")
            conn.close()
            return

        factura_id = factura[0]

        print("=" * 70)
        print(f"📄 ÚLTIMA FACTURA (ID: {factura_id})")
        print("=" * 70)
        print(f"🏪 Establecimiento: {factura[1]}")
        print(f"💰 Total: ${factura[2]:,}" if factura[2] else "💰 Total: No registrado")
        print(f"📅 Fecha factura: {factura[3]}")
        print(f"📊 Estado: {factura[4]}")
        print(f"✅ Estado validación: {factura[5]}")
        print(f"📦 Productos detectados: {factura[6]}")
        print(f"💾 Productos guardados: {factura[7]}")
        print(f"📤 Fecha cargue: {factura[8]}")
        print(f"🔄 Fecha procesamiento: {factura[9]}")

        # Obtener items de la factura
        cursor.execute("""
            SELECT
                COUNT(*) as total_items,
                SUM(CASE WHEN producto_maestro_id IS NOT NULL THEN 1 ELSE 0 END) as con_producto_maestro
            FROM items_factura
            WHERE factura_id = %s
        """, (factura_id,))

        items_info = cursor.fetchone()

        print(f"\n📦 ITEMS DE LA FACTURA:")
        print(f"   Total items: {items_info[0]}")
        print(f"   Con producto_maestro_id: {items_info[1]}")
        print(f"   Sin producto_maestro_id: {items_info[0] - items_info[1]}")

        # Ver algunos items
        cursor.execute("""
            SELECT
                id,
                codigo_leido,
                nombre_leido,
                precio_pagado,
                cantidad,
                producto_maestro_id
            FROM items_factura
            WHERE factura_id = %s
            LIMIT 5
        """, (factura_id,))

        print(f"\n📋 PRIMEROS 5 ITEMS:")
        for item in cursor.fetchall():
            print(f"   - {item[2]}: ${item[3]:,} (PM_ID: {item[5]})")

        # Verificar inventario del usuario
        cursor.execute("""
            SELECT COUNT(*)
            FROM inventario_usuario
            WHERE usuario_id = %s
        """, (usuario_id,))

        total_inventario = cursor.fetchone()[0]

        print(f"\n📋 INVENTARIO DEL USUARIO:")
        print(f"   Total productos en inventario: {total_inventario}")

        # Verificar si se actualizó el inventario después de esta factura
        cursor.execute("""
            SELECT COUNT(*)
            FROM inventario_usuario
            WHERE usuario_id = %s
              AND ultima_factura_id = %s
        """, (usuario_id, factura_id))

        items_de_esta_factura = cursor.fetchone()[0]

        print(f"   Productos de esta factura en inventario: {items_de_esta_factura}")

        # Diagnóstico
        print(f"\n" + "=" * 70)
        print("🔍 DIAGNÓSTICO:")
        print("=" * 70)

        if items_info[0] == 0:
            print("❌ PROBLEMA: No hay items en la factura")
            print("   Causa: El OCR no detectó productos o no se guardaron")
        elif items_info[1] == 0:
            print("❌ PROBLEMA: Ningún item tiene producto_maestro_id")
            print("   Causa: El matching de productos falló completamente")
        elif items_de_esta_factura == 0:
            print("⚠️ PROBLEMA: Los items NO se agregaron al inventario")
            print("   Causa: La función actualizar_inventario_desde_factura() no se ejecutó")
            print("\n🔧 SOLUCIÓN: Ejecutar manualmente la actualización del inventario")
        else:
            print("✅ Todo parece estar bien")
            print(f"   {items_de_esta_factura} productos se agregaron al inventario")

        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    diagnosticar_ultima_factura()
