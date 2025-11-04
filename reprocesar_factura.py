"""
Reprocesar Factura Manualmente
================================
Procesa los items de la factura 13 y les asigna producto_maestro_id
"""

import psycopg
from normalizador_codigos import (
    normalizar_codigo_por_establecimiento,
    buscar_o_crear_producto_inteligente
)

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def reprocesar_factura_13():
    """Reprocesa la factura 13 asignando producto_maestro_id a cada item"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        factura_id = 13
        usuario_id = 1

        # Obtener información de la factura
        cursor.execute("""
            SELECT establecimiento, total_factura
            FROM facturas
            WHERE id = %s
        """, (factura_id,))

        factura = cursor.fetchone()
        if not factura:
            print(f"❌ Factura {factura_id} no encontrada")
            return

        establecimiento = factura[0]
        total_factura = factura[1]

        print(f"📄 Reprocesando Factura #{factura_id}")
        print(f"   🏪 Establecimiento: {establecimiento}")
        print(f"   💰 Total: ${total_factura:,}")
        print("=" * 70)

        # Obtener items sin producto_maestro_id
        cursor.execute("""
            SELECT
                id,
                codigo_leido,
                nombre_leido,
                precio_pagado,
                cantidad
            FROM items_factura
            WHERE factura_id = %s
              AND producto_maestro_id IS NULL
        """, (factura_id,))

        items = cursor.fetchall()

        if not items:
            print("✅ No hay items sin procesar")
            conn.close()
            return

        print(f"\n📦 Procesando {len(items)} items...\n")

        procesados = 0
        errores = 0

        for item in items:
            item_id = item[0]
            codigo_raw = item[1] if item[1] else ""
            nombre = item[2]
            precio = int(item[3])
            cantidad = int(item[4])

            try:
                print(f"🔄 Item #{item_id}: {nombre}")
                print(f"   Código: '{codigo_raw}'")
                print(f"   Precio: ${precio:,}")

                # Normalizar código
                codigo, tipo_codigo, confianza = normalizar_codigo_por_establecimiento(
                    codigo_raw, establecimiento
                )

                print(f"   📟 Normalizado: {codigo} ({tipo_codigo}, {confianza}%)")

                # Buscar o crear producto
                producto_maestro_id, accion = buscar_o_crear_producto_inteligente(
                    cursor, conn,
                    codigo, tipo_codigo, nombre, establecimiento, precio,
                    codigo_raw=codigo_raw
                )

                if producto_maestro_id:
                    # Actualizar item con producto_maestro_id
                    cursor.execute("""
                        UPDATE items_factura
                        SET producto_maestro_id = %s,
                            matching_confianza = %s
                        WHERE id = %s
                    """, (producto_maestro_id, confianza, item_id))

                    conn.commit()
                    procesados += 1
                    print(f"   ✅ Producto asignado: ID={producto_maestro_id} ({accion})\n")
                else:
                    errores += 1
                    print(f"   ❌ No se pudo asignar producto\n")

            except Exception as e:
                errores += 1
                print(f"   ❌ Error: {e}\n")
                conn.rollback()
                continue

        print("=" * 70)
        print(f"✅ REPROCESAMIENTO COMPLETADO")
        print(f"   Procesados exitosamente: {procesados}")
        print(f"   Errores: {errores}")
        print("=" * 70)

        # Actualizar inventario del usuario
        if procesados > 0:
            print("\n🔄 Actualizando inventario del usuario...")
            from database import actualizar_inventario_desde_factura

            exito = actualizar_inventario_desde_factura(factura_id, usuario_id)

            if exito:
                print("✅ Inventario actualizado correctamente")
            else:
                print("⚠️ Hubo problemas actualizando el inventario")

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    reprocesar_factura_13()
