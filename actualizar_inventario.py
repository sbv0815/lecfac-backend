"""
Actualizar Inventario Manualmente - Factura 13
===============================================
"""

import psycopg

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def actualizar_inventario_factura_13():
    """Actualiza el inventario del usuario desde la factura 13"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        factura_id = 13
        usuario_id = 1

        # Obtener información de la factura
        cursor.execute("""
            SELECT establecimiento, fecha_factura
            FROM facturas
            WHERE id = %s
        """, (factura_id,))

        factura = cursor.fetchone()
        establecimiento = factura[0]
        fecha_compra = factura[1]

        print(f"📄 Actualizando inventario desde Factura #{factura_id}")
        print(f"   🏪 Establecimiento: {establecimiento}")
        print(f"   📅 Fecha: {fecha_compra}")
        print("=" * 70)

        # Obtener establecimiento_id
        cursor.execute("""
            SELECT id FROM establecimientos
            WHERE LOWER(TRIM(nombre_normalizado)) = LOWER(TRIM(%s))
        """, (establecimiento,))

        est_row = cursor.fetchone()
        establecimiento_id = est_row[0] if est_row else None

        # Obtener items con producto_maestro_id
        cursor.execute("""
            SELECT
                producto_maestro_id,
                nombre_leido,
                precio_pagado,
                cantidad
            FROM items_factura
            WHERE factura_id = %s
              AND producto_maestro_id IS NOT NULL
        """, (factura_id,))

        items = cursor.fetchall()

        if not items:
            print("⚠️ No hay items con producto_maestro_id")
            return

        print(f"\n📦 Procesando {len(items)} items...\n")

        actualizados = 0
        creados = 0

        for item in items:
            producto_maestro_id = item[0]
            nombre = item[1]
            precio = int(item[2])
            cantidad = int(item[3])

            try:
                # Verificar si ya existe en inventario
                cursor.execute("""
                    SELECT id, cantidad_actual, numero_compras, total_gastado
                    FROM inventario_usuario
                    WHERE usuario_id = %s
                      AND producto_maestro_id = %s
                """, (usuario_id, producto_maestro_id))

                # ✅ CORRECCIÓN: Verificar si hay resultado
                try:
                    inventario = cursor.fetchone()
                except:
                    inventario = None

                if inventario:
                    # ACTUALIZAR
                    inv_id = inventario[0]
                    cantidad_actual = float(inventario[1])
                    num_compras = int(inventario[2])
                    total_gastado = float(inventario[3])

                    nueva_cantidad = cantidad_actual + cantidad
                    nuevo_num_compras = num_compras + 1
                    nuevo_total_gastado = total_gastado + (precio * cantidad)
                    nuevo_precio_promedio = int(nuevo_total_gastado / (nueva_cantidad if nueva_cantidad > 0 else 1))

                    cursor.execute("""
                        UPDATE inventario_usuario
                        SET cantidad_actual = %s,
                            precio_ultima_compra = %s,
                            precio_promedio = %s,
                            fecha_ultima_compra = %s,
                            numero_compras = %s,
                            total_gastado = %s,
                            ultima_factura_id = %s,
                            fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (nueva_cantidad, precio, nuevo_precio_promedio, fecha_compra,
                          nuevo_num_compras, nuevo_total_gastado, factura_id, inv_id))

                    conn.commit()
                    actualizados += 1
                    print(f"   ✅ {nombre}: {cantidad_actual} → {nueva_cantidad}")

                else:
                    # CREAR
                    cursor.execute("""
                        INSERT INTO inventario_usuario (
                            usuario_id,
                            producto_maestro_id,
                            cantidad_actual,
                            precio_ultima_compra,
                            precio_promedio,
                            precio_minimo,
                            precio_maximo,
                            establecimiento,
                            establecimiento_id,
                            fecha_ultima_compra,
                            numero_compras,
                            cantidad_total_comprada,
                            total_gastado,
                            ultima_factura_id,
                            nivel_alerta,
                            unidad_medida
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'unidades'
                        )
                    """, (
                        usuario_id, producto_maestro_id,
                        cantidad, precio, precio, precio, precio,
                        establecimiento, establecimiento_id,
                        fecha_compra, 1, cantidad,
                        precio * cantidad, factura_id,
                        cantidad * 0.3
                    ))

                    conn.commit()
                    creados += 1
                    print(f"   ➕ {nombre}: nuevo producto ({cantidad} unidades)")

            except Exception as e:
                print(f"   ❌ Error con {nombre}: {e}")
                conn.rollback()
                continue

        print("\n" + "=" * 70)
        print(f"✅ INVENTARIO ACTUALIZADO")
        print(f"   Productos actualizados: {actualizados}")
        print(f"   Productos nuevos: {creados}")
        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    actualizar_inventario_factura_13()
