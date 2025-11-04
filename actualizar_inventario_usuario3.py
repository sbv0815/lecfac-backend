"""
Script para actualizar inventario del usuario 3
Conexión directa a PostgreSQL
"""

import psycopg2

# Conexión directa
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def actualizar_inventario_usuario_3():
    """Actualiza el inventario del usuario 3 con todas sus facturas"""

    print("\n" + "="*80)
    print("📦 ACTUALIZANDO INVENTARIO DEL USUARIO 3")
    print("="*80)

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    try:
        # Obtener facturas del usuario 3
        cursor.execute("""
            SELECT id, establecimiento, total_factura, fecha_cargue
            FROM facturas
            WHERE usuario_id = 3
            ORDER BY id
        """)

        facturas = cursor.fetchall()

        if not facturas:
            print("❌ No se encontraron facturas para el usuario 3")
            return

        print(f"\n📋 Encontradas {len(facturas)} facturas del usuario 3")
        print("-"*80)

        exitosas = 0
        fallidas = 0
        total_productos_agregados = 0

        for factura in facturas:
            factura_id = factura[0]
            establecimiento = factura[1]
            total = factura[2]
            fecha = factura[3]

            print(f"\n🔄 Procesando Factura #{factura_id}")
            print(f"   📍 Establecimiento: {establecimiento}")
            print(f"   💰 Total: ${total:,}")
            print(f"   📅 Fecha: {fecha}")

            # Verificar items con producto_maestro_id
            cursor.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN producto_maestro_id IS NOT NULL THEN 1 ELSE 0 END) as con_id
                FROM items_factura
                WHERE factura_id = %s
            """, (factura_id,))

            stats = cursor.fetchone()
            total_items = stats[0]
            items_con_id = stats[1]

            print(f"   📦 Items: {total_items} total, {items_con_id} con producto_maestro_id")

            if items_con_id == 0:
                print(f"   ⚠️ Saltando - no hay items con producto_maestro_id")
                fallidas += 1
                continue

            # ========================================
            # ACTUALIZAR INVENTARIO MANUALMENTE
            # ========================================
            try:
                # Obtener datos de la factura
                cursor.execute("""
                    SELECT establecimiento_id, fecha_cargue
                    FROM facturas
                    WHERE id = %s
                """, (factura_id,))

                factura_data = cursor.fetchone()
                establecimiento_id = factura_data[0]
                fecha_compra = factura_data[1]

                # Obtener items válidos
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
                productos_procesados = 0

                for item in items:
                    producto_maestro_id = item[0]
                    nombre = item[1]
                    precio = int(item[2]) if item[2] else 0
                    cantidad = int(item[3]) if item[3] else 1

                    # Verificar si ya existe en inventario
                    cursor.execute("""
                        SELECT
                            id, cantidad_actual, precio_promedio,
                            numero_compras, total_gastado
                        FROM inventario_usuario
                        WHERE usuario_id = 3
                          AND producto_maestro_id = %s
                    """, (producto_maestro_id,))

                    existente = cursor.fetchone()

                    if existente:
                        # ACTUALIZAR
                        inv_id = existente[0]
                        cant_actual = float(existente[1] or 0)
                        precio_prom = int(existente[2] or 0)
                        num_compras = int(existente[3] or 0)
                        total_gast = float(existente[4] or 0)

                        nueva_cant = cant_actual + cantidad
                        nuevo_num = num_compras + 1
                        nuevo_total = total_gast + (precio * cantidad)
                        nuevo_prom = int(nuevo_total / (nueva_cant))

                        cursor.execute("""
                            UPDATE inventario_usuario
                            SET cantidad_actual = %s,
                                precio_ultima_compra = %s,
                                precio_promedio = %s,
                                fecha_ultima_compra = %s,
                                numero_compras = %s,
                                total_gastado = %s,
                                ultima_factura_id = %s
                            WHERE id = %s
                        """, (nueva_cant, precio, nuevo_prom, fecha_compra,
                              nuevo_num, nuevo_total, factura_id, inv_id))

                        print(f"      ✅ {nombre[:30]}: {cant_actual} → {nueva_cant}")
                    else:
                        # CREAR NUEVO
                        cursor.execute("""
                            INSERT INTO inventario_usuario (
                                usuario_id, producto_maestro_id,
                                cantidad_actual, precio_ultima_compra,
                                precio_promedio, establecimiento_id,
                                fecha_ultima_compra, numero_compras,
                                total_gastado, ultima_factura_id,
                                unidad_medida
                            ) VALUES (
                                3, %s, %s, %s, %s, %s, %s, 1, %s, %s, 'unidades'
                            )
                        """, (producto_maestro_id, cantidad, precio, precio,
                              establecimiento_id, fecha_compra,
                              precio * cantidad, factura_id))

                        print(f"      ➕ {nombre[:30]}: nuevo ({cantidad} unidades)")

                    productos_procesados += 1

                conn.commit()
                total_productos_agregados += productos_procesados
                print(f"   ✅ {productos_procesados} productos procesados")
                exitosas += 1

            except Exception as e:
                print(f"   ❌ Error: {e}")
                import traceback
                traceback.print_exc()
                conn.rollback()
                fallidas += 1

        print("\n" + "="*80)
        print("📊 RESUMEN:")
        print(f"   ✅ Facturas procesadas: {exitosas}")
        print(f"   ❌ Facturas con errores: {fallidas}")
        print(f"   📦 Total productos agregados: {total_productos_agregados}")
        print("="*80)

        # Verificar inventario final
        cursor.execute("""
            SELECT COUNT(*)
            FROM inventario_usuario
            WHERE usuario_id = 3
        """)

        total_productos = cursor.fetchone()[0]
        print(f"\n🏠 INVENTARIO FINAL DEL USUARIO 3: {total_productos} productos")

        if total_productos > 0:
            print("\n📦 Primeros 10 productos:")
            cursor.execute("""
                SELECT
                    pm.nombre_normalizado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = 3
                ORDER BY iu.fecha_ultima_compra DESC
                LIMIT 10
            """)

            for row in cursor.fetchall():
                print(f"   • {row[0]}: {row[1]} unidades, ${row[2]:,}")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()

    print("\n✅ Proceso completado")

if __name__ == "__main__":
    actualizar_inventario_usuario_3()
