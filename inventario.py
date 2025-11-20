"""
Sistema de Inventario Personal del Usuario
Gestiona el stock de productos del usuario y genera alertas

🔄 MODIFICADO: Sistema NO ACUMULATIVO
- Cada nueva compra REEMPLAZA la cantidad anterior
- Se usa el codigo_lecfac para identificar el producto único
- El sistema aprende la frecuencia de consumo basándose en las fechas de compra
"""

import os
from datetime import datetime, timedelta
from database import get_db_connection


def actualizar_inventario_desde_factura(factura_id: int, usuario_id: int):
    """
    Actualiza el inventario del usuario automáticamente cuando sube una factura

    🔄 CAMBIO IMPORTANTE: NO ACUMULATIVO
    - Si el producto ya existe, REEMPLAZA la cantidad (no suma)
    - Esto permite detectar el consumo real del usuario
    - Calcula frecuencia de compra basándose en el historial

    Args:
        factura_id: ID de la factura procesada
        usuario_id: ID del usuario

    Returns:
        dict: Resumen de productos actualizados
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Obtener todos los productos de la factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    producto_maestro_id,
                    cantidad,
                    precio_pagado
                FROM items_factura
                WHERE factura_id = %s AND usuario_id = %s
                  AND producto_maestro_id IS NOT NULL
            """,
                (factura_id, usuario_id),
            )
        else:
            cursor.execute(
                """
                SELECT
                    producto_maestro_id,
                    cantidad,
                    precio_pagado
                FROM items_factura
                WHERE factura_id = ? AND usuario_id = ?
                  AND producto_maestro_id IS NOT NULL
            """,
                (factura_id, usuario_id),
            )

        items = cursor.fetchall()
        productos_actualizados = 0

        for item in items:
            producto_id = item[0]
            cantidad = item[1] or 1
            precio = item[2]

            # Verificar si el producto ya existe en el inventario del usuario
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT id, cantidad_actual, frecuencia_compra_dias
                    FROM inventario_usuario
                    WHERE usuario_id = %s AND producto_maestro_id = %s
                """,
                    (usuario_id, producto_id),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, cantidad_actual, frecuencia_compra_dias
                    FROM inventario_usuario
                    WHERE usuario_id = ? AND producto_maestro_id = ?
                """,
                    (usuario_id, producto_id),
                )

            inventario_existente = cursor.fetchone()

            if inventario_existente:
                # ✅ CAMBIO CRÍTICO: REEMPLAZAR en lugar de SUMAR
                inv_id = inventario_existente[0]

                # 🔄 NO ACUMULATIVO: La nueva cantidad REEMPLAZA la anterior
                cantidad_nueva = cantidad  # ← NO SUMA, solo reemplaza

                print(f"🔄 Actualizando inventario (NO ACUMULATIVO):")
                print(f"   Producto ID: {producto_id}")
                print(f"   Cantidad anterior: {inventario_existente[1]}")
                print(f"   Cantidad nueva: {cantidad_nueva} ← REEMPLAZA (no suma)")

                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        UPDATE inventario_usuario
                        SET cantidad_actual = %s,
                            fecha_ultima_compra = CURRENT_DATE,
                            fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """,
                        (cantidad_nueva, inv_id),  # ← REEMPLAZA
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE inventario_usuario
                        SET cantidad_actual = ?,
                            fecha_ultima_compra = DATE('now'),
                            fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """,
                        (cantidad_nueva, inv_id),  # ← REEMPLAZA
                    )

                # Calcular frecuencia de compra (detecta consumo)
                calcular_frecuencia_compra(usuario_id, producto_id, cursor)

            else:
                # Crear nuevo registro en inventario
                print(f"➕ Creando nuevo producto en inventario:")
                print(f"   Producto ID: {producto_id}")
                print(f"   Cantidad inicial: {cantidad}")

                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        INSERT INTO inventario_usuario
                        (usuario_id, producto_maestro_id, cantidad_actual,
                         fecha_ultima_compra, nivel_alerta)
                        VALUES (%s, %s, %s, CURRENT_DATE, %s)
                    """,
                        (usuario_id, producto_id, cantidad, cantidad * 0.2),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO inventario_usuario
                        (usuario_id, producto_maestro_id, cantidad_actual,
                         fecha_ultima_compra, nivel_alerta)
                        VALUES (?, ?, ?, DATE('now'), ?)
                    """,
                        (usuario_id, producto_id, cantidad, cantidad * 0.2),
                    )

            productos_actualizados += 1

        conn.commit()

        # Verificar alertas después de actualizar
        verificar_alertas_stock(usuario_id, cursor)

        conn.close()

        print(f"✅ Inventario actualizado: {productos_actualizados} productos")

        return {
            "success": True,
            "productos_actualizados": productos_actualizados,
            "mensaje": f"{productos_actualizados} productos agregados/actualizados en tu inventario",
        }

    except Exception as e:
        print(f"❌ Error actualizando inventario: {e}")
        conn.rollback()
        conn.close()
        return {"success": False, "error": str(e)}


def calcular_frecuencia_compra(usuario_id: int, producto_id: int, cursor):
    """
    Calcula la frecuencia de compra de un producto basándose en el historial

    🧠 INTELIGENCIA DEL SISTEMA:
    - Analiza las últimas 3 compras del producto
    - Calcula promedio de días entre compras
    - Estima cuándo se agotará el producto
    - Esto permite alertas predictivas sin que el usuario registre consumo
    """
    try:
        # Obtener las últimas 3 fechas de compra
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT f.fecha_factura
                FROM facturas f
                JOIN items_factura if ON f.id = if.factura_id
                WHERE f.usuario_id = %s
                  AND if.producto_maestro_id = %s
                  AND f.fecha_factura IS NOT NULL
                ORDER BY f.fecha_factura DESC
                LIMIT 3
            """,
                (usuario_id, producto_id),
            )
        else:
            cursor.execute(
                """
                SELECT f.fecha_factura
                FROM facturas f
                JOIN items_factura if ON f.id = if.factura_id
                WHERE f.usuario_id = ?
                  AND if.producto_maestro_id = ?
                  AND f.fecha_factura IS NOT NULL
                ORDER BY f.fecha_factura DESC
                LIMIT 3
            """,
                (usuario_id, producto_id),
            )

        fechas = cursor.fetchall()

        if len(fechas) >= 2:
            # Calcular promedio de días entre compras
            intervalos = []
            for i in range(len(fechas) - 1):
                fecha1 = fechas[i][0]
                fecha2 = fechas[i + 1][0]
                if isinstance(fecha1, str):
                    fecha1 = datetime.strptime(fecha1, "%Y-%m-%d")
                if isinstance(fecha2, str):
                    fecha2 = datetime.strptime(fecha2, "%Y-%m-%d")

                dias = (fecha1 - fecha2).days
                if dias > 0:
                    intervalos.append(dias)

            if intervalos:
                frecuencia_promedio = sum(intervalos) // len(intervalos)
                fecha_ultima = fechas[0][0]
                if isinstance(fecha_ultima, str):
                    fecha_ultima = datetime.strptime(fecha_ultima, "%Y-%m-%d")

                fecha_estimada = fecha_ultima + timedelta(days=frecuencia_promedio)

                print(f"📊 Frecuencia calculada:")
                print(f"   Producto ID: {producto_id}")
                print(f"   Intervalos: {intervalos}")
                print(f"   Promedio: cada {frecuencia_promedio} días")
                print(f"   Próxima compra estimada: {fecha_estimada.date()}")

                # Actualizar en la base de datos
                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        UPDATE inventario_usuario
                        SET frecuencia_compra_dias = %s,
                            fecha_estimada_agotamiento = %s
                        WHERE usuario_id = %s AND producto_maestro_id = %s
                    """,
                        (
                            frecuencia_promedio,
                            fecha_estimada.date(),
                            usuario_id,
                            producto_id,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        UPDATE inventario_usuario
                        SET frecuencia_compra_dias = ?,
                            fecha_estimada_agotamiento = ?
                        WHERE usuario_id = ? AND producto_maestro_id = ?
                    """,
                        (
                            frecuencia_promedio,
                            fecha_estimada.strftime("%Y-%m-%d"),
                            usuario_id,
                            producto_id,
                        ),
                    )

    except Exception as e:
        print(f"❌ Error calculando frecuencia: {e}")


def verificar_alertas_stock(usuario_id: int, cursor=None):
    """
    Verifica si hay productos con stock bajo y crea alertas
    """
    debe_cerrar_conn = False
    if cursor is None:
        conn = get_db_connection()
        cursor = conn.cursor()
        debe_cerrar_conn = True

    try:
        # Buscar productos bajo el nivel de alerta
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    iu.cantidad_actual,
                    iu.nivel_alerta,
                    pm.nombre_normalizado
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                  AND iu.alerta_activa = TRUE
                  AND iu.cantidad_actual <= iu.nivel_alerta
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    iu.cantidad_actual,
                    iu.nivel_alerta,
                    pm.nombre_normalizado
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                  AND iu.alerta_activa = 1
                  AND iu.cantidad_actual <= iu.nivel_alerta
            """,
                (usuario_id,),
            )

        productos_bajos = cursor.fetchall()
        alertas_creadas = []

        for producto in productos_bajos:
            producto_id = producto[0]
            cantidad = producto[1]
            nombre = producto[3]

            # Verificar si ya existe una alerta activa para este producto
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT id FROM alertas_usuario
                    WHERE usuario_id = %s
                      AND producto_maestro_id = %s
                      AND tipo_alerta = 'stock_bajo'
                      AND activa = TRUE
                      AND enviada = FALSE
                """,
                    (usuario_id, producto_id),
                )
            else:
                cursor.execute(
                    """
                    SELECT id FROM alertas_usuario
                    WHERE usuario_id = ?
                      AND producto_maestro_id = ?
                      AND tipo_alerta = 'stock_bajo'
                      AND activa = 1
                      AND enviada = 0
                """,
                    (usuario_id, producto_id),
                )

            if not cursor.fetchone():
                # Crear nueva alerta
                mensaje = f"⚠️ Stock bajo: {nombre} (Quedan {cantidad} unidades)"

                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        INSERT INTO alertas_usuario
                        (usuario_id, producto_maestro_id, tipo_alerta,
                         mensaje_personalizado, prioridad)
                        VALUES (%s, %s, 'stock_bajo', %s, 'media')
                    """,
                        (usuario_id, producto_id, mensaje),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO alertas_usuario
                        (usuario_id, producto_maestro_id, tipo_alerta,
                         mensaje_personalizado, prioridad)
                        VALUES (?, ?, 'stock_bajo', ?, 'media')
                    """,
                        (usuario_id, producto_id, mensaje),
                    )

                alertas_creadas.append(mensaje)

        if debe_cerrar_conn:
            conn.commit()
            conn.close()

        return alertas_creadas

    except Exception as e:
        print(f"❌ Error verificando alertas: {e}")
        if debe_cerrar_conn:
            conn.close()
        return []


def obtener_inventario_usuario(usuario_id: int):
    """
    Obtiene el inventario completo del usuario con información de productos
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    iu.cantidad_actual,
                    iu.unidad_medida,
                    iu.nivel_alerta,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    iu.alerta_activa,
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 'bajo'
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 'medio'
                        ELSE 'normal'
                    END as estado_stock
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    pm.nombre_normalizado
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    iu.cantidad_actual,
                    iu.unidad_medida,
                    iu.nivel_alerta,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    iu.alerta_activa,
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 'bajo'
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 'medio'
                        ELSE 'normal'
                    END as estado_stock
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    pm.nombre_normalizado
            """,
                (usuario_id,),
            )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "categoria": row[4],
                    "cantidad": float(row[5]) if row[5] else 0,
                    "unidad": row[6],
                    "nivel_alerta": float(row[7]) if row[7] else 0,
                    "ultima_compra": str(row[8]) if row[8] else None,
                    "frecuencia_dias": row[9],
                    "fecha_agotamiento": str(row[10]) if row[10] else None,
                    "alerta_activa": bool(row[11]),
                    "estado": row[12],
                }
            )

        conn.close()
        return productos

    except Exception as e:
        print(f"❌ Error obteniendo inventario: {e}")
        conn.close()
        return []


def actualizar_cantidad_manual(
    usuario_id: int, producto_id: int, nueva_cantidad: float
):
    """
    Permite al usuario actualizar manualmente la cantidad de un producto
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET cantidad_actual = %s,
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE usuario_id = %s AND producto_maestro_id = %s
            """,
                (nueva_cantidad, usuario_id, producto_id),
            )
        else:
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET cantidad_actual = ?,
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE usuario_id = ? AND producto_maestro_id = ?
            """,
                (nueva_cantidad, usuario_id, producto_id),
            )

        conn.commit()
        conn.close()

        return {"success": True, "mensaje": "Cantidad actualizada correctamente"}

    except Exception as e:
        print(f"❌ Error actualizando cantidad: {e}")
        conn.close()
        return {"success": False, "error": str(e)}


def obtener_alertas_usuario(usuario_id: int, solo_activas: bool = True):
    """
    Obtiene las alertas del usuario
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        query = """
            SELECT
                a.id,
                a.tipo_alerta,
                a.mensaje_personalizado,
                a.prioridad,
                a.fecha_creacion,
                pm.nombre_normalizado as producto,
                e.nombre_normalizado as establecimiento
            FROM alertas_usuario a
            LEFT JOIN productos_maestros pm ON a.producto_maestro_id = pm.id
            LEFT JOIN establecimientos e ON a.establecimiento_id = e.id
            WHERE a.usuario_id = {}
        """.format(
            "%s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "?"
        )

        if solo_activas:
            query += " AND a.activa = {}".format(
                "TRUE" if os.environ.get("DATABASE_TYPE") == "postgresql" else "1"
            )

        query += " ORDER BY a.prioridad DESC, a.fecha_creacion DESC"

        cursor.execute(query, (usuario_id,))

        alertas = []
        for row in cursor.fetchall():
            alertas.append(
                {
                    "id": row[0],
                    "tipo": row[1],
                    "mensaje": row[2],
                    "prioridad": row[3],
                    "fecha": str(row[4]),
                    "producto": row[5],
                    "establecimiento": row[6],
                }
            )

        conn.close()
        return alertas

    except Exception as e:
        print(f"❌ Error obteniendo alertas: {e}")
        conn.close()
        return []
