"""
analytics_updater.py - Actualización automática de tablas analíticas
=====================================================================
Este módulo actualiza automáticamente las tablas críticas después de
procesar una factura:
- historial_compras_usuario
- patrones_compra
- productos_por_establecimiento

Autor: Santiago
Fecha: 2025-11-13
"""

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def actualizar_historial_compras(cursor, conn, factura_id: int, usuario_id: int) -> int:
    """
    Actualiza historial_compras_usuario con los items de una factura.

    Args:
        cursor: Cursor de PostgreSQL
        conn: Conexión a PostgreSQL
        factura_id: ID de la factura procesada
        usuario_id: ID del usuario

    Returns:
        int: Cantidad de registros insertados
    """
    try:
        query = """
            INSERT INTO historial_compras_usuario
                (usuario_id, producto_id, factura_id, fecha_compra, precio_pagado)
            SELECT
                %s,
                if.producto_maestro_id,
                if.factura_id,
                if.fecha_creacion,
                if.precio_pagado
            FROM items_factura if
            WHERE if.factura_id = %s
              AND if.producto_maestro_id IS NOT NULL
            ON CONFLICT (id) DO NOTHING
            RETURNING id;
        """

        cursor.execute(query, (usuario_id, factura_id))
        result = cursor.fetchall()
        conn.commit()

        count = len(result)
        logger.info(f"✅ Historial compras: {count} registros insertados para factura {factura_id}")
        return count

    except Exception as e:
        logger.error(f"❌ Error actualizando historial_compras: {e}")
        conn.rollback()
        return 0


def actualizar_patrones_compra(cursor, conn, usuario_id: int, productos_ids: list) -> int:
    """
    Recalcula patrones_compra para los productos de un usuario.

    Args:
        cursor: Cursor de PostgreSQL
        conn: Conexión a PostgreSQL
        usuario_id: ID del usuario
        productos_ids: Lista de IDs de productos a actualizar

    Returns:
        int: Cantidad de patrones actualizados
    """
    try:
        if not productos_ids:
            return 0

        # Convertir lista a string para el query
        productos_str = ','.join(str(pid) for pid in productos_ids)

        query = f"""
            INSERT INTO patrones_compra
                (usuario_id, producto_maestro_id, ultima_compra, veces_comprado, precio_promedio_pagado)
            SELECT
                if.usuario_id,
                if.producto_maestro_id,
                MAX(if.fecha_creacion)::DATE,
                COUNT(DISTINCT if.factura_id),
                AVG(if.precio_pagado)::INTEGER
            FROM items_factura if
            WHERE if.usuario_id = %s
              AND if.producto_maestro_id IN ({productos_str})
              AND if.producto_maestro_id IS NOT NULL
            GROUP BY if.usuario_id, if.producto_maestro_id
            ON CONFLICT (usuario_id, producto_maestro_id) DO UPDATE SET
                ultima_compra = EXCLUDED.ultima_compra,
                veces_comprado = EXCLUDED.veces_comprado,
                precio_promedio_pagado = EXCLUDED.precio_promedio_pagado,
                ultima_actualizacion = NOW()
            RETURNING id;
        """

        cursor.execute(query, (usuario_id,))
        result = cursor.fetchall()
        conn.commit()

        count = len(result)
        logger.info(f"✅ Patrones compra: {count} patrones actualizados para usuario {usuario_id}")
        return count

    except Exception as e:
        logger.error(f"❌ Error actualizando patrones_compra: {e}")
        conn.rollback()
        return 0


def actualizar_productos_por_establecimiento(
    cursor,
    conn,
    establecimiento_id: int,
    productos_ids: list
) -> int:
    """
    Actualiza precios de productos por establecimiento.

    Args:
        cursor: Cursor de PostgreSQL
        conn: Conexión a PostgreSQL
        establecimiento_id: ID del establecimiento
        productos_ids: Lista de IDs de productos a actualizar

    Returns:
        int: Cantidad de productos actualizados
    """
    try:
        if not productos_ids:
            return 0

        productos_str = ','.join(str(pid) for pid in productos_ids)

        query = f"""
            INSERT INTO productos_por_establecimiento
                (producto_maestro_id, establecimiento_id, precio_actual, precio_minimo,
                 precio_maximo, ultima_actualizacion, total_reportes)
            SELECT
                if.producto_maestro_id,
                f.establecimiento_id,
                AVG(if.precio_pagado)::INTEGER,
                MIN(if.precio_pagado)::INTEGER,
                MAX(if.precio_pagado)::INTEGER,
                MAX(f.fecha_factura)::TIMESTAMP,
                COUNT(*)
            FROM items_factura if
            INNER JOIN facturas f ON if.factura_id = f.id
            WHERE f.establecimiento_id = %s
              AND if.producto_maestro_id IN ({productos_str})
              AND if.producto_maestro_id IS NOT NULL
            GROUP BY if.producto_maestro_id, f.establecimiento_id
            ON CONFLICT (producto_maestro_id, establecimiento_id) DO UPDATE SET
                precio_actual = EXCLUDED.precio_actual,
                precio_minimo = LEAST(productos_por_establecimiento.precio_minimo, EXCLUDED.precio_minimo),
                precio_maximo = GREATEST(productos_por_establecimiento.precio_maximo, EXCLUDED.precio_maximo),
                ultima_actualizacion = EXCLUDED.ultima_actualizacion,
                total_reportes = productos_por_establecimiento.total_reportes + 1,
                fecha_actualizacion = NOW()
            RETURNING id;
        """

        cursor.execute(query, (establecimiento_id,))
        result = cursor.fetchall()
        conn.commit()

        count = len(result)
        logger.info(f"✅ Productos por establecimiento: {count} registros actualizados")
        return count

    except Exception as e:
        logger.error(f"❌ Error actualizando productos_por_establecimiento: {e}")
        conn.rollback()
        return 0


def actualizar_todas_las_tablas_analiticas(
    cursor,
    conn,
    factura_id: int,
    usuario_id: int,
    establecimiento_id: int,
    productos_ids: list
) -> dict:
    """
    Actualiza todas las tablas analíticas después de procesar una factura.

    Args:
        cursor: Cursor de PostgreSQL
        conn: Conexión a PostgreSQL
        factura_id: ID de la factura procesada
        usuario_id: ID del usuario
        establecimiento_id: ID del establecimiento
        productos_ids: Lista de IDs de productos en la factura

    Returns:
        dict: Resumen de actualizaciones realizadas
    """
    logger.info(f"🔄 Iniciando actualización de tablas analíticas para factura {factura_id}")

    resultado = {
        "historial_compras": 0,
        "patrones_compra": 0,
        "productos_por_establecimiento": 0,
        "exito": True,
        "errores": []
    }

    try:
        # 1. Actualizar historial de compras
        resultado["historial_compras"] = actualizar_historial_compras(
            cursor, conn, factura_id, usuario_id
        )

        # 2. Actualizar patrones de compra
        resultado["patrones_compra"] = actualizar_patrones_compra(
            cursor, conn, usuario_id, productos_ids
        )

        # 3. Actualizar productos por establecimiento
        resultado["productos_por_establecimiento"] = actualizar_productos_por_establecimiento(
            cursor, conn, establecimiento_id, productos_ids
        )

        logger.info(f"✅ Actualización completa: {resultado}")

    except Exception as e:
        logger.error(f"❌ Error en actualización de tablas analíticas: {e}")
        resultado["exito"] = False
        resultado["errores"].append(str(e))

    return resultado


def actualizar_codigo_producto(cursor, conn, producto_maestro_id: int, codigo: str) -> bool:
    """
    Actualiza el código PLU/EAN de un producto en productos_maestros.

    Args:
        cursor: Cursor de PostgreSQL
        conn: Conexión a PostgreSQL
        producto_maestro_id: ID del producto
        codigo: Código PLU o EAN

    Returns:
        bool: True si se actualizó correctamente
    """
    try:
        # Solo actualizar si el producto no tiene código o el código es diferente
        query = """
            UPDATE productos_maestros
            SET codigo_ean = %s,
                fecha_actualizacion = NOW()
            WHERE id = %s
              AND (codigo_ean IS NULL OR codigo_ean = '' OR codigo_ean != %s)
            RETURNING id;
        """

        cursor.execute(query, (codigo, producto_maestro_id, codigo))
        result = cursor.fetchone()
        conn.commit()

        if result:
            logger.info(f"✅ Código actualizado para producto {producto_maestro_id}: {codigo}")
            return True
        return False

    except Exception as e:
        logger.error(f"❌ Error actualizando código de producto {producto_maestro_id}: {e}")
        conn.rollback()
        return False
