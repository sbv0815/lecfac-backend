# VERSION: 2024-11-18-19:45 - FIX LIMITE 5000 PARA PAPA-DASHBOARD
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import logging
from database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/v2/productos/")
async def listar_productos_v2(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=5000),  # ✅ AUMENTADO DE 100 A 5000
    search: Optional[str] = None,
    categoria_id: Optional[int] = None,
    limite: int = Query(500, ge=1, le=5000),  # ✅ AUMENTADO DE 1000 A 5000
    busqueda: Optional[str] = None,
):
    """Lista productos con búsqueda"""

    print("=" * 80)
    print(f"🔥 /api/v2/productos/ LLAMADO")
    print(f"   busqueda={busqueda}, search={search}")
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        search_term = busqueda or search
        final_limit = limite if limite != 500 else limit

        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                COALESCE(c.nombre, 'Sin categoría') as categoria,
                COUNT(DISTINCT pe.establecimiento_id) as num_establecimientos
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            LEFT JOIN productos_por_establecimiento pe ON pm.id = pe.producto_maestro_id
        """

        where_conditions = []
        params = []

        if search_term:
            where_conditions.append(
                """
                (LOWER(pm.nombre_consolidado) LIKE LOWER(%s)
                OR pm.codigo_ean LIKE %s
                OR LOWER(pm.marca) LIKE LOWER(%s))
            """
            )
            search_param = f"%{search_term}%"
            params.extend([search_param, search_param, search_param])
            print(f"🔍 Aplicando búsqueda: '{search_term}'")

        if categoria_id:
            where_conditions.append("pm.categoria_id = %s")
            params.append(categoria_id)

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        query += """
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.marca, c.nombre
            ORDER BY pm.id DESC
            LIMIT %s OFFSET %s
        """
        params.extend([final_limit, skip])

        cursor.execute(query, params)
        productos = cursor.fetchall()

        print(f"✅ Encontrados {len(productos)} productos")

        resultado = []
        for producto in productos:
            producto_id = producto[0]

            cursor.execute(
                """
                SELECT
                    pe.codigo_plu,
                    e.nombre_normalizado as establecimiento,
                    pe.precio_unitario,
                    pe.ultima_actualizacion as ultima_compra
                FROM productos_por_establecimiento pe
                JOIN establecimientos e ON pe.establecimiento_id = e.id
                WHERE pe.producto_maestro_id = %s
                ORDER BY pe.ultima_actualizacion DESC
            """,
                (producto_id,),
            )

            plus = cursor.fetchall()

            plus_info = []
            for plu in plus:
                plus_info.append(
                    {
                        "codigo": plu[0],
                        "establecimiento": plu[1],
                        "precio": float(plu[2]) if plu[2] else 0.0,
                        "ultima_compra": plu[3],
                    }
                )

            resultado.append(
                {
                    "id": producto[0],
                    "codigo_ean": producto[1],
                    "nombre": producto[2],
                    "marca": producto[3],
                    "categoria": producto[4],
                    "num_establecimientos": producto[5],
                    "plus": plus_info,
                }
            )

        cursor.close()
        return {"success": True, "productos": resultado, "total": len(resultado)}

    except Exception as e:
        logger.error(f"❌ Error listando productos: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/productos/categorias")
async def listar_categorias():
    """Lista categorías"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, nombre, icono
            FROM categorias
            ORDER BY nombre
        """
        )

        categorias = cursor.fetchall()
        cursor.close()

        return {
            "categorias": [
                {"id": cat[0], "nombre": cat[1], "icono": cat[2]} for cat in categorias
            ]
        }
    except Exception as e:
        logger.error(f"Error listando categorías: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/productos/{producto_id}")
async def obtener_producto_individual(producto_id: int):
    """
    Obtiene UN producto por ID
    VERSION: 2024-11-18-17:30
    """
    print(f"📋 [Router] GET producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                pm.categoria_id,
                c.nombre as categoria_nombre,
                pm.veces_visto,
                pm.es_producto_papa
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            WHERE pm.id = %s
        """,
            (producto_id,),
        )

        producto = cursor.fetchone()

        if not producto:
            cursor.close()
            conn.close()
            logger.error(f"❌ Producto {producto_id} no encontrado")
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        logger.info(f"✅ Producto: {producto[2]}")

        # Obtener PLUs
        cursor.execute(
            """
            SELECT
                pe.codigo_plu,
                e.nombre_normalizado,
                pe.precio_unitario,
                pe.ultima_actualizacion
            FROM productos_por_establecimiento pe
            JOIN establecimientos e ON pe.establecimiento_id = e.id
            WHERE pe.producto_maestro_id = %s
            ORDER BY pe.ultima_actualizacion DESC
        """,
            (producto_id,),
        )

        plus_rows = cursor.fetchall()

        plus_info = []
        for plu in plus_rows:
            plus_info.append(
                {
                    "codigo_plu": plu[0],
                    "establecimiento": plu[1],
                    "precio": float(plu[2]) if plu[2] else 0.0,
                    "ultima_vez_visto": plu[3],
                }
            )

        cursor.close()
        conn.close()

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre": producto[2],
            "nombre_consolidado": producto[2],
            "marca": producto[3],
            "categoria": producto[5] or "Sin categoría",
            "categoria_id": producto[4],
            "veces_comprado": producto[6] or 0,
            "num_establecimientos": len(plus_info),
            "es_producto_papa": producto[7] or False,
            "plus": plus_info,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/productos/{producto_id}/plus")
async def obtener_plus_producto(producto_id: int):
    """
    Obtiene los PLUs de un producto en diferentes establecimientos
    VERSION: 2024-11-18-18:00
    """
    print(f"📍 [Router] GET PLUs del producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que el producto existe
        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )

        if not cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener PLUs
        cursor.execute(
            """
            SELECT
                ppe.id,
                ppe.codigo_plu,
                ppe.establecimiento_id,
                e.nombre_normalizado as establecimiento,
                ppe.precio_unitario,
                ppe.precio_minimo,
                ppe.precio_maximo,
                ppe.total_reportes,
                ppe.fecha_actualizacion
            FROM productos_por_establecimiento ppe
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
            ORDER BY ppe.fecha_actualizacion DESC
        """,
            (producto_id,),
        )

        plus = []
        for row in cursor.fetchall():
            plus.append(
                {
                    "id": row[0],
                    "codigo_plu": row[1],
                    "establecimiento_id": row[2],
                    "establecimiento": row[3],
                    "precio_unitario": float(row[4]) if row[4] else 0.0,
                    "precio_minimo": float(row[5]) if row[5] else 0.0,
                    "precio_maximo": float(row[6]) if row[6] else 0.0,
                    "total_reportes": row[7] or 0,
                    "fecha_actualizacion": row[8],
                }
            )

        cursor.close()
        conn.close()

        logger.info(f"✅ {len(plus)} PLUs encontrados para producto {producto_id}")

        return {
            "success": True,
            "producto_id": producto_id,
            "plus": plus,
            "total": len(plus),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error obteniendo PLUs: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.put("/api/v2/productos/{producto_id}")
async def actualizar_producto(producto_id: int, request: dict):
    """
    Actualiza un producto (nombre, marca, categoría, EAN)
    VERSION: 2024-11-18-18:00
    """
    print(f"✏️ [Router] PUT producto ID: {producto_id}")
    print(f"   Datos: {request}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )

        if not cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Construir UPDATE dinámico
        updates = []
        params = []

        if "nombre_consolidado" in request:
            updates.append("nombre_consolidado = %s")
            params.append(request["nombre_consolidado"])

        if "marca" in request:
            updates.append("marca = %s")
            params.append(request["marca"])

        if "codigo_ean" in request:
            updates.append("codigo_ean = %s")
            params.append(request["codigo_ean"])

        if "categoria_id" in request:
            updates.append("categoria_id = %s")
            params.append(request["categoria_id"])

        if not updates:
            cursor.close()
            conn.close()
            return {"success": False, "error": "No hay campos para actualizar"}

        # Agregar fecha de actualización
        updates.append("fecha_actualizacion = CURRENT_TIMESTAMP")
        params.append(producto_id)

        query = f"""
            UPDATE productos_maestros_v2
            SET {', '.join(updates)}
            WHERE id = %s
            RETURNING id, nombre_consolidado, marca, codigo_ean, categoria_id
        """

        cursor.execute(query, params)
        updated = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Producto {producto_id} actualizado: {updated[1]}")

        return {
            "success": True,
            "producto": {
                "id": updated[0],
                "nombre_consolidado": updated[1],
                "marca": updated[2],
                "codigo_ean": updated[3],
                "categoria_id": updated[4],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error actualizando producto: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/v2/productos/{producto_id}")
async def eliminar_producto(producto_id: int):
    """
    Elimina un producto y TODAS sus referencias
    VERSION: 2024-11-18-19:40
    """
    print(f"🗑️ [Router] DELETE producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            "SELECT nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (producto_id,),
        )

        producto = cursor.fetchone()

        if not producto:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        nombre = producto[0]

        # ============================================================
        # ELIMINACIONES CRÍTICAS (deben funcionar)
        # ============================================================

        # 1. Eliminar de precios_productos
        cursor.execute(
            "DELETE FROM precios_productos WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        precios_eliminados = cursor.rowcount
        logger.info(f"   💰 {precios_eliminados} precios eliminados")

        # 2. Eliminar PLUs asociados
        cursor.execute(
            "DELETE FROM productos_por_establecimiento WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        plus_eliminados = cursor.rowcount
        logger.info(f"   🗑️ {plus_eliminados} PLUs eliminados")

        # 3. Desvincular items_factura
        cursor.execute(
            "UPDATE items_factura SET producto_maestro_id = NULL WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        items_desvinculados = cursor.rowcount
        logger.info(f"   🔗 {items_desvinculados} items desvinculados")

        # 4. Eliminar de inventario_usuario
        cursor.execute(
            "DELETE FROM inventario_usuario WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        inventario_eliminado = cursor.rowcount
        logger.info(f"   📦 {inventario_eliminado} registros de inventario eliminados")

        # ============================================================
        # ELIMINACIONES OPCIONALES (pueden fallar sin romper)
        # ============================================================

        historial_eliminado = 0
        patrones_eliminados = 0

        # Intentar eliminar de historial (puede no tener la columna)
        try:
            cursor.execute(
                "DELETE FROM historial_compras_usuario WHERE producto_id = %s",
                (producto_id,),
            )
            historial_eliminado = cursor.rowcount
            logger.info(
                f"   📊 {historial_eliminado} registros de historial eliminados"
            )
        except Exception as e:
            # Si falla, hacer rollback del error pero continuar
            conn.rollback()
            # Reabrir transacción
            cursor = conn.cursor()
            logger.warning(f"   ⚠️ Historial no limpiado (columna no existe)")

        # Intentar eliminar de patrones_compra
        try:
            cursor.execute(
                "DELETE FROM patrones_compra WHERE producto_maestro_id = %s",
                (producto_id,),
            )
            patrones_eliminados = cursor.rowcount
            logger.info(f"   📈 {patrones_eliminados} patrones eliminados")
        except Exception as e:
            conn.rollback()
            cursor = conn.cursor()
            logger.warning(f"   ⚠️ Patrones no limpiados")

        # ============================================================
        # ELIMINACIÓN FINAL DEL PRODUCTO duplicado
        # ============================================================

        cursor.execute(
            "DELETE FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Producto {producto_id} eliminado completamente: {nombre}")

        return {
            "success": True,
            "mensaje": f"Producto '{nombre}' eliminado correctamente",
            "detalles": {
                "precios_eliminados": precios_eliminados,
                "plus_eliminados": plus_eliminados,
                "items_desvinculados": items_desvinculados,
                "inventario_eliminado": inventario_eliminado,
                "historial_eliminado": historial_eliminado,
                "patrones_eliminados": patrones_eliminados,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error eliminando producto: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))
