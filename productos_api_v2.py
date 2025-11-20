# VERSION: 2024-11-19-CODIGO-LECFAC - Soporte para codigo_lecfac
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import logging
from database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/v2/productos/")
async def listar_productos_v2(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=5000),
    search: Optional[str] = None,
    categoria_id: Optional[int] = None,
    limite: int = Query(500, ge=1, le=5000),
    busqueda: Optional[str] = None,
):
    """Lista productos con búsqueda - INCLUYE codigo_lecfac"""

    print("=" * 80)
    print(f"🔥 [MAIN] /api/v2/productos llamado - busqueda={busqueda}")
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        search_term = busqueda or search
        final_limit = limite if limite != 500 else limit

        # ✅ AGREGAMOS codigo_lecfac AL SELECT
        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                pm.codigo_lecfac,
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
                OR LOWER(pm.marca) LIKE LOWER(%s)
                OR pm.codigo_lecfac LIKE %s)
            """
            )
            search_param = f"%{search_term}%"
            params.extend([search_param, search_param, search_param, search_param])
            print(f"🔍 Aplicando búsqueda: '{search_term}'")

        if categoria_id:
            where_conditions.append("pm.categoria_id = %s")
            params.append(categoria_id)

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)

        # ✅ AGREGAMOS codigo_lecfac AL GROUP BY
        query += """
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.marca, pm.codigo_lecfac, c.nombre
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

            # ✅ INCLUIMOS codigo_lecfac EN LA RESPUESTA
            resultado.append(
                {
                    "id": producto[0],
                    "codigo_ean": producto[1],
                    "nombre": producto[2],
                    "marca": producto[3],
                    "codigo_lecfac": producto[4],  # ← NUEVO CAMPO
                    "categoria": producto[5],
                    "num_establecimientos": producto[6],
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
    Obtiene UN producto por ID - INCLUYE codigo_lecfac
    VERSION: 2024-11-19-CODIGO-LECFAC
    """
    print(f"📋 [Router] GET producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # ✅ AGREGAMOS codigo_lecfac AL SELECT
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
                pm.es_producto_papa,
                pm.codigo_lecfac
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

        # ✅ INCLUIMOS codigo_lecfac EN LA RESPUESTA
        return {
            "id": producto[0],
            "codigo_ean": producto[1] or "",
            "nombre": producto[2],
            "nombre_consolidado": producto[2],
            "marca": producto[3] or "",
            "categoria": producto[5] or "Sin categoría",
            "categoria_id": producto[4],
            "veces_comprado": producto[6] or 0,
            "num_establecimientos": len(plus_info),
            "es_producto_papa": producto[7] or False,
            "codigo_lecfac": producto[8],  # ← NUEVO CAMPO
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
    VERSION: 2024-11-19-CORREGIDO - Fix EAN vacío
    """
    print(f"✏️ [Router] PUT producto ID: {producto_id}")
    print(f"   Datos recibidos: {request}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            "SELECT nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (producto_id,),
        )

        producto_actual = cursor.fetchone()

        if not producto_actual:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        print(f"   Producto actual: {producto_actual[0]}")

        # Construir UPDATE dinámico
        updates = []
        params = []

        if "nombre_consolidado" in request:
            nombre = request["nombre_consolidado"].strip()
            if nombre:  # Solo actualizar si no está vacío
                updates.append("nombre_consolidado = %s")
                params.append(nombre)
                print(f"   ✅ Actualizando nombre: {nombre}")

        if "marca" in request:
            marca = request["marca"].strip() if request["marca"] else None
            updates.append("marca = %s")
            params.append(marca)
            print(f"   ✅ Actualizando marca: {marca}")

        if "codigo_ean" in request:
            # Leer EAN actual del producto
            cursor.execute(
                "SELECT codigo_ean FROM productos_maestros_v2 WHERE id = %s",
                (producto_id,),
            )
            ean_actual = cursor.fetchone()[0]

            # Procesar EAN nuevo
            ean_value = request["codigo_ean"].strip() if request["codigo_ean"] else None
            ean_final = ean_value if ean_value else None

            # Solo actualizar si es diferente
            if ean_actual != ean_final:
                # ✅ NUEVA VALIDACIÓN: Verificar que no existe en OTRO producto DEL MISMO ESTABLECIMIENTO
                if ean_final:
                    cursor.execute(
                        """
                    SELECT DISTINCT pm.id, pm.nombre_consolidado, e.nombre_normalizado
                    FROM productos_maestros_v2 pm
                    INNER JOIN productos_por_establecimiento ppe1 ON pm.id = ppe1.producto_maestro_id
                    INNER JOIN productos_por_establecimiento ppe2 ON ppe2.producto_maestro_id = %s
                    INNER JOIN establecimientos e ON ppe1.establecimiento_id = e.id
                    WHERE pm.codigo_ean = %s
                    AND pm.id != %s
                    AND ppe1.establecimiento_id = ppe2.establecimiento_id
                    LIMIT 1
                    """,
                        (producto_id, ean_final, producto_id),
                    )

                conflicto = cursor.fetchone()
                if conflicto:
                    cursor.close()
                    conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"El código EAN '{ean_final}' ya existe para el producto '{conflicto[1]}' en {conflicto[2]}",
                )

            updates.append("codigo_ean = %s")
            params.append(ean_final)
            print(f"   ✅ Actualizando EAN: {ean_actual} → {ean_final}")
        else:
            print(f"   ℹ️  EAN sin cambios: {ean_actual}")

        if "categoria_id" in request:
            updates.append("categoria_id = %s")
            params.append(request["categoria_id"])
            print(f"   ✅ Actualizando categoria_id: {request['categoria_id']}")

        if "categoria" in request and "categoria_id" not in request:
            # Si se envió nombre de categoría, buscar su ID
            categoria_nombre = request["categoria"].strip()
            if categoria_nombre and categoria_nombre != "Sin categoría":
                cursor.execute(
                    "SELECT id FROM categorias WHERE LOWER(nombre) = LOWER(%s)",
                    (categoria_nombre,),
                )
                cat_row = cursor.fetchone()
                if cat_row:
                    updates.append("categoria_id = %s")
                    params.append(cat_row[0])
                    print(
                        f"   ✅ Actualizando categoría por nombre: {categoria_nombre} (ID: {cat_row[0]})"
                    )

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
            RETURNING id, nombre_consolidado, marca, codigo_ean, categoria_id, codigo_lecfac
        """

        print(f"   🔍 Query: {query}")
        print(f"   🔍 Params: {params}")

        cursor.execute(query, params)
        updated = cursor.fetchone()

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"✅ Producto {producto_id} actualizado exitosamente")
        print(f"   ✅ Resultado: {updated}")

        return {
            "success": True,
            "producto": {
                "id": updated[0],
                "nombre_consolidado": updated[1],
                "marca": updated[2],
                "codigo_ean": updated[3],
                "categoria_id": updated[4],
                "codigo_lecfac": updated[5],  # ← INCLUIDO EN RESPUESTA
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


@router.put("/api/v2/productos/{producto_id}/plus")
async def actualizar_plus_producto(producto_id: int, request: dict):
    """Actualiza los PLUs de un producto - VERSION: 2024-11-19"""
    print(f"🔧 [Router] PUT PLUs del producto ID: {producto_id}")
    print(f"   Datos: {request}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar producto existe
        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # 1. ELIMINAR PLUs marcados
        plus_eliminados = 0
        if "plus_a_eliminar" in request and request["plus_a_eliminar"]:
            for plu_id in request["plus_a_eliminar"]:
                cursor.execute(
                    "DELETE FROM productos_por_establecimiento WHERE id = %s AND producto_maestro_id = %s",
                    (plu_id, producto_id),
                )
                plus_eliminados += cursor.rowcount
            print(f"   🗑️ {plus_eliminados} PLUs eliminados")

        # 2. ACTUALIZAR/CREAR PLUs
        plus_actualizados = 0
        plus_creados = 0

        if "plus" in request and request["plus"]:
            for plu in request["plus"]:
                plu_id = plu.get("id")
                codigo = plu.get("codigo_plu", "").strip()
                est_id = plu.get("establecimiento_id")
                precio = plu.get("precio_unitario", 0)

                if not codigo or not est_id:
                    print(f"   ⚠️ PLU incompleto: {plu}")
                    continue

                if plu_id:
                    # Actualizar existente
                    cursor.execute(
                        """
                        UPDATE productos_por_establecimiento
                        SET codigo_plu = %s, precio_unitario = %s, fecha_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = %s AND producto_maestro_id = %s
                    """,
                        (codigo, precio, plu_id, producto_id),
                    )
                    if cursor.rowcount > 0:
                        plus_actualizados += 1
                        print(f"   ✅ PLU {plu_id} actualizado: {codigo}")
                else:
                    # Crear nuevo
                    cursor.execute(
                        """
                        SELECT id FROM productos_por_establecimiento
                        WHERE producto_maestro_id = %s AND establecimiento_id = %s AND codigo_plu = %s
                    """,
                        (producto_id, est_id, codigo),
                    )

                    if cursor.fetchone():
                        print(f"   ⚠️ PLU {codigo} ya existe")
                        continue

                    cursor.execute(
                        """
                        INSERT INTO productos_por_establecimiento (
                            producto_maestro_id, establecimiento_id, codigo_plu,
                            precio_unitario, precio_actual, precio_minimo, precio_maximo,
                            total_reportes, fecha_creacion, ultima_actualizacion, fecha_actualizacion
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                        (producto_id, est_id, codigo, precio, precio, precio, precio),
                    )
                    plus_creados += 1
                    print(f"   ✅ PLU creado: {codigo}")

        conn.commit()

        logger.info(
            f"✅ PLUs - Actualizados: {plus_actualizados}, Creados: {plus_creados}, Eliminados: {plus_eliminados}"
        )

        return {
            "success": True,
            "actualizados": plus_actualizados,
            "creados": plus_creados,
            "eliminados": plus_eliminados,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error actualizando PLUs: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


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

        # ELIMINACIONES CRÍTICAS
        cursor.execute(
            "DELETE FROM precios_productos WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        precios_eliminados = cursor.rowcount
        logger.info(f"   💰 {precios_eliminados} precios eliminados")

        cursor.execute(
            "DELETE FROM productos_por_establecimiento WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        plus_eliminados = cursor.rowcount
        logger.info(f"   🗑️ {plus_eliminados} PLUs eliminados")

        cursor.execute(
            "UPDATE items_factura SET producto_maestro_id = NULL WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        items_desvinculados = cursor.rowcount
        logger.info(f"   🔗 {items_desvinculados} items desvinculados")

        cursor.execute(
            "DELETE FROM inventario_usuario WHERE producto_maestro_id = %s",
            (producto_id,),
        )
        inventario_eliminado = cursor.rowcount
        logger.info(f"   📦 {inventario_eliminado} registros de inventario eliminados")

        # ELIMINACIÓN FINAL
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
