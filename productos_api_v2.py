# VERSION: 2024-11-27-PAPA-V4 - Sistema de Aprendizaje PAPA
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import logging
from database import get_db_connection
import base64
import httpx


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
    solo_papas: bool = Query(False),
    filtro: Optional[str] = None,  # 🆕 V4: Filtro por fuente
):
    """
    Lista productos con búsqueda - INCLUYE campos PAPA
    """
    print("=" * 80)
    print(
        f"🔥 [MAIN] /api/v2/productos llamado - busqueda={busqueda}, solo_papas={solo_papas}, filtro={filtro}"
    )
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        search_term = busqueda or search
        final_limit = limite if limite != 500 else limit

        # 🆕 V4: Incluir campos PAPA en el SELECT
        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                pm.codigo_lecfac,
                pm.es_producto_papa,
                COALESCE(c.nombre, 'Sin categoría') as categoria,
                COUNT(DISTINCT pe.establecimiento_id) as num_establecimientos,
                pm.confianza_datos,
                pm.fuente_datos
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            LEFT JOIN productos_por_establecimiento pe ON pm.id = pe.producto_maestro_id
        """

        where_conditions = []
        params = []

        if solo_papas:
            where_conditions.append("pm.es_producto_papa = TRUE")
            print("👑 Filtrando solo productos PAPA")

        # 🆕 V4: Filtros por fuente de datos
        if filtro:
            if filtro == "papa":
                where_conditions.append("pm.es_producto_papa = TRUE")
                print("👑 Filtrando productos PAPA validados")
            elif filtro == "web":
                where_conditions.append(
                    "pm.fuente_datos = 'WEB' AND pm.es_producto_papa = FALSE"
                )
                print("🌐 Filtrando productos WEB")
            elif filtro == "ocr":
                where_conditions.append(
                    "pm.fuente_datos = 'OCR' AND pm.es_producto_papa = FALSE"
                )
                print("📝 Filtrando productos OCR")
            elif filtro == "sin_ean":
                where_conditions.append("pm.codigo_ean IS NULL")
                print("⚠️ Filtrando productos sin EAN")
            elif filtro == "sin_marca":
                where_conditions.append("(pm.marca IS NULL OR pm.marca = '')")
                print("⚠️ Filtrando productos sin marca")

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

        query += """
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.marca, pm.codigo_lecfac,
                    pm.es_producto_papa, c.nombre,
                    pm.confianza_datos, pm.fuente_datos
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

            # 🆕 V4: Incluir campos PAPA en respuesta
            resultado.append(
                {
                    "id": producto[0],
                    "codigo_ean": producto[1],
                    "nombre": producto[2],
                    "marca": producto[3],
                    "codigo_lecfac": producto[4],
                    "es_producto_papa": producto[5] or False,
                    "categoria": producto[6],
                    "num_establecimientos": producto[7],
                    "confianza_datos": float(producto[8]) if producto[8] else 0.5,
                    "fuente_datos": producto[9] or "OCR",
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
    Obtiene UN producto por ID - INCLUYE campos PAPA
    """
    print(f"📋 [Router] GET producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 🆕 V4: Incluir campos PAPA
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
                pm.codigo_lecfac,
                pm.confianza_datos,
                pm.fuente_datos,
                pm.fecha_validacion
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

        # 🆕 V4: Respuesta con campos PAPA
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
            "codigo_lecfac": producto[8],
            "confianza_datos": float(producto[9]) if producto[9] else 0.5,
            "fuente_datos": producto[10] or "OCR",
            "fecha_validacion": producto[11].isoformat() if producto[11] else None,
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
    """
    print(f"📍 [Router] GET PLUs del producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )

        if not cursor.fetchone():
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

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
    Actualiza un producto (nombre, marca, categoría, EAN) + campos PAPA
    VERSION: 2024-11-27-PAPA-V4 - Soporte sistema de aprendizaje
    """
    print(f"✏️ [Router] PUT producto ID: {producto_id}")
    print(f"   Datos recibidos: {request}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            "SELECT nombre_consolidado, codigo_ean, es_producto_papa, fuente_datos FROM productos_maestros_v2 WHERE id = %s",
            (producto_id,),
        )

        producto_actual = cursor.fetchone()

        if not producto_actual:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        print(f"   Producto actual: {producto_actual[0]}")

        ean_actual = producto_actual[1]
        era_papa = producto_actual[2]
        fuente_actual = producto_actual[3]

        # Construir UPDATE dinámico
        updates = []
        params = []

        # Procesar nombre_consolidado
        if "nombre_consolidado" in request:
            nombre = (
                request["nombre_consolidado"].strip()
                if request["nombre_consolidado"]
                else ""
            )
            if nombre:
                updates.append("nombre_consolidado = %s")
                params.append(nombre)
                print(f"   ✅ Actualizando nombre: {nombre}")

        # Procesar marca
        if "marca" in request:
            marca = request["marca"].strip() if request["marca"] else None
            updates.append("marca = %s")
            params.append(marca)
            print(f"   ✅ Actualizando marca: {marca}")

        # Procesar codigo_ean
        advertencia_ean = None
        if "codigo_ean" in request:
            ean_value = request["codigo_ean"].strip() if request["codigo_ean"] else None
            ean_final = ean_value if ean_value else None

            if ean_actual != ean_final:
                if ean_final:
                    cursor.execute(
                        """
                        SELECT pm.id, pm.nombre_consolidado
                        FROM productos_maestros_v2 pm
                        WHERE pm.codigo_ean = %s
                        AND pm.id != %s
                        LIMIT 5
                        """,
                        (ean_final, producto_id),
                    )

                    conflictos = cursor.fetchall()

                    if conflictos:
                        productos_con_ean = ", ".join(
                            [f"'{c[1]}' (ID:{c[0]})" for c in conflictos]
                        )
                        advertencia_ean = f"⚠️ Este EAN ya existe en: {productos_con_ean}. Considera fusionar estos productos."
                        print(
                            f"   ⚠️ ADVERTENCIA: EAN {ean_final} existe en otros productos"
                        )

                updates.append("codigo_ean = %s")
                params.append(ean_final)
                print(f"   ✅ Actualizando EAN: {ean_actual} → {ean_final}")

                # 🆕 V4: Si se agrega EAN, actualizar fuente a WEB (si no era PAPA)
                if ean_final and not era_papa and "es_producto_papa" not in request:
                    updates.append("fuente_datos = 'WEB'")
                    updates.append("confianza_datos = 0.8")
                    print(f"   🌐 Actualizando fuente a WEB por tener EAN")

        # Procesar categoria_id
        if "categoria_id" in request:
            updates.append("categoria_id = %s")
            params.append(request["categoria_id"])
            print(f"   ✅ Actualizando categoria_id: {request['categoria_id']}")

        # Procesar categoría por nombre
        if "categoria" in request and "categoria_id" not in request:
            categoria_nombre = (
                request["categoria"].strip() if request["categoria"] else ""
            )
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
                        f"   ✅ Actualizando categoría: {categoria_nombre} (ID: {cat_row[0]})"
                    )

        # ========================================
        # 🆕 V4: CAMPOS DEL SISTEMA PAPA
        # ========================================

        # Procesar es_producto_papa
        if "es_producto_papa" in request:
            es_papa = request["es_producto_papa"]

            if es_papa:
                # Marcar como PAPA
                updates.append("es_producto_papa = TRUE")
                updates.append("fecha_validacion = CURRENT_TIMESTAMP")
                updates.append("fuente_datos = 'PAPA'")
                updates.append("confianza_datos = 1.0")
                print(f"   👑 Marcando como PAPA (validado)")
            else:
                # Quitar marca de PAPA
                updates.append("es_producto_papa = FALSE")
                updates.append("fecha_validacion = NULL")

                # Restaurar fuente basada en si tiene EAN
                cursor.execute(
                    "SELECT codigo_ean FROM productos_maestros_v2 WHERE id = %s",
                    (producto_id,),
                )
                row = cursor.fetchone()
                tiene_ean = row and row[0] and len(row[0]) >= 8

                if tiene_ean:
                    updates.append("fuente_datos = 'WEB'")
                    updates.append("confianza_datos = 0.8")
                    print(f"   🌐 Restaurando fuente a WEB (tiene EAN)")
                else:
                    updates.append("fuente_datos = 'OCR'")
                    updates.append("confianza_datos = 0.5")
                    print(f"   📝 Restaurando fuente a OCR (sin EAN)")

        # Procesar confianza_datos (si se envía explícitamente)
        if "confianza_datos" in request and request["confianza_datos"] is not None:
            if "es_producto_papa" not in request:  # Solo si no se está cambiando PAPA
                confianza = float(request["confianza_datos"])
                updates.append("confianza_datos = %s")
                params.append(confianza)
                print(f"   📊 Actualizando confianza: {confianza}")

        # Procesar fuente_datos (si se envía explícitamente)
        if "fuente_datos" in request and request["fuente_datos"]:
            if "es_producto_papa" not in request:  # Solo si no se está cambiando PAPA
                fuente = request["fuente_datos"]
                updates.append("fuente_datos = %s")
                params.append(fuente)
                print(f"   📌 Actualizando fuente: {fuente}")

        if not updates:
            cursor.close()
            conn.close()
            return {"success": False, "error": "No hay campos para actualizar"}

        params.append(producto_id)

        query = f"""
            UPDATE productos_maestros_v2
            SET {', '.join(updates)}
            WHERE id = %s
            RETURNING id, nombre_consolidado, marca, codigo_ean, categoria_id, codigo_lecfac,
                      es_producto_papa, confianza_datos, fuente_datos
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

        response = {
            "success": True,
            "producto": {
                "id": updated[0],
                "nombre_consolidado": updated[1],
                "marca": updated[2],
                "codigo_ean": updated[3],
                "categoria_id": updated[4],
                "codigo_lecfac": updated[5],
                "es_producto_papa": updated[6],
                "confianza_datos": float(updated[7]) if updated[7] else 0.5,
                "fuente_datos": updated[8] or "OCR",
            },
        }

        if advertencia_ean:
            response["advertencia"] = advertencia_ean

        return response

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
    """Actualiza los PLUs de un producto"""
    print(f"🔧 [Router] PUT PLUs del producto ID: {producto_id}")
    print(f"   Datos: {request}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        plus_eliminados = 0
        if "plus_a_eliminar" in request and request["plus_a_eliminar"]:
            for plu_id in request["plus_a_eliminar"]:
                cursor.execute(
                    "DELETE FROM productos_por_establecimiento WHERE id = %s AND producto_maestro_id = %s",
                    (plu_id, producto_id),
                )
                plus_eliminados += cursor.rowcount
            print(f"   🗑️ {plus_eliminados} PLUs eliminados")

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
    Elimina un producto y TODAS sus referencias (12 tablas)
    VERSION: 2024-11-25-FIX-DELETE-COMPLETE - Limpieza completa de todas las foreign keys
    """
    print(f"🗑️ [Router] DELETE producto ID: {producto_id}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

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
        print(f"   🗑️  Eliminando producto: {nombre}")

        # ========== LIMPIEZA DE TODAS LAS TABLAS RELACIONADAS ==========
        # Función para verificar si una tabla existe
        def table_exists(table_name):
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = %s
                )
            """,
                (table_name,),
            )
            return cursor.fetchone()[0]

        # Función auxiliar para eliminar de tablas
        def safe_delete(table_name, column_name="producto_maestro_id"):
            if not table_exists(table_name):
                return 0
            cursor.execute(
                f"DELETE FROM {table_name} WHERE {column_name} = %s",
                (producto_id,),
            )
            return cursor.rowcount

        def safe_update(table_name, column_name="producto_maestro_id"):
            if not table_exists(table_name):
                return 0
            cursor.execute(
                f"UPDATE {table_name} SET {column_name} = NULL WHERE {column_name} = %s",
                (producto_id,),
            )
            return cursor.rowcount

        # 1. Eliminar precios históricos
        precios_eliminados = safe_delete("precios_productos")
        logger.info(f"   💰 {precios_eliminados} precios eliminados")

        # 2. Eliminar PLUs por establecimiento
        plus_eliminados = safe_delete("productos_por_establecimiento")
        logger.info(f"   🗑️ {plus_eliminados} PLUs eliminados")

        # 3. Desvincular items de factura (UPDATE a NULL)
        items_desvinculados = safe_update("items_factura")
        logger.info(f"   🔗 {items_desvinculados} items desvinculados")

        # 4. Eliminar inventario de usuario
        inventario_eliminado = safe_delete("inventario_usuario")
        logger.info(f"   📦 {inventario_eliminado} registros de inventario eliminados")

        # 5. Eliminar historial de compras
        historial_eliminado = safe_delete("historial_compras_usuario", "producto_id")
        logger.info(f"   📊 {historial_eliminado} registros de historial eliminados")

        # 6. Eliminar patrones de compra
        patrones_eliminados = safe_delete("patrones_compra")
        logger.info(f"   📈 {patrones_eliminados} patrones de compra eliminados")

        # 7-11. Eliminar de tablas opcionales (pueden no existir)
        safe_delete("productos_en_grupo")
        safe_delete("codigos_alternativos")
        safe_delete("variantes_nombres")
        safe_delete("precios_historicos_v2")
        safe_delete("log_mejoras_nombres")

        # 12. FINALMENTE - Eliminar el producto maestro
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


# ============================================================
# 🆕 V4: ENDPOINTS DE ESTADÍSTICAS PAPA
# ============================================================


@router.get("/api/v2/productos/estadisticas/fuentes")
async def estadisticas_por_fuente():
    """
    Obtiene estadísticas de productos por fuente de datos
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                fuente_datos,
                es_producto_papa,
                COUNT(*) as cantidad,
                ROUND(AVG(confianza_datos)::numeric, 2) as confianza_promedio
            FROM productos_maestros_v2
            GROUP BY fuente_datos, es_producto_papa
            ORDER BY fuente_datos, es_producto_papa
        """
        )

        rows = cursor.fetchall()

        # Contar totales
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN es_producto_papa = TRUE THEN 1 ELSE 0 END) as papas,
                SUM(CASE WHEN fuente_datos = 'WEB' AND es_producto_papa = FALSE THEN 1 ELSE 0 END) as web,
                SUM(CASE WHEN fuente_datos = 'OCR' AND es_producto_papa = FALSE THEN 1 ELSE 0 END) as ocr,
                SUM(CASE WHEN codigo_ean IS NOT NULL THEN 1 ELSE 0 END) as con_ean,
                SUM(CASE WHEN codigo_ean IS NULL THEN 1 ELSE 0 END) as sin_ean
            FROM productos_maestros_v2
        """
        )

        totales = cursor.fetchone()

        cursor.close()

        total = totales[0] or 0
        papas = totales[1] or 0
        web = totales[2] or 0
        ocr = totales[3] or 0

        calidad = round(((papas + web) / total * 100), 1) if total > 0 else 0

        return {
            "success": True,
            "estadisticas": {
                "total": total,
                "papas": papas,
                "web": web,
                "ocr": ocr,
                "con_ean": totales[4] or 0,
                "sin_ean": totales[5] or 0,
                "porcentaje_calidad": calidad,
            },
            "detalle": [
                {
                    "fuente": row[0],
                    "es_papa": row[1],
                    "cantidad": row[2],
                    "confianza_promedio": float(row[3]) if row[3] else 0.5,
                }
                for row in rows
            ],
        }

    except Exception as e:
        logger.error(f"❌ Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# ============================================================
# ENDPOINT DE ENRIQUECIMIENTO (mantener compatibilidad)
# ============================================================

from lecfac_enricher import ProductEnricher

enricher = ProductEnricher()


@router.post("/api/productos/enriquecer/{plu}")
async def enriquecer_producto(plu: str):
    """
    Enriquece un producto usando scraping de Carulla
    """
    try:
        producto = await enricher.enriquecer_por_plu(plu, "Carulla")

        if producto:
            return {"success": True, "data": producto}
        else:
            return {"success": False, "error": "Producto no encontrado"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@router.get("/api/v2/productos/{producto_id}/factura")
async def obtener_factura_producto(producto_id: int):
    """
    Obtiene la factura original donde apareció el producto.
    Retorna la imagen en base64 y los datos del OCR original.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Buscar en qué factura(s) apareció este producto
        cursor.execute(
            """
            SELECT
                i.id as item_id,
                i.factura_id,
                i.codigo_leido,
                i.nombre_leido,
                i.precio_pagado,
                i.cantidad,
                f.numero_factura,
                f.fecha_factura,
                f.tiene_imagen,
                f.imagen_data,
                f.imagen_mime,
                e.nombre_normalizado as establecimiento
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
            WHERE i.producto_maestro_id = %s
            ORDER BY f.fecha_factura DESC
            LIMIT 5
        """,
            (producto_id,),
        )

        items = cursor.fetchall()

        if not items:
            cursor.close()
            conn.close()
            return {
                "success": False,
                "error": "No se encontraron facturas para este producto",
                "facturas": [],
            }

        facturas_resultado = []

        for item in items:
            (
                item_id,
                factura_id,
                codigo_leido,
                nombre_leido,
                precio_pagado,
                cantidad,
                numero_factura,
                fecha_factura,
                tiene_imagen,
                imagen_data,
                imagen_mime,
                establecimiento,
            ) = item

            factura_info = {
                "factura_id": factura_id,
                "numero_factura": numero_factura,
                "fecha": fecha_factura.isoformat() if fecha_factura else None,
                "establecimiento": establecimiento,
                "item": {
                    "codigo_leido": codigo_leido,
                    "nombre_leido": nombre_leido,
                    "precio_pagado": precio_pagado,
                    "cantidad": cantidad,
                },
                "tiene_imagen": tiene_imagen or False,
                "imagen_base64": None,
                "imagen_mime": imagen_mime,
            }

            # Convertir imagen a base64 si existe
            if tiene_imagen and imagen_data:
                try:
                    imagen_b64 = base64.b64encode(imagen_data).decode("utf-8")
                    factura_info["imagen_base64"] = imagen_b64
                except Exception as e:
                    print(f"Error convirtiendo imagen: {e}")

            facturas_resultado.append(factura_info)

        cursor.close()
        conn.close()

        return {
            "success": True,
            "producto_id": producto_id,
            "total_facturas": len(facturas_resultado),
            "facturas": facturas_resultado,
        }

    except Exception as e:
        print(f"❌ Error obteniendo factura: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 🔍 BUSCAR PLU EN SUPERMERCADO (VTEX)
# ============================================================

# Configuración de supermercados VTEX
VTEX_CONFIG = {
    "OLIMPICA": {
        "domain": "www.olimpica.com",
        "search_url": "https://www.olimpica.com/api/catalog_system/pub/products/search",
    },
    "EXITO": {
        "domain": "www.exito.com",
        "search_url": "https://www.exito.com/api/catalog_system/pub/products/search",
    },
    "CARULLA": {
        "domain": "www.carulla.com",
        "search_url": "https://www.carulla.com/api/catalog_system/pub/products/search",
    },
    "JUMBO": {
        "domain": "www.jumbocolombia.com",
        "search_url": "https://www.jumbocolombia.com/api/catalog_system/pub/products/search",
    },
}


@router.get("/api/v2/buscar-plu/{establecimiento}/{codigo_plu}")
async def buscar_plu_en_supermercado(establecimiento: str, codigo_plu: str):
    """
    Busca un código PLU/EAN en el catálogo web del supermercado.
    Usa la API VTEX para obtener el nombre real del producto.
    """
    establecimiento_upper = establecimiento.upper().strip()

    # Normalizar nombre del establecimiento
    establecimiento_map = {
        "OLIMPICA": "OLIMPICA",
        "OLÍMPICA": "OLIMPICA",
        "EXITO": "EXITO",
        "ÉXITO": "EXITO",
        "CARULLA": "CARULLA",
        "JUMBO": "JUMBO",
        "CENCOSUD": "JUMBO",
        "METRO": "JUMBO",  # Metro usa plataforma Jumbo
        "D1": "D1",
        "ARA": "ARA",
        "FARMATODO": "FARMATODO",
        "CRUZ BLANCA": "CRUZ BLANCA",
    }

    establecimiento_norm = establecimiento_map.get(establecimiento_upper)

    if not establecimiento_norm or establecimiento_norm not in VTEX_CONFIG:
        return {
            "success": False,
            "error": f"Supermercado '{establecimiento}' no soporta búsqueda web",
            "supermercados_disponibles": list(VTEX_CONFIG.keys()),
        }

    config = VTEX_CONFIG[establecimiento_norm]

    try:
        # Intentar buscar por el código
        async with httpx.AsyncClient(timeout=15.0) as client:
            # Primero intentar búsqueda directa por código
            search_url = f"{config['search_url']}?fq=alternateIds_RefId:{codigo_plu}"

            headers = {
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }

            response = await client.get(search_url, headers=headers)

            productos = []

            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    productos = data

            # Si no encontró, intentar búsqueda por EAN
            if not productos:
                search_url = f"{config['search_url']}?fq=alternateIds_Ean:{codigo_plu}"
                response = await client.get(search_url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        productos = data

            # Si aún no encontró, buscar como texto
            if not productos:
                search_url = f"{config['search_url']}?ft={codigo_plu}"
                response = await client.get(search_url, headers=headers)

                if response.status_code == 200:
                    data = response.json()
                    if data and len(data) > 0:
                        productos = data[:3]  # Limitar a 3 resultados

            if not productos:
                return {
                    "success": False,
                    "error": f"No se encontró el código '{codigo_plu}' en {establecimiento_norm}",
                    "codigo_buscado": codigo_plu,
                    "establecimiento": establecimiento_norm,
                }

            # Procesar resultados
            resultados = []
            for prod in productos[:5]:  # Máximo 5 resultados
                # Extraer EAN
                ean = None
                if "items" in prod and prod["items"]:
                    item = prod["items"][0]
                    ean = item.get("ean") or item.get("referenceId", [{}])[0].get(
                        "Value"
                    )

                # Extraer precio
                precio = None
                if "items" in prod and prod["items"]:
                    item = prod["items"][0]
                    if "sellers" in item and item["sellers"]:
                        precio = (
                            item["sellers"][0].get("commertialOffer", {}).get("Price")
                        )

                resultado = {
                    "nombre": prod.get("productName", "Sin nombre"),
                    "marca": prod.get("brand", ""),
                    "ean": ean,
                    "precio": precio,
                    "categoria": (
                        prod.get("categories", [""])[0]
                        if prod.get("categories")
                        else ""
                    ),
                    "imagen": prod.get("items", [{}])[0]
                    .get("images", [{}])[0]
                    .get("imageUrl", ""),
                }
                resultados.append(resultado)

            return {
                "success": True,
                "codigo_buscado": codigo_plu,
                "establecimiento": establecimiento_norm,
                "resultados": resultados,
                "total": len(resultados),
            }

    except httpx.TimeoutException:
        return {
            "success": False,
            "error": f"Timeout buscando en {establecimiento_norm}. El servidor tardó demasiado.",
            "codigo_buscado": codigo_plu,
        }
    except Exception as e:
        print(f"❌ Error buscando PLU: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": str(e), "codigo_buscado": codigo_plu}


# ============================================================
# 📋 OBTENER HISTORIAL DE UN PLU
# ============================================================


@router.get("/api/v2/historial-plu/{establecimiento}/{codigo_plu}")
async def obtener_historial_plu(establecimiento: str, codigo_plu: str):
    """
    Obtiene todas las veces que un PLU apareció en facturas.
    Útil para ver variaciones del nombre leído por OCR.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar el establecimiento
        cursor.execute(
            """
            SELECT id, nombre_normalizado
            FROM establecimientos
            WHERE UPPER(nombre_normalizado) LIKE %s
            LIMIT 1
        """,
            (f"%{establecimiento.upper()}%",),
        )

        est = cursor.fetchone()
        if not est:
            return {
                "success": False,
                "error": f"Establecimiento '{establecimiento}' no encontrado",
            }

        establecimiento_id = est[0]
        establecimiento_nombre = est[1]

        # Buscar todas las apariciones de este PLU
        cursor.execute(
            """
            SELECT
                i.nombre_leido,
                i.precio_pagado,
                f.fecha_factura,
                f.numero_factura,
                COUNT(*) as veces
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            WHERE i.codigo_leido = %s
              AND f.establecimiento_id = %s
            GROUP BY i.nombre_leido, i.precio_pagado, f.fecha_factura, f.numero_factura
            ORDER BY f.fecha_factura DESC
            LIMIT 20
        """,
            (codigo_plu, establecimiento_id),
        )

        historial = cursor.fetchall()

        cursor.close()
        conn.close()

        if not historial:
            return {
                "success": False,
                "error": f"No se encontró historial para PLU '{codigo_plu}' en {establecimiento_nombre}",
            }

        # Encontrar el nombre más común
        nombres_count = {}
        for h in historial:
            nombre = h[0]
            if nombre:
                nombres_count[nombre] = nombres_count.get(nombre, 0) + 1

        nombre_mas_comun = (
            max(nombres_count, key=nombres_count.get) if nombres_count else None
        )

        return {
            "success": True,
            "codigo_plu": codigo_plu,
            "establecimiento": establecimiento_nombre,
            "nombre_sugerido": nombre_mas_comun,
            "total_apariciones": len(historial),
            "historial": [
                {
                    "nombre_leido": h[0],
                    "precio": h[1],
                    "fecha": h[2].isoformat() if h[2] else None,
                    "factura": h[3],
                    "veces": h[4],
                }
                for h in historial
            ],
        }

    except Exception as e:
        print(f"❌ Error obteniendo historial PLU: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================
# ENDPOINT: GET /api/v2/productos/{producto_id}/factura
# VERSIÓN 2.0: CON POSICIÓN VERTICAL DEL PRODUCTO
# =============================================================
# Agregar a productos_api_v2.py
# =============================================================


# =============================================================
# ENDPOINT: GET /api/v2/productos/{producto_id}/factura
# VERSIÓN 2.0: CON POSICIÓN VERTICAL DEL PRODUCTO
# =============================================================
# Agregar a productos_api_v2.py (usa router, NO app)
# =============================================================


@router.get("/productos/{producto_id}/factura")
async def obtener_factura_producto(producto_id: int):
    """
    Obtiene la(s) factura(s) donde apareció un producto.
    Incluye la imagen y la posición vertical del producto.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Query con posicion_vertical
        cursor.execute(
            """
            SELECT
                i.id as item_id,
                i.codigo_leido,
                i.nombre_leido,
                i.precio_pagado,
                i.cantidad,
                i.posicion_vertical,
                f.id as factura_id,
                f.numero_factura,
                f.fecha_factura,
                f.imagen_data,
                f.imagen_mime,
                f.tiene_imagen,
                e.nombre_normalizado as establecimiento
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
            WHERE i.producto_maestro_id = %s
            ORDER BY f.fecha_factura DESC
            LIMIT 5
        """,
            (producto_id,),
        )

        rows = cursor.fetchall()

        if not rows:
            return {
                "success": False,
                "mensaje": "No se encontraron facturas para este producto",
                "producto_id": producto_id,
                "total_facturas": 0,
                "facturas": [],
            }

        facturas = []
        for row in rows:
            # Convertir imagen a base64 si existe
            imagen_base64 = None
            if row[9]:  # imagen_data
                import base64

                imagen_base64 = base64.b64encode(row[9]).decode("utf-8")

            facturas.append(
                {
                    "factura_id": row[6],
                    "numero_factura": row[7],
                    "fecha": str(row[8]) if row[8] else None,
                    "establecimiento": row[12],
                    "item": {
                        "item_id": row[0],
                        "codigo_leido": row[1],
                        "nombre_leido": row[2],
                        "precio_pagado": row[3],
                        "cantidad": float(row[4]) if row[4] else 1,
                        "posicion_vertical": (
                            row[5] if row[5] is not None else 50
                        ),  # Default: mitad
                    },
                    "tiene_imagen": row[11] or False,
                    "imagen_base64": imagen_base64,
                    "imagen_mime": row[10] or "image/jpeg",
                }
            )

        return {
            "success": True,
            "producto_id": producto_id,
            "total_facturas": len(facturas),
            "facturas": facturas,
        }

    except Exception as e:
        print(f"❌ Error obteniendo factura: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


# =============================================================
# ENDPOINT: BUSCAR PLU EN VTEX
# =============================================================
# Agregar a productos_api_v2.py
# Usa el WebEnricher existente
# =============================================================

from web_enricher import WebEnricher, es_tienda_vtex, SUPERMERCADOS_VTEX


@router.get("/buscar-vtex/{establecimiento}/{codigo}")
async def buscar_en_vtex(establecimiento: str, codigo: str):
    """
    Busca un código PLU/EAN en la API VTEX del supermercado.

    Ejemplos:
        GET /api/v2/buscar-vtex/OLIMPICA/632967
        GET /api/v2/buscar-vtex/CARULLA/7702004001234
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    try:
        cursor = conn.cursor()

        # Normalizar establecimiento
        establecimiento_upper = establecimiento.upper().strip()

        # Verificar si es supermercado VTEX
        if not es_tienda_vtex(establecimiento_upper):
            supermercados_disponibles = list(set(SUPERMERCADOS_VTEX.values()))
            return {
                "success": False,
                "error": f"'{establecimiento}' no tiene API VTEX disponible",
                "supermercados_disponibles": [
                    "OLIMPICA",
                    "EXITO",
                    "CARULLA",
                    "JUMBO",
                    "ALKOSTO",
                    "MAKRO",
                    "COLSUBSIDIO",
                ],
                "codigo_buscado": codigo,
            }

        # Crear enricher y buscar
        enricher = WebEnricher(cursor, conn)

        resultado = enricher.enriquecer(
            codigo=codigo.strip(),
            nombre_ocr="",  # No tenemos nombre OCR en búsqueda manual
            establecimiento=establecimiento_upper,
            precio_ocr=0,  # Sin precio de referencia
        )

        if resultado.encontrado:
            return {
                "success": True,
                "codigo_buscado": codigo,
                "establecimiento": establecimiento_upper,
                "fuente": resultado.fuente,
                "resultado": {
                    "nombre": resultado.nombre_web,
                    "marca": resultado.marca,
                    "ean": resultado.codigo_ean,
                    "plu": resultado.codigo_plu,
                    "precio": resultado.precio_web,
                    "presentacion": resultado.presentacion,
                    "categoria": resultado.categoria,
                    "url": resultado.url_producto,
                },
            }
        else:
            return {
                "success": False,
                "codigo_buscado": codigo,
                "establecimiento": establecimiento_upper,
                "error": "Producto no encontrado en el catálogo web",
                "sugerencias": [
                    "Verifica que el código PLU sea correcto",
                    "Intenta con otro supermercado",
                    "El producto puede no estar en el catálogo online",
                ],
            }

    except Exception as e:
        print(f"❌ Error buscando en VTEX: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()


@router.get("/supermercados-vtex")
async def listar_supermercados_vtex():
    """
    Lista los supermercados con API VTEX disponible.
    """
    return {
        "success": True,
        "supermercados": [
            {"codigo": "OLIMPICA", "nombre": "Olímpica", "disponible": True},
            {"codigo": "EXITO", "nombre": "Éxito", "disponible": True},
            {"codigo": "CARULLA", "nombre": "Carulla", "disponible": True},
            {"codigo": "JUMBO", "nombre": "Jumbo", "disponible": True},
            {"codigo": "ALKOSTO", "nombre": "Alkosto", "disponible": True},
            {"codigo": "MAKRO", "nombre": "Makro", "disponible": True},
            {"codigo": "COLSUBSIDIO", "nombre": "Colsubsidio", "disponible": True},
        ],
        "sin_soporte": [
            {"codigo": "D1", "nombre": "D1", "razon": "No tiene API pública"},
            {"codigo": "ARA", "nombre": "Ara", "razon": "No tiene API pública"},
            {
                "codigo": "FARMATODO",
                "nombre": "Farmatodo",
                "razon": "No tiene API pública",
            },
        ],
    }
