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
    Obtiene los PLUs de un producto en diferentes establecimientos.
    Incluye el origen del PLU (FACTURA, VTEX, MANUAL).
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

        # Verificar si la columna origen_codigo existe
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_por_establecimiento'
            AND column_name = 'origen_codigo'
        """
        )
        tiene_origen = cursor.fetchone() is not None

        if tiene_origen:
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
                    ppe.fecha_actualizacion,
                    ppe.origen_codigo
                FROM productos_por_establecimiento ppe
                JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE ppe.producto_maestro_id = %s
                ORDER BY ppe.fecha_actualizacion DESC
            """,
                (producto_id,),
            )
        else:
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
                    ppe.fecha_actualizacion,
                    'FACTURA' as origen_codigo
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
                    "origen_codigo": row[9] or "FACTURA",  # 🆕 Origen del PLU
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
    """
    Actualiza los PLUs de un producto.
    Soporta origen_codigo (FACTURA, VTEX, MANUAL).
    """
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

        # Verificar si la columna origen_codigo existe
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_por_establecimiento'
            AND column_name = 'origen_codigo'
        """
        )
        tiene_origen = cursor.fetchone() is not None

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
                origen = plu.get("origen_codigo", "MANUAL")  # 🆕 Origen

                if not codigo or not est_id:
                    print(f"   ⚠️ PLU incompleto: {plu}")
                    continue

                if plu_id:
                    # Actualizar existente
                    if tiene_origen:
                        cursor.execute(
                            """
                            UPDATE productos_por_establecimiento
                            SET codigo_plu = %s,
                                precio_unitario = %s,
                                origen_codigo = %s,
                                fecha_actualizacion = CURRENT_TIMESTAMP
                            WHERE id = %s AND producto_maestro_id = %s
                        """,
                            (codigo, precio, origen, plu_id, producto_id),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE productos_por_establecimiento
                            SET codigo_plu = %s,
                                precio_unitario = %s,
                                fecha_actualizacion = CURRENT_TIMESTAMP
                            WHERE id = %s AND producto_maestro_id = %s
                        """,
                            (codigo, precio, plu_id, producto_id),
                        )

                    if cursor.rowcount > 0:
                        plus_actualizados += 1
                        print(f"   ✅ PLU {plu_id} actualizado: {codigo} ({origen})")
                else:
                    # Verificar que no exista ya este PLU para este producto y establecimiento
                    cursor.execute(
                        """
                        SELECT id FROM productos_por_establecimiento
                        WHERE producto_maestro_id = %s
                        AND establecimiento_id = %s
                        AND codigo_plu = %s
                    """,
                        (producto_id, est_id, codigo),
                    )

                    if cursor.fetchone():
                        print(
                            f"   ⚠️ PLU {codigo} ya existe para este producto en este establecimiento"
                        )
                        continue

                    # Crear nuevo
                    if tiene_origen:
                        cursor.execute(
                            """
                            INSERT INTO productos_por_establecimiento (
                                producto_maestro_id, establecimiento_id, codigo_plu,
                                precio_unitario, precio_actual, precio_minimo, precio_maximo,
                                total_reportes, origen_codigo,
                                fecha_creacion, ultima_actualizacion, fecha_actualizacion
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s,
                                      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                            (
                                producto_id,
                                est_id,
                                codigo,
                                precio,
                                precio,
                                precio,
                                precio,
                                origen,
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO productos_por_establecimiento (
                                producto_maestro_id, establecimiento_id, codigo_plu,
                                precio_unitario, precio_actual, precio_minimo, precio_maximo,
                                total_reportes,
                                fecha_creacion, ultima_actualizacion, fecha_actualizacion
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1,
                                      CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                            (
                                producto_id,
                                est_id,
                                codigo,
                                precio,
                                precio,
                                precio,
                                precio,
                            ),
                        )

                    plus_creados += 1
                    print(f"   ✅ PLU creado: {codigo} ({origen})")

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


@router.get("/api/v2/productos/{producto_id}/factura")
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


@router.get("/api/v2/buscar-vtex/{establecimiento}/{codigo}")
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


@router.get("/api/v2/supermercados-vtex")
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


# =============================================================
# ENDPOINT: BUSCAR PRODUCTOS EN VTEX - BÚSQUEDA PARCIAL
# =============================================================
# Busca con los primeros dígitos y muestra TODAS las opciones
# El usuario elige cuál es el correcto
# =============================================================

import requests
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuración VTEX
VTEX_URLS = {
    "OLIMPICA": "https://www.olimpica.com",
    "EXITO": "https://www.exito.com",
    "CARULLA": "https://www.carulla.com",
    "JUMBO": "https://www.tiendasjumbo.co",
    "ALKOSTO": "https://www.alkosto.com",
    "MAKRO": "https://www.makro.com.co",
    "COLSUBSIDIO": "https://www.mercadocolsubsidio.com",
}


def _buscar_plu_variante(args):
    """Función helper para búsqueda paralela"""
    codigo, base_url, headers, establecimiento = args
    try:
        url = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_RefId:{codigo}"
        resp = requests.get(url, headers=headers, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            resultados = []
            for item in data:
                prod = _parsear_producto(item, base_url, establecimiento)
                if prod:
                    prod["plu_buscado"] = codigo
                    resultados.append(prod)
            return resultados
    except:
        pass
    return []


@router.get("/api/v2/buscar-productos/{establecimiento}")
async def buscar_productos_vtex(
    establecimiento: str,
    q: str = None,  # Término de búsqueda (PLU parcial o nombre)
    limite: int = 15,
):
    """
    Busca productos en VTEX con coincidencia parcial.
    Devuelve múltiples opciones para que el usuario elija.

    Ejemplos:
        GET /api/v2/buscar-productos/OLIMPICA?q=23264
        GET /api/v2/buscar-productos/CARULLA?q=leche
        GET /api/v2/buscar-productos/EXITO?q=queso&limite=20
    """

    establecimiento_upper = establecimiento.upper().strip()

    if establecimiento_upper not in VTEX_URLS:
        return {
            "success": False,
            "error": f"'{establecimiento}' no tiene catálogo web disponible",
            "disponibles": list(VTEX_URLS.keys()),
        }

    if not q or len(q.strip()) < 2:
        return {"success": False, "error": "Ingresa al menos 2 caracteres para buscar"}

    termino = q.strip()
    base_url = VTEX_URLS[establecimiento_upper]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "es-CO,es;q=0.9",
    }

    resultados = []

    try:
        # Estrategia 1: Buscar como texto libre
        url = f"{base_url}/api/catalog_system/pub/products/search?ft={urllib.parse.quote(termino)}&_from=0&_to={limite + 10}"

        print(f"🔍 Buscando en VTEX: {termino}")

        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            print(f"   Búsqueda texto: {len(data)} productos")

            for item in data:
                prod = _parsear_producto(item, base_url, establecimiento_upper)
                if prod and not _ya_existe(prod, resultados):
                    resultados.append(prod)

        # Estrategia 2: Si es un número, buscar variantes en PARALELO
        if termino.isdigit() and len(termino) >= 4 and len(resultados) < limite:
            # Generar variantes inteligentes
            variantes = set()

            # Original
            variantes.add(termino)

            # Variantes primer dígito (más común en OCR)
            for i in range(10):
                variantes.add(f"{i}{termino[1:]}")

            # Variantes segundo dígito
            for i in range(10):
                variantes.add(f"{termino[0]}{i}{termino[2:]}")

            # Variantes dos primeros dígitos
            for i in range(10):
                for j in range(10):
                    variantes.add(f"{i}{j}{termino[2:]}")

            # Variantes último dígito
            for i in range(10):
                variantes.add(f"{termino[:-1]}{i}")

            # Variantes penúltimo dígito
            if len(termino) >= 2:
                for i in range(10):
                    variantes.add(f"{termino[:-2]}{i}{termino[-1]}")

            variantes.discard(termino)  # Quitar original (ya buscado)
            variantes_lista = list(variantes)[:30]  # Máximo 30 variantes

            print(f"   Probando {len(variantes_lista)} variantes en paralelo...")

            # Búsqueda paralela (5 threads)
            args_list = [
                (v, base_url, headers, establecimiento_upper) for v in variantes_lista
            ]

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(_buscar_plu_variante, args): args[0]
                    for args in args_list
                }

                for future in as_completed(futures, timeout=10):
                    try:
                        prods = future.result()
                        for prod in prods:
                            if not _ya_existe(prod, resultados):
                                resultados.append(prod)
                                print(
                                    f"   ✅ Variante {prod.get('plu_buscado')}: {prod['nombre'][:40]}"
                                )

                            if len(resultados) >= limite:
                                break
                    except:
                        pass

                    if len(resultados) >= limite:
                        break

        # Ordenar por relevancia
        resultados.sort(
            key=lambda x: (
                0 if termino.lower() in x["nombre"].lower() else 1,
                0 if termino in str(x.get("plu", "")) else 1,
                x["nombre"],
            )
        )

        return {
            "success": True,
            "termino": termino,
            "establecimiento": establecimiento_upper,
            "total": len(resultados),
            "resultados": resultados[:limite],
        }

    except Exception as e:
        print(f"❌ Error buscando en VTEX: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": f"Error de conexión: {str(e)}"}


def _parsear_producto(item: dict, base_url: str, establecimiento: str) -> dict:
    """Parsea un producto del JSON de VTEX"""
    try:
        nombre = item.get("productName", "")
        if not nombre:
            return None

        link = item.get("link", "")
        plu = ""
        ean = ""
        precio = 0
        imagen = ""
        marca = item.get("brand", "")

        if item.get("items") and len(item["items"]) > 0:
            sku = item["items"][0]
            ean = sku.get("ean", "") or ""

            # Imagen
            imagenes = sku.get("images", [])
            if imagenes and len(imagenes) > 0:
                imagen = imagenes[0].get("imageUrl", "")

            # PLU de referenceId
            ref_ids = sku.get("referenceId", [])
            if ref_ids and isinstance(ref_ids, list):
                for ref in ref_ids:
                    if isinstance(ref, dict) and ref.get("Value"):
                        plu = ref["Value"]
                        break

            if not plu:
                plu = item.get("productReference", "") or item.get("productId", "")

            # Precio
            sellers = sku.get("sellers", [])
            if sellers:
                oferta = sellers[0].get("commertialOffer", {})
                precio = int(oferta.get("Price", 0) or 0)

        # URL completa
        if link and not link.startswith("http"):
            link = f"{base_url}{link}"

        return {
            "nombre": nombre,
            "marca": marca,
            "plu": str(plu) if plu else "",
            "ean": str(ean) if ean else "",
            "precio": precio,
            "imagen": imagen,
            "url": link,
            "establecimiento": establecimiento,
        }

    except Exception as e:
        print(f"Error parseando producto: {e}")
        return None


def _ya_existe(prod: dict, lista: list) -> bool:
    """Verifica si el producto ya está en la lista (evita duplicados)"""
    for p in lista:
        if p.get("nombre") == prod.get("nombre") and p.get("plu") == prod.get("plu"):
            return True
    return False


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
import httpx
import base64
import logging

logger = logging.getLogger(__name__)


# Modelo para recibir datos del producto VTEX
class ProductoVTEXCache(BaseModel):
    establecimiento: str
    plu: Optional[str] = None
    ean: Optional[str] = None
    nombre: str
    marca: Optional[str] = None
    precio: Optional[int] = None
    categoria: Optional[str] = None
    presentacion: Optional[str] = None
    url_producto: Optional[str] = None
    imagen_url: Optional[str] = None


# ============================================================
# ENDPOINT CORREGIDO: GUARDAR CACHE VTEX CON IMAGEN
# Reemplazar en productos_api_v2.py
# ============================================================


@router.post("/api/v2/vtex-cache/guardar")
async def guardar_producto_vtex_cache(producto: ProductoVTEXCache):
    """
    Guarda un producto del catálogo VTEX en cache local.
    Descarga la imagen y la convierte a base64.
    """
    conn = None
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Validar que tenga al menos PLU o EAN
        if not producto.plu and not producto.ean:
            return {
                "success": False,
                "error": "Se requiere PLU o EAN para guardar el producto",
            }

        # Descargar imagen si hay URL
        imagen_base64 = None
        imagen_mime = "image/jpeg"

        if producto.imagen_url:
            try:
                async with httpx.AsyncClient(
                    timeout=15.0, follow_redirects=True
                ) as client:
                    # Usar la URL tal cual viene (NO modificarla)
                    imagen_url = producto.imagen_url.strip()

                    # Solo limpiar parámetros de query si existen y causan problemas
                    # Pero mantener la URL base intacta

                    print(f"📷 Descargando imagen: {imagen_url[:80]}...")

                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
                        "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
                        "Referer": "https://www.olimpica.com/",
                    }

                    response = await client.get(imagen_url, headers=headers)

                    if response.status_code == 200:
                        content_length = len(response.content)

                        # Verificar que sea una imagen real (más de 1KB)
                        if content_length > 1000:
                            # Detectar tipo de imagen por content-type o por contenido
                            content_type = response.headers.get(
                                "content-type", ""
                            ).lower()

                            # También detectar por magic bytes
                            content_start = response.content[:10]

                            if b"\x89PNG" in content_start:
                                imagen_mime = "image/png"
                            elif (
                                b"JFIF" in content_start
                                or b"\xff\xd8\xff" in content_start
                            ):
                                imagen_mime = "image/jpeg"
                            elif b"WEBP" in content_start or b"RIFF" in content_start:
                                imagen_mime = "image/webp"
                            elif b"GIF8" in content_start:
                                imagen_mime = "image/gif"
                            elif "png" in content_type:
                                imagen_mime = "image/png"
                            elif "webp" in content_type:
                                imagen_mime = "image/webp"
                            elif "gif" in content_type:
                                imagen_mime = "image/gif"
                            else:
                                imagen_mime = "image/jpeg"

                            # Convertir a base64
                            imagen_base64 = base64.b64encode(response.content).decode(
                                "utf-8"
                            )

                            print(
                                f"✅ Imagen descargada: {content_length} bytes, tipo: {imagen_mime}"
                            )
                        else:
                            print(
                                f"⚠️ Imagen muy pequeña ({content_length} bytes), ignorando"
                            )
                    else:
                        print(
                            f"⚠️ No se pudo descargar imagen: HTTP {response.status_code}"
                        )

            except httpx.TimeoutException:
                print(f"⚠️ Timeout descargando imagen")
            except Exception as e:
                print(f"⚠️ Error descargando imagen: {e}")
                import traceback

                traceback.print_exc()

        # Verificar si ya existe (por PLU o EAN en el mismo establecimiento)
        existe = False
        producto_existente_id = None

        if producto.plu:
            cursor.execute(
                """SELECT id FROM productos_vtex_cache
                   WHERE establecimiento = %s AND plu = %s""",
                (producto.establecimiento.upper(), producto.plu),
            )
            row = cursor.fetchone()
            if row:
                existe = True
                producto_existente_id = row[0]

        if not existe and producto.ean:
            cursor.execute(
                """SELECT id FROM productos_vtex_cache
                   WHERE establecimiento = %s AND ean = %s""",
                (producto.establecimiento.upper(), producto.ean),
            )
            row = cursor.fetchone()
            if row:
                existe = True
                producto_existente_id = row[0]

        if existe:
            # Actualizar existente
            cursor.execute(
                """
                UPDATE productos_vtex_cache SET
                    nombre = %s,
                    marca = %s,
                    precio = %s,
                    categoria = %s,
                    presentacion = %s,
                    url_producto = %s,
                    imagen_url = %s,
                    imagen_base64 = COALESCE(%s, imagen_base64),
                    imagen_mime = %s,
                    fecha_actualizacion = CURRENT_TIMESTAMP,
                    veces_usado = veces_usado + 1
                WHERE id = %s
                RETURNING id
            """,
                (
                    producto.nombre,
                    producto.marca,
                    producto.precio,
                    producto.categoria,
                    producto.presentacion,
                    producto.url_producto,
                    producto.imagen_url,
                    imagen_base64,
                    imagen_mime,
                    producto_existente_id,
                ),
            )

            conn.commit()
            cursor.close()

            print(
                f"🔄 Cache VTEX actualizado: {producto.nombre} (ID: {producto_existente_id})"
            )

            return {
                "success": True,
                "accion": "actualizado",
                "id": producto_existente_id,
                "mensaje": f"Producto actualizado en cache: {producto.nombre}",
                "tiene_imagen": imagen_base64 is not None,
            }

        else:
            # Insertar nuevo
            cursor.execute(
                """
                INSERT INTO productos_vtex_cache (
                    establecimiento, plu, ean, nombre, marca, precio,
                    categoria, presentacion, url_producto, imagen_url,
                    imagen_base64, imagen_mime, veces_usado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                RETURNING id
            """,
                (
                    producto.establecimiento.upper(),
                    producto.plu,
                    producto.ean,
                    producto.nombre,
                    producto.marca,
                    producto.precio,
                    producto.categoria,
                    producto.presentacion,
                    producto.url_producto,
                    producto.imagen_url,
                    imagen_base64,
                    imagen_mime,
                ),
            )

            nuevo_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()

            print(f"✅ Cache VTEX creado: {producto.nombre} (ID: {nuevo_id})")

            return {
                "success": True,
                "accion": "creado",
                "id": nuevo_id,
                "mensaje": f"Producto guardado en cache: {producto.nombre}",
                "tiene_imagen": imagen_base64 is not None,
            }

    except Exception as e:
        logger.error(f"❌ Error guardando en cache VTEX: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if conn:
            conn.close()


@router.get("/api/v2/vtex-cache/buscar")
async def buscar_en_cache_vtex(
    q: str = None, establecimiento: str = None, limite: int = 20
):
    """
    Busca productos en el cache local de VTEX.
    Búsqueda rápida sin llamar a APIs externas.

    Ejemplos:
        GET /api/v2/vtex-cache/buscar?q=leche
        GET /api/v2/vtex-cache/buscar?q=632967&establecimiento=OLIMPICA
    """
    conn = None
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                id, establecimiento, plu, ean, nombre, marca, precio,
                categoria, presentacion, url_producto, imagen_url,
                imagen_base64 IS NOT NULL as tiene_imagen,
                fecha_actualizacion, veces_usado
            FROM productos_vtex_cache
            WHERE 1=1
        """
        params = []

        if q:
            query += """ AND (
                LOWER(nombre) LIKE LOWER(%s)
                OR plu LIKE %s
                OR ean LIKE %s
                OR LOWER(marca) LIKE LOWER(%s)
            )"""
            search_term = f"%{q}%"
            params.extend([search_term, search_term, search_term, search_term])

        if establecimiento:
            query += " AND establecimiento = %s"
            params.append(establecimiento.upper())

        query += " ORDER BY veces_usado DESC, fecha_actualizacion DESC LIMIT %s"
        params.append(limite)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        resultados = []
        for row in rows:
            resultados.append(
                {
                    "id": row[0],
                    "establecimiento": row[1],
                    "plu": row[2],
                    "ean": row[3],
                    "nombre": row[4],
                    "marca": row[5],
                    "precio": row[6],
                    "categoria": row[7],
                    "presentacion": row[8],
                    "url_producto": row[9],
                    "imagen_url": row[10],
                    "tiene_imagen_local": row[11],
                    "fecha_actualizacion": row[12].isoformat() if row[12] else None,
                    "veces_usado": row[13],
                }
            )

        return {
            "success": True,
            "termino": q,
            "establecimiento": establecimiento,
            "total": len(resultados),
            "resultados": resultados,
        }

    except Exception as e:
        logger.error(f"❌ Error buscando en cache VTEX: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/vtex-cache/{cache_id}/imagen")
async def obtener_imagen_cache(cache_id: int):
    """
    Obtiene la imagen en base64 de un producto del cache.

    Uso en frontend:
        <img src="data:image/jpeg;base64,{imagen_base64}">
    """
    conn = None
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """SELECT nombre, imagen_base64, imagen_mime
               FROM productos_vtex_cache WHERE id = %s""",
            (cache_id,),
        )

        row = cursor.fetchone()

        if not row:
            raise HTTPException(
                status_code=404, detail="Producto no encontrado en cache"
            )

        if not row[1]:
            return {
                "success": False,
                "error": "Este producto no tiene imagen guardada",
                "nombre": row[0],
            }

        return {
            "success": True,
            "nombre": row[0],
            "imagen_base64": row[1],
            "imagen_mime": row[2] or "image/jpeg",
            "data_url": f"data:{row[2] or 'image/jpeg'};base64,{row[1]}",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error obteniendo imagen: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/vtex-cache/estadisticas")
async def estadisticas_cache_vtex():
    """
    Estadísticas del cache de productos VTEX.
    """
    conn = None
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN imagen_base64 IS NOT NULL THEN 1 END) as con_imagen,
                COUNT(DISTINCT establecimiento) as establecimientos
            FROM productos_vtex_cache
        """
        )

        totales = cursor.fetchone()

        cursor.execute(
            """
            SELECT establecimiento, COUNT(*) as cantidad
            FROM productos_vtex_cache
            GROUP BY establecimiento
            ORDER BY cantidad DESC
        """
        )

        por_establecimiento = cursor.fetchall()

        return {
            "success": True,
            "estadisticas": {
                "total_productos": totales[0] or 0,
                "con_imagen": totales[1] or 0,
                "sin_imagen": (totales[0] or 0) - (totales[1] or 0),
                "establecimientos": totales[2] or 0,
            },
            "por_establecimiento": [
                {"establecimiento": row[0], "cantidad": row[1]}
                for row in por_establecimiento
            ],
        }

    except Exception as e:
        logger.error(f"❌ Error obteniendo estadísticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# ============================================
# ENDPOINTS PARA IMÁGENES VTEX CACHE
# Agregar a productos_api_v2.py
# ============================================


@router.get("/vtex-cache/{cache_id}/imagen")
async def obtener_imagen_vtex_cache(cache_id: int):
    """
    Obtiene la imagen almacenada en el cache VTEX
    Retorna la imagen en formato base64 con data URL listo para usar en <img src="">
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT nombre, establecimiento, ean, imagen_base64, imagen_mime
            FROM productos_vtex_cache
            WHERE id = %s AND imagen_base64 IS NOT NULL
        """,
            (cache_id,),
        )

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Imagen no encontrada")

        nombre, establecimiento, ean, imagen_base64, imagen_mime = row

        # Construir data URL
        mime_type = imagen_mime or "image/jpeg"
        data_url = f"data:{mime_type};base64,{imagen_base64}"

        return {
            "id": cache_id,
            "nombre": nombre,
            "establecimiento": establecimiento,
            "ean": ean,
            "mime_type": mime_type,
            "data_url": data_url,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error obteniendo imagen cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vtex-cache/buscar")
async def buscar_en_vtex_cache(q: str, establecimiento: str = None, limite: int = 20):
    """
    Busca productos en el cache local VTEX (sin llamar a VTEX)
    Útil para verificar si ya tenemos imagen de un producto
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construir query
        query = """
            SELECT id, establecimiento, plu, ean, nombre, marca, precio,
                CASE WHEN imagen_base64 IS NOT NULL THEN TRUE ELSE FALSE END as tiene_imagen_local,
                veces_usado
            FROM productos_vtex_cache
            WHERE (
                nombre ILIKE %s
                OR ean ILIKE %s
                OR plu ILIKE %s
            )
        """
        params = [f"%{q}%", f"%{q}%", f"%{q}%"]

        if establecimiento:
            query += " AND establecimiento = %s"
            params.append(establecimiento.upper())

        query += " ORDER BY veces_usado DESC, fecha_actualizacion DESC LIMIT %s"
        params.append(limite)

        cursor.execute(query, params)
        rows = cursor.fetchall()

        productos = []
        for row in rows:
            productos.append(
                {
                    "id": row[0],
                    "establecimiento": row[1],
                    "plu": row[2],
                    "ean": row[3],
                    "nombre": row[4],
                    "marca": row[5],
                    "precio": row[6],
                    "tiene_imagen_local": row[7],
                    "veces_usado": row[8],
                }
            )

        cursor.close()
        conn.close()

        return {
            "query": q,
            "establecimiento": establecimiento,
            "total": len(productos),
            "productos": productos,
        }

    except Exception as e:
        print(f"❌ Error buscando en cache VTEX: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/vtex-cache/estadisticas")
async def estadisticas_vtex_cache():
    """
    Retorna estadísticas del cache VTEX
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN imagen_base64 IS NOT NULL THEN 1 END) as con_imagen,
                COUNT(CASE WHEN imagen_base64 IS NULL THEN 1 END) as sin_imagen
            FROM productos_vtex_cache
        """
        )
        row = cursor.fetchone()
        total, con_imagen, sin_imagen = row

        # Por establecimiento
        cursor.execute(
            """
            SELECT establecimiento, COUNT(*) as cantidad,
            COUNT(CASE WHEN imagen_base64 IS NOT NULL THEN 1 END) as con_imagen
            FROM productos_vtex_cache
            GROUP BY establecimiento
            ORDER BY cantidad DESC
        """
        )
        por_establecimiento = []
        for row in cursor.fetchall():
            por_establecimiento.append(
                {"establecimiento": row[0], "cantidad": row[1], "con_imagen": row[2]}
            )

        cursor.close()
        conn.close()

        return {
            "total_productos": total,
            "con_imagen": con_imagen,
            "sin_imagen": sin_imagen,
            "por_establecimiento": por_establecimiento,
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))
