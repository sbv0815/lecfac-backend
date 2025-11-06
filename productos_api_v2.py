# productos_api_v2.py
# API de productos con manejo de PLUs y establecimientos
# Versión 2.2 - Fix columna nombre

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime
import json
import psycopg2
from psycopg2 import extras
import os
from urllib.parse import urlparse

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/productos", tags=["productos_v2"])

# Función de conexión a la base de datos
def get_db_connection():
    """Establecer conexión con PostgreSQL"""
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logger.error("DATABASE_URL no está configurada")
            raise Exception("DATABASE_URL no está configurada")

        logger.info("🔗 Intentando conectar a PostgreSQL (psycopg2)...")

        # Parsear la URL
        parsed = urlparse(database_url)
        logger.info(f"🔍 Parseando DATABASE_URL:")
        logger.info(f"   Host: {parsed.hostname}")
        logger.info(f"   Port: {parsed.port or 5432}")
        logger.info(f"   Database: {parsed.path[1:] if parsed.path else 'N/A'}")
        logger.info(f"   User: {parsed.username}")

        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path[1:] if parsed.path else None,
            user=parsed.username,
            password=parsed.password
        )

        logger.info("✅ Conexión PostgreSQL exitosa (psycopg2)")
        return conn
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
        raise

# =====================================================
# ENDPOINT PRINCIPAL: LISTADO DE PRODUCTOS CON PLUs
# =====================================================
@router.get("")
async def obtener_productos(
    pagina: int = Query(1, ge=1),
    limite: int = Query(50, ge=1, le=100),
    busqueda: Optional[str] = None,
    marca: Optional[str] = None,
    categoria: Optional[str] = None,
    subcategoria: Optional[str] = None,
    con_ean: Optional[bool] = None,
    con_plu: Optional[bool] = None,
    establecimiento_id: Optional[int] = None
):
    """
    Obtener productos con información de PLUs agregada
    """
    logger.info(f"📦 [API] Obteniendo productos - Página {pagina}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Query mejorada con PLUs - CORREGIDA sin e.nombre
        query = """
            WITH producto_plus AS (
                SELECT
                    ppe.producto_maestro_id,
                    STRING_AGG(
                        ppe.codigo_plu || ' (' ||
                        COALESCE(e.nombre_normalizado, 'Est. ' || e.id::text) ||
                        ')',
                        ', '
                        ORDER BY COALESCE(e.nombre_normalizado, '')
                    ) as plus_texto,
                    COUNT(DISTINCT ppe.establecimiento_id) as num_establecimientos
                FROM productos_por_establecimiento ppe
                LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE ppe.codigo_plu IS NOT NULL
                GROUP BY ppe.producto_maestro_id
            )
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.nombre_comercial,
                pm.marca,
                pm.categoria,
                pm.subcategoria,
                pm.presentacion,
                pm.total_reportes,
                pm.precio_promedio_global,
                pp.plus_texto as codigo_plu,
                COALESCE(pp.num_establecimientos, 0) as num_establecimientos
            FROM productos_maestros pm
            LEFT JOIN producto_plus pp ON pp.producto_maestro_id = pm.id
            WHERE 1=1
        """

        params = []
        param_counter = 1

        # Agregar filtros
        if busqueda:
            query += f" AND (pm.nombre_normalizado ILIKE %s OR pm.nombre_comercial ILIKE %s OR pm.codigo_ean = %s)"
            busqueda_param = f"%{busqueda}%"
            params.extend([busqueda_param, busqueda_param, busqueda])
            param_counter += 3

        if marca:
            query += f" AND pm.marca ILIKE %s"
            params.append(f"%{marca}%")
            param_counter += 1

        if categoria:
            query += f" AND pm.categoria ILIKE %s"
            params.append(f"%{categoria}%")
            param_counter += 1

        if subcategoria:
            query += f" AND pm.subcategoria ILIKE %s"
            params.append(f"%{subcategoria}%")
            param_counter += 1

        if con_ean is not None:
            if con_ean:
                query += " AND pm.codigo_ean IS NOT NULL"
            else:
                query += " AND pm.codigo_ean IS NULL"

        if con_plu is not None:
            if con_plu:
                query += " AND pp.plus_texto IS NOT NULL"
            else:
                query += " AND pp.plus_texto IS NULL"

        if establecimiento_id:
            query += f"""
                AND pm.id IN (
                    SELECT producto_maestro_id
                    FROM productos_por_establecimiento
                    WHERE establecimiento_id = %s
                )
            """
            params.append(establecimiento_id)
            param_counter += 1

        # Ordenamiento
        query += " ORDER BY pm.total_reportes DESC NULLS LAST, pm.id DESC"

        # Paginación
        offset = (pagina - 1) * limite
        query += f" LIMIT {limite} OFFSET {offset}"

        logger.info(f"🔍 [API] Ejecutando query con {len(params)} parámetros")
        cursor.execute(query, params)
        productos = cursor.fetchall()

        # Contar total
        count_query = """
            WITH producto_plus AS (
                SELECT
                    ppe.producto_maestro_id,
                    STRING_AGG(ppe.codigo_plu::text, ',') as plus_texto
                FROM productos_por_establecimiento ppe
                WHERE ppe.codigo_plu IS NOT NULL
                GROUP BY ppe.producto_maestro_id
            )
            SELECT COUNT(*)
            FROM productos_maestros pm
            LEFT JOIN producto_plus pp ON pp.producto_maestro_id = pm.id
            WHERE 1=1
        """

        # Aplicar los mismos filtros para el conteo
        count_params = []
        if busqueda:
            count_query += f" AND (pm.nombre_normalizado ILIKE %s OR pm.nombre_comercial ILIKE %s OR pm.codigo_ean = %s)"
            count_params.extend([busqueda_param, busqueda_param, busqueda])

        if marca:
            count_query += f" AND pm.marca ILIKE %s"
            count_params.append(f"%{marca}%")

        if categoria:
            count_query += f" AND pm.categoria ILIKE %s"
            count_params.append(f"%{categoria}%")

        if subcategoria:
            count_query += f" AND pm.subcategoria ILIKE %s"
            count_params.append(f"%{subcategoria}%")

        if con_ean is not None:
            if con_ean:
                count_query += " AND pm.codigo_ean IS NOT NULL"
            else:
                count_query += " AND pm.codigo_ean IS NULL"

        if con_plu is not None:
            if con_plu:
                count_query += " AND pp.plus_texto IS NOT NULL"
            else:
                count_query += " AND pp.plus_texto IS NULL"

        if establecimiento_id:
            count_query += f"""
                AND pm.id IN (
                    SELECT producto_maestro_id
                    FROM productos_por_establecimiento
                    WHERE establecimiento_id = %s
                )
            """
            count_params.append(establecimiento_id)

        cursor.execute(count_query, count_params)
        total = cursor.fetchone()['count']

        # Log de debug
        if productos and len(productos) > 0:
            primer_producto = productos[0]
            logger.info(f"✅ [API] Producto 1: ID={primer_producto['id']}, PLU={primer_producto.get('codigo_plu', 'N/A')}, Nombre={primer_producto['nombre_normalizado']}")

        logger.info(f"✅ [API] Respuesta: {len(productos)} productos de {total} total")

        return {
            "productos": productos,
            "total": total,
            "pagina": pagina,
            "limite": limite,
            "total_paginas": (total + limite - 1) // limite
        }

    except Exception as e:
        logger.error(f"❌ [API] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: OBTENER PLUs DE UN PRODUCTO
# =====================================================
@router.get("/{producto_id}/plus")
async def obtener_plus_producto(producto_id: int):
    """
    Obtener todos los PLUs de un producto específico
    """
    logger.info(f"🏪 [API] Obteniendo PLUs del producto {producto_id}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        query = """
            SELECT
                ppe.id,
                ppe.codigo_plu,
                ppe.establecimiento_id,
                e.nombre_normalizado as establecimiento_nombre,
                ppe.precio_unitario,
                ppe.fecha_creacion,
                ppe.fecha_actualizacion
            FROM productos_por_establecimiento ppe
            LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
            ORDER BY e.nombre_normalizado, ppe.codigo_plu
        """

        cursor.execute(query, (producto_id,))
        plus = cursor.fetchall()

        logger.info(f"✅ [API] Encontrados {len(plus)} PLUs para producto {producto_id}")

        return {
            "producto_id": producto_id,
            "plus": plus,
            "total": len(plus)
        }

    except Exception as e:
        logger.error(f"❌ [API] Error obteniendo PLUs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: ACTUALIZAR PLUs DE UN PRODUCTO
# =====================================================
@router.put("/{producto_id}/plus")
async def actualizar_plus_producto(producto_id: int, plus_data: List[Dict]):
    """
    Actualizar PLUs de un producto
    Formato esperado: [{"establecimiento_id": 1, "codigo_plu": "12345", "precio_unitario": 5000}]
    """
    logger.info(f"📝 [API] Actualizando PLUs del producto {producto_id}")
    logger.info(f"📝 [API] Datos recibidos: {plus_data}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Comenzar transacción
        conn.autocommit = False

        # Primero, eliminar PLUs existentes que no están en la nueva lista
        establecimiento_ids = [p['establecimiento_id'] for p in plus_data if p.get('establecimiento_id')]

        if establecimiento_ids:
            delete_query = """
                DELETE FROM productos_por_establecimiento
                WHERE producto_maestro_id = %s
                AND establecimiento_id NOT IN %s
            """
            cursor.execute(delete_query, (producto_id, tuple(establecimiento_ids)))
        else:
            # Si no hay PLUs nuevos, eliminar todos
            delete_query = """
                DELETE FROM productos_por_establecimiento
                WHERE producto_maestro_id = %s
            """
            cursor.execute(delete_query, (producto_id,))

        # Insertar o actualizar PLUs
        for plu_item in plus_data:
            if not plu_item.get('establecimiento_id') or not plu_item.get('codigo_plu'):
                continue

            upsert_query = """
                INSERT INTO productos_por_establecimiento
                    (producto_maestro_id, establecimiento_id, codigo_plu, precio_unitario, fecha_actualizacion)
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (producto_maestro_id, establecimiento_id)
                DO UPDATE SET
                    codigo_plu = EXCLUDED.codigo_plu,
                    precio_unitario = EXCLUDED.precio_unitario,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """

            cursor.execute(upsert_query, (
                producto_id,
                plu_item['establecimiento_id'],
                plu_item['codigo_plu'],
                plu_item.get('precio_unitario')
            ))

        # Confirmar transacción
        conn.commit()

        logger.info(f"✅ [API] PLUs actualizados para producto {producto_id}")

        # Devolver los PLUs actualizados
        return await obtener_plus_producto(producto_id)

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ [API] Error actualizando PLUs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.autocommit = True
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: DETECTAR DUPLICADOS
# =====================================================
@router.get("/duplicados")
async def obtener_duplicados(
    umbral_similitud: float = Query(0.8, ge=0, le=1),
    limite: int = Query(50, ge=1, le=100)
):
    """
    Detectar productos potencialmente duplicados
    """
    logger.info(f"🔍 [API] Detectando duplicados con umbral {umbral_similitud}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Query para encontrar duplicados por nombre similar y misma marca
        query = """
            WITH duplicados AS (
                SELECT
                    p1.id as id1,
                    p1.nombre_normalizado as nombre1,
                    p1.codigo_ean as ean1,
                    p1.marca as marca1,
                    p2.id as id2,
                    p2.nombre_normalizado as nombre2,
                    p2.codigo_ean as ean2,
                    p2.marca as marca2,
                    similarity(p1.nombre_normalizado, p2.nombre_normalizado) as similitud
                FROM productos_maestros p1
                CROSS JOIN productos_maestros p2
                WHERE p1.id < p2.id
                AND p1.marca = p2.marca
                AND similarity(p1.nombre_normalizado, p2.nombre_normalizado) >= %s
            )
            SELECT * FROM duplicados
            ORDER BY similitud DESC
            LIMIT %s
        """

        cursor.execute(query, (umbral_similitud, limite))
        duplicados = cursor.fetchall()

        logger.info(f"✅ [API] Encontrados {len(duplicados)} posibles duplicados")

        return {
            "duplicados": duplicados,
            "total": len(duplicados),
            "umbral_similitud": umbral_similitud
        }

    except Exception as e:
        logger.error(f"❌ [API] Error detectando duplicados: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: ANALIZAR DUPLICADOS COMPLEJOS
# =====================================================
@router.post("/duplicados/detectar")
async def detectar_duplicados_complejos(configuracion: Dict[str, Any]):
    """
    Detectar duplicados con configuración avanzada
    """
    logger.info(f"🔍 [API] Análisis complejo de duplicados")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Configuración por defecto
        config = {
            "similitud_nombre": configuracion.get("similitud_nombre", 0.8),
            "misma_marca": configuracion.get("misma_marca", True),
            "misma_categoria": configuracion.get("misma_categoria", False),
            "diferencia_precio_max": configuracion.get("diferencia_precio_max", None),
            "limite": configuracion.get("limite", 100)
        }

        # Construir query dinámicamente
        conditions = ["p1.id < p2.id"]
        params = []

        # Similitud de nombre
        conditions.append("similarity(p1.nombre_normalizado, p2.nombre_normalizado) >= %s")
        params.append(config["similitud_nombre"])

        # Misma marca
        if config["misma_marca"]:
            conditions.append("p1.marca = p2.marca")

        # Misma categoría
        if config["misma_categoria"]:
            conditions.append("p1.categoria = p2.categoria")

        # Diferencia de precio
        if config["diferencia_precio_max"]:
            conditions.append("""
                ABS(COALESCE(p1.precio_promedio_global, 0) -
                    COALESCE(p2.precio_promedio_global, 0)) <= %s
            """)
            params.append(config["diferencia_precio_max"])

        where_clause = " AND ".join(conditions)

        query = f"""
            WITH duplicados AS (
                SELECT
                    p1.id as id1,
                    p1.nombre_normalizado as nombre1,
                    p1.codigo_ean as ean1,
                    p1.marca as marca1,
                    p1.categoria as categoria1,
                    p1.precio_promedio_global as precio1,
                    p1.total_reportes as reportes1,
                    p2.id as id2,
                    p2.nombre_normalizado as nombre2,
                    p2.codigo_ean as ean2,
                    p2.marca as marca2,
                    p2.categoria as categoria2,
                    p2.precio_promedio_global as precio2,
                    p2.total_reportes as reportes2,
                    similarity(p1.nombre_normalizado, p2.nombre_normalizado) as similitud
                FROM productos_maestros p1
                CROSS JOIN productos_maestros p2
                WHERE {where_clause}
            )
            SELECT * FROM duplicados
            ORDER BY similitud DESC
            LIMIT %s
        """

        params.append(config["limite"])

        cursor.execute(query, params)
        duplicados = cursor.fetchall()

        # Agrupar duplicados por clusters
        clusters = []
        productos_procesados = set()

        for dup in duplicados:
            if dup['id1'] not in productos_procesados and dup['id2'] not in productos_procesados:
                cluster = {
                    "productos": [
                        {
                            "id": dup['id1'],
                            "nombre": dup['nombre1'],
                            "ean": dup['ean1'],
                            "marca": dup['marca1'],
                            "categoria": dup['categoria1'],
                            "precio": dup['precio1'],
                            "reportes": dup['reportes1']
                        },
                        {
                            "id": dup['id2'],
                            "nombre": dup['nombre2'],
                            "ean": dup['ean2'],
                            "marca": dup['marca2'],
                            "categoria": dup['categoria2'],
                            "precio": dup['precio2'],
                            "reportes": dup['reportes2']
                        }
                    ],
                    "similitud": dup['similitud']
                }
                clusters.append(cluster)
                productos_procesados.add(dup['id1'])
                productos_procesados.add(dup['id2'])

        logger.info(f"✅ [API] Encontrados {len(clusters)} clusters de duplicados")

        return {
            "clusters": clusters,
            "total_clusters": len(clusters),
            "configuracion": config
        }

    except Exception as e:
        logger.error(f"❌ [API] Error en análisis de duplicados: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: OBTENER DETALLE DE UN PRODUCTO
# =====================================================
@router.get("/{producto_id}")
async def obtener_producto_detalle(producto_id: int):
    """
    Obtener información completa de un producto incluyendo PLUs
    """
    logger.info(f"📦 [API] Obteniendo detalle del producto {producto_id}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Obtener información del producto
        query = """
            SELECT
                pm.*,
                (
                    SELECT COUNT(DISTINCT establecimiento_id)
                    FROM productos_por_establecimiento
                    WHERE producto_maestro_id = pm.id
                ) as num_establecimientos,
                (
                    SELECT COUNT(*)
                    FROM items_factura
                    WHERE producto_maestro_id = pm.id
                ) as veces_comprado
            FROM productos_maestros pm
            WHERE pm.id = %s
        """

        cursor.execute(query, (producto_id,))
        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener PLUs
        plus_query = """
            SELECT
                ppe.codigo_plu,
                ppe.establecimiento_id,
                e.nombre_normalizado as establecimiento_nombre,
                ppe.precio_unitario,
                ppe.fecha_actualizacion
            FROM productos_por_establecimiento ppe
            LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
            ORDER BY e.nombre_normalizado
        """

        cursor.execute(plus_query, (producto_id,))
        plus = cursor.fetchall()

        producto['plus'] = plus

        logger.info(f"✅ [API] Detalle obtenido para producto {producto_id}")

        return producto

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [API] Error obteniendo detalle: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# =====================================================
# ENDPOINT: ACTUALIZAR PRODUCTO
# =====================================================
@router.put("/{producto_id}")
async def actualizar_producto(producto_id: int, datos_producto: Dict[str, Any]):
    """
    Actualizar información de un producto
    """
    logger.info(f"📝 [API] Actualizando producto {producto_id}")
    logger.info(f"📝 [API] Datos: {datos_producto}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Campos permitidos para actualización
        campos_permitidos = [
            'codigo_ean', 'nombre_normalizado', 'nombre_comercial',
            'marca', 'categoria', 'subcategoria', 'presentacion'
        ]

        # Filtrar solo campos permitidos
        campos_actualizar = {k: v for k, v in datos_producto.items() if k in campos_permitidos}

        if not campos_actualizar:
            raise HTTPException(status_code=400, detail="No hay campos válidos para actualizar")

        # Construir query dinámicamente
        set_clause = ", ".join([f"{campo} = %s" for campo in campos_actualizar.keys()])
        valores = list(campos_actualizar.values())
        valores.append(producto_id)

        query = f"""
            UPDATE productos_maestros
            SET {set_clause}, fecha_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING id
        """

        cursor.execute(query, valores)
        resultado = cursor.fetchone()

        if not resultado:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        conn.commit()

        logger.info(f"✅ [API] Producto {producto_id} actualizado exitosamente")

        # Devolver el producto actualizado
        return await obtener_producto_detalle(producto_id)

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ [API] Error actualizando producto: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

logger.info("✅ productos_v2_router inicializado correctamente")
