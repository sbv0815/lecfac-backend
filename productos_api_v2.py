# productos_api_v2.py
# API de productos con manejo de PLUs y establecimientos
# Versión 3.0 - Usa productos_maestros_v2

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import logging
from datetime import datetime
import psycopg2
from psycopg2 import extras
import os
from urllib.parse import urlparse

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ⭐ CAMBIO: Prefijo correcto para v2
router = APIRouter(prefix="/api/v2/productos", tags=["productos_v2"])


# Modelo para actualización
class ProductoUpdate(BaseModel):
    codigo_ean: Optional[str] = None
    nombre_consolidado: Optional[str] = None
    marca: Optional[str] = None
    categoria_id: Optional[int] = None
    peso_neto: Optional[float] = None
    unidad_medida: Optional[str] = None
    estado: Optional[str] = None


# Función de conexión a la base de datos
def get_db_connection():
    """Establecer conexión con PostgreSQL"""
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            logger.error("DATABASE_URL no está configurada")
            raise Exception("DATABASE_URL no está configurada")

        logger.info("🔗 Intentando conectar a PostgreSQL (psycopg2)...")

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
            password=parsed.password,
        )

        logger.info("✅ Conexión PostgreSQL exitosa (psycopg2)")
        return conn
    except Exception as e:
        logger.error(f"❌ Error conectando a PostgreSQL: {str(e)}")
        raise


# =====================================================
# ENDPOINT PRINCIPAL: LISTADO DE PRODUCTOS
# =====================================================
@router.get("/")
async def obtener_productos(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    busqueda: Optional[str] = None,
    filtro: Optional[str] = None,
    marca: Optional[str] = None,
    estado: Optional[str] = None,
):
    """
    Obtener productos de productos_maestros_v2 con información de PLUs
    """
    logger.info(f"📦 [API] Obteniendo productos - Skip {skip}, Limit {limit}")
    if busqueda:
        logger.info(f"🔍 Búsqueda: {busqueda}")
    if filtro:
        logger.info(f"🏷️ Filtro: {filtro}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Query principal usando productos_maestros_v2
        query = """
            WITH producto_plus AS (
                SELECT
                    ppe.producto_maestro_id,
                    json_agg(
                        json_build_object(
                            'codigo_plu', ppe.codigo_plu,
                            'establecimiento', COALESCE(e.nombre_normalizado, 'Desconocido'),
                            'precio', ppe.precio_unitario
                        )
                    ) as plus_info,
                    COUNT(DISTINCT ppe.establecimiento_id) as num_establecimientos
                FROM productos_por_establecimiento ppe
                LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
                GROUP BY ppe.producto_maestro_id
            ),
            producto_stats AS (
                SELECT
                    producto_maestro_id,
                    COUNT(*) as veces_comprado,
                    AVG(precio_pagado) as precio_promedio
                FROM items_factura
                WHERE producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
            )
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado as nombre,
                pm.marca,
                pm.categoria_id,
                pm.peso_neto,
                pm.unidad_medida,
                pm.confianza_datos,
                pm.veces_visto,
                pm.estado,
                pm.fecha_primera_vez,
                pm.fecha_ultima_actualizacion,
                COALESCE(pp.plus_info, '[]'::json) as plus,
                COALESCE(pp.num_establecimientos, 0) as num_establecimientos,
                COALESCE(ps.veces_comprado, 0) as veces_comprado,
                COALESCE(ps.precio_promedio, 0)::integer as precio_promedio
            FROM productos_maestros_v2 pm
            LEFT JOIN producto_plus pp ON pp.producto_maestro_id = pm.id
            LEFT JOIN producto_stats ps ON ps.producto_maestro_id = pm.id
            WHERE 1=1
        """

        params = []

        # Filtro de búsqueda
        if busqueda and busqueda.strip():
            query += " AND (pm.nombre_consolidado ILIKE %s OR pm.codigo_ean ILIKE %s OR pm.marca ILIKE %s)"
            busqueda_param = f"%{busqueda.strip()}%"
            params.extend([busqueda_param, busqueda_param, busqueda_param])

        # Filtros especiales
        if filtro:
            if filtro == "sin_marca":
                query += " AND (pm.marca IS NULL OR pm.marca = '')"
            elif filtro == "sin_ean":
                query += " AND (pm.codigo_ean IS NULL OR pm.codigo_ean = '')"
            elif filtro == "sin_categoria":
                query += " AND pm.categoria_id IS NULL"
            elif filtro == "pendiente":
                query += " AND pm.estado = 'pendiente'"
            elif filtro == "conflicto":
                query += " AND pm.estado = 'conflicto'"

        # Filtro por marca específica
        if marca:
            query += " AND pm.marca ILIKE %s"
            params.append(f"%{marca}%")

        # Filtro por estado
        if estado:
            query += " AND pm.estado = %s"
            params.append(estado)

        # Ordenamiento y paginación
        query += " ORDER BY pm.veces_visto DESC NULLS LAST, pm.id DESC"
        query += f" LIMIT {limit} OFFSET {skip}"

        logger.info(f"🔍 [API] Ejecutando query con {len(params)} parámetros")
        cursor.execute(query, params)
        productos = cursor.fetchall()

        # Contar total
        count_query = """
            SELECT COUNT(*) as total
            FROM productos_maestros_v2 pm
            WHERE 1=1
        """
        count_params = []

        if busqueda and busqueda.strip():
            count_query += " AND (pm.nombre_consolidado ILIKE %s OR pm.codigo_ean ILIKE %s OR pm.marca ILIKE %s)"
            count_params.extend([busqueda_param, busqueda_param, busqueda_param])

        if filtro:
            if filtro == "sin_marca":
                count_query += " AND (pm.marca IS NULL OR pm.marca = '')"
            elif filtro == "sin_ean":
                count_query += " AND (pm.codigo_ean IS NULL OR pm.codigo_ean = '')"
            elif filtro == "sin_categoria":
                count_query += " AND pm.categoria_id IS NULL"
            elif filtro == "pendiente":
                count_query += " AND pm.estado = 'pendiente'"
            elif filtro == "conflicto":
                count_query += " AND pm.estado = 'conflicto'"

        if marca:
            count_query += " AND pm.marca ILIKE %s"
            count_params.append(f"%{marca}%")

        if estado:
            count_query += " AND pm.estado = %s"
            count_params.append(estado)

        cursor.execute(count_query, count_params)
        total = cursor.fetchone()["total"]

        logger.info(f"✅ [API] Respuesta: {len(productos)} productos de {total} total")

        return {"productos": productos, "total": total, "skip": skip, "limit": limit}

    except Exception as e:
        logger.error(f"❌ [API] Error: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
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
    Obtener información completa de un producto
    """
    logger.info(f"📦 [API] Obteniendo detalle del producto {producto_id}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Obtener producto de productos_maestros_v2
        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado as nombre_normalizado,
                pm.nombre_consolidado as nombre_comercial,
                pm.marca,
                pm.categoria_id,
                pm.peso_neto,
                pm.unidad_medida,
                pm.confianza_datos,
                pm.veces_visto,
                pm.estado,
                pm.fecha_primera_vez,
                pm.fecha_ultima_actualizacion,
                '' as subcategoria,
                '' as presentacion,
                (
                    SELECT COUNT(DISTINCT establecimiento_id)
                    FROM productos_por_establecimiento
                    WHERE producto_maestro_id = pm.id
                ) as num_establecimientos,
                (
                    SELECT COUNT(*)
                    FROM items_factura
                    WHERE producto_maestro_id = pm.id
                ) as veces_comprado,
                (
                    SELECT AVG(precio_pagado)::integer
                    FROM items_factura
                    WHERE producto_maestro_id = pm.id
                ) as precio_promedio
            FROM productos_maestros_v2 pm
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
                COALESCE(e.nombre_normalizado, 'Desconocido') as nombre_establecimiento,
                ppe.precio_unitario,
                ppe.fecha_actualizacion as ultima_vez_visto
            FROM productos_por_establecimiento ppe
            LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
            ORDER BY e.nombre_normalizado
        """

        cursor.execute(plus_query, (producto_id,))
        plus = cursor.fetchall()

        producto["plus"] = plus

        logger.info(f"✅ [API] Detalle obtenido para producto {producto_id}")

        return dict(producto)

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
async def actualizar_producto(producto_id: int, datos: Dict[str, Any]):
    """
    Actualizar información de un producto en productos_maestros_v2
    """
    logger.info(f"📝 [API] Actualizando producto {producto_id}")
    logger.info(f"📝 [API] Datos recibidos: {datos}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)

        # Mapeo de campos del frontend a la tabla v2
        campo_mapping = {
            "codigo_ean": "codigo_ean",
            "nombre_normalizado": "nombre_consolidado",
            "nombre_consolidado": "nombre_consolidado",
            "nombre_comercial": "nombre_consolidado",  # Usa el mismo campo
            "marca": "marca",
            "categoria": "categoria_id",  # Nota: ahora es categoria_id
            "categoria_id": "categoria_id",
            "subcategoria": None,  # No existe en v2
            "presentacion": None,  # No existe en v2
            "peso_neto": "peso_neto",
            "unidad_medida": "unidad_medida",
            "estado": "estado",
        }

        # Filtrar campos válidos
        campos_actualizar = {}
        for key, value in datos.items():
            if key in campo_mapping and campo_mapping[key] is not None:
                db_field = campo_mapping[key]
                # Manejar categoria como texto (temporal hasta que tengas la tabla de categorías)
                if key == "categoria" and isinstance(value, str):
                    # Por ahora, ignorar categoría texto ya que necesita categoria_id
                    logger.info(
                        f"⚠️ Ignorando categoría texto '{value}', se necesita categoria_id"
                    )
                    continue
                campos_actualizar[db_field] = value

        if not campos_actualizar:
            raise HTTPException(
                status_code=400, detail="No hay campos válidos para actualizar"
            )

        # Construir query
        set_clauses = []
        valores = []
        for campo, valor in campos_actualizar.items():
            set_clauses.append(f"{campo} = %s")
            valores.append(valor)

        set_clause = ", ".join(set_clauses)
        valores.append(producto_id)

        query = f"""
            UPDATE productos_maestros_v2
            SET {set_clause}, fecha_ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING id
        """

        logger.info(f"📝 [API] Query: {query}")
        logger.info(f"📝 [API] Valores: {valores}")

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
        import traceback

        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================
# ENDPOINT: ELIMINAR PRODUCTO
# =====================================================
@router.delete("/{producto_id}")
async def eliminar_producto(producto_id: int):
    """
    Eliminar un producto de productos_maestros_v2
    """
    logger.info(f"🗑️ [API] Eliminando producto {producto_id}")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Eliminar PLUs asociados primero
        cursor.execute(
            "DELETE FROM productos_por_establecimiento WHERE producto_maestro_id = %s",
            (producto_id,),
        )

        # Eliminar producto
        cursor.execute(
            "DELETE FROM productos_maestros_v2 WHERE id = %s", (producto_id,)
        )

        conn.commit()

        logger.info(f"✅ [API] Producto {producto_id} eliminado")

        return {"message": f"Producto {producto_id} eliminado correctamente"}

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ [API] Error eliminando producto: {str(e)}")
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
                COALESCE(e.nombre_normalizado, 'Desconocido') as establecimiento_nombre,
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

        logger.info(
            f"✅ [API] Encontrados {len(plus)} PLUs para producto {producto_id}"
        )

        return {"producto_id": producto_id, "plus": plus, "total": len(plus)}

    except Exception as e:
        logger.error(f"❌ [API] Error obteniendo PLUs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


logger.info("✅ productos_v2_router inicializado correctamente con /api/v2/productos")
