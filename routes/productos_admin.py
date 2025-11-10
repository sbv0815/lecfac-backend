# ============================================================================
# routes/productos_admin.py - ENDPOINTS PARA GESTIÓN DE PRODUCTOS
# ✅ COMPATIBLE CON PSYCOPG2
# ✅ ACTUALIZADO: Incluye información de establecimientos en el listado
# ============================================================================

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import psycopg2  # ✅ CAMBIADO: psycopg2 en lugar de psycopg
import psycopg2.extras  # Para dict cursor
import os

router = APIRouter(prefix="/api/v2/productos", tags=["productos"])


# ============================================================================
# MODELOS
# ============================================================================

class PLUItem(BaseModel):
    nombre_establecimiento: str
    codigo_plu: str
    ultima_vez_visto: Optional[str] = None


class ProductoUpdate(BaseModel):
    codigo_ean: Optional[str] = None
    nombre_consolidado: str
    nombre_comercial: Optional[str] = None
    marca: Optional[str] = None
    categoria: Optional[str] = None
    subcategoria: Optional[str] = None
    presentacion: Optional[str] = None
    plus: Optional[List[PLUItem]] = []


# ============================================================================
# DEPENDENCIAS
# ============================================================================

def get_db():
    """Obtiene conexión a PostgreSQL usando psycopg2"""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise HTTPException(500, "DATABASE_URL no configurada")

    try:
        # Parsear URL manualmente para psycopg2
        from urllib.parse import urlparse
        url = urlparse(database_url)

        conn = psycopg2.connect(
            host=url.hostname,
            database=url.path[1:],
            user=url.username,
            password=url.password,
            port=url.port or 5432,
            cursor_factory=psycopg2.extras.RealDictCursor  # Para retornar dicts
        )
        yield conn
    finally:
        conn.close()


# ============================================================================
# ENDPOINTS - OBTENER DATOS
# ============================================================================

@router.get("/{producto_id}")
async def obtener_producto(producto_id: int, db = Depends(get_db)):
    """Obtiene detalles completos de un producto incluyendo PLUs"""

    cursor = db.cursor()

    try:
        # Obtener producto base
        cursor.execute("""
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.nombre_comercial,
                pm.marca,
                COALESCE(c.nombre, 'Sin categoría') as categoria,
                pm.subcategoria,
                pm.presentacion,
                pm.veces_visto,
                pm.fecha_creacion,
                pm.fecha_ultima_actualizacion
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            WHERE pm.id = %s
        """, (producto_id,))

        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(404, "Producto no encontrado")

        # Obtener PLUs
        cursor.execute("""
            SELECT
                pe.codigo_plu,
                e.nombre_normalizado,
                pe.ultima_compra,
                pe.veces_comprado,
                pe.precio_unitario
            FROM productos_por_establecimiento pe
            JOIN establecimientos e ON pe.establecimiento_id = e.id
            WHERE pe.producto_maestro_id = %s
              AND pe.codigo_plu IS NOT NULL
            ORDER BY pe.veces_comprado DESC
        """, (producto_id,))

        plus = []
        for row in cursor.fetchall():
            plus.append({
                "codigo_plu": row['codigo_plu'],
                "nombre_establecimiento": row['nombre_normalizado'],
                "ultima_vez_visto": str(row['ultima_compra']) if row['ultima_compra'] else None,
                "veces_visto": row['veces_comprado'],
                "precio": row['precio_unitario']
            })

        # Calcular estadísticas
        cursor.execute("""
            SELECT
                AVG(precio_pagado)::INTEGER as precio_promedio,
                COUNT(DISTINCT f.establecimiento_id) as num_establecimientos
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            WHERE i.producto_maestro_id = %s
        """, (producto_id,))

        stats = cursor.fetchone()

        return {
            "id": producto['id'],
            "codigo_ean": producto['codigo_ean'],
            "nombre_normalizado": producto['nombre_consolidado'],
            "nombre_comercial": producto['nombre_comercial'],
            "marca": producto['marca'],
            "categoria": producto['categoria'],
            "subcategoria": producto['subcategoria'],
            "presentacion": producto['presentacion'],
            "veces_comprado": producto['veces_visto'],
            "fecha_creacion": str(producto['fecha_creacion']),
            "ultima_actualizacion": str(producto['fecha_ultima_actualizacion']),
            "plus": plus,
            "precio_promedio": stats['precio_promedio'] if stats else 0,
            "num_establecimientos": stats['num_establecimientos'] if stats else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error obteniendo producto: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        cursor.close()


# ============================================================================
# ENDPOINTS - ACTUALIZAR
# ============================================================================

@router.put("/{producto_id}")
async def actualizar_producto(
    producto_id: int,
    datos: ProductoUpdate,
    db = Depends(get_db)
):
    """Actualiza un producto y sus PLUs"""

    cursor = db.cursor()

    try:
        # Verificar que existe
        cursor.execute(
            "SELECT id FROM productos_maestros_v2 WHERE id = %s",
            (producto_id,)
        )

        if not cursor.fetchone():
            raise HTTPException(404, "Producto no encontrado")

        # Actualizar datos principales
        cursor.execute("""
            UPDATE productos_maestros_v2
            SET codigo_ean = %s,
                nombre_consolidado = %s,
                nombre_comercial = %s,
                marca = %s,
                categoria = %s,
                subcategoria = %s,
                presentacion = %s,
                fecha_ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (
            datos.codigo_ean,
            datos.nombre_consolidado,
            datos.nombre_comercial,
            datos.marca,
            datos.categoria,
            datos.subcategoria,
            datos.presentacion,
            producto_id
        ))

        # Actualizar PLUs
        if datos.plus:
            # Eliminar PLUs antiguos
            cursor.execute("""
                DELETE FROM productos_por_establecimiento
                WHERE producto_maestro_id = %s
            """, (producto_id,))

            # Insertar nuevos PLUs
            for plu in datos.plus:
                # Obtener establecimiento_id
                cursor.execute("""
                    SELECT id FROM establecimientos
                    WHERE nombre_normalizado ILIKE %s
                    LIMIT 1
                """, (plu.nombre_establecimiento,))

                est = cursor.fetchone()

                if est:
                    cursor.execute("""
                        INSERT INTO productos_por_establecimiento
                        (producto_maestro_id, establecimiento_id, codigo_plu, ultima_compra)
                        VALUES (%s, %s, %s, CURRENT_DATE)
                        ON CONFLICT (producto_maestro_id, establecimiento_id)
                        DO UPDATE SET
                            codigo_plu = EXCLUDED.codigo_plu,
                            fecha_actualizacion = CURRENT_TIMESTAMP
                    """, (producto_id, est['id'], plu.codigo_plu))

        db.commit()

        return {
            "mensaje": "Producto actualizado correctamente",
            "producto_id": producto_id
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error actualizando producto: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        cursor.close()


# ============================================================================
# ENDPOINTS - ELIMINAR
# ============================================================================

@router.delete("/{producto_id}")
async def eliminar_producto(producto_id: int, db = Depends(get_db)):
    """Elimina un producto y todos sus datos relacionados"""

    cursor = db.cursor()

    try:
        # Verificar que existe
        cursor.execute(
            "SELECT nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (producto_id,)
        )

        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(404, "Producto no encontrado")

        nombre = producto['nombre_consolidado']

        # Verificar si tiene compras asociadas
        cursor.execute("""
            SELECT COUNT(*) as count FROM items_factura
            WHERE producto_maestro_id = %s
        """, (producto_id,))

        compras = cursor.fetchone()['count']

        if compras > 0:
            raise HTTPException(400,
                f"No se puede eliminar: tiene {compras} compras asociadas. "
                "Considera marcarlo como inactivo en lugar de eliminarlo."
            )

        # Eliminar producto (cascada elimina PLUs)
        cursor.execute("""
            DELETE FROM productos_maestros_v2
            WHERE id = %s
        """, (producto_id,))

        db.commit()

        return {
            "mensaje": f"Producto '{nombre}' eliminado correctamente",
            "producto_id": producto_id
        }

    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"Error eliminando producto: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        cursor.close()


# ============================================================================
# ENDPOINTS - LIMPIAR DUPLICADOS
# ============================================================================

@router.delete("/limpiar-duplicados")
async def limpiar_duplicados(db = Depends(get_db)):
    """Elimina productos duplicados manteniendo el más antiguo"""

    cursor = db.cursor()

    try:
        # Buscar duplicados por EAN
        cursor.execute("""
            WITH duplicados AS (
                SELECT
                    codigo_ean,
                    array_agg(id ORDER BY id) as ids
                FROM productos_maestros_v2
                WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                GROUP BY codigo_ean
                HAVING COUNT(*) > 1
            )
            SELECT codigo_ean, ids FROM duplicados
        """)

        duplicados_ean = cursor.fetchall()
        eliminados = 0

        for row in duplicados_ean:
            ean = row['codigo_ean']
            ids = row['ids']

            # Mantener el primero, eliminar los demás
            ids_eliminar = ids[1:]

            for id_eliminar in ids_eliminar:
                cursor.execute("""
                    DELETE FROM productos_maestros_v2
                    WHERE id = %s
                """, (id_eliminar,))
                eliminados += 1

        db.commit()

        return {
            "mensaje": f"Se eliminaron {eliminados} duplicados",
            "grupos_duplicados": len(duplicados_ean),
            "productos_eliminados": eliminados
        }

    except Exception as e:
        db.rollback()
        print(f"Error limpiando duplicados: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        cursor.close()


# ============================================================================
# ENDPOINTS - LISTAR (⭐ ACTUALIZADO CON ESTABLECIMIENTOS)
# ============================================================================

@router.get("/")
async def listar_productos(
    skip: int = 0,
    limit: int = 50,
    filtro: str = "todos",
    busqueda: str = "",
    db = Depends(get_db)
):
    """Lista productos con filtros e información de establecimientos"""

    cursor = db.cursor()

    try:
        # Construir WHERE
        where_clauses = []
        params = []

        if filtro == "sin_ean":
            where_clauses.append("codigo_ean IS NULL OR codigo_ean = ''")
        elif filtro == "sin_marca":
            where_clauses.append("marca IS NULL OR marca = ''")
        elif filtro == "sin_categoria":
            where_clauses.append("categoria IS NULL OR categoria = ''")

        if busqueda:
            where_clauses.append("""
                (nombre_consolidado ILIKE %s
                 OR codigo_ean ILIKE %s
                 OR marca ILIKE %s)
            """)
            busqueda_param = f"%{busqueda}%"
            params.extend([busqueda_param, busqueda_param, busqueda_param])

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Query principal
        query = f"""
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                COALESCE(c.nombre, 'Sin categoría') as categoria,
                pm.veces_visto,
                (
                    SELECT COUNT(*)
                    FROM productos_por_establecimiento pe
                    WHERE pe.producto_maestro_id = pm.id
                      AND pe.codigo_plu IS NOT NULL
                ) as num_plus,
                (
                    SELECT AVG(i.precio_pagado)::INTEGER
                    FROM items_factura i
                    WHERE i.producto_maestro_id = pm.id
                ) as precio_promedio,
                (
                    SELECT COUNT(DISTINCT f.establecimiento_id)
                    FROM items_factura i
                    JOIN facturas f ON i.factura_id = f.id
                    WHERE i.producto_maestro_id = pm.id
                ) as num_establecimientos
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            WHERE {where_sql}
            ORDER BY pm.veces_visto DESC, pm.id DESC
            LIMIT %s OFFSET %s
        """

        params.extend([limit, skip])
        cursor.execute(query, params)

        productos = []
        for row in cursor.fetchall():
            producto_id = row['id']

            # ⭐ OBTENER PLUs CON ESTABLECIMIENTOS Y PRECIOS
            cursor.execute("""
                SELECT
                    pe.codigo_plu,
                    e.nombre_normalizado as establecimiento,
                    pe.precio_unitario,
                    pe.ultima_compra
                FROM productos_por_establecimiento pe
                JOIN establecimientos e ON pe.establecimiento_id = e.id
                WHERE pe.producto_maestro_id = %s
                  AND pe.codigo_plu IS NOT NULL
                ORDER BY pe.veces_comprado DESC
            """, (producto_id,))

            plus_info = []
            for plu_row in cursor.fetchall():
                plus_info.append({
                    "codigo_plu": plu_row['codigo_plu'],
                    "establecimiento": plu_row['establecimiento'],
                    "precio": plu_row['precio_unitario'],
                    "ultima_compra": str(plu_row['ultima_compra']) if plu_row['ultima_compra'] else None
                })

            productos.append({
                "id": row['id'],
                "codigo_ean": row['codigo_ean'],
                "nombre": row['nombre_consolidado'],
                "marca": row['marca'],
                "categoria": row['categoria'],
                "veces_comprado": row['veces_visto'],
                "num_plus": row['num_plus'],
                "plus": plus_info,  # ⭐ ARRAY CON DETALLES DE PLUs Y ESTABLECIMIENTOS
                "precio_promedio": row['precio_promedio'],
                "num_establecimientos": row['num_establecimientos'],
                "estado": []
            })

            # Agregar badges
            if not row['codigo_ean']:
                productos[-1]["estado"].append("sin_ean")
            if not row['marca']:
                productos[-1]["estado"].append("sin_marca")
            if not row['categoria']:
                productos[-1]["estado"].append("sin_categoria")

        # Contar total
        count_query = f"SELECT COUNT(*) as count FROM productos_maestros_v2 WHERE {where_sql}"
        cursor.execute(count_query, params[:-2])  # Sin LIMIT/OFFSET
        total = cursor.fetchone()['count']

        return {
            "productos": productos,
            "total": total,
            "skip": skip,
            "limit": limit
        }

    except Exception as e:
        print(f"Error listando productos: {e}")
        raise HTTPException(500, f"Error: {str(e)}")
    finally:
        cursor.close()


print("✅ Endpoints de administración de productos cargados (psycopg2)")
print("✅ Incluye información de establecimientos en listado de productos")
