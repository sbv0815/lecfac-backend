from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
import logging
from database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/api/v2/productos/")
async def listar_productos_v2(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=100),
    search: Optional[str] = None,
    categoria_id: Optional[int] = None
):
    """
    Lista productos con información de PLUs por establecimiento
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query base con LEFT JOIN a categorias
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

        if search:
            where_conditions.append("""
                (LOWER(pm.nombre_consolidado) LIKE LOWER(%s)
                OR pm.codigo_ean LIKE %s)
            """)
            search_param = f"%{search}%"
            params.extend([search_param, search_param])

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
        params.extend([limit, skip])

        cursor.execute(query, params)
        productos = cursor.fetchall()

        resultado = []
        for producto in productos:
            producto_id = producto[0]

            # Obtener PLUs por establecimiento (incluyendo productos sin PLU)
            cursor.execute("""
                SELECT
                    pe.codigo_plu,
                    e.nombre_normalizado as establecimiento,
                    pe.precio_unitario,
                    pe.ultima_actualizacion as ultima_compra
                FROM productos_por_establecimiento pe
                JOIN establecimientos e ON pe.establecimiento_id = e.id
                WHERE pe.producto_maestro_id = %s
                ORDER BY pe.ultima_actualizacion DESC
            """, (producto_id,))

            plus = cursor.fetchall()

            plus_info = []
            for plu in plus:
                plus_info.append({
                    "codigo": plu[0],
                    "establecimiento": plu[1],
                    "precio": float(plu[2]) if plu[2] else 0.0,
                    "ultima_compra": plu[3]
                })

            resultado.append({
                "id": producto[0],
                "codigo_ean": producto[1],
                "nombre": producto[2],
                "marca": producto[3],
                "categoria": producto[4],
                "num_establecimientos": producto[5],
                "plus": plus_info
            })

        cursor.close()
        return {"productos": resultado, "total": len(resultado)}

    except Exception as e:
        logger.error(f"Error listando productos: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/api/v2/productos/categorias")
async def listar_categorias():
    """
    Lista todas las categorías disponibles
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, nombre, icono
            FROM categorias
            ORDER BY nombre
        """)

        categorias = cursor.fetchall()
        cursor.close()

        return {
            "categorias": [
                {
                    "id": cat[0],
                    "nombre": cat[1],
                    "icono": cat[2]
                }
                for cat in categorias
            ]
        }
    except Exception as e:
        logger.error(f"Error listando categorías: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
