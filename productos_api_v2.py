"""
API de productos para productos.html v2
Incluye PLUs desde productos_por_establecimiento

✅ VERSIÓN ACTUALIZADA:
- Soporte dual: SQLite + PostgreSQL
- PLUs obtenidos desde productos_por_establecimiento (tabla nueva)
- Separación correcta de EAN vs PLU
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import os
from database import get_db_connection

router = APIRouter(prefix="/api/productos", tags=["productos-v2"])


@router.get("")
async def obtener_productos(
    pagina: int = Query(1, ge=1),
    limite: int = Query(50, ge=1, le=100),
    busqueda: Optional[str] = None,
    filtro: Optional[str] = None
):
    """
    Obtener lista de productos con paginación
    Incluye PLUs desde productos_por_establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        offset = (pagina - 1) * limite

        # Construir WHERE
        where_clauses = []
        params = []

        if busqueda:
            if database_type == "postgresql":
                where_clauses.append("(pm.nombre_normalizado ILIKE %s OR pm.codigo_ean ILIKE %s OR pm.marca ILIKE %s)")
            else:
                where_clauses.append("(pm.nombre_normalizado LIKE ? OR pm.codigo_ean LIKE ? OR pm.marca LIKE ?)")

            search_pattern = f"%{busqueda}%"
            params.extend([search_pattern, search_pattern, search_pattern])

        if filtro == "sin_ean":
            where_clauses.append("(pm.codigo_ean IS NULL OR pm.codigo_ean = '')")
        elif filtro == "sin_marca":
            where_clauses.append("(pm.marca IS NULL OR pm.marca = '')")
        elif filtro == "sin_categoria":
            where_clauses.append("(pm.categoria IS NULL OR pm.categoria = '')")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query con productos_por_establecimiento (adaptado según BD)
        if database_type == "postgresql":
            query = f"""
                WITH producto_plus AS (
                    -- Obtener PLUs desde productos_por_establecimiento
                    SELECT
                        ppe.producto_maestro_id,
                        STRING_AGG(
                            CONCAT(ppe.codigo_plu, ' (', e.nombre_normalizado, ')'),
                            ', '
                            ORDER BY e.nombre_normalizado
                        ) as plus_texto,
                        COUNT(DISTINCT ppe.establecimiento_id) as num_establecimientos
                    FROM productos_por_establecimiento ppe
                    JOIN establecimientos e ON ppe.establecimiento_id = e.id
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
                    COALESCE(pp.plus_texto, '-') as plus_con_tienda,
                    COALESCE(pp.num_establecimientos, 0) as num_establecimientos
                FROM productos_maestros pm
                LEFT JOIN producto_plus pp ON pp.producto_maestro_id = pm.id
                WHERE {where_sql}
                ORDER BY pm.total_reportes DESC
                LIMIT %s OFFSET %s
            """
        else:
            # SQLite - Fallback a items_factura si productos_por_establecimiento no existe
            query = f"""
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
                    GROUP_CONCAT(DISTINCT
                        items.codigo_leido || ' (' || COALESCE(f.establecimiento, 'Sin tienda') || ')',
                        ', '
                    ) as plus_con_tienda,
                    COUNT(DISTINCT f.establecimiento) as num_establecimientos
                FROM productos_maestros pm
                LEFT JOIN items_factura items ON pm.id = items.producto_maestro_id
                    AND items.codigo_leido IS NOT NULL
                    AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                LEFT JOIN facturas f ON f.id = items.factura_id
                WHERE {where_sql}
                GROUP BY pm.id, pm.codigo_ean, pm.nombre_normalizado, pm.nombre_comercial,
                         pm.marca, pm.categoria, pm.subcategoria, pm.presentacion,
                         pm.total_reportes, pm.precio_promedio_global
                ORDER BY pm.total_reportes DESC
                LIMIT ? OFFSET ?
            """

        params.extend([limite, offset])
        cursor.execute(query, params)

        productos = []
        for row in cursor.fetchall():
            # Detectar problemas
            problemas = []
            if not row[1]:  # codigo_ean
                problemas.append("sin_ean")
            if not row[4]:  # marca
                problemas.append("sin_marca")
            if not row[5]:  # categoria
                problemas.append("sin_categoria")

            productos.append({
                "id": row[0],
                "codigo_ean": row[1],
                "codigo_plu": row[10] if row[10] != '-' else None,  # PLUs desde productos_por_establecimiento
                "nombre_normalizado": row[2],
                "nombre_comercial": row[3],
                "marca": row[4],
                "categoria": row[5],
                "subcategoria": row[6],
                "presentacion": row[7],
                "total_reportes": row[8],
                "precio_promedio": row[9],
                "num_establecimientos": row[11],
                "problemas": problemas
            })

        # Contar total
        count_query = f"SELECT COUNT(*) FROM productos_maestros pm WHERE {where_sql}"
        cursor.execute(count_query, params[:-2])  # Sin LIMIT/OFFSET
        total = cursor.fetchone()[0]

        conn.close()

        total_paginas = (total + limite - 1) // limite

        return {
            "success": True,
            "productos": productos,
            "paginacion": {
                "pagina": pagina,
                "limite": limite,
                "total": total,
                "paginas": total_paginas
            }
        }

    except Exception as e:
        print(f"❌ Error en obtener_productos: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/estadisticas/calidad")
async def obtener_estadisticas_calidad():
    """
    Estadísticas de calidad del catálogo
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        total_productos = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros WHERE codigo_ean IS NOT NULL AND codigo_ean != ''")
        con_ean = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros WHERE marca IS NOT NULL AND marca != ''")
        con_marca = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros WHERE categoria IS NOT NULL AND categoria != ''")
        con_categoria = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros WHERE total_reportes = 0 OR total_reportes IS NULL")
        productos_huerfanos = cursor.fetchone()[0]

        conn.close()

        porcentaje_ean = round((con_ean / total_productos * 100), 1) if total_productos > 0 else 0
        porcentaje_marca = round((con_marca / total_productos * 100), 1) if total_productos > 0 else 0
        porcentaje_categoria = round((con_categoria / total_productos * 100), 1) if total_productos > 0 else 0

        return {
            "total_productos": total_productos,
            "con_ean": con_ean,
            "porcentaje_ean": porcentaje_ean,
            "sin_ean": total_productos - con_ean,
            "con_marca": con_marca,
            "porcentaje_marca": porcentaje_marca,
            "sin_marca": total_productos - con_marca,
            "con_categoria": con_categoria,
            "porcentaje_categoria": porcentaje_categoria,
            "sin_categoria": total_productos - con_categoria,
            "productos_huerfanos": productos_huerfanos,
            "duplicados_potenciales": 0
        }

    except Exception as e:
        print(f"❌ Error en estadisticas_calidad: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}")
async def obtener_producto(producto_id: int):
    """
    Obtener un producto específico con sus PLUs desde productos_por_establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Obtener datos base
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, presentacion,
                    total_reportes, precio_promedio_global
                FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, presentacion,
                    total_reportes, precio_promedio_global
                FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

        row = cursor.fetchone()

        if not row:
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener PLUs desde productos_por_establecimiento
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    ppe.codigo_plu,
                    e.nombre_normalizado as establecimiento,
                    ppe.precio_actual,
                    ppe.total_reportes
                FROM productos_por_establecimiento ppe
                JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE ppe.producto_maestro_id = %s
                  AND ppe.codigo_plu IS NOT NULL
                ORDER BY e.nombre_normalizado, ppe.codigo_plu
            """, (producto_id,))
        else:
            # SQLite - Fallback a items_factura
            cursor.execute("""
                SELECT DISTINCT
                    items.codigo_leido,
                    f.establecimiento,
                    items.precio_pagado,
                    1 as reportes
                FROM items_factura items
                INNER JOIN facturas f ON f.id = items.factura_id
                WHERE items.producto_maestro_id = ?
                  AND items.codigo_leido IS NOT NULL
                  AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                  AND f.establecimiento IS NOT NULL
                ORDER BY f.establecimiento, items.codigo_leido
            """, (producto_id,))

        plus = cursor.fetchall()
        conn.close()

        return {
            "id": row[0],
            "codigo_ean": row[1],
            "codigo_plu": ", ".join([p[0] for p in plus]) if plus else None,
            "plus_por_establecimiento": [
                {
                    "plu": p[0],
                    "establecimiento": p[1],
                    "precio_actual": p[2] if len(p) > 2 else None,
                    "reportes": p[3] if len(p) > 3 else None
                }
                for p in plus
            ],
            "nombre_normalizado": row[2],
            "nombre_comercial": row[3],
            "marca": row[4],
            "categoria": row[5],
            "subcategoria": row[6],
            "presentacion": row[7],
            "total_reportes": row[8],
            "precio_promedio_global": row[9]
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en obtener_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{producto_id}")
async def actualizar_producto(producto_id: int, data: dict):
    """
    Actualizar un producto
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Construir UPDATE dinámico
        campos_permitidos = [
            'codigo_ean', 'nombre_normalizado', 'nombre_comercial',
            'marca', 'categoria', 'subcategoria', 'presentacion'
        ]

        updates = []
        params = []

        for campo in campos_permitidos:
            if campo in data:
                if database_type == "postgresql":
                    updates.append(f"{campo} = %s")
                else:
                    updates.append(f"{campo} = ?")
                params.append(data[campo])

        if not updates:
            conn.close()
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(producto_id)

        if database_type == "postgresql":
            query = f"""
                UPDATE productos_maestros
                SET {', '.join(updates)}
                WHERE id = %s
            """
        else:
            query = f"""
                UPDATE productos_maestros
                SET {', '.join(updates)}
                WHERE id = ?
            """

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Producto actualizado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en actualizar_producto: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API de productos v2 cargada (con soporte para productos_por_establecimiento)")
