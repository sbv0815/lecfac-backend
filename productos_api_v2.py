"""
API de productos para productos.html v2
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
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        offset = (pagina - 1) * limite

        # Construir WHERE
        where_clauses = []
        params = []

        if busqueda:
            where_clauses.append("(nombre_normalizado ILIKE %s OR codigo_ean ILIKE %s OR marca ILIKE %s)")
            search_pattern = f"%{busqueda}%"
            params.extend([search_pattern, search_pattern, search_pattern])

        if filtro == "sin_ean":
            where_clauses.append("(codigo_ean IS NULL OR codigo_ean = '')")
        elif filtro == "sin_marca":
            where_clauses.append("(marca IS NULL OR marca = '')")
        elif filtro == "sin_categoria":
            where_clauses.append("(categoria IS NULL OR categoria = '')")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query principal
        query = f"""
            SELECT
                id,
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                marca,
                categoria,
                subcategoria,
                presentacion,
                total_reportes,
                precio_promedio_global
            FROM productos_maestros
            WHERE {where_sql}
            ORDER BY total_reportes DESC
            LIMIT %s OFFSET %s
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
                "codigo_plu": None,  # No existe en productos_maestros
                "nombre_normalizado": row[2],
                "nombre_comercial": row[3],
                "marca": row[4],
                "categoria": row[5],
                "subcategoria": row[6],
                "presentacion": row[7],
                "total_reportes": row[8],
                "precio_promedio": row[9],
                "problemas": problemas
            })

        # Contar total
        count_query = f"SELECT COUNT(*) FROM productos_maestros WHERE {where_sql}"
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
        print(f"❌ Error: {e}")
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
            "duplicados_potenciales": 0  # Implementar después
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}")
async def obtener_producto(producto_id: int):
    """
    Obtener un producto específico
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                id, codigo_ean, nombre_normalizado, nombre_comercial,
                marca, categoria, subcategoria, presentacion,
                total_reportes, precio_promedio_global
            FROM productos_maestros
            WHERE id = %s
        """, (producto_id,))

        row = cursor.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        return {
            "id": row[0],
            "codigo_ean": row[1],
            "codigo_plu": None,
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
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{producto_id}")
async def actualizar_producto(producto_id: int, data: dict):
    """
    Actualizar un producto
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construir UPDATE dinámico
        campos_permitidos = [
            'codigo_ean', 'nombre_normalizado', 'nombre_comercial',
            'marca', 'categoria', 'subcategoria', 'presentacion'
        ]

        updates = []
        params = []

        for campo in campos_permitidos:
            if campo in data:
                updates.append(f"{campo} = %s")
                params.append(data[campo])

        if not updates:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(producto_id)

        query = f"""
            UPDATE productos_maestros
            SET {', '.join(updates)}
            WHERE id = %s
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
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API de productos v2 cargada")
