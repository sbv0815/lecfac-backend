"""
productos_api_v2.py - VERSIÓN FINAL CORREGIDA
✅ Maneja establecimientos sin nombre_normalizado
✅ Devuelve PLUs correctamente
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import os
from database import get_db_connection

router = APIRouter(prefix="/api/productos", tags=["productos-v2"])

print("✅ productos_api_v2.py - VERSIÓN FINAL con PLUs corregidos")


@router.get("")
async def obtener_productos(
    pagina: int = Query(1, ge=1),
    limite: int = Query(50, ge=1, le=100),
    busqueda: Optional[str] = None,
    filtro: Optional[str] = None
):
    """
    Obtener lista de productos con paginación
    ✅ CORREGIDO: Maneja establecimientos sin nombre
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        offset = (pagina - 1) * limite

        print(f"📦 [API] Obteniendo productos - Página {pagina}")

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

        # ✅ QUERY FINAL CORREGIDA - Maneja establecimientos sin nombre
        if database_type == "postgresql":
            query = f"""
                WITH producto_plus AS (
                    SELECT
                        ppe.producto_maestro_id,
                        STRING_AGG(
                            ppe.codigo_plu || ' (' ||
                            COALESCE(e.nombre_normalizado, e.nombre, 'Est. ' || e.id::text) ||
                            ')',
                            ', '
                            ORDER BY COALESCE(e.nombre_normalizado, e.nombre, '')
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
                WHERE {where_sql}
                ORDER BY pm.total_reportes DESC NULLS LAST, pm.id DESC
                LIMIT %s OFFSET %s
            """
        else:
            # SQLite - sin PLUs
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
                    NULL as codigo_plu,
                    0 as num_establecimientos
                FROM productos_maestros pm
                WHERE {where_sql}
                ORDER BY pm.total_reportes DESC, pm.id DESC
                LIMIT ? OFFSET ?
            """

        params.extend([limite, offset])

        print(f"🔍 [API] Ejecutando query con {len(params)} parámetros")
        cursor.execute(query, params)

        productos = []
        for row in cursor.fetchall():
            producto = {
                "id": row[0],
                "codigo_ean": row[1] or None,
                "codigo_plu": row[10] if (row[10] and row[10] != '-') else None,  # ✅ CAMPO CRÍTICO
                "nombre_normalizado": row[2],
                "nombre_comercial": row[3],
                "nombre": row[2] or row[3],
                "marca": row[4] or "Sin marca",
                "categoria": row[5] or "Sin categoría",
                "subcategoria": row[6],
                "presentacion": row[7],
                "total_reportes": row[8] or 0,
                "veces_comprado": row[8] or 0,
                "precio_promedio": float(row[9]) if row[9] else 0,
                "num_establecimientos": row[11] if len(row) > 11 else 0
            }

            productos.append(producto)

        # Debug: Mostrar primeros 3 productos
        if productos:
            for i, p in enumerate(productos[:3]):
                print(f"✅ [API] Producto {i+1}: ID={p['id']}, PLU={p['codigo_plu']}, Nombre={p['nombre'][:30]}")

        # Contar total
        count_query = f"SELECT COUNT(*) FROM productos_maestros pm WHERE {where_sql}"
        cursor.execute(count_query, params[:-2])
        total = cursor.fetchone()[0]

        conn.close()

        total_paginas = (total + limite - 1) // limite

        print(f"✅ [API] Respuesta: {len(productos)} productos de {total} total")

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
        print(f"❌ [API] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}")
async def obtener_producto(producto_id: int):
    """Obtener un producto específico con sus PLUs"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        print(f"🔍 [API] Obteniendo producto {producto_id}")

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

        # Obtener PLUs
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    ppe.codigo_plu,
                    COALESCE(e.nombre_normalizado, e.nombre, 'Est. ' || e.id::text) as establecimiento,
                    ppe.precio_actual,
                    ppe.total_reportes
                FROM productos_por_establecimiento ppe
                LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE ppe.producto_maestro_id = %s
                  AND ppe.codigo_plu IS NOT NULL
                ORDER BY COALESCE(e.nombre_normalizado, e.nombre, '')
            """, (producto_id,))
            plus = cursor.fetchall()
        else:
            plus = []

        conn.close()

        return {
            "id": row[0],
            "codigo_ean": row[1],
            "codigo_plu": ", ".join([f"{p[0]} ({p[1]})" for p in plus]) if plus else None,
            "plus_detalle": [
                {
                    "codigo_plu": p[0],
                    "establecimiento": p[1],
                    "precio_actual": float(p[2]) if p[2] else None,
                    "total_reportes": p[3] or 0
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
            "precio_promedio_global": float(row[9]) if row[9] else 0
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [API] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}/plus")
async def obtener_plus_producto(producto_id: int):
    """Obtener todos los PLUs de un producto"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        print(f"🏪 [API] Obteniendo PLUs del producto {producto_id}")

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    ppe.codigo_plu,
                    COALESCE(e.nombre_normalizado, e.nombre, 'Establecimiento ' || e.id::text) as establecimiento,
                    ppe.precio_actual,
                    ppe.total_reportes,
                    ppe.ultima_actualizacion,
                    e.id as establecimiento_id
                FROM productos_por_establecimiento ppe
                LEFT JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE ppe.producto_maestro_id = %s
                  AND ppe.codigo_plu IS NOT NULL
                ORDER BY COALESCE(e.nombre_normalizado, e.nombre, '')
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT DISTINCT
                    i.codigo_leido as codigo_plu,
                    f.establecimiento,
                    i.precio_pagado as precio_actual,
                    1 as total_reportes,
                    f.fecha_factura as ultima_actualizacion,
                    f.establecimiento_id
                FROM items_factura i
                JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = ?
                  AND i.codigo_leido IS NOT NULL
                  AND LENGTH(i.codigo_leido) BETWEEN 3 AND 7
                ORDER BY f.establecimiento
            """, (producto_id,))

        plus = []
        for row in cursor.fetchall():
            plus.append({
                "codigo_plu": row[0],
                "establecimiento": row[1],
                "precio_actual": float(row[2]) if row[2] else None,
                "total_reportes": row[3] or 0,
                "ultima_actualizacion": str(row[4]) if row[4] else None,
                "establecimiento_id": row[5]
            })

        conn.close()

        print(f"✅ [API] {len(plus)} PLUs encontrados")

        return {
            "success": True,
            "plus": plus,
            "total": len(plus)
        }

    except Exception as e:
        print(f"❌ [API] Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{producto_id}")
async def actualizar_producto(producto_id: int, data: dict):
    """Actualizar un producto"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

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
            query = f"UPDATE productos_maestros SET {', '.join(updates)} WHERE id = %s"
        else:
            query = f"UPDATE productos_maestros SET {', '.join(updates)} WHERE id = ?"

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
        print(f"❌ [API] Error: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}/historial")
async def obtener_historial(producto_id: int):
    """Obtener historial de compras"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    f.fecha_factura,
                    f.establecimiento,
                    i.precio_pagado,
                    i.cantidad
                FROM items_factura i
                JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = %s
                ORDER BY f.fecha_factura DESC
                LIMIT 50
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    f.fecha_factura,
                    f.establecimiento,
                    i.precio_pagado,
                    i.cantidad
                FROM items_factura i
                JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = ?
                ORDER BY f.fecha_factura DESC
                LIMIT 50
            """, (producto_id,))

        compras = []
        for row in cursor.fetchall():
            compras.append({
                "fecha": str(row[0]) if row[0] else None,
                "establecimiento": row[1] or "Sin establecimiento",
                "precio": float(row[2]) if row[2] else 0,
                "cantidad": int(row[3]) if row[3] else 1
            })

        conn.close()

        return {"compras": compras}

    except Exception as e:
        print(f"❌ [API] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/duplicados")
async def obtener_duplicados():
    """Obtener productos duplicados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        duplicados = []

        if database_type == "postgresql":
            cursor.execute("""
                SELECT codigo_ean, COUNT(*) as total
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                GROUP BY codigo_ean
                HAVING COUNT(*) > 1
                LIMIT 20
            """)
        else:
            cursor.execute("""
                SELECT codigo_ean, COUNT(*) as total
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                GROUP BY codigo_ean
                HAVING COUNT(*) > 1
                LIMIT 20
            """)

        for row in cursor.fetchall():
            ean = row[0]

            if database_type == "postgresql":
                cursor.execute("""
                    SELECT id, nombre_normalizado, codigo_ean, marca, total_reportes
                    FROM productos_maestros
                    WHERE codigo_ean = %s
                    ORDER BY total_reportes DESC
                """, (ean,))
            else:
                cursor.execute("""
                    SELECT id, nombre_normalizado, codigo_ean, marca, total_reportes
                    FROM productos_maestros
                    WHERE codigo_ean = ?
                    ORDER BY total_reportes DESC
                """, (ean,))

            productos_dup = []
            for p in cursor.fetchall():
                productos_dup.append({
                    "id": p[0],
                    "nombre_normalizado": p[1],
                    "codigo_ean": p[2],
                    "marca": p[3],
                    "total_reportes": p[4] or 0
                })

            if len(productos_dup) > 1:
                duplicados.append({
                    "tipo": "ean",
                    "valor": ean,
                    "productos": productos_dup
                })

        conn.close()

        return {
            "success": True,
            "duplicados": duplicados,
            "total": len(duplicados)
        }

    except Exception as e:
        print(f"❌ [API] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/duplicados/detectar")
async def detectar_duplicados():
    """Detectar duplicados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                ) AS duplicados
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*)
                FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                )
            """)

        total_duplicados = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "success": True,
            "total_duplicados": total_duplicados,
            "message": f"Se encontraron {total_duplicados} grupos de duplicados"
        }

    except Exception as e:
        print(f"❌ [API] Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ productos_api_v2.py FINAL - Con manejo correcto de establecimientos sin nombre")
