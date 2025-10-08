# admin_dashboard.py - VERSIÓN ACTUALIZADA CON NUEVA ARQUITECTURA
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import List, Optional
from difflib import SequenceMatcher
from database import get_db_connection
import os

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/stats")
async def estadisticas():
    """Obtener estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        # 2. Productos únicos en catálogo NUEVO
        cursor.execute("""
            SELECT COUNT(DISTINCT id) 
            FROM productos_maestros
        """)
        productos_unicos = cursor.fetchone()[0]
        
        # 3. Facturas pendientes de revisión
        cursor.execute("""
            SELECT COUNT(*) FROM facturas 
            WHERE COALESCE(estado_validacion, 'pendiente') NOT IN ('revisada', 'validada')
        """)
        facturas_pendientes = cursor.fetchone()[0]
        
        # 4. Alertas activas (productos con variación de precio)
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT producto_maestro_id
                FROM precios_productos
                WHERE producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
                HAVING COUNT(DISTINCT precio) > 1
            ) AS cambios
        """)
        alertas_activas = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "total_facturas": total_facturas,
            "productos_unicos": productos_unicos,
            "alertas_activas": alertas_activas,
            "pendientes_revision": facturas_pendientes
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats-detailed")
async def estadisticas_detalladas():
    """Obtener estadísticas detalladas con desglose por estado"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        # 2. Productos únicos en catálogo global
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        productos_unicos = cursor.fetchone()[0]
        
        # 3. Alertas activas
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT producto_maestro_id
                FROM precios_productos
                WHERE producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
                HAVING COUNT(DISTINCT precio) > 1
            ) AS cambios
        """)
        alertas_activas = cursor.fetchone()[0]
        
        # 4. Desglose por estado
        cursor.execute("""
            SELECT 
                COALESCE(estado_validacion, 'sin_estado') as estado,
                COUNT(*) as cantidad
            FROM facturas
            GROUP BY estado_validacion
        """)
        
        por_estado = {}
        for row in cursor.fetchall():
            estado = row[0]
            cantidad = row[1]
            por_estado[estado] = cantidad
        
        # 5. Total pendientes
        cursor.execute("""
            SELECT COUNT(*) FROM facturas 
            WHERE COALESCE(estado_validacion, 'pendiente') NOT IN ('revisada', 'validada')
        """)
        pendientes_total = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "total_facturas": total_facturas,
            "productos_unicos": productos_unicos,
            "alertas_activas": alertas_activas,
            "por_estado": por_estado,
            "pendientes_total": pendientes_total
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/catalogo")
async def obtener_catalogo_productos():
    """Obtener catálogo global de productos (productos_maestros)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                pm.id,
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.marca,
                pm.total_reportes,
                pm.precio_promedio_global,
                pm.precio_minimo_historico,
                pm.precio_maximo_historico
            FROM productos_maestros pm
            ORDER BY pm.total_reportes DESC
            LIMIT 500
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2] or "Sin nombre",
                "marca": row[3],
                "veces_visto": row[4] or 0,
                "precio_promedio": float(row[5]) if row[5] else 0,
                "precio_min": float(row[6]) if row[6] else 0,
                "precio_max": float(row[7]) if row[7] else 0,
                "verificado": False,
                "necesita_revision": False
            })
        
        cursor.close()
        conn.close()
        
        return {"productos": productos, "total": len(productos)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error en catálogo: {str(e)}")


@router.get("/facturas")
async def obtener_facturas():
    """Obtener todas las facturas con contador de productos de items_factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                f.id,
                f.establecimiento,
                f.total_factura,
                f.fecha_cargue,
                f.estado_validacion,
                f.tiene_imagen,
                COUNT(i.id) as num_productos
            FROM facturas f
            LEFT JOIN items_factura i ON i.factura_id = f.id
            GROUP BY f.id, f.establecimiento, f.total_factura, f.fecha_cargue, f.estado_validacion, f.tiene_imagen
            ORDER BY f.fecha_cargue DESC
            LIMIT 100
        """)
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1] or "Sin datos",
                "total": float(row[2]) if row[2] else 0,
                "fecha": str(row[3]) if row[3] else "",
                "estado": row[4] or "pendiente",
                "tiene_imagen": row[5] or False,
                "productos": row[6] or 0
            })
        
        cursor.close()
        conn.close()
        
        return {"facturas": facturas, "total": len(facturas)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facturas/{factura_id}")
async def get_factura_detalle(factura_id: int):
    """
    ✅ NUEVA VERSIÓN - Obtener factura con productos de items_factura
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Obtener datos generales de la factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT 
                    id, usuario_id, establecimiento, cadena, total_factura, 
                    fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen,
                    establecimiento_id
                FROM facturas 
                WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT 
                    id, usuario_id, establecimiento, cadena, total_factura, 
                    fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen,
                    establecimiento_id
                FROM facturas 
                WHERE id = ?
            """, (factura_id,))
        
        factura = cursor.fetchone()
        
        if not factura:
            cursor.close()
            conn.close()
            raise HTTPException(404, "Factura no encontrada")
        
        # 2. Obtener productos de items_factura (NUEVA TABLA)
        productos = []
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT 
                    i.id,
                    i.codigo_leido,
                    i.nombre_leido,
                    i.precio_pagado,
                    i.cantidad,
                    pm.codigo_ean,
                    pm.nombre_normalizado
                FROM items_factura i
                LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
                WHERE i.factura_id = %s
                ORDER BY i.id
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT 
                    i.id,
                    i.codigo_leido,
                    i.nombre_leido,
                    i.precio_pagado,
                    i.cantidad,
                    pm.codigo_ean,
                    pm.nombre_normalizado
                FROM items_factura i
                LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
                WHERE i.factura_id = ?
                ORDER BY i.id
            """, (factura_id,))
        
        for row in cursor.fetchall():
            # Usar nombre del catálogo si existe, sino el nombre leído
            nombre = row[6] if row[6] else (row[2] or "Sin nombre")
            codigo = row[5] if row[5] else (row[1] or "")
            
            productos.append({
                "id": row[0],
                "codigo": codigo,
                "nombre": nombre,
                "precio": float(row[3]) if row[3] else 0,
                "cantidad": row[4] or 1
            })
        
        cursor.close()
        conn.close()
        
        print(f"✅ Factura {factura_id}: {len(productos)} productos de items_factura")
        
        # 3. Construir respuesta
        return {
            "id": factura[0],
            "usuario_id": factura[1],
            "establecimiento": factura[2] or "",
            "cadena": factura[3] or "",
            "total": float(factura[4]) if factura[4] else 0,
            "total_factura": float(factura[4]) if factura[4] else 0,
            "fecha": str(factura[5]) if factura[5] else "",
            "estado": factura[6] or "pendiente",
            "puntaje": factura[7] or 0,
            "tiene_imagen": factura[8] or False,
            "productos": productos
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en get_factura_detalle: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, str(e))


# AGREGAR ESTE ENDPOINT en admin_dashboard.py
# Colócalo después del endpoint @router.get("/facturas/{factura_id}")

# REEMPLAZAR el endpoint @router.put("/facturas/{factura_id}") 
# con esta versión CORREGIDA:

@router.put("/facturas/{factura_id}")
async def update_factura(factura_id: int, request: dict):
    """
    Actualizar datos generales de una factura (establecimiento, total, fecha)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Obtener datos del request
        establecimiento = request.get("establecimiento", "").strip()
        total = request.get("total")
        fecha = request.get("fecha")
        
        print(f"📝 Actualizando factura {factura_id}")
        print(f"   Establecimiento: {establecimiento}")
        print(f"   Total: {total}")
        print(f"   Fecha: {fecha}")
        print(f"   DB Type: {database_type}")
        
        # Verificar que la factura existe
        if database_type == "postgresql":
            cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT id FROM facturas WHERE id = ?", (factura_id,))
        
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # Para PostgreSQL, construir query con placeholders %s
        if database_type == "postgresql":
            updates = []
            params = []
            
            if establecimiento:
                updates.append("establecimiento = %s")
                params.append(establecimiento)
            
            if total is not None:
                updates.append("total_factura = %s")
                params.append(float(total))
            
            if fecha:
                updates.append("fecha_cargue = %s")
                params.append(fecha)
            
            if not updates:
                conn.close()
                raise HTTPException(status_code=400, detail="No hay datos para actualizar")
            
            params.append(factura_id)
            
            query = f"""
                UPDATE facturas 
                SET {', '.join(updates)}
                WHERE id = %s
                RETURNING id, establecimiento, total_factura, fecha_cargue
            """
            
            print(f"🔍 Query: {query}")
            print(f"🔍 Params: {params}")
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
        else:
            # SQLite
            updates = []
            params = []
            
            if establecimiento:
                updates.append("establecimiento = ?")
                params.append(establecimiento)
            
            if total is not None:
                updates.append("total_factura = ?")
                params.append(float(total))
            
            if fecha:
                updates.append("fecha_cargue = ?")
                params.append(fecha)
            
            if not updates:
                conn.close()
                raise HTTPException(status_code=400, detail="No hay datos para actualizar")
            
            params.append(factura_id)
            
            query = f"""
                UPDATE facturas 
                SET {', '.join(updates)}
                WHERE id = ?
            """
            
            cursor.execute(query, params)
            
            cursor.execute("""
                SELECT id, establecimiento, total_factura, fecha_cargue 
                FROM facturas WHERE id = ?
            """, (factura_id,))
            result = cursor.fetchone()
        
        conn.commit()
        conn.close()
        
        print(f"✅ Factura actualizada: {result}")
        
        return {
            "success": True,
            "message": "Factura actualizada correctamente",
            "id": result[0],
            "establecimiento": result[1],
            "total": float(result[2]) if result[2] else 0,
            "fecha": str(result[3]) if result[3] else None
        }
        
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facturas/{factura_id}/imagen")
async def get_factura_imagen(factura_id: int):
    """Obtener imagen de factura en ALTA CALIDAD"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    if database_type == "postgresql":
        cursor.execute(
            "SELECT imagen_data, imagen_mime FROM facturas WHERE id = %s",
            (factura_id,)
        )
    else:
        cursor.execute(
            "SELECT imagen_data, imagen_mime FROM facturas WHERE id = ?",
            (factura_id,)
        )
    
    resultado = cursor.fetchone()
    conn.close()
    
    if not resultado or not resultado[0]:
        raise HTTPException(status_code=404, detail="Imagen no encontrada")
    
    imagen_data = resultado[0]
    imagen_mime = resultado[1] or "image/jpeg"
    
    # Retornar con headers para evitar compresión
    return Response(
        content=bytes(imagen_data),
        media_type=imagen_mime,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Content-Disposition": "inline"
        }
    )


@router.delete("/items/{item_id}")
async def delete_item(item_id: int):
    """Eliminar un item de factura duplicado"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Verificar que el item existe y obtener factura_id
        if database_type == "postgresql":
            cursor.execute(
                "SELECT factura_id FROM items_factura WHERE id = %s",
                (item_id,)
            )
        else:
            cursor.execute(
                "SELECT factura_id FROM items_factura WHERE id = ?",
                (item_id,)
            )
        
        resultado = cursor.fetchone()
        
        if not resultado:
            conn.close()
            raise HTTPException(status_code=404, detail="Item no encontrado")
        
        factura_id = resultado[0]
        
        # Eliminar el item
        if database_type == "postgresql":
            cursor.execute("DELETE FROM items_factura WHERE id = %s", (item_id,))
        else:
            cursor.execute("DELETE FROM items_factura WHERE id = ?", (item_id,))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "factura_id": factura_id, "message": "Item eliminado correctamente"}
        
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/facturas/{factura_id}")
async def eliminar_factura_admin(factura_id: int):
    """Eliminar una factura y todos sus datos asociados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT id FROM facturas WHERE id = ?", (factura_id,))
        
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # Eliminar items_factura (NUEVA TABLA)
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("DELETE FROM items_factura WHERE factura_id = %s", (factura_id,))
            items_eliminados = cursor.rowcount
            
            # Eliminar precios_productos
            cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
            precios_eliminados = cursor.rowcount
            
            # Eliminar productos legacy (por si acaso)
            cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
            
            # Eliminar factura
            cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("DELETE FROM items_factura WHERE factura_id = ?", (factura_id,))
            items_eliminados = cursor.rowcount
            
            cursor.execute("DELETE FROM precios_productos WHERE factura_id = ?", (factura_id,))
            precios_eliminados = cursor.rowcount
            
            cursor.execute("DELETE FROM productos WHERE factura_id = ?", (factura_id,))
            
            cursor.execute("DELETE FROM facturas WHERE id = ?", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True, 
            "message": "Factura eliminada exitosamente", 
            "id": factura_id,
            "items_eliminados": items_eliminados,
            "precios_eliminados": precios_eliminados
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


def similitud_texto(a: str, b: str) -> float:
    """Calcular similitud entre dos strings (0-100)"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


# Agregar estos endpoints en admin_dashboard.py

@router.put("/items/{item_id}")
async def update_item(item_id: int, request: dict):
    """
    Actualizar un item de factura (nombre, código, precio, cantidad)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Obtener datos del request
        nombre = request.get("nombre")
        codigo = request.get("codigo")  # Puede ser None o vacío
        precio = request.get("precio")
        cantidad = request.get("cantidad", 1)
        
        if not nombre or precio is None:
            raise HTTPException(status_code=400, detail="Nombre y precio son requeridos")
        
        # Verificar que el item existe Y obtener el código actual
        if database_type == "postgresql":
            cursor.execute(
                "SELECT factura_id, usuario_id, codigo_leido FROM items_factura WHERE id = %s",
                (item_id,)
            )
        else:
            cursor.execute(
                "SELECT factura_id, usuario_id, codigo_leido FROM items_factura WHERE id = ?",
                (item_id,)
            )
        
        resultado = cursor.fetchone()
        if not resultado:
            conn.close()
            raise HTTPException(status_code=404, detail="Item no encontrado")
        
        factura_id = resultado[0]
        usuario_id = resultado[1]
        codigo_actual = resultado[2]
        
        # Si no viene código en el request O está vacío, mantener el actual
        if not codigo or codigo.strip() == '':
            codigo = codigo_actual
            print(f"✓ Manteniendo código actual: {codigo_actual}")
        else:
            print(f"✓ Actualizando código: {codigo_actual} → {codigo}")
        
        # Si hay código, intentar vincular con producto maestro
        producto_maestro_id = None
        if codigo and len(codigo) >= 8:
            if database_type == "postgresql":
                cursor.execute(
                    "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
                    (codigo,)
                )
            else:
                cursor.execute(
                    "SELECT id FROM productos_maestros WHERE codigo_ean = ?",
                    (codigo,)
                )
            
            prod_result = cursor.fetchone()
            if prod_result:
                producto_maestro_id = prod_result[0]
            else:
                # Crear nuevo producto maestro
                if database_type == "postgresql":
                    cursor.execute("""
                        INSERT INTO productos_maestros 
                        (codigo_ean, nombre_normalizado, total_reportes)
                        VALUES (%s, %s, 1)
                        RETURNING id
                    """, (codigo, nombre))
                    producto_maestro_id = cursor.fetchone()[0]
                else:
                    cursor.execute("""
                        INSERT INTO productos_maestros 
                        (codigo_ean, nombre_normalizado, total_reportes)
                        VALUES (?, ?, 1)
                    """, (codigo, nombre))
                    producto_maestro_id = cursor.lastrowid
        
        # Actualizar el item
        if database_type == "postgresql":
            cursor.execute("""
                UPDATE items_factura
                SET nombre_leido = %s,
                    codigo_leido = %s,
                    precio_pagado = %s,
                    cantidad = %s,
                    producto_maestro_id = %s
                WHERE id = %s
            """, (nombre, codigo, precio, cantidad, producto_maestro_id, item_id))
        else:
            cursor.execute("""
                UPDATE items_factura
                SET nombre_leido = ?,
                    codigo_leido = ?,
                    precio_pagado = ?,
                    cantidad = ?,
                    producto_maestro_id = ?
                WHERE id = ?
            """, (nombre, codigo, precio, cantidad, producto_maestro_id, item_id))
        
        # Actualizar precio en precios_productos si hay producto_maestro_id
        if producto_maestro_id:
            # Obtener establecimiento_id
            if database_type == "postgresql":
                cursor.execute(
                    "SELECT establecimiento_id FROM facturas WHERE id = %s",
                    (factura_id,)
                )
            else:
                cursor.execute(
                    "SELECT establecimiento_id FROM facturas WHERE id = ?",
                    (factura_id,)
                )
            
            est_result = cursor.fetchone()
            if est_result and est_result[0]:
                establecimiento_id = est_result[0]
                
                # Insertar o actualizar precio
                if database_type == "postgresql":
                    cursor.execute("""
                        INSERT INTO precios_productos 
                        (producto_maestro_id, establecimiento_id, precio, fecha_registro, usuario_id, factura_id)
                        VALUES (%s, %s, %s, CURRENT_DATE, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, (producto_maestro_id, establecimiento_id, precio, usuario_id, factura_id))
                else:
                    cursor.execute("""
                        INSERT OR IGNORE INTO precios_productos 
                        (producto_maestro_id, establecimiento_id, precio, fecha_registro, usuario_id, factura_id)
                        VALUES (?, ?, ?, date('now'), ?, ?)
                    """, (producto_maestro_id, establecimiento_id, precio, usuario_id, factura_id))
        
        conn.commit()
        conn.close()
        
        print(f"✅ Item {item_id} actualizado: {nombre} - ${precio} - Código: {codigo}")
        
        return {
            "success": True,
            "message": "Item actualizado correctamente",
            "item_id": item_id,
            "factura_id": factura_id,
            "producto_maestro_id": producto_maestro_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/facturas/{factura_id}/items")
async def add_item(factura_id: int, request: dict):
    """
    Agregar un nuevo item a una factura
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    try:
        # Obtener datos del request
        nombre = request.get("nombre")
        codigo = request.get("codigo", "")
        precio = request.get("precio")
        cantidad = request.get("cantidad", 1)
        
        if not nombre or precio is None:
            raise HTTPException(status_code=400, detail="Nombre y precio son requeridos")
        
        # Verificar que la factura existe y obtener usuario_id
        if database_type == "postgresql":
            cursor.execute(
                "SELECT usuario_id, establecimiento_id FROM facturas WHERE id = %s",
                (factura_id,)
            )
        else:
            cursor.execute(
                "SELECT usuario_id, establecimiento_id FROM facturas WHERE id = ?",
                (factura_id,)
            )
        
        factura_info = cursor.fetchone()
        if not factura_info:
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        usuario_id = factura_info[0]
        establecimiento_id = factura_info[1]
        
        # Si hay código, buscar o crear producto maestro
        producto_maestro_id = None
        if codigo and len(codigo) >= 8:
            if database_type == "postgresql":
                cursor.execute(
                    "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
                    (codigo,)
                )
            else:
                cursor.execute(
                    "SELECT id FROM productos_maestros WHERE codigo_ean = ?",
                    (codigo,)
                )
            
            prod_result = cursor.fetchone()
            if prod_result:
                producto_maestro_id = prod_result[0]
            else:
                # Crear nuevo producto maestro
                if database_type == "postgresql":
                    cursor.execute("""
                        INSERT INTO productos_maestros 
                        (codigo_ean, nombre_normalizado, total_reportes)
                        VALUES (%s, %s, 1)
                        RETURNING id
                    """, (codigo, nombre))
                    producto_maestro_id = cursor.fetchone()[0]
                else:
                    cursor.execute("""
                        INSERT INTO productos_maestros 
                        (codigo_ean, nombre_normalizado, total_reportes)
                        VALUES (?, ?, 1)
                    """, (codigo, nombre))
                    producto_maestro_id = cursor.lastrowid
        
        # Insertar el nuevo item
        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO items_factura 
                (factura_id, usuario_id, nombre_leido, codigo_leido, precio_pagado, cantidad, producto_maestro_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (factura_id, usuario_id, nombre, codigo, precio, cantidad, producto_maestro_id))
            new_item_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO items_factura 
                (factura_id, usuario_id, nombre_leido, codigo_leido, precio_pagado, cantidad, producto_maestro_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (factura_id, usuario_id, nombre, codigo, precio, cantidad, producto_maestro_id))
            new_item_id = cursor.lastrowid
        
        # Si hay producto_maestro_id y establecimiento_id, agregar a precios_productos
        if producto_maestro_id and establecimiento_id:
            if database_type == "postgresql":
                cursor.execute("""
                    INSERT INTO precios_productos 
                    (producto_maestro_id, establecimiento_id, precio, fecha_registro, usuario_id, factura_id)
                    VALUES (%s, %s, %s, CURRENT_DATE, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (producto_maestro_id, establecimiento_id, precio, usuario_id, factura_id))
            else:
                cursor.execute("""
                    INSERT OR IGNORE INTO precios_productos 
                    (producto_maestro_id, establecimiento_id, precio, fecha_registro, usuario_id, factura_id)
                    VALUES (?, ?, ?, date('now'), ?, ?)
                """, (producto_maestro_id, establecimiento_id, precio, usuario_id, factura_id))
        
        # Actualizar contador de productos en factura
        if database_type == "postgresql":
            cursor.execute("""
                UPDATE facturas 
                SET productos_guardados = (
                    SELECT COUNT(*) FROM items_factura WHERE factura_id = %s
                )
                WHERE id = %s
            """, (factura_id, factura_id))
        else:
            cursor.execute("""
                UPDATE facturas 
                SET productos_guardados = (
                    SELECT COUNT(*) FROM items_factura WHERE factura_id = ?
                )
                WHERE id = ?
            """, (factura_id, factura_id))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "message": "Item agregado correctamente",
            "item_id": new_item_id,
            "factura_id": factura_id,
            "producto_maestro_id": producto_maestro_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# EJEMPLO DE USO EN FRONTEND
# ========================================

"""
// Editar un producto existente
async function editarProducto(itemId, nombre, codigo, precio, cantidad) {
    const response = await fetch(`/admin/items/${itemId}`, {
        method: 'PUT',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            nombre: nombre,
            codigo: codigo,
            precio: parseFloat(precio),
            cantidad: parseInt(cantidad)
        })
    });
    
    const result = await response.json();
    if (result.success) {
        alert('Producto actualizado');
        location.reload();
    }
}

// Agregar un producto nuevo
async function agregarProducto(facturaId, nombre, codigo, precio, cantidad) {
    const response = await fetch(`/admin/facturas/${facturaId}/items`, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            nombre: nombre,
            codigo: codigo,
            precio: parseFloat(precio),
            cantidad: parseInt(cantidad)
        })
    });
    
    const result = await response.json();
    if (result.success) {
        alert('Producto agregado');
        location.reload();
    }
}

// Eliminar un producto (ya existe)
async function eliminarProducto(itemId) {
    const response = await fetch(`/admin/items/${itemId}`, {
        method: 'DELETE'
    });
    
    const result = await response.json();
    if (result.success) {
        alert('Producto eliminado');
        location.reload();
    }
}
"""


@router.post("/duplicados/productos/fusionar")
async def fusionar_productos_admin(request: dict):
    """Fusionar dos productos duplicados"""
    try:
        producto_mantener_id = request.get("producto_mantener_id")
        producto_eliminar_id = request.get("producto_eliminar_id")
        
        if not producto_mantener_id or not producto_eliminar_id:
            raise HTTPException(status_code=400, detail="Se requieren ambos IDs")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Simplemente eliminar el duplicado de productos_maestros
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("DELETE FROM productos_maestros WHERE id = %s", (producto_eliminar_id,))
        else:
            cursor.execute("DELETE FROM productos_maestros WHERE id = ?", (producto_eliminar_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "Producto duplicado eliminado exitosamente"
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alertas/cambios-precio")
async def detectar_cambios_precio(dias: int = 30):
    """Detectar cambios significativos de precio usando precios_productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Query para PostgreSQL
            cursor.execute("""
                WITH precios_recientes AS (
                    SELECT 
                        pm.id as producto_id,
                        pm.nombre_normalizado,
                        pm.codigo_ean,
                        e.nombre_normalizado as establecimiento,
                        pp.precio,
                        pp.fecha_registro,
                        ROW_NUMBER() OVER (PARTITION BY pm.id, e.id ORDER BY pp.fecha_registro DESC) as rn
                    FROM precios_productos pp
                    JOIN productos_maestros pm ON pp.producto_maestro_id = pm.id
                    JOIN establecimientos e ON pp.establecimiento_id = e.id
                    WHERE pp.fecha_registro >= CURRENT_DATE - INTERVAL '%s days'
                ),
                estadisticas AS (
                    SELECT 
                        producto_id,
                        establecimiento,
                        AVG(precio) as precio_promedio,
                        MIN(precio) as precio_min,
                        MAX(precio) as precio_max,
                        COUNT(*) as num_registros
                    FROM precios_recientes
                    GROUP BY producto_id, establecimiento
                    HAVING COUNT(*) >= 2
                ),
                ultimo_precio AS (
                    SELECT 
                        pr.producto_id,
                        pr.nombre_normalizado,
                        pr.codigo_ean,
                        pr.establecimiento,
                        pr.precio as precio_actual,
                        pr.fecha_registro
                    FROM precios_recientes pr
                    WHERE pr.rn = 1
                )
                SELECT 
                    up.codigo_ean,
                    up.nombre_normalizado,
                    up.establecimiento,
                    up.precio_actual,
                    up.fecha_registro,
                    e.precio_promedio,
                    e.precio_min,
                    e.precio_max,
                    up.producto_id
                FROM ultimo_precio up
                JOIN estadisticas e ON up.producto_id = e.producto_id 
                    AND up.establecimiento = e.establecimiento
                WHERE (
                    up.precio_actual > e.precio_max * 1.15 
                    OR up.precio_actual < e.precio_min * 0.85
                    OR ABS(up.precio_actual - e.precio_promedio) / NULLIF(e.precio_promedio, 0) > 0.15
                )
                ORDER BY up.fecha_registro DESC
                LIMIT 50
            """ % dias)
        else:
            # Query simplificada para SQLite
            cursor.execute("""
                SELECT DISTINCT
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    'establecimiento' as establecimiento,
                    pp.precio as precio_actual,
                    pp.fecha_registro,
                    pm.precio_promedio_global as precio_promedio,
                    pm.precio_minimo_historico as precio_min,
                    pm.precio_maximo_historico as precio_max,
                    pm.id as producto_id
                FROM precios_productos pp
                JOIN productos_maestros pm ON pp.producto_maestro_id = pm.id
                WHERE pp.fecha_registro >= date('now', '-%s days')
                ORDER BY pp.fecha_registro DESC
                LIMIT 50
            """ % dias)
        
        alertas = []
        for row in cursor.fetchall():
            precio_actual = float(row[3])
            precio_promedio = float(row[5]) if row[5] else precio_actual
            precio_min = float(row[6]) if row[6] else precio_actual
            precio_max = float(row[7]) if row[7] else precio_actual
            
            cambio_porcentaje = ((precio_actual - precio_promedio) / precio_promedio) * 100 if precio_promedio > 0 else 0
            
            # Determinar tipo de alerta
            if precio_actual > precio_max * 1.15:
                tipo_alerta = "AUMENTO_SIGNIFICATIVO"
            elif precio_actual < precio_min * 0.85:
                tipo_alerta = "DISMINUCIÓN_SIGNIFICATIVA"
            else:
                tipo_alerta = "CAMBIO_ATÍPICO"
            
            alertas.append({
                "codigo": row[0] or "N/A",
                "nombre": row[1],
                "establecimiento": row[2],
                "precio_actual": precio_actual,
                "fecha": str(row[4]),
                "precio_promedio": precio_promedio,
                "precio_min": precio_min,
                "precio_max": precio_max,
                "tipo_alerta": tipo_alerta,
                "cambio_porcentaje": round(cambio_porcentaje, 1),
                "producto_id": row[8]
            })
        
        cursor.close()
        conn.close()
        
        return {"alertas": alertas, "total": len(alertas)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/{producto_id}/comparar-establecimientos")
async def comparar_precios_establecimientos(producto_id: int):
    """Comparar precio de un producto en diferentes establecimientos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener info del producto de productos_maestros
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT nombre_normalizado, codigo_ean 
                FROM productos_maestros 
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT nombre_normalizado, codigo_ean 
                FROM productos_maestros 
                WHERE id = ?
            """, (producto_id,))
        
        prod_info = cursor.fetchone()
        if not prod_info:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        # Comparar precios por establecimiento
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT DISTINCT ON (e.nombre_normalizado)
                    e.nombre_normalizado as establecimiento,
                    e.cadena,
                    pp.precio,
                    pp.fecha_registro
                FROM precios_productos pp
                JOIN establecimientos e ON pp.establecimiento_id = e.id
                WHERE pp.producto_maestro_id = %s
                  AND pp.precio > 0
                ORDER BY e.nombre_normalizado, pp.fecha_registro DESC
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT 
                    'establecimiento' as establecimiento,
                    'cadena' as cadena,
                    pp.precio,
                    pp.fecha_registro
                FROM precios_productos pp
                WHERE pp.producto_maestro_id = ?
                  AND pp.precio > 0
                ORDER BY pp.fecha_registro DESC
            """, (producto_id,))
        
        comparacion = []
        precios = []
        
        for row in cursor.fetchall():
            precio = float(row[2])
            precios.append(precio)
            comparacion.append({
                "establecimiento": row[0],
                "cadena": row[1] or "N/A",
                "precio": precio,
                "fecha": str(row[3])
            })
        
        # Marcar el más barato y más caro
        if precios:
            precio_min = min(precios)
            precio_max = max(precios)
            
            for item in comparacion:
                item["es_mas_barato"] = (item["precio"] == precio_min)
                item["es_mas_caro"] = (item["precio"] == precio_max) and (precio_min != precio_max)
                item["diferencia_vs_min"] = item["precio"] - precio_min
        
        ahorro_maximo = max(precios) - min(precios) if len(precios) > 0 else 0
        
        cursor.close()
        conn.close()
        
        return {
            "producto_id": producto_id,
            "nombre": prod_info[0],
            "codigo": prod_info[1],
            "comparacion": sorted(comparacion, key=lambda x: x["precio"]),
            "ahorro_maximo": round(ahorro_maximo, 2),
            "num_establecimientos": len(comparacion)
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

print("✅ admin_dashboard.py cargado correctamente")
