# admin_dashboard.py - VERSIÓN CORREGIDA
from fastapi import HTTPException, APIRouter
from typing import List, Optional
from difflib import SequenceMatcher
from database import get_db_connection

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/stats")
async def estadisticas():
    """Obtener estadísticas generales del sistema - VERSIÓN SIMPLE"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        # 2. Productos únicos
        cursor.execute("""
            SELECT COUNT(DISTINCT COALESCE(codigo, nombre)) 
            FROM productos 
            WHERE nombre IS NOT NULL AND nombre != ''
        """)
        productos_unicos = cursor.fetchone()[0]
        
        # 3. Facturas pendientes de revisión
        cursor.execute("""
            SELECT COUNT(*) FROM facturas 
            WHERE COALESCE(estado_validacion, 'pendiente') NOT IN ('revisada', 'validada')
        """)
        facturas_pendientes = cursor.fetchone()[0]
        
        # 4. Alertas activas
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT codigo 
                FROM productos
                WHERE codigo IS NOT NULL AND codigo != ''
                GROUP BY codigo
                HAVING COUNT(DISTINCT valor) > 1
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
        
        # 2. Productos únicos
        cursor.execute("""
            SELECT COUNT(DISTINCT COALESCE(codigo, nombre)) 
            FROM productos 
            WHERE nombre IS NOT NULL AND nombre != ''
        """)
        productos_unicos = cursor.fetchone()[0]
        
        # 3. Alertas activas
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT codigo 
                FROM productos
                WHERE codigo IS NOT NULL AND codigo != ''
                GROUP BY codigo
                HAVING COUNT(DISTINCT valor) > 1
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
    """Obtener todos los productos únicos del sistema - CORREGIDO"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query CORREGIDO con GROUP BY completo
        cursor.execute("""
            SELECT 
                MIN(p.id) as id,
                COALESCE(p.codigo, 'SIN_CODIGO_' || MIN(p.id)) as codigo,
                p.nombre,
                COUNT(DISTINCT p.factura_id) as num_facturas,
                COUNT(p.id) as veces_visto,
                MIN(p.valor) as precio_min,
                MAX(p.valor) as precio_max,
                AVG(p.valor) as precio_promedio
            FROM productos p
            WHERE p.nombre IS NOT NULL AND p.nombre != ''
            GROUP BY p.codigo, p.nombre
            ORDER BY COUNT(p.id) DESC
            LIMIT 500
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2] or "Sin nombre",
                "num_facturas": row[3] or 0,
                "veces_visto": row[4] or 0,
                "precio_min": float(row[5]) if row[5] else 0,
                "precio_max": float(row[6]) if row[6] else 0,
                "precio_promedio": float(row[7]) if row[7] else 0,
                "verificado": False,
                "necesita_revision": False
            })
        
        cursor.close()
        conn.close()
        
        return {"productos": productos, "total": len(productos)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error en catalogo: {str(e)}")


@router.get("/facturas")
async def obtener_facturas():
    """Obtener todas las facturas"""
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
                COUNT(p.id) as num_productos
            FROM facturas f
            LEFT JOIN productos p ON p.factura_id = f.id
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


@router.delete("/facturas/{factura_id}")
async def eliminar_factura_admin(factura_id: int):
    """Eliminar una factura y todos sus productos asociados - ADMIN"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # Eliminar productos
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        productos_eliminados = cursor.rowcount
        
        # Eliminar factura
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True, 
            "message": "Factura eliminada exitosamente", 
            "id": factura_id,
            "productos_eliminados": productos_eliminados
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


@router.post("/duplicados/productos/fusionar")
async def fusionar_productos_admin(request: dict):
    """Fusionar dos productos duplicados - ADMIN (Deprecado, usar duplicados_routes)"""
    try:
        producto_mantener_id = request.get("producto_mantener_id")
        producto_eliminar_id = request.get("producto_eliminar_id")
        
        if not producto_mantener_id or not producto_eliminar_id:
            raise HTTPException(status_code=400, detail="Se requieren ambos IDs")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener datos de ambos productos
        cursor.execute("SELECT codigo, nombre FROM productos WHERE id = %s", (producto_mantener_id,))
        prod_mantener = cursor.fetchone()
        
        cursor.execute("SELECT codigo, nombre FROM productos WHERE id = %s", (producto_eliminar_id,))
        prod_eliminar = cursor.fetchone()
        
        if not prod_mantener or not prod_eliminar:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        # Simplemente eliminar el duplicado
        cursor.execute("DELETE FROM productos WHERE id = %s", (producto_eliminar_id,))
        
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
    """Detectar cambios significativos de precio - CORREGIDO"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Query CORREGIDO - Detectar productos con variación de precio
        cursor.execute("""
            WITH precios_recientes AS (
                SELECT 
                    p.codigo,
                    p.nombre,
                    f.establecimiento,
                    p.valor as precio,
                    f.fecha_cargue,
                    ROW_NUMBER() OVER (PARTITION BY p.codigo, f.establecimiento ORDER BY f.fecha_cargue DESC) as rn
                FROM productos p
                JOIN facturas f ON p.factura_id = f.id
                WHERE p.codigo IS NOT NULL 
                  AND p.codigo != ''
                  AND p.valor > 0
                  AND f.fecha_cargue >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            ),
            estadisticas AS (
                SELECT 
                    codigo,
                    establecimiento,
                    AVG(precio) as precio_promedio,
                    MIN(precio) as precio_min,
                    MAX(precio) as precio_max,
                    COUNT(*) as num_registros
                FROM precios_recientes
                GROUP BY codigo, establecimiento
                HAVING COUNT(*) >= 2
            ),
            ultimo_precio AS (
                SELECT 
                    pr.codigo,
                    pr.nombre,
                    pr.establecimiento,
                    pr.precio as precio_actual,
                    pr.fecha_cargue
                FROM precios_recientes pr
                WHERE pr.rn = 1
            )
            SELECT 
                up.codigo,
                up.nombre,
                up.establecimiento,
                up.precio_actual,
                up.fecha_cargue,
                e.precio_promedio,
                e.precio_min,
                e.precio_max
            FROM ultimo_precio up
            JOIN estadisticas e ON up.codigo = e.codigo AND up.establecimiento = e.establecimiento
            WHERE (
                up.precio_actual > e.precio_max * 1.15 
                OR up.precio_actual < e.precio_min * 0.85
                OR ABS(up.precio_actual - e.precio_promedio) / NULLIF(e.precio_promedio, 0) > 0.15
            )
            ORDER BY up.fecha_cargue DESC
            LIMIT 50
        """ % dias)
        
        alertas = []
        for row in cursor.fetchall():
            precio_actual = float(row[3])
            precio_promedio = float(row[5])
            precio_min = float(row[6])
            precio_max = float(row[7])
            
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
                "cambio_porcentaje": round(cambio_porcentaje, 1)
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
    """Comparar precio de un producto en diferentes establecimientos - CORREGIDO"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener info del producto
        cursor.execute("""
            SELECT nombre, codigo 
            FROM productos 
            WHERE id = %s
        """, (producto_id,))
        
        prod_info = cursor.fetchone()
        if not prod_info:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        codigo_producto = prod_info[1]
        
        if not codigo_producto:
            raise HTTPException(status_code=400, detail="Producto sin código EAN, no se puede comparar")
        
        # Comparar precios por establecimiento (último precio registrado)
        cursor.execute("""
            SELECT DISTINCT ON (f.establecimiento)
                f.establecimiento,
                f.cadena,
                p.valor as precio,
                f.fecha_cargue
            FROM productos p
            JOIN facturas f ON p.factura_id = f.id
            WHERE p.codigo = %s
              AND p.valor > 0
            ORDER BY f.establecimiento, f.fecha_cargue DESC
        """, (codigo_producto,))
        
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
            "codigo": codigo_producto,
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

print("✅ admin_dashboard.py cargado - VERSIÓN CORREGIDA")
