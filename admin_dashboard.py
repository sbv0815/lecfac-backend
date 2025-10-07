# admin_dashboard.py - STATS CORREGIDO
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
            SELECT COUNT(DISTINCT codigo) 
            FROM (
                SELECT codigo, COUNT(DISTINCT valor) as variaciones
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
            SELECT COUNT(DISTINCT codigo) 
            FROM (
                SELECT codigo, COUNT(DISTINCT valor) as variaciones
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
        
        # 5. Total pendientes (todo lo que NO sea revisada o validada)
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
        
        # Agrupar productos por código (o nombre si no tiene código)
        cursor.execute("""
            SELECT 
                MIN(p.id) as id,
                COALESCE(p.codigo, 'SIN_CODIGO') as codigo,
                p.nombre,
                COUNT(DISTINCT p.factura_id) as num_facturas,
                COUNT(p.id) as veces_visto,
                MIN(p.valor) as precio_min,
                MAX(p.valor) as precio_max,
                AVG(p.valor) as precio_promedio
            FROM productos p
            WHERE p.nombre IS NOT NULL AND p.nombre != ''
            GROUP BY COALESCE(p.codigo, p.nombre), p.nombre
            ORDER BY COUNT(p.id) DESC
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
async def eliminar_factura(factura_id: int):
    """Eliminar una factura y todos sus productos asociados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Eliminar productos de la factura
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        
        # Eliminar la factura
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"success": True, "message": "Factura eliminada exitosamente", "id": factura_id}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/duplicados/productos")
async def detectar_productos_duplicados(umbral: float = 85.0, criterio: str = "todos"):
    """Detectar productos duplicados - CORREGIDO para tabla productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener productos únicos agrupados
        cursor.execute("""
            SELECT 
                MIN(id) as id,
                codigo,
                nombre,
                COUNT(*) as veces_visto,
                AVG(valor) as precio_promedio,
                MAX(factura_id) as ultima_factura
            FROM productos
            WHERE nombre IS NOT NULL AND nombre != ''
            GROUP BY COALESCE(codigo, nombre), nombre
            HAVING COUNT(*) >= 1
            ORDER BY COUNT(*) DESC
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo": row[1] or "",
                "nombre": row[2],
                "veces_visto": row[3] or 0,
                "precio": float(row[4]) if row[4] else 0,
                "ultima_actualizacion": None
            })
        
        duplicados = []
        procesados = set()
        
        for i, p1 in enumerate(productos):
            for j, p2 in enumerate(productos[i+1:], start=i+1):
                par_key = tuple(sorted([p1["id"], p2["id"]]))
                if par_key in procesados:
                    continue
                
                mismo_codigo = False
                nombre_similar = False
                
                # REGLA 1: Mismo código
                if p1["codigo"] and p2["codigo"]:
                    if p1["codigo"] == p2["codigo"]:
                        mismo_codigo = True
                    else:
                        continue
                
                # REGLA 2: Similitud de nombres
                if not mismo_codigo:
                    sim = similitud_texto(p1["nombre"], p2["nombre"])
                    if sim >= umbral:
                        nombre_similar = True
                    else:
                        continue
                
                # Aplicar filtro de criterio
                if criterio == "codigo" and not mismo_codigo:
                    continue
                if criterio == "nombre" and not nombre_similar:
                    continue
                
                similitud_valor = 100 if mismo_codigo else similitud_texto(p1["nombre"], p2["nombre"])
                
                razones = []
                if mismo_codigo:
                    razones.append("Mismo código")
                if nombre_similar:
                    razones.append(f"Nombres similares ({similitud_valor:.1f}%)")
                
                duplicados.append({
                    "id": len(duplicados),
                    "producto1": {
                        "id": p1["id"],
                        "nombre": p1["nombre"],
                        "codigo": p1["codigo"],
                        "precio": p1["precio"],
                        "veces_visto": p1["veces_visto"]
                    },
                    "producto2": {
                        "id": p2["id"],
                        "nombre": p2["nombre"],
                        "codigo": p2["codigo"],
                        "precio": p2["precio"],
                        "veces_visto": p2["veces_visto"]
                    },
                    "similitud": round(similitud_valor, 1),
                    "mismo_codigo": mismo_codigo,
                    "nombre_similar": nombre_similar,
                    "razon": ", ".join(razones)
                })
                
                procesados.add(par_key)
        
        cursor.close()
        conn.close()
        
        return {"duplicados": duplicados, "total": len(duplicados)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def similitud_texto(a: str, b: str) -> float:
    """Calcular similitud entre dos strings (0-100)"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


@router.post("/duplicados/productos/fusionar")
async def fusionar_productos(request: dict):
    """Fusionar dos productos duplicados - CORREGIDO"""
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
        
        # Actualizar todos los productos con el ID a eliminar
        cursor.execute("""
            UPDATE productos
            SET codigo = %s, nombre = %s
            WHERE id = %s
        """, (prod_mantener[0], prod_mantener[1], producto_eliminar_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "Productos fusionados exitosamente"
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alertas/cambios-precio")
async def detectar_cambios_precio(dias: int = 7):
    """Detectar cambios significativos de precio - CORREGIDO"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar productos con variación de precio significativa
        cursor.execute("""
            WITH productos_con_precios AS (
                SELECT 
                    p.codigo,
                    p.nombre,
                    f.establecimiento,
                    p.valor as precio,
                    f.fecha_cargue,
                    AVG(p.valor) OVER (PARTITION BY p.codigo, f.establecimiento) as precio_promedio,
                    MIN(p.valor) OVER (PARTITION BY p.codigo, f.establecimiento) as precio_min,
                    MAX(p.valor) OVER (PARTITION BY p.codigo, f.establecimiento) as precio_max
                FROM productos p
                JOIN facturas f ON p.factura_id = f.id
                WHERE p.codigo IS NOT NULL 
                  AND p.codigo != ''
                  AND f.fecha_cargue >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            )
            SELECT DISTINCT
                codigo,
                nombre,
                establecimiento,
                precio as precio_actual,
                fecha_cargue,
                precio_promedio,
                precio_min,
                precio_max
            FROM productos_con_precios
            WHERE (precio > precio_max * 1.2 
                OR precio < precio_min * 0.8
                OR ABS(precio - precio_promedio) / precio_promedio > 0.2)
            ORDER BY fecha_cargue DESC
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
            if precio_actual > precio_max:
                tipo_alerta = "MÁXIMO_HISTÓRICO"
            elif precio_actual < precio_min:
                tipo_alerta = "MÍNIMO_HISTÓRICO"
            else:
                tipo_alerta = "CAMBIO_SIGNIFICATIVO"
            
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
        
        # Comparar precios por establecimiento
        cursor.execute("""
            SELECT DISTINCT ON (f.establecimiento)
                f.establecimiento,
                f.cadena,
                p.valor as precio,
                f.fecha_cargue
            FROM productos p
            JOIN facturas f ON p.factura_id = f.id
            WHERE p.codigo = (SELECT codigo FROM productos WHERE id = %s)
            ORDER BY f.establecimiento, f.fecha_cargue DESC
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
        
        ahorro_maximo = max(precios) - min(precios) if precios else 0
        
        cursor.close()
        conn.close()
        
        return {
            "producto_id": producto_id,
            "nombre": prod_info[0],
            "codigo": prod_info[1] or "N/A",
            "comparacion": sorted(comparacion, key=lambda x: x["precio"]),
            "ahorro_maximo": ahorro_maximo
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
