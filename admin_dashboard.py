# admin_dashboard.py
from fastapi import HTTPException, APIRouter
from typing import List, Optional
from difflib import SequenceMatcher
from database import get_db_connection

router = APIRouter(prefix="/admin", tags=["admin"])

@router.delete("/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """Eliminar una factura y todos sus productos asociados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Factura eliminada exitosamente", "id": factura_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/facturas/eliminar-multiple")
async def eliminar_facturas_multiples(request: dict):
    """Eliminar múltiples facturas de una vez"""
    try:
        factura_ids = request.get("ids", [])
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for factura_id in factura_ids:
            cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
            cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": f"{len(factura_ids)} facturas eliminadas", "ids": factura_ids}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/catalogo")
async def obtener_catalogo_productos():
    """Obtener todos los productos del catálogo con estadísticas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                p.id,
                p.codigo_ean,
                p.nombre_producto,
                p.veces_visto,
                p.verificado,
                p.necesita_revision,
                COUNT(DISTINCT pp.factura_id) as num_facturas,
                MIN(pp.precio) as precio_min,
                MAX(pp.precio) as precio_max,
                AVG(pp.precio) as precio_promedio
            FROM productos_catalogo p
            LEFT JOIN precios_productos pp ON p.id = pp.producto_id
            GROUP BY p.id, p.codigo_ean, p.nombre_producto, p.veces_visto, p.verificado, p.necesita_revision
            ORDER BY p.veces_visto DESC
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "veces_visto": row[3],
                "verificado": row[4],
                "necesita_revision": row[5],
                "num_facturas": row[6] or 0,
                "precio_min": float(row[7]) if row[7] else 0,
                "precio_max": float(row[8]) if row[8] else 0,
                "precio_promedio": float(row[9]) if row[9] else 0
            })
        
        cursor.close()
        conn.close()
        
        return {"productos": productos, "total": len(productos)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/duplicados/facturas")
async def detectar_facturas_duplicadas():
    """Detectar facturas potencialmente duplicadas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, establecimiento, total_factura, fecha_cargue,
                (SELECT COUNT(*) FROM precios_productos WHERE factura_id = facturas.id) as num_productos
            FROM facturas
            ORDER BY fecha_cargue DESC
        """)
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "total": float(row[2]) if row[2] else 0,
                "fecha": str(row[3]),
                "num_productos": row[4]
            })
        
        duplicados = []
        for i, f1 in enumerate(facturas):
            for f2 in facturas[i+1:]:
                if (f1["establecimiento"] == f2["establecimiento"] and 
                    abs(f1["total"] - f2["total"]) < 100):
                    duplicados.append({
                        "factura1": f1,
                        "factura2": f2,
                        "razon": "Mismo establecimiento y total similar",
                        "similitud": 90
                    })
        
        cursor.close()
        conn.close()
        
        return {"duplicados": duplicados, "total": len(duplicados)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def similitud_texto(a: str, b: str) -> float:
    """Calcular similitud entre dos strings (0-100)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


@router.get("/duplicados/productos")
async def detectar_productos_duplicados(umbral: float = 85.0):
    """Detectar productos con nombres similares"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, codigo_ean, nombre_producto, veces_visto
            FROM productos_catalogo
            ORDER BY veces_visto DESC
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo": row[1],
                "nombre": row[2],
                "veces_visto": row[3]
            })
        
        duplicados = []
        for i, p1 in enumerate(productos):
            for p2 in productos[i+1:]:
                sim = similitud_texto(p1["nombre"], p2["nombre"])
                
                if sim >= umbral:
                    duplicados.append({
                        "producto1": p1,
                        "producto2": p2,
                        "similitud": round(sim, 1),
                        "razon": "Nombres muy similares"
                    })
        
        cursor.close()
        conn.close()
        
        return {"duplicados": duplicados, "total": len(duplicados)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/productos/fusionar")
async def fusionar_productos(request: dict):
    """Fusionar dos productos duplicados"""
    try:
        producto_mantener_id = request.get("producto_mantener_id")
        producto_eliminar_id = request.get("producto_eliminar_id")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE precios_productos 
            SET producto_id = %s 
            WHERE producto_id = %s
        """, (producto_mantener_id, producto_eliminar_id))
        
        cursor.execute("""
            UPDATE productos_catalogo 
            SET veces_visto = veces_visto + (
                SELECT veces_visto FROM productos_catalogo WHERE id = %s
            )
            WHERE id = %s
        """, (producto_eliminar_id, producto_mantener_id))
        
        cursor.execute("DELETE FROM productos_catalogo WHERE id = %s", (producto_eliminar_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "message": "Productos fusionados exitosamente",
            "producto_final": producto_mantener_id,
            "producto_eliminado": producto_eliminar_id
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/productos/{producto_id}/editar")
async def editar_producto_catalogo(producto_id: int, request: dict):
    """Editar nombre y código de un producto del catálogo"""
    try:
        nombre = request.get("nombre")
        codigo_ean = request.get("codigo_ean")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if codigo_ean:
            cursor.execute("""
                UPDATE productos_catalogo 
                SET nombre_producto = %s, codigo_ean = %s, verificado = TRUE
                WHERE id = %s
            """, (nombre, codigo_ean, producto_id))
        else:
            cursor.execute("""
                UPDATE productos_catalogo 
                SET nombre_producto = %s, verificado = TRUE
                WHERE id = %s
            """, (nombre, producto_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Producto actualizado", "id": producto_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/productos/{producto_id}")
async def eliminar_producto_catalogo(producto_id: int):
    """Eliminar un producto del catálogo y todos sus precios"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM precios_productos WHERE producto_id = %s", (producto_id,))
        cursor.execute("DELETE FROM productos_catalogo WHERE id = %s", (producto_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Producto eliminado", "id": producto_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# admin_dashboard.py - AGREGAR ESTOS ENDPOINTS

@router.get("/productos/{producto_id}/historico-precios")
async def obtener_historico_precios(producto_id: int):
    """Ver histórico de precios de un producto por establecimiento"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                pp.precio,
                pp.establecimiento,
                pp.cadena,
                pp.fecha_reporte,
                f.id as factura_id
            FROM precios_productos pp
            JOIN facturas f ON pp.factura_id = f.id
            WHERE pp.producto_id = %s
            ORDER BY pp.fecha_reporte DESC
        """, (producto_id,))
        
        precios = []
        for row in cursor.fetchall():
            precios.append({
                "precio": float(row[0]),
                "establecimiento": row[1],
                "cadena": row[2],
                "fecha": str(row[3]),
                "factura_id": row[4]
            })
        
        cursor.close()
        conn.close()
        
        return {"precios": precios, "total": len(precios)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alertas/cambios-precio")
async def detectar_cambios_precio(dias: int = 7):
    """Detectar cambios significativos de precio en los últimos N días"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener precios recientes y compararlos con histórico
        cursor.execute("""
            WITH precios_recientes AS (
                SELECT 
                    pp.producto_id,
                    pc.nombre_producto,
                    pc.codigo_ean,
                    pp.establecimiento,
                    pp.precio as precio_actual,
                    pp.fecha_reporte,
                    ROW_NUMBER() OVER (PARTITION BY pp.producto_id, pp.establecimiento ORDER BY pp.fecha_reporte DESC) as rn
                FROM precios_productos pp
                JOIN productos_catalogo pc ON pp.producto_id = pc.id
                WHERE pp.fecha_reporte >= NOW() - INTERVAL '%s days'
            ),
            precios_historicos AS (
                SELECT 
                    pp.producto_id,
                    pp.establecimiento,
                    AVG(pp.precio) as precio_promedio,
                    MIN(pp.precio) as precio_minimo,
                    MAX(pp.precio) as precio_maximo
                FROM precios_productos pp
                WHERE pp.fecha_reporte < NOW() - INTERVAL '%s days'
                GROUP BY pp.producto_id, pp.establecimiento
            )
            SELECT 
                pr.producto_id,
                pr.nombre_producto,
                pr.codigo_ean,
                pr.establecimiento,
                pr.precio_actual,
                ph.precio_promedio,
                ph.precio_minimo,
                ph.precio_maximo,
                pr.fecha_reporte,
                CASE 
                    WHEN pr.precio_actual > ph.precio_maximo THEN 'MAXIMO_HISTORICO'
                    WHEN pr.precio_actual < ph.precio_minimo THEN 'MINIMO_HISTORICO'
                    WHEN ABS(pr.precio_actual - ph.precio_promedio) / ph.precio_promedio > 0.20 THEN 'CAMBIO_SIGNIFICATIVO'
                    ELSE 'NORMAL'
                END as tipo_alerta
            FROM precios_recientes pr
            LEFT JOIN precios_historicos ph ON pr.producto_id = ph.producto_id 
                AND pr.establecimiento = ph.establecimiento
            WHERE pr.rn = 1 
                AND ph.precio_promedio IS NOT NULL
                AND (
                    pr.precio_actual > ph.precio_maximo 
                    OR pr.precio_actual < ph.precio_minimo
                    OR ABS(pr.precio_actual - ph.precio_promedio) / ph.precio_promedio > 0.20
                )
            ORDER BY pr.fecha_reporte DESC
        """ % (dias, dias))
        
        alertas = []
        for row in cursor.fetchall():
            cambio_porcentaje = ((row[4] - row[5]) / row[5] * 100) if row[5] else 0
            
            alertas.append({
                "producto_id": row[0],
                "nombre": row[1],
                "codigo": row[2],
                "establecimiento": row[3],
                "precio_actual": float(row[4]),
                "precio_promedio": float(row[5]) if row[5] else 0,
                "precio_min": float(row[6]) if row[6] else 0,
                "precio_max": float(row[7]) if row[7] else 0,
                "fecha": str(row[8]),
                "tipo_alerta": row[9],
                "cambio_porcentaje": round(cambio_porcentaje, 1)
            })
        
        cursor.close()
        conn.close()
        
        return {"alertas": alertas, "total": len(alertas)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/{producto_id}/comparar-establecimientos")
async def comparar_precios_establecimientos(producto_id: int):
    """Comparar precio actual de un producto en diferentes establecimientos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener el precio más reciente por establecimiento
        cursor.execute("""
            WITH ultimos_precios AS (
                SELECT 
                    establecimiento,
                    cadena,
                    precio,
                    fecha_reporte,
                    ROW_NUMBER() OVER (PARTITION BY establecimiento ORDER BY fecha_reporte DESC) as rn
                FROM precios_productos
                WHERE producto_id = %s
            )
            SELECT establecimiento, cadena, precio, fecha_reporte
            FROM ultimos_precios
            WHERE rn = 1
            ORDER BY precio ASC
        """, (producto_id,))
        
        comparacion = []
        precios = []
        
        for row in cursor.fetchall():
            precio = float(row[2])
            precios.append(precio)
            comparacion.append({
                "establecimiento": row[0],
                "cadena": row[1],
                "precio": precio,
                "fecha": str(row[3])
            })
        
        # Calcular estadísticas
        if precios:
            precio_min = min(precios)
            precio_max = max(precios)
            ahorro_maximo = precio_max - precio_min
            
            # Marcar el más barato y el más caro
            for item in comparacion:
                if item["precio"] == precio_min:
                    item["es_mas_barato"] = True
                if item["precio"] == precio_max:
                    item["es_mas_caro"] = True
        else:
            ahorro_maximo = 0
        
        cursor.close()
        conn.close()
        
        return {
            "producto_id": producto_id,
            "comparacion": comparacion,
            "ahorro_maximo": ahorro_maximo,
            "total_establecimientos": len(comparacion)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/precios/{precio_id}/validar")
async def validar_precio_inusual(precio_id: int, es_correcto: bool, observaciones: str = ""):
    """Validar un precio que parece inusual"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Registrar la validación
        cursor.execute("""
            INSERT INTO validaciones_facturas 
            (factura_id, campo_validado, valor_original, valor_corregido, usuario_validador, tipo_correccion)
            SELECT 
                factura_id,
                'precio_validado',
                precio::TEXT,
                %s,
                'admin',
                'validacion_precio'
            FROM precios_productos
            WHERE id = %s
        """, (observaciones if observaciones else 'precio_correcto', precio_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Precio validado", "es_correcto": es_correcto}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
