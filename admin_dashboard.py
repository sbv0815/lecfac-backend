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
        
        # Corrección: Usar los nombres de columnas correctos según database.py
        cursor.execute("""
            SELECT 
                p.id,
                p.codigo_ean,
                p.nombre,
                p.veces_reportado,
                COALESCE(p.verificado, FALSE) as verificado,
                COALESCE(p.necesita_revision, FALSE) as necesita_revision,
                COUNT(DISTINCT pp.factura_id) as num_facturas,
                MIN(pp.precio) as precio_min,
                MAX(pp.precio) as precio_max,
                AVG(pp.precio) as precio_promedio
            FROM productos_maestro p
            LEFT JOIN precios_productos pp ON p.id = pp.producto_id
            GROUP BY p.id, p.codigo_ean, p.nombre, p.veces_reportado, p.verificado, p.necesita_revision
            ORDER BY p.veces_reportado DESC
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
async def detectar_facturas_duplicadas(criterio: str = "all"):
    """Detectar facturas potencialmente duplicadas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id, establecimiento, total_factura, fecha_cargue,
                (SELECT COUNT(*) FROM productos WHERE factura_id = facturas.id) as num_productos
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
                # Aplicar criterio de filtro
                if criterio != "all":
                    if criterio == "same_establishment" and f1["establecimiento"] != f2["establecimiento"]:
                        continue
                    if criterio == "same_date" and f1["fecha"][:10] != f2["fecha"][:10]:  # Comparar solo la fecha, no la hora
                        continue
                    if criterio == "same_total" and abs(f1["total"] - f2["total"]) > 100:
                        continue
                
                # Si pasa el criterio o es "all", verificar si es duplicado
                if (f1["establecimiento"] == f2["establecimiento"] and 
                    (abs(f1["total"] - f2["total"]) < 100 or f1["fecha"][:10] == f2["fecha"][:10])):
                    
                    # Determinar criterios cumplidos
                    mismo_establecimiento = f1["establecimiento"] == f2["establecimiento"]
                    misma_fecha = f1["fecha"][:10] == f2["fecha"][:10]
                    total_iguales = abs(f1["total"] - f2["total"]) < 100
                    
                    # Calcular similitud estimada
                    similitud = 0
                    if mismo_establecimiento: similitud += 30
                    if misma_fecha: similitud += 30
                    if total_iguales: similitud += 30
                    
                    # Generar razón
                    razones = []
                    if mismo_establecimiento: razones.append("Mismo establecimiento")
                    if misma_fecha: razones.append("Misma fecha")
                    if total_iguales: razones.append("Total similar")
                    razon = ", ".join(razones)
                    
                    duplicados.append({
                        "id": str(len(duplicados)),
                        "factura1": f1,
                        "factura2": f2,
                        "razon": razon,
                        "similitud": similitud,
                        "misma_fecha": misma_fecha,
                        "mismo_establecimiento": mismo_establecimiento,
                        "total_iguales": total_iguales
                    })
        
        cursor.close()
        conn.close()
        
        return {"duplicados": duplicados, "total": len(duplicados)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def similitud_texto(a: str, b: str) -> float:
    """Calcular similitud entre dos strings (0-100)"""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() * 100


@router.get("/duplicados/productos")
async def detectar_productos_duplicados(umbral: float = 85.0, criterio: str = "todos"):
    """Detectar productos con nombres similares considerando múltiples criterios"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # CORRECCIÓN: Usar los nombres correctos de columnas según database.py
        cursor.execute("""
            SELECT id, codigo_ean, nombre, veces_reportado, precio_promedio, ultima_actualizacion
            FROM productos_maestro
            ORDER BY veces_reportado DESC
        """)
        
        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo": row[1],
                "nombre": row[2],
                "veces_visto": row[3] or 0,
                "precio": row[4],
                "ultima_actualizacion": row[5]
            })
        
        duplicados = []
        for i, p1 in enumerate(productos):
            for p2 in productos[i+1:]:
                # Inicializar valores
                mismo_codigo = False
                mismo_establecimiento = True  # Simplificado para esta versión
                nombre_similar = False
                
                # REGLA 1: Si ambos tienen código EAN, solo son duplicados si son el mismo código
                if p1["codigo"] and p2["codigo"]:
                    if p1["codigo"] != p2["codigo"]:
                        continue  # No son duplicados, saltar a la siguiente iteración
                    mismo_codigo = True
                
                # REGLA 2: Si no coinciden por código, verificar similitud de nombres
                if not mismo_codigo and p1["nombre"] and p2["nombre"]:
                    sim = similitud_texto(p1["nombre"], p2["nombre"])
                    
                    # Si la similitud es alta, considerar nombre similar
                    if sim >= umbral:
                        nombre_similar = True
                    else:
                        continue  # No son suficientemente similares
                
                # REGLA 3: Aplicar filtros según criterio seleccionado
                if criterio != "todos":
                    if criterio == "codigo" and not mismo_codigo:
                        continue
                    if criterio == "nombre" and not nombre_similar:
                        continue
                    if criterio == "establecimiento" and not mismo_establecimiento:
                        continue
                
                # Si llegamos aquí, son potenciales duplicados
                # Calcular valor de similitud para mostrar
                if mismo_codigo:
                    similitud_valor = 100  # 100% de similitud si tienen el mismo código
                else:
                    similitud_valor = similitud_texto(p1["nombre"], p2["nombre"])
                
                # Crear razón descriptiva
                razones = []
                if mismo_codigo:
                    razones.append("Mismo código EAN")
                if nombre_similar:
                    razones.append("Nombres similares")
                if mismo_establecimiento:
                    razones.append("Mismo establecimiento")
                
                # Crear entrada de duplicado
                duplicados.append({
                    "id": str(len(duplicados)),  # ID único para este duplicado
                    "producto1": {
                        "id": p1["id"],
                        "nombre": p1["nombre"],
                        "codigo": p1["codigo"],
                        "establecimiento": "Desconocido",  # Simplificado para esta versión
                        "precio": p1["precio"],
                        "ultima_actualizacion": p1["ultima_actualizacion"],
                        "veces_visto": p1["veces_visto"]
                    },
                    "producto2": {
                        "id": p2["id"],
                        "nombre": p2["nombre"],
                        "codigo": p2["codigo"],
                        "establecimiento": "Desconocido",  # Simplificado para esta versión
                        "precio": p2["precio"],
                        "ultima_actualizacion": p2["ultima_actualizacion"],
                        "veces_visto": p2["veces_visto"]
                    },
                    "similitud": round(similitud_valor, 1),
                    "mismo_codigo": mismo_codigo,
                    "mismo_establecimiento": mismo_establecimiento,
                    "nombre_similar": nombre_similar,
                    "razon": ", ".join(razones) if razones else "Posibles duplicados"
                })
        
        cursor.close()
        conn.close()
        
        return {"duplicados": duplicados, "total": len(duplicados)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error detectando duplicados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/duplicados/productos/fusionar")
async def fusionar_productos(request: dict):
    """Fusionar dos productos duplicados"""
    try:
        producto_mantener_id = request.get("producto_mantener_id")
        producto_eliminar_id = request.get("producto_eliminar_id")
        
        if not producto_mantener_id or not producto_eliminar_id:
            raise HTTPException(status_code=400, detail="Se requieren ambos IDs: producto a mantener y producto a eliminar")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que los productos existen
        cursor.execute("SELECT * FROM productos_maestro WHERE id = %s", (producto_mantener_id,))
        producto_mantener = cursor.fetchone()
        
        cursor.execute("SELECT * FROM productos_maestro WHERE id = %s", (producto_eliminar_id,))
        producto_eliminar = cursor.fetchone()
        
        if not producto_mantener or not producto_eliminar:
            conn.close()
            raise HTTPException(status_code=404, detail="Uno o ambos productos no existen")
        
        # Verificar códigos EAN
        if producto_mantener[1] and producto_eliminar[1] and producto_mantener[1] != producto_eliminar[1]:
            conn.close()
            raise HTTPException(status_code=400, detail="No se pueden fusionar productos con códigos EAN diferentes")
        
        # Actualizar referencias en precios
        cursor.execute("""
            UPDATE precios_historicos
            SET producto_id = %s
            WHERE producto_id = %s
        """, (producto_mantener_id, producto_eliminar_id))
        
        # Actualizar referencias en historial de compras
        cursor.execute("""
            UPDATE historial_compras_usuario
            SET producto_id = %s
            WHERE producto_id = %s
        """, (producto_mantener_id, producto_eliminar_id))
        
        # Actualizar referencias en patrones de compra
        cursor.execute("""
            UPDATE patrones_compra
            SET producto_id = %s
            WHERE producto_id = %s
        """, (producto_mantener_id, producto_eliminar_id))
        
        # Actualizar estadísticas
        cursor.execute("""
            UPDATE productos_maestro
            SET veces_reportado = veces_reportado + %s,
                ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (producto_eliminar[6] or 0, producto_mantener_id))
        
        # Eliminar producto duplicado
        cursor.execute("DELETE FROM productos_maestro WHERE id = %s", (producto_eliminar_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "Productos fusionados exitosamente",
            "producto_mantener_id": producto_mantener_id,
            "producto_eliminar_id": producto_eliminar_id
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/productos/{producto_id}/editar")
async def editar_producto_catalogo(producto_id: int, request: dict):
    """Editar nombre y código de un producto del catálogo"""
    try:
        nombre = request.get("nombre")
        codigo_ean = request.get("codigo_ean")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # CORRECCIÓN: Usar nombre en lugar de nombre_producto
        if codigo_ean:
            cursor.execute("""
                UPDATE productos_maestro 
                SET nombre = %s, codigo_ean = %s, verificado = TRUE
                WHERE id = %s
            """, (nombre, codigo_ean, producto_id))
        else:
            cursor.execute("""
                UPDATE productos_maestro 
                SET nombre = %s, verificado = TRUE
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
        
        # Eliminar referencias en precios históricos
        cursor.execute("DELETE FROM precios_historicos WHERE producto_id = %s", (producto_id,))
        
        # Eliminar producto
        cursor.execute("DELETE FROM productos_maestro WHERE id = %s", (producto_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {"message": "Producto eliminado", "id": producto_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/{producto_id}/comparar-establecimientos")
async def comparar_precios_establecimientos(producto_id: int):
    """Comparar precio actual de un producto en diferentes establecimientos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # CORRECCIÓN: Usar nombre en lugar de nombre_producto
        cursor.execute("""
            SELECT nombre, codigo_ean 
            FROM productos_maestro 
            WHERE id = %s
        """, (producto_id,))
        
        prod_info = cursor.fetchone()
        if not prod_info:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        cursor.execute("""
            SELECT DISTINCT ON (establecimiento)
                establecimiento,
                cadena,
                precio,
                fecha_reporte
            FROM precios_historicos
            WHERE producto_id = %s
            ORDER BY establecimiento, fecha_reporte DESC
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
                "fecha": str(row[3]),
                "es_mas_barato": False,
                "es_mas_caro": False
            })
        
        ahorro_maximo = 0
        if precios:
            precio_min = min(precios)
            precio_max = max(precios)
            ahorro_maximo = precio_max - precio_min
            
            for item in comparacion:
                if item["precio"] == precio_min:
                    item["es_mas_barato"] = True
                if item["precio"] == precio_max:
                    item["es_mas_caro"] = True
        
        cursor.close()
        conn.close()
        
        return {
            "producto_id": producto_id,
            "nombre": prod_info[0],
            "codigo": prod_info[1],
            "comparacion": sorted(comparacion, key=lambda x: x["precio"]),
            "ahorro_maximo": ahorro_maximo,
            "total_establecimientos": len(comparacion)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alertas/cambios-precio")
async def detectar_cambios_precio(dias: int = 7):
    """Detectar cambios significativos de precio en los últimos N días"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # CORRECCIÓN: Usar productos_maestro y nombre
        cursor.execute("""
            SELECT DISTINCT ON (ph.producto_id, ph.establecimiento)
                ph.id,
                ph.producto_id,
                pm.nombre,
                pm.codigo_ean,
                ph.establecimiento,
                ph.cadena,
                ph.precio,
                ph.fecha_reporte
            FROM precios_historicos ph
            JOIN productos_maestro pm ON ph.producto_id = pm.id
            ORDER BY ph.producto_id, ph.establecimiento, ph.fecha_reporte DESC
        """)
        
        productos_actuales = {}
        for row in cursor.fetchall():
            key = f"{row[1]}_{row[4]}"
            productos_actuales[key] = {
                "id": row[0],
                "producto_id": row[1],
                "nombre": row[2],
                "codigo": row[3],
                "establecimiento": row[4],
                "cadena": row[5],
                "precio_actual": float(row[6]),
                "fecha": str(row[7])
            }
        
        cursor.execute("""
            SELECT 
                producto_id,
                establecimiento,
                AVG(precio) as precio_promedio,
                MIN(precio) as precio_minimo,
                MAX(precio) as precio_maximo,
                COUNT(*) as num_registros
            FROM precios_historicos
            WHERE fecha_reporte >= CURRENT_TIMESTAMP - INTERVAL '%s days'
            GROUP BY producto_id, establecimiento
            HAVING COUNT(*) > 1
        """, (dias,))
        
        historicos = {}
        for row in cursor.fetchall():
            key = f"{row[0]}_{row[1]}"
            historicos[key] = {
                "precio_promedio": float(row[2]),
                "precio_minimo": float(row[3]),
                "precio_maximo": float(row[4]),
                "num_registros": row[5]
            }
        
        cursor.close()
        conn.close()
        
        alertas = []
        for key, actual in productos_actuales.items():
            if key not in historicos:
                continue
            
            hist = historicos[key]
            precio = actual["precio_actual"]
            
            if hist["num_registros"] < 2:
                continue
            
            tipo_alerta = None
            cambio_porcentaje = 0
            
            if precio > hist["precio_maximo"]:
                tipo_alerta = "MÁXIMO_HISTÓRICO"
                cambio_porcentaje = ((precio - hist["precio_promedio"]) / hist["precio_promedio"]) * 100
            elif precio < hist["precio_minimo"]:
                tipo_alerta = "MÍNIMO_HISTÓRICO"
                cambio_porcentaje = ((precio - hist["precio_promedio"]) / hist["precio_promedio"]) * 100
            elif abs(precio - hist["precio_promedio"]) / hist["precio_promedio"] > 0.20:
                tipo_alerta = "CAMBIO_SIGNIFICATIVO"
                cambio_porcentaje = ((precio - hist["precio_promedio"]) / hist["precio_promedio"]) * 100
            
            if tipo_alerta:
                alertas.append({
                    **actual,
                    "precio_promedio": hist["precio_promedio"],
                    "precio_min": hist["precio_minimo"],
                    "precio_max": hist["precio_maximo"],
                    "tipo_alerta": tipo_alerta,
                    "cambio_porcentaje": round(cambio_porcentaje, 1)
                })
        
        return {"alertas": alertas, "total": len(alertas)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint de diagnóstico para ayudar en la depuración
@router.get("/diagnostico")
async def diagnostico():
    """Obtener información de diagnóstico sobre la base de datos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Información de la estructura de la tabla productos_maestro
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'productos_maestro' ORDER BY ordinal_position")
        columnas_maestro = [row[0] for row in cursor.fetchall()]
        
        # Conteo de registros en tablas principales
        cursor.execute("SELECT COUNT(*) FROM productos_maestro")
        count_maestro = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM precios_historicos")
        count_precios = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM facturas")
        count_facturas = cursor.fetchone()[0]
        
        # Muestra de datos
        cursor.execute("SELECT id, codigo_ean, nombre, veces_reportado FROM productos_maestro LIMIT 5")
        muestra_productos = []
        for row in cursor.fetchall():
            muestra_productos.append({
                "id": row[0],
                "codigo": row[1],
                "nombre": row[2],
                "veces_visto": row[3]
            })
        
        conn.close()
        
        return {
            "estado": "ok",
            "estructura_productos_maestro": columnas_maestro,
            "conteo": {
                "productos_maestro": count_maestro,
                "precios_historicos": count_precios,
                "facturas": count_facturas
            },
            "muestra_productos": muestra_productos
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "estado": "error",
            "error": str(e)
        }


# Endpoint para estadísticas
@router.get("/stats")
async def estadisticas():
    """Obtener estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Productos únicos
        cursor.execute("SELECT COUNT(*) FROM productos_maestro")
        productos_unicos = cursor.fetchone()[0]
        
        # Facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        # Precios registrados
        cursor.execute("SELECT COUNT(*) FROM precios_historicos")
        total_precios = cursor.fetchone()[0]
        
        # Establecimientos únicos
        cursor.execute("SELECT COUNT(DISTINCT establecimiento) FROM precios_historicos")
        establecimientos = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "productos_unicos": productos_unicos,
            "total_facturas": total_facturas, 
            "total_precios": total_precios,
            "establecimientos": establecimientos
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
