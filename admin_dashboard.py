# admin_dashboard.py - AGREGAR AL FINAL

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
        
        # Obtener últimos precios y compararlos con histórico
        cursor.execute("""
            SELECT DISTINCT ON (pp.producto_id, pp.establecimiento)
                pp.id,
                pp.producto_id,
                pc.nombre_producto,
                pc.codigo_ean,
                pp.establecimiento,
                pp.cadena,
                pp.precio,
                pp.fecha_reporte
            FROM precios_productos pp
            JOIN productos_catalogo pc ON pp.producto_id = pc.id
            ORDER BY pp.producto_id, pp.establecimiento, pp.fecha_reporte DESC
        """)
        
        productos_actuales = {}
        for row in cursor.fetchall():
            key = f"{row[1]}_{row[4]}"  # producto_id_establecimiento
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
        
        # Obtener histórico para comparar
        cursor.execute("""
            SELECT 
                pp.producto_id,
                pp.establecimiento,
                AVG(pp.precio) as precio_promedio,
                MIN(pp.precio) as precio_minimo,
                MAX(pp.precio) as precio_maximo,
                COUNT(*) as num_registros
            FROM precios_productos pp
            GROUP BY pp.producto_id, pp.establecimiento
            HAVING COUNT(*) > 1
        """)
        
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
        
        # Detectar anomalías
        alertas = []
        for key, actual in productos_actuales.items():
            if key not in historicos:
                continue
            
            hist = historicos[key]
            precio = actual["precio_actual"]
            
            # Solo si hay suficiente histórico
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


@router.get("/productos/{producto_id}/comparar-establecimientos")
async def comparar_precios_establecimientos(producto_id: int):
    """Comparar precio actual de un producto en diferentes establecimientos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener nombre del producto
        cursor.execute("""
            SELECT nombre_producto, codigo_ean 
            FROM productos_catalogo 
            WHERE id = %s
        """, (producto_id,))
        
        prod_info = cursor.fetchone()
        if not prod_info:
            raise HTTPException(status_code=404, detail="Producto no encontrado")
        
        # Obtener el precio más reciente por establecimiento
        cursor.execute("""
            SELECT DISTINCT ON (establecimiento)
                establecimiento,
                cadena,
                precio,
                fecha_reporte
            FROM precios_productos
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
        
        # Calcular estadísticas
        ahorro_maximo = 0
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


@router.get("/tendencias/precio")
async def obtener_tendencias_precio(dias: int = 30):
    """Obtener productos con mayor variación de precio"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                pc.id,
                pc.nombre_producto,
                pc.codigo_ean,
                pp.establecimiento,
                MIN(pp.precio) as precio_min,
                MAX(pp.precio) as precio_max,
                AVG(pp.precio) as precio_promedio,
                COUNT(*) as num_registros,
                (MAX(pp.precio) - MIN(pp.precio)) as variacion
            FROM productos_catalogo pc
            JOIN precios_productos pp ON pc.id = pp.producto_id
            WHERE pp.fecha_reporte >= NOW() - INTERVAL '%s days'
            GROUP BY pc.id, pc.nombre_producto, pc.codigo_ean, pp.establecimiento
            HAVING COUNT(*) > 2
            ORDER BY variacion DESC
            LIMIT 20
        """ % dias)
        
        tendencias = []
        for row in cursor.fetchall():
            variacion_porcentaje = ((row[8] / row[6]) * 100) if row[6] > 0 else 0
            
            tendencias.append({
                "producto_id": row[0],
                "nombre": row[1],
                "codigo": row[2],
                "establecimiento": row[3],
                "precio_min": float(row[4]),
                "precio_max": float(row[5]),
                "precio_promedio": float(row[6]),
                "num_registros": row[7],
                "variacion": float(row[8]),
                "variacion_porcentaje": round(variacion_porcentaje, 1)
            })
        
        cursor.close()
        conn.close()
        
        return {"tendencias": tendencias, "total": len(tendencias)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
