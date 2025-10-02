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
