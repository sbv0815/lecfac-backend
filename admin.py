from fastapi import APIRouter, HTTPException
from fastapi.responses import Response
from typing import Optional
from database import get_db_connection
from storage import get_image_from_db
import os

router = APIRouter(prefix="/admin", tags=["admin"])

@router.get("/stats")
async def estadisticas_dashboard():
    """Estadísticas generales del sistema"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM facturas")
    total_facturas = cursor.fetchone()[0]
    
    # Consultar la tabla correcta que se está usando
    cursor.execute("SELECT COUNT(*) FROM productos_catalogo")
    total_productos_catalogo = cursor.fetchone()[0]
    
    # También contar productos legacy por factura
    cursor.execute("SELECT COUNT(DISTINCT codigo) FROM productos WHERE codigo IS NOT NULL")
    total_productos_legacy = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM facturas WHERE imagen_data IS NOT NULL")
    facturas_con_imagen = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    total_usuarios = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_facturas": total_facturas,
        "productos_catalogo": total_productos_catalogo,
        "productos_unicos_legacy": total_productos_legacy,
        "facturas_con_imagen": facturas_con_imagen,
        "total_usuarios": total_usuarios,
        "porcentaje_imagenes": round((facturas_con_imagen / total_facturas * 100) if total_facturas > 0 else 0, 2)
    }

@router.get("/facturas")
async def listar_facturas(limit: int = 50, offset: int = 0):
    """Lista todas las facturas"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            f.id, f.usuario_id, u.email, f.establecimiento, f.cadena,
            f.total_factura, f.productos_detectados, f.productos_guardados,
            f.fecha_cargue, f.estado,
            CASE WHEN f.imagen_data IS NOT NULL THEN true ELSE false END as tiene_imagen
        FROM facturas f
        JOIN usuarios u ON f.usuario_id = u.id
        ORDER BY f.fecha_cargue DESC
        LIMIT %s OFFSET %s
    """, (limit, offset))
    
    facturas = cursor.fetchall()
    conn.close()
    
    return {
        "total": len(facturas),
        "facturas": [
            {
                "id": f[0],
                "usuario_id": f[1],
                "usuario_email": f[2],
                "establecimiento": f[3],
                "cadena": f[4],
                "total": f[5],
                "productos_detectados": f[6],
                "productos_guardados": f[7],
                "fecha_cargue": str(f[8]),
                "estado": f[9],
                "tiene_imagen": f[10]
            } for f in facturas
        ]
    }

@router.get("/facturas/{factura_id}")
async def detalle_factura(factura_id: int):
    """Detalle de una factura con productos"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            f.id, f.usuario_id, u.email, f.establecimiento, f.cadena,
            f.total_factura, f.productos_detectados, f.productos_guardados,
            f.fecha_cargue, f.estado,
            CASE WHEN f.imagen_data IS NOT NULL THEN true ELSE false END as tiene_imagen
        FROM facturas f
        JOIN usuarios u ON f.usuario_id = u.id
        WHERE f.id = %s
    """, (factura_id,))
    
    factura = cursor.fetchone()
    if not factura:
        conn.close()
        raise HTTPException(404, "Factura no encontrada")
    
    cursor.execute("""
        SELECT id, codigo, nombre, valor
        FROM productos
        WHERE factura_id = %s
        ORDER BY id
    """, (factura_id,))
    
    productos = cursor.fetchall()
    conn.close()
    
    return {
        "factura": {
            "id": factura[0],
            "usuario_id": factura[1],
            "usuario_email": factura[2],
            "establecimiento": factura[3],
            "cadena": factura[4],
            "total": factura[5],
            "productos_detectados": factura[6],
            "productos_guardados": factura[7],
            "fecha_cargue": str(factura[8]),
            "estado": factura[9],
            "tiene_imagen": factura[10]
        },
        "productos": [
            {"id": p[0], "codigo": p[1], "nombre": p[2], "valor": p[3]}
            for p in productos
        ]
    }

@router.get("/facturas/{factura_id}/imagen")
async def obtener_imagen_factura(factura_id: int):
    """Devuelve la imagen de una factura"""
    image_data, mime_type = get_image_from_db(factura_id)
    
    if not image_data:
        raise HTTPException(404, "Imagen no encontrada")
    
    return Response(content=image_data, media_type=mime_type or "image/jpeg")
