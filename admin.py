from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import HTMLResponse, Response
from typing import Optional
from database import get_db_connection
import os

router = APIRouter(prefix="/admin", tags=["admin"])

# ===== ENDPOINTS DE LECTURA =====

@router.get("/facturas")
async def listar_facturas(
    limit: int = 50,
    offset: int = 0,
    usuario_id: Optional[int] = None
):
    """Lista todas las facturas con metadata básica"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT 
            f.id,
            f.usuario_id,
            u.email as usuario_email,
            f.establecimiento,
            f.cadena,
            f.total_factura,
            f.productos_detectados,
            f.productos_guardados,
            f.fecha_cargue,
            f.estado,
            CASE WHEN f.imagen_data IS NOT NULL THEN true ELSE false END as tiene_imagen
        FROM facturas f
        JOIN usuarios u ON f.usuario_id = u.id
    """
    
    params = []
    if usuario_id:
        query += " WHERE f.usuario_id = %s"
        params.append(usuario_id)
    
    query += " ORDER BY f.fecha_cargue DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    
    cursor.execute(query, params)
    facturas = cursor.fetchall()
    
    result = []
    for f in facturas:
        result.append({
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
        })
    
    conn.close()
    
    return {
        "total": len(result),
        "facturas": result
    }

@router.get("/facturas/{factura_id}")
async def detalle_factura(factura_id: int):
    """Detalle completo de una factura con sus productos"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Datos de la factura
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
    
    # Productos de la factura
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
            {
                "id": p[0],
                "codigo": p[1],
                "nombre": p[2],
                "valor": p[3]
            } for p in productos
        ]
    }

@router.get("/productos")
async def listar_productos_catalogo(
    search: Optional[str] = None,
    limit: int = 100
):
    """Lista productos del catálogo maestro"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if search:
        cursor.execute("""
            SELECT id, codigo_ean, nombre, precio_promedio, veces_reportado
            FROM productos_maestro
            WHERE nombre ILIKE %s OR codigo_ean LIKE %s
            ORDER BY veces_reportado DESC
            LIMIT %s
        """, (f"%{search}%", f"%{search}%", limit))
    else:
        cursor.execute("""
            SELECT id, codigo_ean, nombre, precio_promedio, veces_reportado
            FROM productos_maestro
            ORDER BY veces_reportado DESC
            LIMIT %s
        """, (limit,))
    
    productos = cursor.fetchall()
    conn.close()
    
    return {
        "total": len(productos),
        "productos": [
            {
                "id": p[0],
                "codigo_ean": p[1],
                "nombre": p[2],
                "precio_promedio": p[3],
                "veces_reportado": p[4]
            } for p in productos
        ]
    }

@router.get("/stats")
async def estadisticas_dashboard():
    """Estadísticas generales del sistema"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Total de facturas
    cursor.execute("SELECT COUNT(*) FROM facturas")
    total_facturas = cursor.fetchone()[0]
    
    # Total productos únicos
    cursor.execute("SELECT COUNT(*) FROM productos_maestro")
    total_productos = cursor.fetchone()[0]
    
    # Facturas con imagen
    cursor.execute("SELECT COUNT(*) FROM facturas WHERE imagen_data IS NOT NULL")
    facturas_con_imagen = cursor.fetchone()[0]
    
    # Total usuarios
    cursor.execute("SELECT COUNT(*) FROM usuarios")
    total_usuarios = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        "total_facturas": total_facturas,
        "total_productos_unicos": total_productos,
        "facturas_con_imagen": facturas_con_imagen,
        "total_usuarios": total_usuarios,
        "porcentaje_cobertura_imagenes": round((facturas_con_imagen / total_facturas * 100) if total_facturas > 0 else 0, 2)
    }
