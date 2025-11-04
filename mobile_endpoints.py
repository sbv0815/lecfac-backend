"""
mobile_endpoints.py - VERSIÓN UNIFICADA Y COMPLETA
Endpoints para la aplicación móvil Flutter
"""

import os
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Header, HTTPException, UploadFile, File
from pydantic import BaseModel
from database import get_db_connection

router = APIRouter(prefix="/api/mobile", tags=["mobile"])


# ==========================================
# FUNCIÓN AUXILIAR DE AUTENTICACIÓN
# ==========================================

def get_user_id_from_mobile_token(authorization: Optional[str] = None) -> int:
    """
    Extraer usuario_id desde el token para endpoints móviles

    Si no hay token, retorna usuario por defecto (para desarrollo)
    En producción, debería validar el token JWT
    """
    if not authorization:
        print("⚠️ [MOBILE] No hay token, usando usuario_id = 1 por defecto")
        return 1

    try:
        import jwt
        token = authorization.replace("Bearer ", "")

        # Decodificar SIN verificar signature (para desarrollo)
        payload = jwt.decode(token, options={"verify_signature": False})

        # Intentar obtener user_id
        usuario_id = payload.get("user_id") or payload.get("sub") or payload.get("id")

        if usuario_id:
            return int(usuario_id)
        else:
            print("⚠️ [MOBILE] No se encontró user_id en token, usando 1")
            return 1

    except Exception as e:
        print(f"⚠️ [MOBILE] Error decodificando token: {e}")
        return 1


# ==========================================
# MODELOS PYDANTIC
# ==========================================

class EstadisticasUsuario(BaseModel):
    total_facturas: int
    total_gastado: float
    productos_unicos: int
    promedio_por_factura: float
    establecimiento_favorito: Optional[str]
    gasto_mes_actual: float


# ==========================================
# ENDPOINTS
# ==========================================

@router.get("/my-stats")
async def get_my_stats(authorization: Optional[str] = Header(None)):
    """
    📊 Obtener estadísticas del usuario autenticado

    Retorna:
    - Total de facturas escaneadas
    - Total gastado (histórico)
    - Gasto del mes actual
    - Productos únicos en inventario
    - Promedio por factura
    - Establecimiento favorito
    """
    usuario_id = get_user_id_from_mobile_token(authorization)

    print(f"📊 [MOBILE] Obteniendo estadísticas para usuario {usuario_id}")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Total de facturas
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM facturas WHERE usuario_id = %s
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM facturas WHERE usuario_id = ?
            """, (usuario_id,))

        total_facturas = cursor.fetchone()[0] or 0

        # 2. Total gastado (histórico)
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
            """, (usuario_id,))

        total_gastado = float(cursor.fetchone()[0] or 0)

        # 3. Gasto del mes actual
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
                AND EXTRACT(MONTH FROM fecha_cargue) = EXTRACT(MONTH FROM CURRENT_DATE)
                AND EXTRACT(YEAR FROM fecha_cargue) = EXTRACT(YEAR FROM CURRENT_DATE)
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                AND strftime('%m', fecha_cargue) = strftime('%m', 'now')
                AND strftime('%Y', fecha_cargue) = strftime('%Y', 'now')
            """, (usuario_id,))

        gasto_mes = float(cursor.fetchone()[0] or 0)

        # 4. Productos únicos en inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = %s AND producto_maestro_id IS NOT NULL
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = ? AND producto_maestro_id IS NOT NULL
            """, (usuario_id,))

        productos_unicos = cursor.fetchone()[0] or 0

        # 5. Promedio por factura
        promedio = total_gastado / total_facturas if total_facturas > 0 else 0

        # 6. Establecimiento favorito
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT establecimiento, COUNT(*) as visitas
                FROM facturas
                WHERE usuario_id = %s AND establecimiento IS NOT NULL
                GROUP BY establecimiento
                ORDER BY visitas DESC
                LIMIT 1
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT establecimiento, COUNT(*) as visitas
                FROM facturas
                WHERE usuario_id = ? AND establecimiento IS NOT NULL
                GROUP BY establecimiento
                ORDER BY visitas DESC
                LIMIT 1
            """, (usuario_id,))

        establecimiento_row = cursor.fetchone()
        establecimiento_favorito = establecimiento_row[0] if establecimiento_row else None

        conn.close()

        print(f"✅ [MOBILE] Estadísticas: {total_facturas} facturas, ${total_gastado:,.0f} gastado")

        return {
            "success": True,
            "stats": {
                "total_facturas": total_facturas,
                "total_gastado": total_gastado,
                "gasto_mes_actual": gasto_mes,
                "productos_unicos": productos_unicos,
                "promedio_por_factura": promedio,
                "establecimiento_favorito": establecimiento_favorito
            }
        }

    except Exception as e:
        print(f"❌ [MOBILE] Error obteniendo estadísticas: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/my-invoices")
async def get_my_invoices(
    page: int = 1,
    limit: int = 20,
    authorization: Optional[str] = Header(None)
):
    """
    📄 Obtener facturas del usuario con paginación

    Query params:
    - page: Número de página (default: 1)
    - limit: Items por página (default: 20, max: 50)
    """
    usuario_id = get_user_id_from_mobile_token(authorization)

    # Limitar máximo
    if limit > 50:
        limit = 50

    offset = (page - 1) * limit

    print(f"📄 [MOBILE] Obteniendo facturas para usuario {usuario_id} (página {page})")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener facturas
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    f.id,
                    f.establecimiento,
                    f.total_factura,
                    f.fecha_factura,
                    f.fecha_cargue,
                    f.productos_guardados,
                    f.tiene_imagen,
                    f.estado_validacion,
                    f.cadena
                FROM facturas f
                WHERE f.usuario_id = %s
                ORDER BY f.fecha_cargue DESC
                LIMIT %s OFFSET %s
            """, (usuario_id, limit, offset))
        else:
            cursor.execute("""
                SELECT
                    f.id,
                    f.establecimiento,
                    f.total_factura,
                    f.fecha_factura,
                    f.fecha_cargue,
                    f.productos_guardados,
                    f.tiene_imagen,
                    f.estado_validacion,
                    f.cadena
                FROM facturas f
                WHERE f.usuario_id = ?
                ORDER BY f.fecha_cargue DESC
                LIMIT ? OFFSET ?
            """, (usuario_id, limit, offset))

        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1] or "Desconocido",
                "total": float(row[2]) if row[2] else 0.0,
                "fecha": str(row[3]) if row[3] else str(row[4]),  # Usar fecha_factura o fecha_cargue
                "fecha_cargue": str(row[4]) if row[4] else None,
                "productos_guardados": row[5] or 0,
                "tiene_imagen": bool(row[6]),
                "estado": row[7] or "pendiente",
                "cadena": row[8] or "Otro"
            })

        # Contar total
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM facturas WHERE usuario_id = %s
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM facturas WHERE usuario_id = ?
            """, (usuario_id,))

        total = cursor.fetchone()[0] or 0

        conn.close()

        print(f"✅ [MOBILE] {len(facturas)} facturas obtenidas (total: {total})")

        return {
            "success": True,
            "facturas": facturas,
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": (total + limit - 1) // limit if limit > 0 else 0
        }

    except Exception as e:
        print(f"❌ [MOBILE] Error obteniendo facturas: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/invoice/{factura_id}")
async def get_invoice_detail(
    factura_id: int,
    authorization: Optional[str] = Header(None)
):
    """
    🔍 Obtener detalles completos de una factura específica

    Incluye:
    - Datos generales de la factura
    - Lista de productos con cantidades y precios
    """
    usuario_id = get_user_id_from_mobile_token(authorization)

    print(f"🔍 [MOBILE] Obteniendo detalles de factura {factura_id}")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que la factura pertenece al usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    f.id,
                    f.establecimiento,
                    f.cadena,
                    f.total_factura,
                    f.fecha_factura,
                    f.fecha_cargue,
                    f.productos_guardados,
                    f.tiene_imagen,
                    f.estado_validacion
                FROM facturas f
                WHERE f.id = %s AND f.usuario_id = %s
            """, (factura_id, usuario_id))
        else:
            cursor.execute("""
                SELECT
                    f.id,
                    f.establecimiento,
                    f.cadena,
                    f.total_factura,
                    f.fecha_factura,
                    f.fecha_cargue,
                    f.productos_guardados,
                    f.tiene_imagen,
                    f.estado_validacion
                FROM facturas f
                WHERE f.id = ? AND f.usuario_id = ?
            """, (factura_id, usuario_id))

        factura_row = cursor.fetchone()

        if not factura_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        # Obtener productos de la factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    i.id,
                    i.codigo_leido,
                    i.nombre_leido,
                    i.precio_pagado,
                    i.cantidad,
                    pm.nombre_normalizado,
                    pm.categoria
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
                    pm.nombre_normalizado,
                    pm.categoria
                FROM items_factura i
                LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
                WHERE i.factura_id = ?
                ORDER BY i.id
            """, (factura_id,))

        productos = []
        for row in cursor.fetchall():
            productos.append({
                "id": row[0],
                "codigo": row[1] or "",
                "nombre": row[2] or row[5] or "Producto sin nombre",
                "precio": float(row[3]) if row[3] else 0.0,
                "cantidad": row[4] or 1,
                "categoria": row[6] or "-"
            })

        conn.close()

        print(f"✅ [MOBILE] Factura {factura_id} con {len(productos)} productos")

        return {
            "success": True,
            "factura": {
                "id": factura_row[0],
                "establecimiento": factura_row[1] or "Desconocido",
                "cadena": factura_row[2],
                "total": float(factura_row[3]) if factura_row[3] else 0.0,
                "fecha": str(factura_row[4]) if factura_row[4] else str(factura_row[5]),
                "fecha_cargue": str(factura_row[5]) if factura_row[5] else None,
                "productos_guardados": factura_row[6] or 0,
                "tiene_imagen": bool(factura_row[7]),
                "estado": factura_row[8] or "pendiente",
                "productos": productos
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [MOBILE] Error obteniendo detalle de factura: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent-activity")
async def get_recent_activity(
    days: int = 7,
    authorization: Optional[str] = Header(None)
):
    """
    📅 Obtener actividad reciente del usuario

    Query params:
    - days: Días hacia atrás (default: 7)
    """
    usuario_id = get_user_id_from_mobile_token(authorization)

    print(f"📅 [MOBILE] Obteniendo actividad de últimos {days} días")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        fecha_desde = (datetime.now() - timedelta(days=days)).isoformat()

        # Facturas recientes agrupadas por día
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    DATE(fecha_cargue) as fecha,
                    COUNT(*) as cantidad,
                    SUM(total_factura) as total
                FROM facturas
                WHERE usuario_id = %s AND fecha_cargue >= %s
                GROUP BY DATE(fecha_cargue)
                ORDER BY fecha DESC
            """, (usuario_id, fecha_desde))
        else:
            cursor.execute("""
                SELECT
                    DATE(fecha_cargue) as fecha,
                    COUNT(*) as cantidad,
                    SUM(total_factura) as total
                FROM facturas
                WHERE usuario_id = ? AND fecha_cargue >= ?
                GROUP BY DATE(fecha_cargue)
                ORDER BY fecha DESC
            """, (usuario_id, fecha_desde))

        actividad = []
        for row in cursor.fetchall():
            actividad.append({
                "fecha": str(row[0]),
                "facturas": row[1],
                "total_gastado": float(row[2]) if row[2] else 0.0
            })

        conn.close()

        print(f"✅ [MOBILE] {len(actividad)} días con actividad")

        return {
            "success": True,
            "days": days,
            "actividad": actividad
        }

    except Exception as e:
        print(f"❌ [MOBILE] Error obteniendo actividad: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health")
async def mobile_health():
    """✅ Health check para la app móvil"""
    return {
        "success": True,
        "service": "LecFac Mobile API",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }


print("✅ Mobile endpoints cargados (versión unificada)")
