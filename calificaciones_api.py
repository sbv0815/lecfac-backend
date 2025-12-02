# ============================================================================
# calificaciones_api.py - Sistema de Calificaciones de Productos V3
# ============================================================================
# 3 CRITERIOS: Precio, Calidad, Presentación
# ============================================================================

from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
import logging
from database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/calificaciones", tags=["calificaciones"])


# ============================================================================
# MODELOS
# ============================================================================
class CalificacionSimpleCreate(BaseModel):
    """Modelo para calificación con 3 criterios"""

    producto_maestro_id: int
    calificacion: Optional[int] = Field(None, ge=1, le=5)
    calificacion_precio: Optional[int] = Field(None, ge=1, le=5)
    calificacion_calidad: Optional[int] = Field(None, ge=1, le=5)
    calificacion_presentacion: Optional[int] = Field(None, ge=1, le=5)
    comentario: Optional[str] = Field(None, max_length=500)


# ============================================================================
# HELPERS
# ============================================================================
def get_user_id_from_token(authorization: Optional[str] = None) -> int:
    """Extraer usuario_id del token JWT"""
    if not authorization:
        return 1
    try:
        import jwt

        token = authorization.replace("Bearer ", "")
        payload = jwt.decode(token, options={"verify_signature": False})
        usuario_id = payload.get("user_id") or payload.get("sub") or payload.get("id")
        return int(usuario_id) if usuario_id else 1
    except:
        return 1


def buscar_producto(cursor, producto_id: int) -> dict:
    """Busca un producto en múltiples tablas"""
    cursor.execute(
        "SELECT id, nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
        (producto_id,),
    )
    producto = cursor.fetchone()
    if producto:
        return {
            "id": producto[0],
            "nombre": producto[1],
            "tabla": "productos_maestros_v2",
        }

    cursor.execute("SELECT id, nombre FROM productos WHERE id = %s", (producto_id,))
    producto = cursor.fetchone()
    if producto:
        return {"id": producto[0], "nombre": producto[1], "tabla": "productos"}

    return None


# ============================================================================
# ENDPOINTS
# ============================================================================


@router.post("/")
async def crear_o_actualizar_calificacion(
    data: CalificacionSimpleCreate, authorization: Optional[str] = Header(None)
):
    """
    ⭐ Crear o actualizar calificación con 3 criterios
    """
    usuario_id = get_user_id_from_token(authorization)

    # Determinar valores de cada criterio
    if data.calificacion_precio is not None:
        precio = data.calificacion_precio
        calidad = data.calificacion_calidad or precio
        presentacion = data.calificacion_presentacion or precio
    elif data.calificacion is not None:
        precio = data.calificacion
        calidad = data.calificacion
        presentacion = data.calificacion
    else:
        raise HTTPException(
            status_code=400, detail="Debe proporcionar al menos una calificación"
        )

    promedio = round((precio + calidad + presentacion) / 3, 1)

    print(
        f"⭐ [CALIFICACIÓN] Usuario {usuario_id} califica producto {data.producto_maestro_id}"
    )
    print(
        f"   💰 Precio: {precio} | 🏆 Calidad: {calidad} | 📦 Presentación: {presentacion}"
    )
    print(f"   📊 Promedio: {promedio}")
    print(
        f"   💬 Comentario: {data.comentario[:50] if data.comentario else 'Sin comentario'}..."
    )

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        producto = buscar_producto(cursor, data.producto_maestro_id)
        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # UPSERT
        cursor.execute(
            """
            INSERT INTO calificaciones_productos
                (usuario_id, producto_maestro_id, calificacion,
                 calificacion_precio, calificacion_calidad, calificacion_presentacion,
                 comentario)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (usuario_id, producto_maestro_id)
            DO UPDATE SET
                calificacion = EXCLUDED.calificacion,
                calificacion_precio = EXCLUDED.calificacion_precio,
                calificacion_calidad = EXCLUDED.calificacion_calidad,
                calificacion_presentacion = EXCLUDED.calificacion_presentacion,
                comentario = EXCLUDED.comentario,
                fecha_actualizacion = NOW()
            RETURNING id, fecha_calificacion
        """,
            (
                usuario_id,
                data.producto_maestro_id,
                round(promedio),
                precio,
                calidad,
                presentacion,
                data.comentario,
            ),
        )

        result = cursor.fetchone()
        conn.commit()

        # Stats
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                ROUND(AVG(calificacion), 1) as promedio_general,
                ROUND(AVG(calificacion_precio), 1) as promedio_precio,
                ROUND(AVG(calificacion_calidad), 1) as promedio_calidad,
                ROUND(AVG(calificacion_presentacion), 1) as promedio_presentacion
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s
        """,
            (data.producto_maestro_id,),
        )

        stats = cursor.fetchone()
        conn.close()

        return {
            "success": True,
            "mensaje": "¡Gracias por tu calificación!",
            "calificacion_id": result[0],
            "producto": {"id": data.producto_maestro_id, "nombre": producto["nombre"]},
            "mi_calificacion": {
                "precio": precio,
                "calidad": calidad,
                "presentacion": presentacion,
                "promedio": promedio,
                "comentario": data.comentario,
            },
            "estadisticas": {
                "total_calificaciones": stats[0],
                "rating_promedio": float(stats[1]) if stats[1] else promedio,
                "promedio_precio": float(stats[2]) if stats[2] else precio,
                "promedio_calidad": float(stats[3]) if stats[3] else calidad,
                "promedio_presentacion": float(stats[4]) if stats[4] else presentacion,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error guardando calificación: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/producto/{producto_id}")
async def obtener_calificaciones_producto(
    producto_id: int,
    limit: int = Query(10, ge=1, le=50),
    authorization: Optional[str] = Header(None),
):
    """📊 Obtener calificaciones de un producto"""
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        producto = buscar_producto(cursor, producto_id)

        if not producto:
            return {
                "success": True,
                "producto": {"id": producto_id, "nombre": "Producto", "marca": None},
                "estadisticas": {
                    "total_calificaciones": 0,
                    "rating_promedio": 0,
                    "promedio_precio": 0,
                    "promedio_calidad": 0,
                    "promedio_presentacion": 0,
                    "distribucion": {
                        "5_estrellas": 0,
                        "4_estrellas": 0,
                        "3_estrellas": 0,
                        "2_estrellas": 0,
                        "1_estrella": 0,
                    },
                },
                "mi_calificacion": None,
                "comentarios": [],
            }

        # Stats
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COALESCE(ROUND(AVG(calificacion), 1), 0) as promedio,
                COALESCE(ROUND(AVG(calificacion_precio), 1), 0) as prom_precio,
                COALESCE(ROUND(AVG(calificacion_calidad), 1), 0) as prom_calidad,
                COALESCE(ROUND(AVG(calificacion_presentacion), 1), 0) as prom_pres,
                COUNT(CASE WHEN calificacion = 5 THEN 1 END),
                COUNT(CASE WHEN calificacion = 4 THEN 1 END),
                COUNT(CASE WHEN calificacion = 3 THEN 1 END),
                COUNT(CASE WHEN calificacion = 2 THEN 1 END),
                COUNT(CASE WHEN calificacion = 1 THEN 1 END)
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s
        """,
            (producto_id,),
        )

        stats = cursor.fetchone()

        # Mi calificación
        cursor.execute(
            """
            SELECT calificacion, calificacion_precio, calificacion_calidad,
                   calificacion_presentacion, comentario, fecha_calificacion
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s AND usuario_id = %s
        """,
            (producto_id, usuario_id),
        )

        mi_cal = cursor.fetchone()

        # Comentarios
        cursor.execute(
            """
            SELECT
                cp.calificacion, cp.calificacion_precio, cp.calificacion_calidad,
                cp.calificacion_presentacion, cp.comentario, cp.fecha_calificacion,
                COALESCE(u.nombre, 'Usuario') as nombre
            FROM calificaciones_productos cp
            LEFT JOIN usuarios u ON cp.usuario_id = u.id
            WHERE cp.producto_maestro_id = %s
              AND cp.comentario IS NOT NULL AND cp.comentario != ''
            ORDER BY cp.fecha_calificacion DESC
            LIMIT %s
        """,
            (producto_id, limit),
        )

        comentarios_rows = cursor.fetchall()
        conn.close()

        comentarios = []
        for row in comentarios_rows:
            fecha = row[5]
            dias = (datetime.now() - fecha).days if fecha else 0
            fecha_str = (
                "Hoy"
                if dias == 0
                else (
                    "Ayer"
                    if dias == 1
                    else (
                        f"Hace {dias} días" if dias <= 7 else fecha.strftime("%d/%m/%Y")
                    )
                )
            )

            comentarios.append(
                {
                    "calificacion": row[0],
                    "precio": row[1],
                    "calidad": row[2],
                    "presentacion": row[3],
                    "comentario": row[4],
                    "fecha": fecha_str,
                    "usuario": row[6],
                }
            )

        return {
            "success": True,
            "producto": {
                "id": producto_id,
                "nombre": producto["nombre"],
                "marca": None,
            },
            "estadisticas": {
                "total_calificaciones": stats[0],
                "rating_promedio": float(stats[1]) if stats[1] else 0,
                "promedio_precio": float(stats[2]) if stats[2] else 0,
                "promedio_calidad": float(stats[3]) if stats[3] else 0,
                "promedio_presentacion": float(stats[4]) if stats[4] else 0,
                "distribucion": {
                    "5_estrellas": stats[5],
                    "4_estrellas": stats[6],
                    "3_estrellas": stats[7],
                    "2_estrellas": stats[8],
                    "1_estrella": stats[9],
                },
            },
            "mi_calificacion": (
                {
                    "calificacion": mi_cal[0],
                    "precio": mi_cal[1],
                    "calidad": mi_cal[2],
                    "presentacion": mi_cal[3],
                    "comentario": mi_cal[4],
                    "fecha": mi_cal[5].isoformat() if mi_cal and mi_cal[5] else None,
                }
                if mi_cal
                else None
            ),
            "comentarios": comentarios,
        }

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usuario/{usuario_id}")
async def obtener_calificaciones_usuario(
    usuario_id: int, limit: int = Query(20, ge=1, le=100)
):
    """👤 Obtener calificaciones de un usuario"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*), ROUND(AVG(calificacion), 1),
                ROUND(AVG(calificacion_precio), 1), ROUND(AVG(calificacion_calidad), 1),
                ROUND(AVG(calificacion_presentacion), 1),
                COUNT(CASE WHEN comentario IS NOT NULL AND comentario != '' THEN 1 END)
            FROM calificaciones_productos WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        stats = cursor.fetchone()

        cursor.execute(
            """
            SELECT cp.id, cp.producto_maestro_id, pm.nombre_consolidado, pm.marca,
                   cp.calificacion, cp.calificacion_precio, cp.calificacion_calidad,
                   cp.calificacion_presentacion, cp.comentario, cp.fecha_calificacion
            FROM calificaciones_productos cp
            INNER JOIN productos_maestros_v2 pm ON cp.producto_maestro_id = pm.id
            WHERE cp.usuario_id = %s
            ORDER BY cp.fecha_calificacion DESC LIMIT %s
        """,
            (usuario_id, limit),
        )

        rows = cursor.fetchall()
        conn.close()

        calificaciones = []
        for row in rows:
            fecha = row[9]
            dias = (datetime.now() - fecha).days if fecha else 0
            fecha_str = (
                "Hoy"
                if dias == 0
                else (
                    "Ayer"
                    if dias == 1
                    else (
                        f"Hace {dias} días"
                        if dias <= 30
                        else fecha.strftime("%d/%m/%Y")
                    )
                )
            )

            calificaciones.append(
                {
                    "id": row[0],
                    "producto_id": row[1],
                    "producto_nombre": row[2],
                    "marca": row[3],
                    "calificacion": row[4],
                    "precio": row[5],
                    "calidad": row[6],
                    "presentacion": row[7],
                    "comentario": row[8],
                    "fecha": fecha_str,
                }
            )

        return {
            "success": True,
            "usuario_id": usuario_id,
            "estadisticas": {
                "total_calificaciones": stats[0] or 0,
                "promedio_general": float(stats[1]) if stats[1] else 0,
                "promedio_precio": float(stats[2]) if stats[2] else 0,
                "promedio_calidad": float(stats[3]) if stats[3] else 0,
                "promedio_presentacion": float(stats[4]) if stats[4] else 0,
                "con_comentario": stats[5] or 0,
            },
            "calificaciones": calificaciones,
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mi-calificacion/{producto_id}")
async def obtener_mi_calificacion(
    producto_id: int, authorization: Optional[str] = Header(None)
):
    """⭐ Obtener mi calificación"""
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT calificacion, calificacion_precio, calificacion_calidad,
                   calificacion_presentacion, comentario, fecha_calificacion
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s AND usuario_id = %s
        """,
            (producto_id, usuario_id),
        )

        result = cursor.fetchone()
        conn.close()

        if result:
            return {
                "success": True,
                "calificado": True,
                "calificacion": result[0],
                "precio": result[1],
                "calidad": result[2],
                "presentacion": result[3],
                "comentario": result[4],
                "fecha": result[5].isoformat() if result[5] else None,
            }
        return {
            "success": True,
            "calificado": False,
            "calificacion": None,
            "precio": None,
            "calidad": None,
            "presentacion": None,
            "comentario": None,
        }

    except Exception as e:
        if conn:
            conn.close()
        return {"success": False, "calificado": False, "calificacion": None}


@router.delete("/{calificacion_id}")
async def eliminar_calificacion(
    calificacion_id: int, authorization: Optional[str] = Header(None)
):
    """🗑️ Eliminar calificación"""
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "DELETE FROM calificaciones_productos WHERE id = %s AND usuario_id = %s RETURNING id",
            (calificacion_id, usuario_id),
        )
        result = cursor.fetchone()
        conn.commit()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Calificación no encontrada")
        return {"success": True, "mensaje": "Calificación eliminada"}

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API de Calificaciones V3 cargada (3 criterios)")
