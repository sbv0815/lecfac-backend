# ============================================================================
# calificaciones_api.py - Sistema de Calificaciones de Productos
# ============================================================================
# Permite a usuarios calificar productos con estrellas (1-5) y comentarios
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
class CalificacionCreate(BaseModel):
    """Modelo para crear/actualizar una calificación"""

    producto_maestro_id: int
    calificacion: int = Field(..., ge=1, le=5, description="Estrellas del 1 al 5")
    comentario: Optional[str] = Field(
        None, max_length=280, description="Comentario opcional"
    )


class CalificacionResponse(BaseModel):
    """Modelo de respuesta de calificación"""

    id: int
    usuario_id: int
    producto_maestro_id: int
    calificacion: int
    comentario: Optional[str]
    fecha_calificacion: datetime
    nombre_producto: Optional[str] = None
    nombre_usuario: Optional[str] = None


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


# ============================================================================
# ENDPOINTS
# ============================================================================


@router.post("/")
async def crear_o_actualizar_calificacion(
    calificacion: CalificacionCreate, authorization: Optional[str] = Header(None)
):
    """
    ⭐ Crear o actualizar calificación de un producto

    - Si el usuario ya calificó el producto, actualiza la calificación
    - Si no, crea una nueva
    """
    usuario_id = get_user_id_from_token(authorization)

    print(
        f"⭐ [CALIFICACIÓN] Usuario {usuario_id} califica producto {calificacion.producto_maestro_id}"
    )
    print(f"   Estrellas: {calificacion.calificacion}")
    print(
        f"   Comentario: {calificacion.comentario[:50] if calificacion.comentario else 'Sin comentario'}..."
    )

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que el producto existe
        cursor.execute(
            "SELECT id, nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (calificacion.producto_maestro_id,),
        )
        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Insertar o actualizar (UPSERT)
        cursor.execute(
            """
            INSERT INTO calificaciones_productos
                (usuario_id, producto_maestro_id, calificacion, comentario)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (usuario_id, producto_maestro_id)
            DO UPDATE SET
                calificacion = EXCLUDED.calificacion,
                comentario = EXCLUDED.comentario,
                fecha_actualizacion = NOW()
            RETURNING id, fecha_calificacion
        """,
            (
                usuario_id,
                calificacion.producto_maestro_id,
                calificacion.calificacion,
                calificacion.comentario,
            ),
        )

        result = cursor.fetchone()
        conn.commit()

        # Obtener el rating promedio actualizado
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                ROUND(AVG(calificacion), 1) as promedio
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s
        """,
            (calificacion.producto_maestro_id,),
        )

        stats = cursor.fetchone()

        conn.close()

        print(
            f"✅ Calificación guardada. Rating promedio: {stats[1]} ({stats[0]} opiniones)"
        )

        return {
            "success": True,
            "mensaje": "¡Gracias por tu calificación!",
            "calificacion_id": result[0],
            "producto": {
                "id": calificacion.producto_maestro_id,
                "nombre": producto[1],
                "rating_promedio": (
                    float(stats[1]) if stats[1] else calificacion.calificacion
                ),
                "total_calificaciones": stats[0],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error guardando calificación: {e}")
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
    """
    📊 Obtener calificaciones de un producto

    Retorna:
    - Rating promedio
    - Total de calificaciones
    - Lista de comentarios recientes
    - Calificación del usuario actual (si existe)
    """
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener info del producto y stats
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.nombre_consolidado,
                pm.marca,
                COUNT(cp.id) as total_calificaciones,
                COALESCE(ROUND(AVG(cp.calificacion), 1), 0) as rating_promedio,
                COUNT(CASE WHEN cp.calificacion >= 4 THEN 1 END) as positivas,
                COUNT(CASE WHEN cp.calificacion <= 2 THEN 1 END) as negativas
            FROM productos_maestros_v2 pm
            LEFT JOIN calificaciones_productos cp ON pm.id = cp.producto_maestro_id
            WHERE pm.id = %s
            GROUP BY pm.id, pm.nombre_consolidado, pm.marca
        """,
            (producto_id,),
        )

        producto_stats = cursor.fetchone()

        if not producto_stats:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener calificación del usuario actual
        cursor.execute(
            """
            SELECT calificacion, comentario, fecha_calificacion
            FROM calificaciones_productos
            WHERE producto_maestro_id = %s AND usuario_id = %s
        """,
            (producto_id, usuario_id),
        )

        mi_calificacion = cursor.fetchone()

        # Obtener comentarios recientes (solo los que tienen comentario)
        cursor.execute(
            """
            SELECT
                cp.calificacion,
                cp.comentario,
                cp.fecha_calificacion,
                u.nombre as nombre_usuario
            FROM calificaciones_productos cp
            LEFT JOIN usuarios u ON cp.usuario_id = u.id
            WHERE cp.producto_maestro_id = %s
              AND cp.comentario IS NOT NULL
              AND cp.comentario != ''
            ORDER BY cp.fecha_calificacion DESC
            LIMIT %s
        """,
            (producto_id, limit),
        )

        comentarios_rows = cursor.fetchall()

        conn.close()

        # Formatear comentarios
        comentarios = []
        for row in comentarios_rows:
            fecha = row[2]
            if fecha:
                dias = (datetime.now() - fecha).days
                if dias == 0:
                    fecha_str = "Hoy"
                elif dias == 1:
                    fecha_str = "Ayer"
                elif dias <= 7:
                    fecha_str = f"Hace {dias} días"
                else:
                    fecha_str = fecha.strftime("%d/%m/%Y")
            else:
                fecha_str = ""

            comentarios.append(
                {
                    "calificacion": row[0],
                    "comentario": row[1],
                    "fecha": fecha_str,
                    "usuario": row[3] or "Usuario",
                }
            )

        return {
            "success": True,
            "producto": {
                "id": producto_stats[0],
                "nombre": producto_stats[1],
                "marca": producto_stats[2],
            },
            "estadisticas": {
                "total_calificaciones": producto_stats[3],
                "rating_promedio": float(producto_stats[4]) if producto_stats[4] else 0,
                "positivas": producto_stats[5],
                "negativas": producto_stats[6],
            },
            "mi_calificacion": (
                {
                    "calificacion": mi_calificacion[0],
                    "comentario": mi_calificacion[1],
                    "fecha": (
                        mi_calificacion[2].isoformat()
                        if mi_calificacion and mi_calificacion[2]
                        else None
                    ),
                }
                if mi_calificacion
                else None
            ),
            "comentarios": comentarios,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error obteniendo calificaciones: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usuario/{usuario_id}")
async def obtener_calificaciones_usuario(
    usuario_id: int, limit: int = Query(20, ge=1, le=100)
):
    """
    👤 Obtener todas las calificaciones de un usuario (para su perfil)

    Retorna lista de productos calificados con sus estrellas
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener estadísticas del usuario
        cursor.execute(
            """
            SELECT
                COUNT(*) as total_calificaciones,
                ROUND(AVG(calificacion), 1) as promedio_dado,
                COUNT(CASE WHEN calificacion = 5 THEN 1 END) as cinco_estrellas,
                COUNT(CASE WHEN calificacion = 4 THEN 1 END) as cuatro_estrellas,
                COUNT(CASE WHEN calificacion = 3 THEN 1 END) as tres_estrellas,
                COUNT(CASE WHEN calificacion = 2 THEN 1 END) as dos_estrellas,
                COUNT(CASE WHEN calificacion = 1 THEN 1 END) as una_estrella,
                COUNT(CASE WHEN comentario IS NOT NULL AND comentario != '' THEN 1 END) as con_comentario
            FROM calificaciones_productos
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        stats = cursor.fetchone()

        # Obtener calificaciones recientes
        cursor.execute(
            """
            SELECT
                cp.id,
                cp.producto_maestro_id,
                pm.nombre_consolidado,
                pm.marca,
                cp.calificacion,
                cp.comentario,
                cp.fecha_calificacion
            FROM calificaciones_productos cp
            INNER JOIN productos_maestros_v2 pm ON cp.producto_maestro_id = pm.id
            WHERE cp.usuario_id = %s
            ORDER BY cp.fecha_calificacion DESC
            LIMIT %s
        """,
            (usuario_id, limit),
        )

        calificaciones_rows = cursor.fetchall()
        conn.close()

        calificaciones = []
        for row in calificaciones_rows:
            fecha = row[6]
            if fecha:
                dias = (datetime.now() - fecha).days
                if dias == 0:
                    fecha_str = "Hoy"
                elif dias == 1:
                    fecha_str = "Ayer"
                elif dias <= 30:
                    fecha_str = f"Hace {dias} días"
                else:
                    fecha_str = fecha.strftime("%d/%m/%Y")
            else:
                fecha_str = ""

            calificaciones.append(
                {
                    "id": row[0],
                    "producto_id": row[1],
                    "producto_nombre": row[2],
                    "marca": row[3],
                    "calificacion": row[4],
                    "comentario": row[5],
                    "fecha": fecha_str,
                }
            )

        return {
            "success": True,
            "usuario_id": usuario_id,
            "estadisticas": {
                "total_calificaciones": stats[0] or 0,
                "promedio_dado": float(stats[1]) if stats[1] else 0,
                "distribucion": {
                    "5_estrellas": stats[2] or 0,
                    "4_estrellas": stats[3] or 0,
                    "3_estrellas": stats[4] or 0,
                    "2_estrellas": stats[5] or 0,
                    "1_estrella": stats[6] or 0,
                },
                "con_comentario": stats[7] or 0,
            },
            "calificaciones": calificaciones,
        }

    except Exception as e:
        logger.error(f"❌ Error obteniendo calificaciones del usuario: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{calificacion_id}")
async def eliminar_calificacion(
    calificacion_id: int, authorization: Optional[str] = Header(None)
):
    """
    🗑️ Eliminar una calificación propia
    """
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que la calificación pertenece al usuario
        cursor.execute(
            """
            DELETE FROM calificaciones_productos
            WHERE id = %s AND usuario_id = %s
            RETURNING id
        """,
            (calificacion_id, usuario_id),
        )

        result = cursor.fetchone()
        conn.commit()
        conn.close()

        if not result:
            raise HTTPException(
                status_code=404,
                detail="Calificación no encontrada o no tienes permiso para eliminarla",
            )

        return {"success": True, "mensaje": "Calificación eliminada"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error eliminando calificación: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/mi-calificacion/{producto_id}")
async def obtener_mi_calificacion(
    producto_id: int, authorization: Optional[str] = Header(None)
):
    """
    ⭐ Obtener mi calificación para un producto específico

    Útil para mostrar las estrellas en el inventario
    """
    usuario_id = get_user_id_from_token(authorization)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT calificacion, comentario, fecha_calificacion
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
                "comentario": result[1],
                "fecha": result[2].isoformat() if result[2] else None,
            }
        else:
            return {
                "success": True,
                "calificado": False,
                "calificacion": None,
                "comentario": None,
            }

    except Exception as e:
        logger.error(f"❌ Error obteniendo mi calificación: {e}")
        if conn:
            conn.close()
        return {"success": False, "calificado": False, "calificacion": None}


print("✅ API de Calificaciones cargada")
