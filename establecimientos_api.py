"""
API de Establecimientos con Colores
Versión corregida: solo usa nombre_normalizado (sin columna nombre)
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any, List
from pydantic import BaseModel
import os

router = APIRouter(prefix="/api/establecimientos", tags=["establecimientos"])

# ============================================================================
# MODELOS
# ============================================================================

class EstablecimientoCreate(BaseModel):
    nombre_normalizado: str
    cadena: Optional[str] = None
    tipo: Optional[str] = None
    ciudad: Optional[str] = None
    direccion: Optional[str] = None
    color_bg: Optional[str] = "#e9ecef"
    color_text: Optional[str] = "#495057"


class EstablecimientoUpdate(BaseModel):
    nombre_normalizado: Optional[str] = None
    cadena: Optional[str] = None
    tipo: Optional[str] = None
    ciudad: Optional[str] = None
    direccion: Optional[str] = None
    color_bg: Optional[str] = None
    color_text: Optional[str] = None
    activo: Optional[bool] = None


# ============================================================================
# UTILIDADES DE CONEXIÓN
# ============================================================================

def get_db_connection():
    """Obtener conexión según tipo de DB"""
    database_type = os.getenv("DATABASE_TYPE", "sqlite")

    if database_type == "postgresql":
        import psycopg2
        from urllib.parse import urlparse

        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise Exception("DATABASE_URL no configurada")

        result = urlparse(database_url)
        conn = psycopg2.connect(
            host=result.hostname,
            port=result.port,
            database=result.path[1:],
            user=result.username,
            password=result.password
        )
        return conn, "postgresql"
    else:
        import sqlite3
        conn = sqlite3.connect("lecfac.db")
        return conn, "sqlite"


# ============================================================================
# ENDPOINTS
# ============================================================================

@router.get("/", summary="Listar establecimientos")
async def listar_establecimientos(
    activos_solo: bool = Query(True, description="Solo establecimientos activos"),
    incluir_colores: bool = Query(True, description="Incluir información de colores")
):
    """Listar todos los establecimientos con su información de colores"""
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        where_clause = "WHERE activo = TRUE" if activos_solo else ""

        if database_type == "postgresql":
            cursor.execute(f"""
                SELECT
                    id,
                    nombre_normalizado,
                    cadena,
                    color_bg,
                    color_text,
                    ciudad,
                    direccion,
                    activo,
                    (SELECT COUNT(DISTINCT f.id)
                     FROM facturas f
                     WHERE f.establecimiento ILIKE '%' || e.nombre_normalizado || '%'
                    ) as total_facturas
                FROM establecimientos e
                {where_clause}
                ORDER BY nombre_normalizado
            """)
        else:
            cursor.execute(f"""
                SELECT
                    id,
                    nombre_normalizado,
                    cadena,
                    color_bg,
                    color_text,
                    ciudad,
                    direccion,
                    activo,
                    (SELECT COUNT(DISTINCT f.id)
                     FROM facturas f
                     WHERE f.establecimiento LIKE '%' || e.nombre_normalizado || '%'
                    ) as total_facturas
                FROM establecimientos e
                {where_clause}
                ORDER BY nombre_normalizado
            """)

        establecimientos = []
        for row in cursor.fetchall():
            est = {
                "id": row[0],
                "nombre": row[1],  # nombre_normalizado
                "nombre_normalizado": row[1],
                "cadena": row[2],
                "activo": row[7],
                "total_facturas": row[8] or 0
            }

            if incluir_colores:
                est["colores"] = {
                    "bg": row[3] or "#e9ecef",
                    "text": row[4] or "#495057"
                }

            establecimientos.append(est)

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total": len(establecimientos),
            "establecimientos": establecimientos
        }

    except Exception as e:
        print(f"❌ Error en listar_establecimientos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/colores", summary="Obtener colores de establecimientos")
async def obtener_colores():
    """
    Obtener un diccionario de colores por establecimiento
    Formato: { "Exito": { "bg": "#e3f2fd", "text": "#1565c0" }, ... }
    """
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE activo = TRUE
                ORDER BY nombre_normalizado
            """)
        else:
            cursor.execute("""
                SELECT
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE activo = TRUE
                ORDER BY nombre_normalizado
            """)

        colores = {}
        for row in cursor.fetchall():
            nombre = row[0]
            if nombre:
                colores[nombre] = {
                    "bg": row[1] or "#e9ecef",
                    "text": row[2] or "#495057"
                }

        cursor.close()
        conn.close()

        return {
            "success": True,
            "colores": colores
        }

    except Exception as e:
        print(f"❌ Error en obtener_colores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{establecimiento_id}", summary="Obtener establecimiento por ID")
async def obtener_establecimiento(establecimiento_id: int):
    """Obtener información completa de un establecimiento"""
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    id, nombre_normalizado, cadena, tipo, ciudad, direccion,
                    color_bg, color_text, activo, fecha_creacion,
                    latitud, longitud, total_facturas_reportadas
                FROM establecimientos
                WHERE id = %s
            """, (establecimiento_id,))
        else:
            cursor.execute("""
                SELECT
                    id, nombre_normalizado, cadena, tipo, ciudad, direccion,
                    color_bg, color_text, activo, fecha_creacion,
                    latitud, longitud, total_facturas_reportadas
                FROM establecimientos
                WHERE id = ?
            """, (establecimiento_id,))

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        establecimiento = {
            "id": row[0],
            "nombre": row[1],
            "nombre_normalizado": row[1],
            "cadena": row[2],
            "tipo": row[3],
            "ciudad": row[4],
            "direccion": row[5],
            "colores": {
                "bg": row[6] or "#e9ecef",
                "text": row[7] or "#495057"
            },
            "activo": row[8],
            "fecha_creacion": str(row[9]) if row[9] else None,
            "coordenadas": {
                "latitud": row[10],
                "longitud": row[11]
            } if row[10] and row[11] else None,
            "total_facturas": row[12] or 0
        }

        cursor.close()
        conn.close()

        return {
            "success": True,
            "establecimiento": establecimiento
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en obtener_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", summary="Crear establecimiento")
async def crear_establecimiento(data: EstablecimientoCreate):
    """Crear un nuevo establecimiento con colores personalizados"""
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO establecimientos (
                    nombre_normalizado, cadena, tipo, ciudad, direccion,
                    color_bg, color_text, activo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                RETURNING id
            """, (
                data.nombre_normalizado,
                data.cadena,
                data.tipo,
                data.ciudad,
                data.direccion,
                data.color_bg,
                data.color_text
            ))
            establecimiento_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO establecimientos (
                    nombre_normalizado, cadena, tipo, ciudad, direccion,
                    color_bg, color_text, activo
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, (
                data.nombre_normalizado,
                data.cadena,
                data.tipo,
                data.ciudad,
                data.direccion,
                data.color_bg,
                data.color_text
            ))
            establecimiento_id = cursor.lastrowid

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Establecimiento creado exitosamente",
            "id": establecimiento_id
        }

    except Exception as e:
        print(f"❌ Error en crear_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{establecimiento_id}", summary="Actualizar establecimiento")
async def actualizar_establecimiento(establecimiento_id: int, data: EstablecimientoUpdate):
    """Actualizar información de un establecimiento, incluyendo colores"""
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        # Construir UPDATE dinámicamente
        updates = []
        values = []

        if data.nombre_normalizado is not None:
            updates.append("nombre_normalizado = %s" if database_type == "postgresql" else "nombre_normalizado = ?")
            values.append(data.nombre_normalizado)

        if data.cadena is not None:
            updates.append("cadena = %s" if database_type == "postgresql" else "cadena = ?")
            values.append(data.cadena)

        if data.color_bg is not None:
            updates.append("color_bg = %s" if database_type == "postgresql" else "color_bg = ?")
            values.append(data.color_bg)

        if data.color_text is not None:
            updates.append("color_text = %s" if database_type == "postgresql" else "color_text = ?")
            values.append(data.color_text)

        if data.activo is not None:
            updates.append("activo = %s" if database_type == "postgresql" else "activo = ?")
            values.append(data.activo)

        if not updates:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        values.append(establecimiento_id)

        query = f"""
            UPDATE establecimientos
            SET {', '.join(updates)}
            WHERE id = {"%" if database_type == "postgresql" else "?"}s
        """

        cursor.execute(query, values)
        conn.commit()

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Establecimiento actualizado exitosamente"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en actualizar_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{establecimiento_id}", summary="Eliminar/desactivar establecimiento")
async def eliminar_establecimiento(establecimiento_id: int, permanente: bool = False):
    """
    Eliminar (desactivar) un establecimiento
    - Si permanente=False: Solo marca como inactivo
    - Si permanente=True: Elimina de la BD (solo si no tiene facturas)
    """
    try:
        conn, database_type = get_db_connection()
        cursor = conn.cursor()

        if permanente:
            if database_type == "postgresql":
                cursor.execute("""
                    DELETE FROM establecimientos
                    WHERE id = %s
                """, (establecimiento_id,))
            else:
                cursor.execute("""
                    DELETE FROM establecimientos
                    WHERE id = ?
                """, (establecimiento_id,))
        else:
            if database_type == "postgresql":
                cursor.execute("""
                    UPDATE establecimientos
                    SET activo = FALSE
                    WHERE id = %s
                """, (establecimiento_id,))
            else:
                cursor.execute("""
                    UPDATE establecimientos
                    SET activo = 0
                    WHERE id = ?
                """, (establecimiento_id,))

        conn.commit()

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": f"Establecimiento {'eliminado' if permanente else 'desactivado'} exitosamente"
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en eliminar_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API de establecimientos cargada (versión corregida - solo nombre_normalizado)")
