"""
=============================================================================
LECFAC - ESTABLECIMIENTOS_API.PY
=============================================================================
API para gestión de establecimientos (supermercados):
- CRUD completo de establecimientos
- Configuración de colores para frontend
- Detección automática de nuevos establecimientos
- Sincronización con facturas

Permite agregar/editar supermercados sin tocar código del frontend
=============================================================================
"""

from fastapi import APIRouter, HTTPException
from typing import Optional, List
from pydantic import BaseModel
import os
from database import get_db_connection

router = APIRouter(prefix="/api/establecimientos", tags=["establecimientos"])


# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class EstablecimientoCreate(BaseModel):
    nombre: str
    nombre_normalizado: Optional[str] = None
    cadena: Optional[str] = None
    color_bg: str = "#e9ecef"  # Color de fondo por defecto (gris)
    color_text: str = "#495057"  # Color de texto por defecto (gris oscuro)
    ciudad: Optional[str] = None
    direccion: Optional[str] = None
    activo: bool = True


class EstablecimientoUpdate(BaseModel):
    nombre: Optional[str] = None
    nombre_normalizado: Optional[str] = None
    cadena: Optional[str] = None
    color_bg: Optional[str] = None
    color_text: Optional[str] = None
    ciudad: Optional[str] = None
    direccion: Optional[str] = None
    activo: Optional[bool] = None


# ============================================================================
# ENDPOINTS - LISTADO Y CONSULTA
# ============================================================================

@router.get("")
async def listar_establecimientos(
    activos_solo: bool = False,
    incluir_colores: bool = True
):
    """
    Listar todos los establecimientos con sus colores

    Parámetros:
    - activos_solo: Solo mostrar establecimientos activos
    - incluir_colores: Incluir información de colores (para frontend)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Construir query
        where_clause = "WHERE activo = TRUE" if activos_solo else ""

        if database_type == "postgresql":
            cursor.execute(f"""
                SELECT
                    id,
                    nombre,
                    nombre_normalizado,
                    cadena,
                    color_bg,
                    color_text,
                    ciudad,
                    direccion,
                    activo,
                    (SELECT COUNT(DISTINCT f.id)
                     FROM facturas f
                     WHERE f.establecimiento ILIKE '%' || e.nombre || '%'
                        OR f.establecimiento ILIKE '%' || e.nombre_normalizado || '%'
                    ) as total_facturas
                FROM establecimientos e
                {where_clause}
                ORDER BY nombre
            """)
        else:
            cursor.execute(f"""
                SELECT
                    id,
                    nombre,
                    nombre_normalizado,
                    cadena,
                    color_bg,
                    color_text,
                    ciudad,
                    direccion,
                    activo,
                    (SELECT COUNT(DISTINCT f.id)
                     FROM facturas f
                     WHERE f.establecimiento LIKE '%' || e.nombre || '%'
                        OR f.establecimiento LIKE '%' || e.nombre_normalizado || '%'
                    ) as total_facturas
                FROM establecimientos e
                {where_clause}
                ORDER BY nombre
            """)

        establecimientos = []
        for row in cursor.fetchall():
            est = {
                "id": row[0],
                "nombre": row[1],
                "nombre_normalizado": row[2],
                "cadena": row[3],
                "activo": row[8],
                "total_facturas": row[9] or 0
            }

            # Agregar colores si se solicita
            if incluir_colores:
                est["colores"] = {
                    "bg": row[4],
                    "text": row[5]
                }

            # Agregar ubicación si existe
            if row[6] or row[7]:
                est["ubicacion"] = {
                    "ciudad": row[6],
                    "direccion": row[7]
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


@router.get("/colores")
async def obtener_colores_establecimientos():
    """
    Obtener solo los colores de establecimientos (para frontend)
    Formato optimizado para cache del navegador
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    nombre,
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE activo = TRUE
                ORDER BY nombre
            """)
        else:
            cursor.execute("""
                SELECT
                    nombre,
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE activo = 1
                ORDER BY nombre
            """)

        colores = {}
        for row in cursor.fetchall():
            nombre = row[0]
            nombre_norm = row[1] or nombre

            # Agregar múltiples variantes del nombre
            for variante in [nombre, nombre_norm, nombre.upper(), nombre.lower()]:
                if variante:
                    colores[variante] = {
                        "bg": row[2],
                        "text": row[3]
                    }

        cursor.close()
        conn.close()

        return {
            "success": True,
            "colores": colores,
            "cache_max_age": 3600  # Cachear por 1 hora
        }

    except Exception as e:
        print(f"❌ Error en obtener_colores: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/detectar-nuevos")
async def detectar_nuevos_establecimientos():
    """
    Detectar establecimientos en facturas que no están registrados
    Útil para agregar nuevos supermercados automáticamente
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Obtener establecimientos únicos de facturas
        if database_type == "postgresql":
            cursor.execute("""
                SELECT DISTINCT
                    f.establecimiento,
                    COUNT(*) as total_facturas,
                    MIN(f.fecha_cargue) as primera_factura,
                    MAX(f.fecha_cargue) as ultima_factura
                FROM facturas f
                WHERE f.establecimiento IS NOT NULL
                  AND f.establecimiento != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM establecimientos e
                      WHERE f.establecimiento ILIKE '%' || e.nombre || '%'
                         OR f.establecimiento ILIKE '%' || e.nombre_normalizado || '%'
                  )
                GROUP BY f.establecimiento
                ORDER BY COUNT(*) DESC
            """)
        else:
            cursor.execute("""
                SELECT DISTINCT
                    f.establecimiento,
                    COUNT(*) as total_facturas,
                    MIN(f.fecha_cargue) as primera_factura,
                    MAX(f.fecha_cargue) as ultima_factura
                FROM facturas f
                WHERE f.establecimiento IS NOT NULL
                  AND f.establecimiento != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM establecimientos e
                      WHERE f.establecimiento LIKE '%' || e.nombre || '%'
                         OR f.establecimiento LIKE '%' || e.nombre_normalizado || '%'
                  )
                GROUP BY f.establecimiento
                ORDER BY COUNT(*) DESC
            """)

        nuevos = []
        for row in cursor.fetchall():
            nuevos.append({
                "nombre_en_facturas": row[0],
                "total_facturas": row[1],
                "primera_factura": str(row[2]) if row[2] else None,
                "ultima_factura": str(row[3]) if row[3] else None,
                "sugerencia_registro": True
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total_nuevos": len(nuevos),
            "establecimientos_sin_registrar": nuevos,
            "mensaje": f"Se encontraron {len(nuevos)} establecimientos sin registrar"
        }

    except Exception as e:
        print(f"❌ Error en detectar_nuevos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - CREAR Y EDITAR
# ============================================================================

@router.post("")
async def crear_establecimiento(data: EstablecimientoCreate):
    """
    Crear un nuevo establecimiento con sus colores
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Normalizar nombre si no se proporciona
        nombre_norm = data.nombre_normalizado or data.nombre.upper()

        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO establecimientos (
                    nombre, nombre_normalizado, cadena, color_bg, color_text,
                    ciudad, direccion, activo
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.nombre,
                nombre_norm,
                data.cadena,
                data.color_bg,
                data.color_text,
                data.ciudad,
                data.direccion,
                data.activo
            ))
            nuevo_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO establecimientos (
                    nombre, nombre_normalizado, cadena, color_bg, color_text,
                    ciudad, direccion, activo
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data.nombre,
                nombre_norm,
                data.cadena,
                data.color_bg,
                data.color_text,
                data.ciudad,
                data.direccion,
                data.activo
            ))
            nuevo_id = cursor.lastrowid

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "id": nuevo_id,
            "message": f"Establecimiento '{data.nombre}' creado exitosamente"
        }

    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error en crear_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{establecimiento_id}")
async def actualizar_establecimiento(establecimiento_id: int, data: EstablecimientoUpdate):
    """
    Actualizar un establecimiento existente
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Construir UPDATE dinámico
        updates = []
        params = []

        if data.nombre is not None:
            updates.append("nombre = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.nombre)

        if data.nombre_normalizado is not None:
            updates.append("nombre_normalizado = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.nombre_normalizado)

        if data.cadena is not None:
            updates.append("cadena = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.cadena)

        if data.color_bg is not None:
            updates.append("color_bg = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.color_bg)

        if data.color_text is not None:
            updates.append("color_text = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.color_text)

        if data.ciudad is not None:
            updates.append("ciudad = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.ciudad)

        if data.direccion is not None:
            updates.append("direccion = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.direccion)

        if data.activo is not None:
            updates.append("activo = " + ("%s" if database_type == "postgresql" else "?"))
            params.append(data.activo)

        if not updates:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(establecimiento_id)

        query = f"""
            UPDATE establecimientos
            SET {', '.join(updates)}
            WHERE id = {'%s' if database_type == 'postgresql' else '?'}
        """

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": f"Establecimiento {establecimiento_id} actualizado"
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error en actualizar_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{establecimiento_id}")
async def eliminar_establecimiento(establecimiento_id: int, forzar: bool = False):
    """
    Eliminar un establecimiento (solo si no tiene facturas asociadas)

    Parámetros:
    - forzar: Si es True, desactiva en lugar de eliminar
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Verificar si tiene facturas
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM facturas f
                INNER JOIN establecimientos e ON e.id = %s
                WHERE f.establecimiento ILIKE '%' || e.nombre || '%'
                   OR f.establecimiento ILIKE '%' || e.nombre_normalizado || '%'
            """, (establecimiento_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM facturas f
                INNER JOIN establecimientos e ON e.id = ?
                WHERE f.establecimiento LIKE '%' || e.nombre || '%'
                   OR f.establecimiento LIKE '%' || e.nombre_normalizado || '%'
            """, (establecimiento_id,))

        total_facturas = cursor.fetchone()[0]

        if total_facturas > 0 and not forzar:
            cursor.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"No se puede eliminar: tiene {total_facturas} facturas asociadas. Use forzar=true para desactivar."
            )

        if total_facturas > 0 and forzar:
            # Desactivar en lugar de eliminar
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

            mensaje = f"Establecimiento desactivado (tenía {total_facturas} facturas)"
        else:
            # Eliminar definitivamente
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

            mensaje = "Establecimiento eliminado definitivamente"

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": mensaje
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error en eliminar_establecimiento: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# ENDPOINTS - SINCRONIZACIÓN
# ============================================================================

@router.post("/sincronizar")
async def sincronizar_establecimientos():
    """
    Crear automáticamente registros para establecimientos detectados en facturas
    Asigna colores por defecto que luego se pueden personalizar
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Obtener establecimientos no registrados
        if database_type == "postgresql":
            cursor.execute("""
                SELECT DISTINCT f.establecimiento
                FROM facturas f
                WHERE f.establecimiento IS NOT NULL
                  AND f.establecimiento != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM establecimientos e
                      WHERE f.establecimiento ILIKE '%' || e.nombre || '%'
                         OR f.establecimiento ILIKE '%' || e.nombre_normalizado || '%'
                  )
            """)
        else:
            cursor.execute("""
                SELECT DISTINCT f.establecimiento
                FROM facturas f
                WHERE f.establecimiento IS NOT NULL
                  AND f.establecimiento != ''
                  AND NOT EXISTS (
                      SELECT 1 FROM establecimientos e
                      WHERE f.establecimiento LIKE '%' || e.nombre || '%'
                         OR f.establecimiento LIKE '%' || e.nombre_normalizado || '%'
                  )
            """)

        nuevos_nombres = [row[0] for row in cursor.fetchall()]

        # Colores por defecto rotatorios
        colores_defaults = [
            ("#e3f2fd", "#1565c0"),  # Azul
            ("#fff3e0", "#e65100"),  # Naranja
            ("#f3e5f5", "#7b1fa2"),  # Morado
            ("#e8f5e9", "#2e7d32"),  # Verde
            ("#fff9c4", "#f57f17"),  # Amarillo
            ("#ffe0b2", "#ef6c00"),  # Durazno
            ("#ffebee", "#c62828"),  # Rojo
            ("#fce4ec", "#ad1457"),  # Rosa
        ]

        creados = 0
        for i, nombre in enumerate(nuevos_nombres):
            color_bg, color_text = colores_defaults[i % len(colores_defaults)]
            nombre_norm = nombre.upper().strip()

            if database_type == "postgresql":
                cursor.execute("""
                    INSERT INTO establecimientos (
                        nombre, nombre_normalizado, color_bg, color_text, activo
                    )
                    VALUES (%s, %s, %s, %s, TRUE)
                """, (nombre, nombre_norm, color_bg, color_text))
            else:
                cursor.execute("""
                    INSERT INTO establecimientos (
                        nombre, nombre_normalizado, color_bg, color_text, activo
                    )
                    VALUES (?, ?, ?, ?, 1)
                """, (nombre, nombre_norm, color_bg, color_text))

            creados += 1

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "establecimientos_creados": creados,
            "nombres": nuevos_nombres,
            "message": f"Se crearon {creados} nuevos establecimientos con colores por defecto"
        }

    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error en sincronizar: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API de establecimientos cargada (gestión de supermercados con colores)")
