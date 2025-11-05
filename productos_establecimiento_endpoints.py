"""
===============================================================================
ENDPOINTS PARA GESTIÓN DE PRODUCTOS POR ESTABLECIMIENTO
===============================================================================
Maneja códigos PLU específicos por establecimiento
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os

from database import get_db_connection

router = APIRouter(prefix="/api/productos-establecimiento", tags=["productos-establecimiento"])


# ============================================================================
# MODELOS
# ============================================================================

class ProductoEstablecimientoCreate(BaseModel):
    producto_maestro_id: int
    establecimiento_nombre: str
    cadena: Optional[str] = None
    codigo_plu_local: Optional[str] = None
    codigo_interno: Optional[str] = None
    nombre_local: Optional[str] = None
    precio_actual: Optional[int] = None


class ProductoEstablecimientoUpdate(BaseModel):
    codigo_plu_local: Optional[str] = None
    codigo_interno: Optional[str] = None
    nombre_local: Optional[str] = None
    precio_actual: Optional[int] = None
    disponible: Optional[bool] = None


# ============================================================================
# ENDPOINTS CRUD
# ============================================================================

@router.get("/{producto_maestro_id}/establecimientos")
async def obtener_variaciones_producto(producto_maestro_id: int):
    """
    Obtener todas las variaciones de un producto por establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    pe.id,
                    pe.establecimiento_nombre,
                    pe.cadena,
                    pe.codigo_plu_local,
                    pe.codigo_interno,
                    pe.nombre_local,
                    pe.precio_actual,
                    pe.disponible,
                    pe.total_reportes,
                    pe.ultima_actualizacion,
                    pm.nombre_normalizado,
                    pm.codigo_ean
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.producto_maestro_id = %s
                ORDER BY pe.establecimiento_nombre
            """, (producto_maestro_id,))
        else:
            cursor.execute("""
                SELECT
                    pe.id,
                    pe.establecimiento_nombre,
                    pe.cadena,
                    pe.codigo_plu_local,
                    pe.codigo_interno,
                    pe.nombre_local,
                    pe.precio_actual,
                    pe.disponible,
                    pe.total_reportes,
                    pe.ultima_actualizacion,
                    pm.nombre_normalizado,
                    pm.codigo_ean
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.producto_maestro_id = ?
                ORDER BY pe.establecimiento_nombre
            """, (producto_maestro_id,))

        variaciones = []
        for row in cursor.fetchall():
            variaciones.append({
                "id": row[0],
                "establecimiento_nombre": row[1],
                "cadena": row[2],
                "codigo_plu_local": row[3],
                "codigo_interno": row[4],
                "nombre_local": row[5],
                "precio_actual": row[6],
                "disponible": row[7],
                "total_reportes": row[8],
                "ultima_actualizacion": str(row[9]) if row[9] else None,
                "producto_maestro": {
                    "nombre_normalizado": row[10],
                    "codigo_ean": row[11]
                }
            })

        conn.close()

        return {
            "success": True,
            "producto_maestro_id": producto_maestro_id,
            "total_variaciones": len(variaciones),
            "variaciones": variaciones
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def crear_variacion_establecimiento(data: ProductoEstablecimientoCreate):
    """
    Crear una nueva variación de producto para un establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que el producto maestro existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE id = %s",
                (data.producto_maestro_id,)
            )
        else:
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE id = ?",
                (data.producto_maestro_id,)
            )

        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Producto maestro no encontrado")

        # Crear variación
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO productos_por_establecimiento (
                    producto_maestro_id,
                    establecimiento_nombre,
                    cadena,
                    codigo_plu_local,
                    codigo_interno,
                    nombre_local,
                    precio_actual
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                data.producto_maestro_id,
                data.establecimiento_nombre,
                data.cadena,
                data.codigo_plu_local,
                data.codigo_interno,
                data.nombre_local,
                data.precio_actual
            ))

            variacion_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO productos_por_establecimiento (
                    producto_maestro_id,
                    establecimiento_nombre,
                    cadena,
                    codigo_plu_local,
                    codigo_interno,
                    nombre_local,
                    precio_actual
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                data.producto_maestro_id,
                data.establecimiento_nombre,
                data.cadena,
                data.codigo_plu_local,
                data.codigo_interno,
                data.nombre_local,
                data.precio_actual
            ))

            variacion_id = cursor.lastrowid

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Variación creada",
            "variacion_id": variacion_id
        }

    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{variacion_id}")
async def actualizar_variacion_establecimiento(
    variacion_id: int,
    data: ProductoEstablecimientoUpdate
):
    """
    Actualizar una variación existente
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Construir query dinámico
        updates = []
        params = []

        if data.codigo_plu_local is not None:
            updates.append("codigo_plu_local = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "codigo_plu_local = ?")
            params.append(data.codigo_plu_local)

        if data.codigo_interno is not None:
            updates.append("codigo_interno = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "codigo_interno = ?")
            params.append(data.codigo_interno)

        if data.nombre_local is not None:
            updates.append("nombre_local = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "nombre_local = ?")
            params.append(data.nombre_local)

        if data.precio_actual is not None:
            updates.append("precio_actual = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "precio_actual = ?")
            params.append(data.precio_actual)

        if data.disponible is not None:
            updates.append("disponible = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "disponible = ?")
            params.append(data.disponible)

        if not updates:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(variacion_id)

        query = f"UPDATE productos_por_establecimiento SET {', '.join(updates)} WHERE id = {'%s' if os.environ.get('DATABASE_TYPE') == 'postgresql' else '?'}"

        cursor.execute(query, params)

        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(status_code=404, detail="Variación no encontrada")

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Variación actualizada",
            "variacion_id": variacion_id
        }

    except HTTPException:
        if conn:
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/buscar-por-plu")
async def buscar_por_plu(plu: str, establecimiento: str):
    """
    Buscar producto por PLU en un establecimiento específico
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    pe.id,
                    pe.producto_maestro_id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pe.codigo_plu_local,
                    pe.nombre_local,
                    pe.precio_actual,
                    pe.disponible
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.codigo_plu_local = %s
                  AND pe.establecimiento_nombre ILIKE %s
                LIMIT 1
            """, (plu, f"%{establecimiento}%"))
        else:
            cursor.execute("""
                SELECT
                    pe.id,
                    pe.producto_maestro_id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pe.codigo_plu_local,
                    pe.nombre_local,
                    pe.precio_actual,
                    pe.disponible
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.codigo_plu_local = ?
                  AND pe.establecimiento_nombre LIKE ?
                LIMIT 1
            """, (plu, f"%{establecimiento}%"))

        row = cursor.fetchone()
        conn.close()

        if not row:
            return {
                "success": False,
                "encontrado": False,
                "message": f"No se encontró producto con PLU {plu} en {establecimiento}"
            }

        return {
            "success": True,
            "encontrado": True,
            "producto": {
                "variacion_id": row[0],
                "producto_maestro_id": row[1],
                "codigo_ean": row[2],
                "nombre_normalizado": row[3],
                "marca": row[4],
                "categoria": row[5],
                "codigo_plu_local": row[6],
                "nombre_local": row[7],
                "precio_actual": row[8],
                "disponible": row[9]
            }
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conflictos-plu")
async def detectar_conflictos_plu():
    """
    Detectar PLUs duplicados en el mismo establecimiento
    (diferentes productos con el mismo PLU)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    pe.establecimiento_nombre,
                    pe.codigo_plu_local,
                    COUNT(DISTINCT pe.producto_maestro_id) as productos_diferentes,
                    STRING_AGG(DISTINCT pm.nombre_normalizado, ', ') as nombres_productos,
                    STRING_AGG(DISTINCT pm.id::text, ', ') as ids_productos
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.codigo_plu_local IS NOT NULL AND pe.codigo_plu_local != ''
                GROUP BY pe.establecimiento_nombre, pe.codigo_plu_local
                HAVING COUNT(DISTINCT pe.producto_maestro_id) > 1
                ORDER BY productos_diferentes DESC, pe.establecimiento_nombre
            """)
        else:
            cursor.execute("""
                SELECT
                    pe.establecimiento_nombre,
                    pe.codigo_plu_local,
                    COUNT(DISTINCT pe.producto_maestro_id) as productos_diferentes,
                    GROUP_CONCAT(DISTINCT pm.nombre_normalizado) as nombres_productos,
                    GROUP_CONCAT(DISTINCT pm.id) as ids_productos
                FROM productos_por_establecimiento pe
                JOIN productos_maestros pm ON pe.producto_maestro_id = pm.id
                WHERE pe.codigo_plu_local IS NOT NULL AND pe.codigo_plu_local != ''
                GROUP BY pe.establecimiento_nombre, pe.codigo_plu_local
                HAVING COUNT(DISTINCT pe.producto_maestro_id) > 1
                ORDER BY productos_diferentes DESC, pe.establecimiento_nombre
            """)

        conflictos = []
        for row in cursor.fetchall():
            conflictos.append({
                "establecimiento": row[0],
                "plu": row[1],
                "productos_diferentes": row[2],
                "nombres": row[3],
                "ids": row[4],
                "severidad": "alta" if row[2] > 2 else "media"
            })

        conn.close()

        return {
            "success": True,
            "total_conflictos": len(conflictos),
            "conflictos": conflictos
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de productos por establecimiento cargados")
