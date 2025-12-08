"""
api_aprendizaje.py - Endpoints para el Sistema de Aprendizaje
=============================================================
Estos endpoints permiten al admin corregir productos y que el sistema aprenda.

ENDPOINTS:
- POST /api/admin/corregir-item      → Corrige un item y aprende
- POST /api/admin/aprender           → Enseña un alias manualmente
- GET  /api/admin/aprendizaje/stats  → Estadísticas del sistema
- GET  /api/admin/alias/{producto_id} → Lista alias de un producto
- DELETE /api/admin/alias/{alias_id}  → Elimina un alias incorrecto

AUTOR: LecFac Team
VERSIÓN: 1.0
=============================================================
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from database import get_db_connection

# Importar sistema de aprendizaje
from learning_system import (
    aprender_correccion,
    aprender_desde_item_factura,
    buscar_producto_por_alias,
    obtener_estadisticas_aprendizaje,
    importar_correcciones_masivas,
)

router = APIRouter(prefix="/api/admin", tags=["Aprendizaje"])


# =============================================================================
# MODELOS
# =============================================================================
class CorreccionItemRequest(BaseModel):
    """Request para corregir un item de factura"""

    item_factura_id: int
    producto_maestro_id_correcto: int
    usuario_id: Optional[int] = None


class AprenderAliasRequest(BaseModel):
    """Request para enseñar un alias manualmente"""

    texto_ocr: str
    producto_maestro_id: int
    establecimiento_id: Optional[int] = None
    codigo: Optional[str] = None


class ImportarCorreccionesRequest(BaseModel):
    """Request para importar correcciones masivas"""

    correcciones: List[dict]


# =============================================================================
# ENDPOINTS
# =============================================================================


@router.post("/corregir-item")
async def corregir_item_factura(request: CorreccionItemRequest):
    """
    Corrige un item de factura y enseña al sistema.

    Cuando el admin corrige un item:
    1. Actualiza el producto_maestro_id del item
    2. Guarda el texto OCR como alias del producto correcto
    3. Próxima vez que vea ese texto, ya sabrá qué producto es
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Verificar que el item existe
        cursor.execute(
            """
            SELECT itf.id, itf.nombre_leido, itf.producto_maestro_id, f.establecimiento_id
            FROM items_factura itf
            JOIN facturas f ON itf.factura_id = f.id
            WHERE itf.id = %s
        """,
            (request.item_factura_id,),
        )

        item = cursor.fetchone()
        if not item:
            raise HTTPException(status_code=404, detail="Item no encontrado")

        item_id, nombre_ocr, producto_anterior, establecimiento_id = item

        # 2. Verificar que el producto correcto existe
        cursor.execute(
            """
            SELECT id, nombre_consolidado FROM productos_maestros_v2
            WHERE id = %s
        """,
            (request.producto_maestro_id_correcto,),
        )

        producto = cursor.fetchone()
        if not producto:
            raise HTTPException(
                status_code=404, detail="Producto maestro no encontrado"
            )

        producto_id, nombre_producto = producto

        # 3. Actualizar el item con el producto correcto
        cursor.execute(
            """
            UPDATE items_factura
            SET producto_maestro_id = %s,
                matching_confianza = 1.0
            WHERE id = %s
        """,
            (producto_id, item_id),
        )

        # 4. Aprender la corrección
        aprendido = aprender_desde_item_factura(
            cursor=cursor,
            conn=conn,
            item_factura_id=item_id,
            producto_maestro_id_correcto=producto_id,
            usuario_id=request.usuario_id,
        )

        conn.commit()

        return {
            "success": True,
            "message": f"Item corregido y sistema actualizado",
            "item_id": item_id,
            "texto_ocr": nombre_ocr,
            "producto_anterior_id": producto_anterior,
            "producto_correcto_id": producto_id,
            "producto_nombre": nombre_producto,
            "aprendido": aprendido,
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/aprender")
async def aprender_alias_manual(request: AprenderAliasRequest):
    """
    Enseña un alias manualmente sin necesidad de corregir un item.

    Útil para:
    - Agregar variaciones conocidas de productos
    - Pre-cargar alias antes de escanear
    - Corregir errores OCR comunes
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que el producto existe
        cursor.execute(
            """
            SELECT id, nombre_consolidado FROM productos_maestros_v2
            WHERE id = %s
        """,
            (request.producto_maestro_id,),
        )

        producto = cursor.fetchone()
        if not producto:
            raise HTTPException(
                status_code=404, detail="Producto maestro no encontrado"
            )

        producto_id, nombre_producto = producto

        # Aprender el alias
        aprendido = aprender_correccion(
            cursor=cursor,
            conn=conn,
            texto_ocr=request.texto_ocr,
            producto_maestro_id=producto_id,
            establecimiento_id=request.establecimiento_id,
            codigo=request.codigo,
            fuente="manual",
        )

        if not aprendido:
            raise HTTPException(status_code=400, detail="No se pudo guardar el alias")

        return {
            "success": True,
            "message": f"Alias aprendido correctamente",
            "texto_ocr": request.texto_ocr,
            "producto_id": producto_id,
            "producto_nombre": nombre_producto,
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/aprendizaje/stats")
async def obtener_stats_aprendizaje():
    """
    Obtiene estadísticas del sistema de aprendizaje.

    Retorna:
    - Total de alias guardados
    - Alias por fuente (manual, corrección, automático)
    - Alias más usados
    - Correcciones de la última semana
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        stats = obtener_estadisticas_aprendizaje(cursor)

        return {"success": True, "data": stats}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/alias/{producto_maestro_id}")
async def listar_alias_producto(producto_maestro_id: int):
    """
    Lista todos los alias de un producto.

    Útil para:
    - Ver qué textos OCR mapean a un producto
    - Identificar alias incorrectos
    - Debugging
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar producto
        cursor.execute(
            """
            SELECT nombre_consolidado FROM productos_maestros_v2 WHERE id = %s
        """,
            (producto_maestro_id,),
        )

        producto = cursor.fetchone()
        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener alias
        cursor.execute(
            """
            SELECT
                pa.id,
                pa.alias_texto,
                pa.codigo_asociado,
                e.nombre_normalizado as establecimiento,
                pa.fuente,
                pa.confianza,
                pa.veces_usado,
                pa.fecha_creacion
            FROM productos_alias pa
            LEFT JOIN establecimientos e ON pa.establecimiento_id = e.id
            WHERE pa.producto_maestro_id = %s
            ORDER BY pa.veces_usado DESC
        """,
            (producto_maestro_id,),
        )

        alias_list = []
        for row in cursor.fetchall():
            alias_list.append(
                {
                    "id": row[0],
                    "texto": row[1],
                    "codigo": row[2],
                    "establecimiento": row[3],
                    "fuente": row[4],
                    "confianza": float(row[5]),
                    "veces_usado": row[6],
                    "fecha_creacion": row[7].isoformat() if row[7] else None,
                }
            )

        return {
            "success": True,
            "producto": producto[0],
            "producto_id": producto_maestro_id,
            "total_alias": len(alias_list),
            "alias": alias_list,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.delete("/alias/{alias_id}")
async def eliminar_alias(alias_id: int):
    """
    Elimina un alias incorrecto.

    Usar cuando un alias está mal mapeado y el sistema
    está cometiendo errores por culpa de él.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar que existe
        cursor.execute(
            """
            SELECT pa.alias_texto, pm.nombre_consolidado
            FROM productos_alias pa
            JOIN productos_maestros_v2 pm ON pa.producto_maestro_id = pm.id
            WHERE pa.id = %s
        """,
            (alias_id,),
        )

        alias = cursor.fetchone()
        if not alias:
            raise HTTPException(status_code=404, detail="Alias no encontrado")

        alias_texto, producto_nombre = alias

        # Eliminar
        cursor.execute("DELETE FROM productos_alias WHERE id = %s", (alias_id,))
        conn.commit()

        return {
            "success": True,
            "message": "Alias eliminado",
            "alias_texto": alias_texto,
            "producto": producto_nombre,
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/importar-correcciones")
async def importar_correcciones(request: ImportarCorreccionesRequest):
    """
    Importa múltiples correcciones de una vez.

    Útil para:
    - Migrar correcciones de otro sistema
    - Cargar alias conocidos en masa
    - Inicializar el sistema con datos históricos

    Body:
    {
        "correcciones": [
            {"texto_ocr": "P HIG ROSAL30H", "producto_maestro_id": 123},
            {"texto_ocr": "HUEV ORO AA", "producto_maestro_id": 456}
        ]
    }
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = importar_correcciones_masivas(
            cursor=cursor, conn=conn, correcciones=request.correcciones
        )

        return {
            "success": True,
            "importados": resultado["importados"],
            "errores": resultado["errores"],
            "total": resultado["total"],
        }

    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.get("/buscar-alias")
async def buscar_por_alias(texto: str, establecimiento_id: Optional[int] = None):
    """
    Busca un producto por su alias OCR.

    Útil para testing y debugging del sistema de aprendizaje.
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = buscar_producto_por_alias(
            cursor=cursor, texto_ocr=texto, establecimiento_id=establecimiento_id
        )

        if resultado:
            return {"success": True, "encontrado": True, "data": resultado}
        else:
            return {
                "success": True,
                "encontrado": False,
                "message": "No se encontró alias para este texto",
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# =============================================================================
# INTEGRACIÓN CON MAIN.PY
# =============================================================================
"""
Para agregar estos endpoints a tu main.py:

from api_aprendizaje import router as aprendizaje_router
app.include_router(aprendizaje_router)

Esto habilitará todos los endpoints bajo /api/admin/
"""
