"""
api_reportes.py - Endpoints para Reportes de Usuarios
======================================================
IMPORTANTE: El orden de las rutas importa en FastAPI.
Las rutas específicas (/estadisticas) deben ir ANTES de las dinámicas (/{reporte_id})
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from database import get_db_connection

router = APIRouter(tags=["Reportes"])


# =============================================================================
# MODELOS
# =============================================================================


class CrearReporteRequest(BaseModel):
    inventario_id: int
    tipo_problema: str
    descripcion: Optional[str] = None
    nombre_sugerido: Optional[str] = None


class ResolverReporteRequest(BaseModel):
    producto_correcto_id: int
    notas: Optional[str] = None


class DescartarReporteRequest(BaseModel):
    razon: str


# =============================================================================
# ENDPOINTS USUARIO (APP MÓVIL)
# =============================================================================


@router.post("/api/reportes/crear")
async def crear_reporte(request: CrearReporteRequest, usuario_id: int = Query(...)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        tipos_validos = [
            "nombre_incorrecto",
            "producto_equivocado",
            "duplicado",
            "precio_mal",
            "otro",
        ]
        if request.tipo_problema not in tipos_validos:
            raise HTTPException(status_code=400, detail=f"Tipo de problema inválido")

        cursor.execute(
            """
            SELECT iu.id, iu.producto_maestro_id, pm.nombre_consolidado, pm.codigo_ean, iu.establecimiento_id
            FROM inventario_usuario iu
            LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
            WHERE iu.id = %s AND iu.usuario_id = %s
        """,
            (request.inventario_id, usuario_id),
        )

        item = cursor.fetchone()
        if not item:
            raise HTTPException(
                status_code=404, detail="Item de inventario no encontrado"
            )

        (
            inv_id,
            producto_maestro_id,
            nombre_actual,
            codigo_actual,
            establecimiento_id,
        ) = item

        cursor.execute(
            """
            SELECT id FROM reportes_productos WHERE inventario_id = %s AND estado = 'pendiente'
        """,
            (request.inventario_id,),
        )

        if cursor.fetchone():
            raise HTTPException(
                status_code=400,
                detail="Ya existe un reporte pendiente para este producto",
            )

        cursor.execute(
            """
            INSERT INTO reportes_productos (
                usuario_id, inventario_id, producto_maestro_id, nombre_actual, codigo_actual,
                tipo_problema, descripcion, nombre_sugerido, establecimiento_id, estado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'pendiente')
            RETURNING id, prioridad
        """,
            (
                usuario_id,
                request.inventario_id,
                producto_maestro_id,
                nombre_actual or "Sin nombre",
                codigo_actual,
                request.tipo_problema,
                request.descripcion,
                request.nombre_sugerido,
                establecimiento_id,
            ),
        )

        result = cursor.fetchone()
        conn.commit()

        return {
            "success": True,
            "message": "Reporte creado correctamente",
            "reporte_id": result[0],
            "prioridad": result[1],
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


@router.get("/api/reportes/mis-reportes")
async def mis_reportes(usuario_id: int = Query(...)):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT r.id, r.nombre_actual, r.tipo_problema, r.descripcion, r.estado,
                   r.fecha_creacion, r.fecha_resolucion, pm.nombre_consolidado
            FROM reportes_productos r
            LEFT JOIN productos_maestros_v2 pm ON r.producto_corregido_id = pm.id
            WHERE r.usuario_id = %s
            ORDER BY r.fecha_creacion DESC LIMIT 50
        """,
            (usuario_id,),
        )

        reportes = []
        for row in cursor.fetchall():
            reportes.append(
                {
                    "id": row[0],
                    "nombre_actual": row[1],
                    "tipo_problema": row[2],
                    "descripcion": row[3],
                    "estado": row[4],
                    "fecha_creacion": row[5].isoformat() if row[5] else None,
                    "fecha_resolucion": row[6].isoformat() if row[6] else None,
                    "nombre_corregido": row[7],
                }
            )

        return {"success": True, "reportes": reportes, "total": len(reportes)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# =============================================================================
# ENDPOINTS ADMIN
# =============================================================================


@router.get("/api/admin/reportes")
async def listar_reportes_admin(
    estado: Optional[str] = None,
    prioridad: Optional[int] = None,
    limit: int = Query(50, le=100),
    offset: int = 0,
):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        where_clauses = []
        params = []

        if estado:
            where_clauses.append("r.estado = %s")
            params.append(estado)
        else:
            where_clauses.append("r.estado IN ('pendiente', 'en_revision')")

        if prioridad:
            where_clauses.append("r.prioridad = %s")
            params.append(prioridad)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        cursor.execute(
            f"""
            SELECT r.id, r.nombre_actual, r.codigo_actual, r.tipo_problema, r.descripcion,
                   r.nombre_sugerido, r.estado, r.prioridad, r.fecha_creacion, u.email,
                   pm.nombre_consolidado, pm.codigo_ean, e.nombre_normalizado,
                   (SELECT COUNT(DISTINCT usuario_id) FROM inventario_usuario WHERE producto_maestro_id = r.producto_maestro_id)
            FROM reportes_productos r
            JOIN usuarios u ON r.usuario_id = u.id
            LEFT JOIN productos_maestros_v2 pm ON r.producto_maestro_id = pm.id
            LEFT JOIN establecimientos e ON r.establecimiento_id = e.id
            WHERE {where_sql}
            ORDER BY r.prioridad DESC, r.fecha_creacion ASC
            LIMIT %s OFFSET %s
        """,
            params + [limit, offset],
        )

        reportes = []
        for row in cursor.fetchall():
            reportes.append(
                {
                    "id": row[0],
                    "nombre_actual": row[1],
                    "codigo_actual": row[2],
                    "tipo_problema": row[3],
                    "descripcion": row[4],
                    "nombre_sugerido": row[5],
                    "estado": row[6],
                    "prioridad": row[7],
                    "fecha_creacion": row[8].isoformat() if row[8] else None,
                    "usuario_email": row[9],
                    "nombre_producto_actual": row[10],
                    "codigo_ean": row[11],
                    "establecimiento": row[12],
                    "usuarios_afectados": row[13],
                }
            )

        cursor.execute(
            f"SELECT COUNT(*) FROM reportes_productos r WHERE {where_sql}", params
        )
        total = cursor.fetchone()[0]

        return {"success": True, "reportes": reportes, "total": total}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# ⚠️ CRÍTICO: /estadisticas DEBE estar ANTES de /{reporte_id}
@router.get("/api/admin/reportes/estadisticas")
async def estadisticas_reportes():
    """Obtiene estadísticas de los reportes."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE estado = 'pendiente') as pendientes,
                COUNT(*) FILTER (WHERE estado = 'en_revision') as en_revision,
                COUNT(*) FILTER (WHERE estado = 'corregido') as corregidos,
                COUNT(*) FILTER (WHERE estado = 'descartado') as descartados,
                COUNT(*) FILTER (WHERE fecha_creacion >= CURRENT_DATE - INTERVAL '7 days') as ultimos_7_dias,
                COUNT(*) FILTER (WHERE fecha_resolucion >= CURRENT_DATE - INTERVAL '7 days') as resueltos_7_dias,
                COUNT(*) FILTER (WHERE aprendizaje_aplicado = TRUE) as con_aprendizaje
            FROM reportes_productos
        """
        )

        row = cursor.fetchone()

        cursor.execute(
            """
            SELECT tipo_problema, COUNT(*) FROM reportes_productos
            WHERE fecha_creacion >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY tipo_problema ORDER BY COUNT(*) DESC
        """
        )

        por_tipo = {r[0]: r[1] for r in cursor.fetchall()}

        return {
            "success": True,
            "estadisticas": {
                "pendientes": row[0] or 0,
                "en_revision": row[1] or 0,
                "corregidos": row[2] or 0,
                "descartados": row[3] or 0,
                "ultimos_7_dias": row[4] or 0,
                "resueltos_7_dias": row[5] or 0,
                "con_aprendizaje": row[6] or 0,
                "por_tipo_problema": por_tipo,
            },
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


# Rutas con {reporte_id} DESPUÉS de /estadisticas
@router.get("/api/admin/reportes/{reporte_id}")
async def detalle_reporte(reporte_id: int):
    """Obtiene el detalle completo de un reporte."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT r.*, u.email, pm.nombre_consolidado, pm.codigo_ean, pm.marca, e.nombre_normalizado
            FROM reportes_productos r
            JOIN usuarios u ON r.usuario_id = u.id
            LEFT JOIN productos_maestros_v2 pm ON r.producto_maestro_id = pm.id
            LEFT JOIN establecimientos e ON r.establecimiento_id = e.id
            WHERE r.id = %s
        """,
            (reporte_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Reporte no encontrado")

        columns = [desc[0] for desc in cursor.description]
        reporte = dict(zip(columns, row))

        nombre_buscar = reporte.get("nombre_sugerido") or reporte.get(
            "nombre_actual", ""
        )
        if nombre_buscar:
            palabras = [p for p in nombre_buscar.upper().split() if len(p) >= 3][:3]
            if palabras:
                condiciones = " OR ".join(
                    ["UPPER(nombre_consolidado) LIKE %s" for _ in palabras]
                )
                params = [f"%{p}%" for p in palabras]
                cursor.execute(
                    f"""
                    SELECT id, nombre_consolidado, codigo_ean, marca
                    FROM productos_maestros_v2 WHERE {condiciones}
                    ORDER BY veces_visto DESC LIMIT 10
                """,
                    params,
                )
                reporte["productos_sugeridos"] = [
                    {"id": p[0], "nombre": p[1], "codigo_ean": p[2], "marca": p[3]}
                    for p in cursor.fetchall()
                ]

        return {"success": True, "reporte": reporte}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@router.post("/api/admin/reportes/{reporte_id}/resolver")
async def resolver_reporte(
    reporte_id: int, request: ResolverReporteRequest, admin_id: int = Query(...)
):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT estado, nombre_actual, codigo_actual, establecimiento_id, inventario_id
            FROM reportes_productos WHERE id = %s
        """,
            (reporte_id,),
        )

        reporte = cursor.fetchone()
        if not reporte:
            raise HTTPException(status_code=404, detail="Reporte no encontrado")

        if reporte[0] not in ("pendiente", "en_revision"):
            raise HTTPException(status_code=400, detail=f"Reporte ya está {reporte[0]}")

        nombre_ocr, codigo, establecimiento_id, inventario_id = (
            reporte[1],
            reporte[2],
            reporte[3],
            reporte[4],
        )

        cursor.execute(
            "SELECT id, nombre_consolidado FROM productos_maestros_v2 WHERE id = %s",
            (request.producto_correcto_id,),
        )
        producto = cursor.fetchone()
        if not producto:
            raise HTTPException(
                status_code=404, detail="Producto correcto no encontrado"
            )

        producto_id, nombre_producto = producto

        cursor.execute(
            """
            UPDATE reportes_productos SET estado = 'corregido', producto_corregido_id = %s,
            resuelto_por = %s, fecha_resolucion = CURRENT_TIMESTAMP, notas_resolucion = %s
            WHERE id = %s
        """,
            (producto_id, admin_id, request.notas, reporte_id),
        )

        if inventario_id:
            cursor.execute(
                "UPDATE inventario_usuario SET producto_maestro_id = %s WHERE id = %s",
                (producto_id, inventario_id),
            )

        aprendizaje_aplicado = False
        if nombre_ocr and len(nombre_ocr) >= 3:
            try:
                alias_normalizado = nombre_ocr.lower().strip()
                cursor.execute(
                    """
                    INSERT INTO productos_alias (producto_maestro_id, alias_texto, alias_normalizado,
                        codigo_asociado, establecimiento_id, fuente, confianza)
                    VALUES (%s, %s, %s, %s, %s, 'reporte_usuario', 0.95)
                    ON CONFLICT (alias_normalizado, establecimiento_id) DO UPDATE SET
                        producto_maestro_id = EXCLUDED.producto_maestro_id,
                        veces_usado = productos_alias.veces_usado + 1
                """,
                    (
                        producto_id,
                        nombre_ocr,
                        alias_normalizado,
                        codigo,
                        establecimiento_id,
                    ),
                )
                aprendizaje_aplicado = True
            except:
                pass

        if aprendizaje_aplicado:
            cursor.execute(
                "UPDATE reportes_productos SET aprendizaje_aplicado = TRUE WHERE id = %s",
                (reporte_id,),
            )

        cursor.execute(
            """
            UPDATE reportes_productos SET estado = 'duplicado_reporte', producto_corregido_id = %s,
            fecha_resolucion = CURRENT_TIMESTAMP, notas_resolucion = %s
            WHERE producto_maestro_id = (SELECT producto_maestro_id FROM reportes_productos WHERE id = %s)
            AND estado = 'pendiente' AND id != %s RETURNING id
        """,
            (
                producto_id,
                f"Resuelto con reporte #{reporte_id}",
                reporte_id,
                reporte_id,
            ),
        )

        reportes_cerrados = len(cursor.fetchall())
        conn.commit()

        return {
            "success": True,
            "message": "Reporte resuelto correctamente",
            "producto_asignado": nombre_producto,
            "aprendizaje_aplicado": aprendizaje_aplicado,
            "reportes_similares_cerrados": reportes_cerrados,
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


@router.post("/api/admin/reportes/{reporte_id}/descartar")
async def descartar_reporte(
    reporte_id: int, request: DescartarReporteRequest, admin_id: int = Query(...)
):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE reportes_productos SET estado = 'descartado', resuelto_por = %s,
            fecha_resolucion = CURRENT_TIMESTAMP, notas_resolucion = %s
            WHERE id = %s AND estado IN ('pendiente', 'en_revision') RETURNING id
        """,
            (admin_id, request.razon, reporte_id),
        )

        if not cursor.fetchone():
            raise HTTPException(
                status_code=404, detail="Reporte no encontrado o ya procesado"
            )

        conn.commit()
        return {
            "success": True,
            "message": "Reporte descartado",
            "reporte_id": reporte_id,
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


# =============================================================================
# BÚSQUEDA DE PRODUCTOS
# =============================================================================


@router.get("/api/productos/buscar")
async def buscar_productos(
    q: str = Query(..., min_length=2), limit: int = Query(10, le=50)
):
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        busqueda = q.upper().strip()

        cursor.execute(
            """
            SELECT id, nombre_consolidado, codigo_ean, marca, categoria
            FROM productos_maestros_v2 WHERE codigo_ean = %s OR codigo_plu = %s LIMIT 1
        """,
            (busqueda, busqueda),
        )

        resultado = cursor.fetchone()
        if resultado:
            return {
                "success": True,
                "productos": [
                    {
                        "id": resultado[0],
                        "nombre": resultado[1],
                        "ean": resultado[2],
                        "marca": resultado[3],
                    }
                ],
            }

        palabras = [p for p in busqueda.split() if len(p) >= 2][:4]
        if not palabras:
            return {"success": True, "productos": []}

        condiciones = " AND ".join(
            ["UPPER(nombre_consolidado) LIKE %s" for _ in palabras]
        )
        params = [f"%{p}%" for p in palabras]

        cursor.execute(
            f"""
            SELECT id, nombre_consolidado, codigo_ean, marca FROM productos_maestros_v2
            WHERE {condiciones} ORDER BY veces_visto DESC NULLS LAST LIMIT %s
        """,
            params + [limit],
        )

        productos = [
            {"id": r[0], "nombre": r[1], "ean": r[2], "marca": r[3]}
            for r in cursor.fetchall()
        ]
        return {"success": True, "productos": productos}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
