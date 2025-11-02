#from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import List, Optional
from difflib import SequenceMatcher
from database import get_db_connection
import os
from datetime import datetime
from typing import List, Dict, Any


router = APIRouter()

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/stats")
async def estadisticas_stats():
    """Obtener estadísticas generales del sistema (ruta legacy)"""
    return await estadisticas()


@router.get("/estadisticas")
async def estadisticas():
    """Obtener estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Total de usuarios
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0]

        # 2. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]

        # 3. Productos únicos en catálogo
        cursor.execute("SELECT COUNT(DISTINCT id) FROM productos_maestros")
        total_productos = cursor.fetchone()[0]

        # 4. Calidad promedio (puntaje de confianza)
        cursor.execute(
            """
            SELECT COALESCE(AVG(puntaje_confianza), 0)
            FROM facturas
            WHERE puntaje_confianza IS NOT NULL
        """
        )
        calidad_promedio = int(cursor.fetchone()[0] or 0)

        # 5. Facturas con errores (puntaje < 70)
        cursor.execute(
            """
            SELECT COUNT(*) FROM facturas
            WHERE COALESCE(puntaje_confianza, 0) < 70
        """
        )
        facturas_con_errores = cursor.fetchone()[0]

        # 6. Productos sin categoría
        cursor.execute(
            """
            SELECT COUNT(*) FROM productos_maestros
            WHERE categoria IS NULL OR categoria = ''
        """
        )
        productos_sin_categoria = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "total_productos": total_productos,
            "calidad_promedio": calidad_promedio,
            "facturas_con_errores": facturas_con_errores,
            "productos_sin_categoria": productos_sin_categoria,
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats-detailed")
async def estadisticas_detalladas():
    """Obtener estadísticas detalladas con desglose por estado"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]

        # 2. Productos únicos en catálogo global
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        productos_unicos = cursor.fetchone()[0]

        # 3. Alertas activas
        cursor.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT producto_maestro_id
                FROM precios_productos
                WHERE producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
                HAVING COUNT(DISTINCT precio) > 1
            ) AS cambios
        """
        )
        alertas_activas = cursor.fetchone()[0]

        # 4. Desglose por estado
        cursor.execute(
            """
            SELECT
                COALESCE(estado_validacion, 'sin_estado') as estado,
                COUNT(*) as cantidad
            FROM facturas
            GROUP BY estado_validacion
        """
        )

        por_estado = {}
        for row in cursor.fetchall():
            estado = row[0]
            cantidad = row[1]
            por_estado[estado] = cantidad

        # 5. Total pendientes
        cursor.execute(
            """
            SELECT COUNT(*) FROM facturas
            WHERE COALESCE(estado_validacion, 'pendiente') NOT IN ('revisada', 'validada')
        """
        )
        pendientes_total = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "total_facturas": total_facturas,
            "productos_unicos": productos_unicos,
            "alertas_activas": alertas_activas,
            "por_estado": por_estado,
            "pendientes_total": pendientes_total,
        }
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos/catalogo")
async def obtener_catalogo_productos():
    """Obtener catálogo global de productos (productos_maestros)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.marca,
                pm.total_reportes,
                pm.precio_promedio_global,
                pm.precio_minimo_historico,
                pm.precio_maximo_historico
            FROM productos_maestros pm
            ORDER BY pm.total_reportes DESC
            LIMIT 500
        """
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2] or "Sin nombre",
                    "marca": row[3],
                    "veces_visto": row[4] or 0,
                    "precio_promedio": int(row[5]) if row[5] else 0,
                    "precio_min": int(row[6]) if row[6] else 0,
                    "precio_max": int(row[7]) if row[7] else 0,
                    "verificado": False,
                    "necesita_revision": False,
                }
            )

        cursor.close()
        conn.close()

        return {"productos": productos, "total": len(productos)}
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error en catálogo: {str(e)}")


@router.get("/facturas")
async def obtener_facturas():
    """Obtener todas las facturas con contador de productos de items_factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                f.id,
                f.establecimiento,
                f.total_factura,
                f.fecha_cargue,
                f.estado_validacion,
                f.tiene_imagen,
                COUNT(i.id) as num_productos
            FROM facturas f
            LEFT JOIN items_factura i ON i.factura_id = f.id
            GROUP BY f.id, f.establecimiento, f.total_factura, f.fecha_cargue, f.estado_validacion, f.tiene_imagen
            ORDER BY f.fecha_cargue DESC
            LIMIT 100
        """
        )

        facturas = []
        for row in cursor.fetchall():
            facturas.append(
                {
                    "id": row[0],
                    "establecimiento": row[1] or "Sin datos",
                    "total": int(row[2]) if row[2] else 0,
                    "fecha": str(row[3]) if row[3] else "",
                    "estado": row[4] or "pendiente",
                    "tiene_imagen": row[5] or False,
                    "productos": row[6] or 0,
                }
            )

        cursor.close()
        conn.close()

        return {"facturas": facturas, "total": len(facturas)}
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facturas/verificacion")
async def obtener_facturas_verificacion():
    """Obtener facturas para verificación manual"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                f.id,
                f.establecimiento,
                f.total_factura,
                f.fecha_cargue,
                f.estado_validacion,
                f.puntaje_confianza,
                COUNT(i.id) as num_items
            FROM facturas f
            LEFT JOIN items_factura i ON i.factura_id = f.id
            WHERE COALESCE(f.estado_validacion, 'pendiente') IN ('pendiente', 'necesita_revision')
            GROUP BY f.id, f.establecimiento, f.total_factura, f.fecha_cargue, f.estado_validacion, f.puntaje_confianza
            ORDER BY
                CASE
                    WHEN f.puntaje_confianza IS NULL THEN 1
                    ELSE 0
                END,
                f.puntaje_confianza ASC,
                f.fecha_cargue DESC
            LIMIT 50
        """
        )

        facturas = []
        for row in cursor.fetchall():
            facturas.append(
                {
                    "id": row[0],
                    "establecimiento": row[1] or "Sin datos",
                    "total": int(row[2]) if row[2] else 0,
                    "fecha": str(row[3]) if row[3] else "",
                    "estado": row[4] or "pendiente",
                    "puntaje": row[5] or 0,
                    "items": row[6] or 0,
                }
            )

        cursor.close()
        conn.close()

        return {"facturas": facturas, "total": len(facturas)}
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/facturas/{factura_id}/validar")
async def validar_factura(factura_id: int, estado: str):
    """Marcar factura como validada"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE facturas
                SET estado_validacion = %s
                WHERE id = %s
            """,
                (estado, factura_id),
            )
        else:
            cursor.execute(
                """
                UPDATE facturas
                SET estado_validacion = ?
                WHERE id = ?
            """,
                (estado, factura_id),
            )

        conn.commit()
        cursor.close()
        conn.close()

        return {"success": True, "message": f"Factura {factura_id} marcada como {estado}"}
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """Eliminar una factura y sus items asociados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        # Primero eliminar items
        if database_type == "postgresql":
            cursor.execute("DELETE FROM items_factura WHERE factura_id = %s", (factura_id,))
            # Eliminar processing_jobs
            try:
                cursor.execute("DELETE FROM processing_jobs WHERE factura_id = %s", (factura_id,))
                print(f"✓ Processing jobs eliminados para factura {factura_id}")
            except:
                pass
            cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("DELETE FROM items_factura WHERE factura_id = ?", (factura_id,))
            # Eliminar processing_jobs
            try:
                cursor.execute("DELETE FROM processing_jobs WHERE factura_id = ?", (factura_id,))
                print(f"✓ Processing jobs eliminados para factura {factura_id}")
            except:
                pass
            cursor.execute("DELETE FROM facturas WHERE id = ?", (factura_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return {"success": True, "message": f"Factura {factura_id} eliminada"}
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data/clean")
async def limpiar_datos():
    """Eliminar facturas antiguas de baja calidad"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        # Eliminar facturas con bajo puntaje de más de 90 días
        if database_type == "postgresql":
            cursor.execute(
                """
                DELETE FROM items_factura
                WHERE factura_id IN (
                    SELECT id FROM facturas
                    WHERE fecha_cargue < NOW() - INTERVAL '90 days'
                    AND COALESCE(puntaje_confianza, 0) < 50
                )
            """
            )

            cursor.execute(
                """
                DELETE FROM facturas
                WHERE fecha_cargue < NOW() - INTERVAL '90 days'
                AND COALESCE(puntaje_confianza, 0) < 50
            """
            )
        else:
            cursor.execute(
                """
                DELETE FROM items_factura
                WHERE factura_id IN (
                    SELECT id FROM facturas
                    WHERE fecha_cargue < date('now', '-90 days')
                    AND COALESCE(puntaje_confianza, 0) < 50
                )
            """
            )

            cursor.execute(
                """
                DELETE FROM facturas
                WHERE fecha_cargue < date('now', '-90 days')
                AND COALESCE(puntaje_confianza, 0) < 50
            """
            )

        facturas_eliminadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "facturas_eliminadas": facturas_eliminadas,
            "message": "Datos antiguos limpiados",
        }
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/duplicados/detectar")
async def detectar_duplicados():
    """Detectar posibles productos duplicados en el catálogo"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre_normalizado,
                marca,
                total_reportes
            FROM productos_maestros
            WHERE nombre_normalizado IS NOT NULL
            ORDER BY total_reportes DESC
        """
        )

        productos = cursor.fetchall()
        duplicados_encontrados = []

        # Comparar productos por similitud de nombre
        for i, prod1 in enumerate(productos):
            for prod2 in productos[i + 1 :]:
                # Calcular similitud entre nombres
                similitud = SequenceMatcher(
                    None, prod1[2].lower(), prod2[2].lower()
                ).ratio()

                # Si son muy similares pero diferentes IDs
                if similitud > 0.85 and prod1[0] != prod2[0]:
                    duplicados_encontrados.append(
                        {
                            "producto1_id": prod1[0],
                            "producto1_nombre": prod1[2],
                            "producto1_codigo": prod1[1],
                            "producto1_reportes": prod1[4],
                            "producto2_id": prod2[0],
                            "producto2_nombre": prod2[2],
                            "producto2_codigo": prod2[1],
                            "producto2_reportes": prod2[4],
                            "similitud": round(similitud * 100, 1),
                        }
                    )

                # Limitar a 50 duplicados
                if len(duplicados_encontrados) >= 50:
                    break

            if len(duplicados_encontrados) >= 50:
                break

        cursor.close()
        conn.close()

        return {
            "duplicados": duplicados_encontrados,
            "total": len(duplicados_encontrados),
            "productos_analizados": len(productos),
        }
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/duplicados/fusionar")
async def fusionar_productos(producto_principal_id: int, producto_duplicado_id: int):
    """Fusionar dos productos duplicados en uno solo"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        # 1. Actualizar items_factura para que apunten al producto principal
        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE items_factura
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """,
                (producto_principal_id, producto_duplicado_id),
            )
        else:
            cursor.execute(
                """
                UPDATE items_factura
                SET producto_maestro_id = ?
                WHERE producto_maestro_id = ?
            """,
                (producto_principal_id, producto_duplicado_id),
            )

        # 2. Actualizar precios_productos
        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE precios_productos
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """,
                (producto_principal_id, producto_duplicado_id),
            )
        else:
            cursor.execute(
                """
                UPDATE precios_productos
                SET producto_maestro_id = ?
                WHERE producto_maestro_id = ?
            """,
                (producto_principal_id, producto_duplicado_id),
            )

        # 3. Actualizar inventario_usuario
        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """,
                (producto_principal_id, producto_duplicado_id),
            )
        else:
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET producto_maestro_id = ?
                WHERE producto_maestro_id = ?
            """,
                (producto_principal_id, producto_duplicado_id),
            )

        # 4. Eliminar producto duplicado
        if database_type == "postgresql":
            cursor.execute(
                """
                DELETE FROM productos_maestros
                WHERE id = %s
            """,
                (producto_duplicado_id,),
            )
        else:
            cursor.execute(
                """
                DELETE FROM productos_maestros
                WHERE id = ?
            """,
                (producto_duplicado_id,),
            )

        # 5. Recalcular estadísticas del producto principal
        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE productos_maestros
                SET total_reportes = (
                    SELECT COUNT(*) FROM items_factura WHERE producto_maestro_id = %s
                ),
                precio_promedio_global = (
                    SELECT AVG(precio) FROM precios_productos WHERE producto_maestro_id = %s
                )
                WHERE id = %s
            """,
                (producto_principal_id, producto_principal_id, producto_principal_id),
            )
        else:
            cursor.execute(
                """
                UPDATE productos_maestros
                SET total_reportes = (
                    SELECT COUNT(*) FROM items_factura WHERE producto_maestro_id = ?
                ),
                precio_promedio_global = (
                    SELECT AVG(precio) FROM precios_productos WHERE producto_maestro_id = ?
                )
                WHERE id = ?
            """,
                (producto_principal_id, producto_principal_id, producto_principal_id),
            )

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": f"Productos fusionados: {producto_duplicado_id} → {producto_principal_id}",
        }
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventarios")
async def obtener_inventarios():
    """
    Obtener resumen de inventarios por usuario
    Incluye productos en stock bajo, medio y normal
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        # Obtener usuarios con sus productos en inventario
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    u.id as usuario_id,
                    u.email as usuario_email,
                    COUNT(DISTINCT iu.id) as total_productos,
                    SUM(CASE WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1 ELSE 0 END) as productos_bajo,
                    SUM(CASE WHEN iu.cantidad_actual > iu.nivel_alerta
                             AND iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 1 ELSE 0 END) as productos_medio,
                    SUM(CASE WHEN iu.cantidad_actual > (iu.nivel_alerta * 2) THEN 1 ELSE 0 END) as productos_normal
                FROM usuarios u
                LEFT JOIN inventario_usuario iu ON u.id = iu.usuario_id
                GROUP BY u.id, u.email
                HAVING COUNT(DISTINCT iu.id) > 0
                ORDER BY productos_bajo DESC, total_productos DESC
            """
            )
        else:
            cursor.execute(
                """
                SELECT
                    u.id as usuario_id,
                    u.email as usuario_email,
                    COUNT(DISTINCT iu.id) as total_productos,
                    SUM(CASE WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1 ELSE 0 END) as productos_bajo,
                    SUM(CASE WHEN iu.cantidad_actual > iu.nivel_alerta
                             AND iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 1 ELSE 0 END) as productos_medio,
                    SUM(CASE WHEN iu.cantidad_actual > (iu.nivel_alerta * 2) THEN 1 ELSE 0 END) as productos_normal
                FROM usuarios u
                LEFT JOIN inventario_usuario iu ON u.id = iu.usuario_id
                GROUP BY u.id, u.email
                HAVING COUNT(DISTINCT iu.id) > 0
                ORDER BY productos_bajo DESC, total_productos DESC
            """
            )

        inventarios = []
        for row in cursor.fetchall():
            usuario_id = row[0]

            # Obtener productos críticos (stock bajo) para este usuario
            if database_type == "postgresql":
                cursor.execute(
                    """
                    SELECT
                        pm.nombre_normalizado,
                        iu.cantidad_actual,
                        iu.unidad_medida
                    FROM inventario_usuario iu
                    JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                    WHERE iu.usuario_id = %s
                      AND iu.cantidad_actual <= iu.nivel_alerta
                    ORDER BY iu.cantidad_actual ASC
                    LIMIT 10
                """,
                    (usuario_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT
                        pm.nombre_normalizado,
                        iu.cantidad_actual,
                        iu.unidad_medida
                    FROM inventario_usuario iu
                    JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                    WHERE iu.usuario_id = ?
                      AND iu.cantidad_actual <= iu.nivel_alerta
                    ORDER BY iu.cantidad_actual ASC
                    LIMIT 10
                """,
                    (usuario_id,),
                )

            productos_criticos = []
            for prod_row in cursor.fetchall():
                productos_criticos.append(
                    {
                        "nombre": prod_row[0],
                        "cantidad": int(prod_row[1]) if prod_row[1] else 0,
                        "unidad": prod_row[2] or "unidades",
                    }
                )

            inventarios.append(
                {
                    "usuario_id": usuario_id,
                    "usuario_email": row[1],
                    "total_productos": row[2] or 0,
                    "productos_bajo": row[3] or 0,
                    "productos_medio": row[4] or 0,
                    "productos_normal": row[5] or 0,
                    "productos_criticos": productos_criticos,
                }
            )

        # Calcular estadísticas globales
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT iu.usuario_id) as usuarios,
                    SUM(CASE WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1 ELSE 0 END) as bajo,
                    COUNT(DISTINCT iu.producto_maestro_id) as productos_unicos
                FROM inventario_usuario iu
            """
            )
        else:
            cursor.execute(
                """
                SELECT
                    COUNT(DISTINCT iu.usuario_id) as usuarios,
                    SUM(CASE WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1 ELSE 0 END) as bajo,
                    COUNT(DISTINCT iu.producto_maestro_id) as productos_unicos
                FROM inventario_usuario iu
            """
            )

        stats_row = cursor.fetchone()

        # Contar alertas activas
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*) FROM alertas_usuario
                WHERE activa = TRUE AND enviada = FALSE
            """
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*) FROM alertas_usuario
                WHERE activa = 1 AND enviada = 0
            """
            )

        alertas_count = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "success": True,
            "inventarios": inventarios,
            "estadisticas": {
                "usuarios_con_inventario": stats_row[0] or 0,
                "productos_stock_bajo": stats_row[1] or 0,
                "productos_unicos": stats_row[2] or 0,
                "alertas_activas": alertas_count,
            },
        }

    except Exception as e:
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/inventarios/alertas-globales")
async def get_alertas_globales():
    """
    Obtener todas las alertas de stock bajo del sistema
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    a.id,
                    u.email as usuario_email,
                    pm.nombre_normalizado as producto_nombre,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.nivel_alerta,
                    iu.unidad_medida,
                    a.fecha_creacion
                FROM alertas_usuario a
                JOIN usuarios u ON a.usuario_id = u.id
                JOIN productos_maestros pm ON a.producto_maestro_id = pm.id
                JOIN inventario_usuario iu ON iu.usuario_id = a.usuario_id
                    AND iu.producto_maestro_id = a.producto_maestro_id
                WHERE a.tipo_alerta = 'stock_bajo'
                  AND a.activa = TRUE
                  AND a.enviada = FALSE
                ORDER BY a.fecha_creacion DESC
                LIMIT 50
            """
            )
        else:
            cursor.execute(
                """
                SELECT
                    a.id,
                    u.email as usuario_email,
                    pm.nombre_normalizado as producto_nombre,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.nivel_alerta,
                    iu.unidad_medida,
                    a.fecha_creacion
                FROM alertas_usuario a
                JOIN usuarios u ON a.usuario_id = u.id
                JOIN productos_maestros pm ON a.producto_maestro_id = pm.id
                JOIN inventario_usuario iu ON iu.usuario_id = a.usuario_id
                    AND iu.producto_maestro_id = a.producto_maestro_id
                WHERE a.tipo_alerta = 'stock_bajo'
                  AND a.activa = 1
                  AND a.enviada = 0
                ORDER BY a.fecha_creacion DESC
                LIMIT 50
            """
            )

        alertas = []
        for row in cursor.fetchall():
            alertas.append(
                {
                    "id": row[0],
                    "usuario_email": row[1],
                    "producto_nombre": row[2],
                    "producto_codigo": row[3],
                    "cantidad_actual": int(row[4]) if row[4] else 0,
                    "nivel_alerta": int(row[5]) if row[5] else 0,
                    "unidad": row[6] or "unidades",
                    "fecha_creacion": str(row[7]),
                }
            )

        conn.close()

        return {"success": True, "alertas": alertas, "total": len(alertas)}

    except Exception as e:
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 🆕 NUEVOS ENDPOINTS PARA EL EDITOR
# ==========================================

from pydantic import BaseModel
from typing import Optional

class FacturaUpdate(BaseModel):
    """Modelo para actualizar datos generales de factura"""
    establecimiento: Optional[str] = None
    total: Optional[float] = None
    fecha: Optional[str] = None


class ItemUpdate(BaseModel):
    """Modelo para actualizar un item de factura"""
    nombre: str
    precio: float
    codigo_ean: Optional[str] = None


class ItemCreate(BaseModel):
    """Modelo para crear un nuevo item"""
    nombre: str
    precio: float
    codigo_ean: Optional[str] = None


@router.get("/facturas/{factura_id}")
async def obtener_factura_detalle(factura_id: int):
    """
    Obtener detalles completos de una factura con sus items
    Usado por editor.html
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print(f"📥 Obteniendo factura {factura_id}...")

        # 1. Obtener datos de la factura
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT *
                FROM facturas
                WHERE id = %s
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT *
                FROM facturas
                WHERE id = ?
            """,
                (factura_id,),
            )

        factura_row = cursor.fetchone()

        if not factura_row:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        # ✅ Total en pesos enteros (como está en BD)
        total_pesos = int(factura_row[2]) if factura_row[2] else 0

        factura = {
            "id": factura_row[0],
            "establecimiento": factura_row[1] or "",
            "total": total_pesos,
            "fecha": str(factura_row[3]) if factura_row[3] else "",
            "estado": factura_row[4] or "pendiente",
            "tiene_imagen": factura_row[5] or False,
        }

        # 2. Obtener items de la factura
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    id,
                    nombre_leido,
                    precio_pagado,
                    codigo_leido
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    id,
                    nombre_leido,
                    precio_pagado,
                    codigo_leido
                FROM items_factura
                WHERE factura_id = ?
                ORDER BY id
            """,
                (factura_id,),
            )

        items = []
        for item_row in cursor.fetchall():
            # ✅ Precios en pesos enteros (como están en BD)
            precio_pesos = int(item_row[2]) if item_row[2] else 0

            items.append(
                {
                    "id": item_row[0],
                    "nombre": item_row[1] or "",
                    "precio": precio_pesos,
                    "codigo": item_row[3] or "",
                }
            )

        factura["productos"] = items

        cursor.close()
        conn.close()

        print(f"✅ Factura {factura_id} obtenida: {len(items)} items")

        return factura

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/facturas/{factura_id}")
async def actualizar_factura(factura_id: int, data: FacturaUpdate):
    """
    Actualizar datos generales de una factura
    Usado por editor.html para guardar cambios
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print(f"📝 Actualizando factura {factura_id}...")
        print(f"   Datos: {data.dict()}")

        # Construir query dinámico solo con campos proporcionados
        updates = []
        values = []

        if data.establecimiento is not None:
            updates.append("establecimiento = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.establecimiento)

        if data.total is not None:
            updates.append("total_factura = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.total)

        if data.fecha is not None:
            updates.append("fecha_factura = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.fecha)

        if not updates:
            raise HTTPException(status_code=400, detail="No hay datos para actualizar")

        # Agregar factura_id al final
        values.append(factura_id)

        query = f"""
            UPDATE facturas
            SET {', '.join(updates)}
            WHERE id = {'%s' if database_type == 'postgresql' else '?'}
        """

        cursor.execute(query, values)
        conn.commit()

        print(f"✅ Factura {factura_id} actualizada")

        cursor.close()
        conn.close()

        return {"success": True, "message": "Factura actualizada correctamente"}

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/items/{item_id}")
async def actualizar_item(item_id: int, data: ItemUpdate):
    """
    Actualizar un item de factura existente
    Usado por editor.html
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print(f"📝 Actualizando item {item_id}...")
        print(f"   Datos: {data.dict()}")

        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE items_factura
                SET nombre_leido = %s,
                    precio_pagado = %s,
                    codigo_leido = %s
                WHERE id = %s
            """,
                (data.nombre, data.precio, data.codigo_ean, item_id),
            )
        else:
            cursor.execute(
                """
                UPDATE items_factura
                SET nombre_leido = ?,
                    precio_pagado = ?,
                    codigo_leido = ?
                WHERE id = ?
            """,
                (data.nombre, data.precio, data.codigo_ean, item_id),
            )

        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Item no encontrado")

        conn.commit()

        print(f"✅ Item {item_id} actualizado")

        cursor.close()
        conn.close()

        return {"success": True, "message": "Item actualizado correctamente"}

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/facturas/{factura_id}/items")
async def crear_item(factura_id: int, data: ItemCreate):
    """
    Crear un nuevo item para una factura
    Usado por editor.html para agregar productos
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print(f"➕ Creando nuevo item para factura {factura_id}...")
        print(f"   Datos: {data.dict()}")

        # Verificar que la factura existe
        if database_type == "postgresql":
            cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT id FROM facturas WHERE id = ?", (factura_id,))

        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        # Insertar nuevo item
        if database_type == "postgresql":
            cursor.execute(
                """
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    codigo_leido
                )
                SELECT %s, usuario_id, %s, %s, %s, %s
                FROM facturas WHERE id = %s
                RETURNING id
            """,
                (factura_id, data.nombre, data.precio, 1, data.codigo_ean, factura_id),
            )
            nuevo_item_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    codigo_leido
                )
                SELECT ?, usuario_id, ?, ?, ?, ?
                FROM facturas WHERE id = ?
            """,
                (factura_id, data.nombre, data.precio, 1, data.codigo_ean, factura_id),
            )
            nuevo_item_id = cursor.lastrowid

        conn.commit()

        print(f"✅ Nuevo item creado: ID {nuevo_item_id}")

        cursor.close()
        conn.close()

        return {
            "success": True,
            "item_id": nuevo_item_id,
            "message": "Item creado correctamente",
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/facturas/{factura_id}/imagen")
async def obtener_imagen_factura(factura_id: int):
    """
    Obtener la imagen de una factura desde la base de datos
    Usado por editor.html para mostrar la imagen
    """
    try:
        from storage import get_image_from_db

        print(f"📸 Buscando imagen para factura {factura_id}...")

        image_data = get_image_from_db(factura_id)

        if not image_data:
            raise HTTPException(status_code=404, detail="Imagen no encontrada")

        print(f"✅ Imagen encontrada para factura {factura_id}")

        # Detectar tipo de imagen
        content_type = "image/jpeg"
        if image_data[:4] == b"\x89PNG":
            content_type = "image/png"
        elif image_data[:4] == b"RIFF":
            content_type = "image/webp"

        return Response(content=image_data, media_type=content_type)

    except HTTPException:
        raise
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Rutas de inventario para admin agregadas correctamente")
print("✅ Rutas del editor de facturas agregadas correctamente")

class ConsolidacionRequest(BaseModel):
    """Modelo para solicitud de consolidación"""
    producto_maestro_id: int
    productos_duplicados_ids: List[int]


@router.get("/productos/duplicados-sospechosos")
async def detectar_productos_duplicados_sospechosos(
    ean: Optional[str] = None,
    nombre: Optional[str] = None
):
    """
    Detectar grupos de productos potencialmente duplicados.

    VERSIÓN MEJORADA - Ahora muestra el establecimiento REAL desde facturas

    Busca duplicados de dos formas:
    A. Por código EAN idéntico (variantes de precio)
    B. Por nombre similar (>85% similitud)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print("\n" + "="*60)
        print("🔍 BÚSQUEDA DE DUPLICADOS")
        print("="*60)

        if ean:
            print(f"   Filtro EAN: {ean}")
        if nombre:
            print(f"   Filtro Nombre: {nombre}")

        grupos_duplicados = []

        # =====================================
        # PASO 1: Obtener productos con establecimientos REALES
        # =====================================
        print("\n📊 Obteniendo productos con establecimientos...")

        # Query base: productos con información de factura
        # ✅ EXCLUIR productos ya consolidados (que tienen producto_canonico_id)
        query_base = """
            SELECT DISTINCT
                pm.id,
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.marca,
                pm.categoria,
                pm.precio_promedio_global,
                pm.total_reportes,
                f.establecimiento,
                items.precio_pagado,
                f.fecha_cargue
            FROM productos_maestros pm
            INNER JOIN items_factura items ON items.producto_maestro_id = pm.id
            INNER JOIN facturas f ON f.id = items.factura_id
            WHERE pm.nombre_normalizado IS NOT NULL
              AND f.establecimiento IS NOT NULL
              AND pm.producto_canonico_id IS NULL
        """

        params = []

        # Aplicar filtros
        if ean:
            if database_type == "postgresql":
                query_base += " AND pm.codigo_ean LIKE %s"
            else:
                query_base += " AND pm.codigo_ean LIKE ?"
            params.append(f"%{ean}%")

        if nombre:
            if database_type == "postgresql":
                query_base += " AND LOWER(pm.nombre_normalizado) LIKE %s"
            else:
                query_base += " AND LOWER(pm.nombre_normalizado) LIKE ?"
            params.append(f"%{nombre.lower()}%")

        query_base += " ORDER BY pm.total_reportes DESC, pm.nombre_normalizado LIMIT 500"

        if params:
            cursor.execute(query_base, params)
        else:
            cursor.execute(query_base)
        productos_raw = cursor.fetchall()

        print(f"   ✅ {len(productos_raw)} registros obtenidos")

        # =====================================
        # PASO 2: Agrupar por producto_id
        # =====================================
        productos_dict = {}

        for row in productos_raw:
            prod_id = row[0]
            establecimiento = row[7] or "Desconocido"
            precio_unitario = row[8] or 0

            if prod_id not in productos_dict:
                productos_dict[prod_id] = {
                    'id': prod_id,
                    'ean': row[1],
                    'nombre': row[2],
                    'marca': row[3],
                    'categoria': row[4],
                    'precio_promedio': row[5],
                    'reportes': row[6],
                    'establecimientos': {}
                }

            # Agregar precio de este establecimiento
            if establecimiento not in productos_dict[prod_id]['establecimientos']:
                productos_dict[prod_id]['establecimientos'][establecimiento] = []

            productos_dict[prod_id]['establecimientos'][establecimiento].append(int(precio_unitario))

        print(f"   ✅ {len(productos_dict)} productos únicos")

        # =====================================
        # PASO 3: Detectar duplicados por nombre similar
        # =====================================
        print("\n🔍 Detectando productos similares...")

        productos_lista = list(productos_dict.values())
        productos_ya_agrupados = set()

        for i, prod1 in enumerate(productos_lista):
            if prod1['id'] in productos_ya_agrupados:
                continue

            grupo_similar = []

            for j, prod2 in enumerate(productos_lista[i+1:], start=i+1):
                if prod2['id'] in productos_ya_agrupados:
                    continue

                # Calcular similitud de nombres
                similitud = SequenceMatcher(
                    None,
                    prod1['nombre'].lower() if prod1['nombre'] else "",
                    prod2['nombre'].lower() if prod2['nombre'] else ""
                ).ratio() * 100

                # Si nombres son muy similares (>85%)
                if similitud >= 85:
                    if not grupo_similar:
                        grupo_similar.append(prod1)
                        productos_ya_agrupados.add(prod1['id'])

                    grupo_similar.append(prod2)
                    productos_ya_agrupados.add(prod2['id'])

            # Si encontramos grupo, formatear para frontend
            if len(grupo_similar) >= 2:
                productos_formateados = []

                for prod in grupo_similar:
                    # Obtener establecimiento(s) y precio representativo
                    if prod['establecimientos']:
                        # Tomar el primer establecimiento
                        est_nombre = list(prod['establecimientos'].keys())[0]
                        precios = prod['establecimientos'][est_nombre]
                        precio_repr = precios[0] if precios else prod['precio_promedio'] or 0

                        # Crear texto con todos los establecimientos
                        todos_est = ", ".join(prod['establecimientos'].keys())
                    else:
                        todos_est = "Desconocido"
                        precio_repr = prod['precio_promedio'] or 0

                    productos_formateados.append({
                        "id": prod['id'],
                        "nombre": prod['nombre'],
                        "codigo_ean": prod['ean'],
                        "marca": prod['marca'],
                        "precio_promedio": int(precio_repr),
                        "veces_reportado": prod['reportes'] or 0,
                        "establecimiento": todos_est
                    })

                grupos_duplicados.append({
                    "nombre_grupo": grupo_similar[0]['nombre'],
                    "similitud": int(similitud),
                    "productos": productos_formateados
                })

                # Limitar a 50 grupos
                if len(grupos_duplicados) >= 50:
                    break

        print(f"   ✅ {len(grupos_duplicados)} grupos encontrados")

        # =====================================
        # PASO 4: Duplicados por EAN idéntico
        # =====================================
        if not nombre and len(grupos_duplicados) < 50:
            print("\n🔍 Buscando duplicados por EAN...")

            productos_por_ean = {}

            for prod in productos_lista:
                if not prod['ean'] or prod['id'] in productos_ya_agrupados:
                    continue

                if prod['ean'] not in productos_por_ean:
                    productos_por_ean[prod['ean']] = []

                productos_por_ean[prod['ean']].append(prod)

            # Agregar grupos con mismo EAN
            for ean_code, prods in productos_por_ean.items():
                if len(prods) >= 2:
                    productos_formateados = []

                    for prod in prods:
                        if prod['establecimientos']:
                            est_nombre = list(prod['establecimientos'].keys())[0]
                            precios = prod['establecimientos'][est_nombre]
                            precio_repr = precios[0] if precios else prod['precio_promedio'] or 0
                            todos_est = ", ".join(prod['establecimientos'].keys())
                        else:
                            todos_est = "Desconocido"
                            precio_repr = prod['precio_promedio'] or 0

                        productos_formateados.append({
                            "id": prod['id'],
                            "nombre": prod['nombre'],
                            "codigo_ean": prod['ean'],
                            "marca": prod['marca'],
                            "precio_promedio": int(precio_repr),
                            "veces_reportado": prod['reportes'] or 0,
                            "establecimiento": todos_est
                        })

                    grupos_duplicados.append({
                        "nombre_grupo": f"Mismo EAN: {ean_code}",
                        "similitud": 100,
                        "productos": productos_formateados
                    })

                    if len(grupos_duplicados) >= 50:
                        break

        # =====================================
        # 🆕 PASO 5: Duplicados por código PLU del supermercado
        # =====================================
        if len(grupos_duplicados) < 50:
            print("\n🔍 Buscando duplicados por código PLU interno...")

            # Obtener productos con códigos PLU
            if database_type == "postgresql":
                cursor.execute("""
                    SELECT DISTINCT
                        pm.id,
                        pm.codigo_ean,
                        pm.nombre_normalizado,
                        pm.marca,
                        pm.precio_promedio_global,
                        pm.total_reportes,
                        items.codigo_leido,
                        f.establecimiento
                    FROM productos_maestros pm
                    INNER JOIN items_factura items ON items.producto_maestro_id = pm.id
                    INNER JOIN facturas f ON f.id = items.factura_id
                    WHERE pm.producto_canonico_id IS NULL
                      AND items.codigo_leido IS NOT NULL
                      AND LENGTH(items.codigo_leido) >= 3
                      AND LENGTH(items.codigo_leido) <= 7
                      AND pm.id NOT IN (
                          SELECT unnest(ARRAY[%s])
                      )
                    ORDER BY items.codigo_leido, pm.total_reportes DESC
                    LIMIT 200
                """, ([pid for pid in productos_ya_agrupados] or [0],))
            else:
                cursor.execute("""
                    SELECT DISTINCT
                        pm.id,
                        pm.codigo_ean,
                        pm.nombre_normalizado,
                        pm.marca,
                        pm.precio_promedio_global,
                        pm.total_reportes,
                        items.codigo_leido,
                        f.establecimiento
                    FROM productos_maestros pm
                    INNER JOIN items_factura items ON items.producto_maestro_id = pm.id
                    INNER JOIN facturas f ON f.id = items.factura_id
                    WHERE pm.producto_canonico_id IS NULL
                      AND items.codigo_leido IS NOT NULL
                      AND LENGTH(items.codigo_leido) >= 3
                      AND LENGTH(items.codigo_leido) <= 7
                    ORDER BY items.codigo_leido, pm.total_reportes DESC
                    LIMIT 200
                """)

            productos_plu = cursor.fetchall()
            print(f"   📦 {len(productos_plu)} productos con códigos PLU")

            # Agrupar por código PLU + establecimiento
            plu_grupos = {}

            for row in productos_plu:
                prod_id = row[0]

                if prod_id in productos_ya_agrupados:
                    continue

                codigo_plu = row[6]
                establecimiento = row[7] or "Varios"

                # Clave: código_plu + establecimiento
                # (mismo PLU pero en diferentes supermercados NO son duplicados)
                clave = f"{codigo_plu}_{establecimiento}"

                if clave not in plu_grupos:
                    plu_grupos[clave] = []

                plu_grupos[clave].append({
                    'id': prod_id,
                    'ean': row[1],
                    'nombre': row[2],
                    'marca': row[3],
                    'precio_promedio': row[4],
                    'reportes': row[5],
                    'codigo_plu': codigo_plu,
                    'establecimiento': establecimiento
                })

            # Crear grupos de duplicados
            for clave, prods in plu_grupos.items():
                if len(prods) >= 2:
                    codigo_plu = prods[0]['codigo_plu']
                    establecimiento = prods[0]['establecimiento']

                    productos_formateados = []

                    for prod in prods:
                        productos_formateados.append({
                            "id": prod['id'],
                            "nombre": prod['nombre'],
                            "codigo_ean": prod['ean'] or f"PLU {codigo_plu}",
                            "marca": prod['marca'],
                            "precio_promedio": int(prod['precio_promedio'] or 0),
                            "veces_reportado": prod['reportes'] or 0,
                            "establecimiento": establecimiento
                        })

                        # Marcar como agrupado
                        productos_ya_agrupados.add(prod['id'])

                    grupos_duplicados.append({
                        "nombre_grupo": f"Mismo PLU ({codigo_plu}) en {establecimiento}",
                        "similitud": 95,
                        "productos": productos_formateados
                    })

                    if len(grupos_duplicados) >= 50:
                        break

            print(f"   ✅ {sum(1 for k, v in plu_grupos.items() if len(v) >= 2)} grupos PLU encontrados")

        cursor.close()
        conn.close()

        print(f"\n✅ Total: {len(grupos_duplicados)} grupos de duplicados")
        print("="*60 + "\n")

        return {
            "success": True,
            "grupos": grupos_duplicados,
            "total_grupos": len(grupos_duplicados)
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 🆕 ENDPOINTS PARA CORRECCIÓN MANUAL
# ==========================================

class ProductoCorreccion(BaseModel):
    """Modelo para corrección manual de producto"""
    nombre_completo: Optional[str] = None
    codigo_ean: Optional[str] = None
    marca: Optional[str] = None
    categoria: Optional[str] = None
    subcategoria: Optional[str] = None
    es_ean_valido: Optional[bool] = None
    razon_correccion: Optional[str] = None


@router.get("/productos/{producto_id}/detalle")
async def obtener_detalle_producto(producto_id: int):
    """
    Obtiene información detallada de un producto para revisión/corrección
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        # Obtener datos del producto
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    id,
                    codigo_ean,
                    nombre_normalizado,
                    marca,
                    categoria,
                    subcategoria,
                    precio_promedio_global,
                    precio_minimo_historico,
                    precio_maximo_historico,
                    total_reportes
                FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    id,
                    codigo_ean,
                    nombre_normalizado,
                    marca,
                    categoria,
                    subcategoria,
                    precio_promedio_global,
                    precio_minimo_historico,
                    precio_maximo_historico,
                    total_reportes
                FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

        producto = cursor.fetchone()

        if not producto:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener establecimientos y precios
        if database_type == "postgresql":
            cursor.execute("""
                SELECT DISTINCT
                    COALESCE(e.nombre_normalizado, f.establecimiento, 'Desconocido') as establecimiento,
                    pp.precio,
                    pp.fecha_registro
                FROM precios_productos pp
                LEFT JOIN establecimientos e ON pp.establecimiento_id = e.id
                LEFT JOIN items_factura items ON items.producto_maestro_id = pp.producto_maestro_id
                LEFT JOIN facturas f ON f.id = items.factura_id
                WHERE pp.producto_maestro_id = %s
                ORDER BY pp.fecha_registro DESC
                LIMIT 10
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT DISTINCT
                    COALESCE(e.nombre_normalizado, f.establecimiento, 'Desconocido') as establecimiento,
                    pp.precio,
                    pp.fecha_registro
                FROM precios_productos pp
                LEFT JOIN establecimientos e ON pp.establecimiento_id = e.id
                LEFT JOIN items_factura items ON items.producto_maestro_id = pp.producto_maestro_id
                LEFT JOIN facturas f ON f.id = items.factura_id
                WHERE pp.producto_maestro_id = ?
                ORDER BY pp.fecha_registro DESC
                LIMIT 10
            """, (producto_id,))

        precios_por_establecimiento = {}
        for row in cursor.fetchall():
            est = row[0]
            precio = int(row[1]) if row[1] else 0
            fecha = str(row[2]) if row[2] else ""

            if est not in precios_por_establecimiento:
                precios_por_establecimiento[est] = []

            precios_por_establecimiento[est].append({
                "precio": precio,
                "fecha": fecha
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "producto": {
                "id": producto[0],
                "codigo_ean": producto[1],
                "nombre": producto[2],
                "marca": producto[3],
                "categoria": producto[4],
                "subcategoria": producto[5],
                "precio_promedio": int(producto[6]) if producto[6] else 0,
                "precio_minimo": int(producto[7]) if producto[7] else 0,
                "precio_maximo": int(producto[8]) if producto[8] else 0,
                "total_reportes": producto[9] or 0,
                "precios_por_establecimiento": precios_por_establecimiento
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/productos/{producto_id}/corregir")
async def corregir_producto(producto_id: int, data: ProductoCorreccion):
    """
    Corrige/actualiza información de un producto
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print(f"📝 Corrigiendo producto {producto_id}...")
        print(f"   Datos recibidos: {data.dict()}")

        # Obtener datos actuales para auditoría
        if database_type == "postgresql":
            cursor.execute("""
                SELECT nombre_normalizado, codigo_ean, marca, categoria, subcategoria
                FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT nombre_normalizado, codigo_ean, marca, categoria, subcategoria
                FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

        datos_anteriores = cursor.fetchone()

        if not datos_anteriores:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Construir UPDATE dinámico
        updates = []
        values = []

        if data.nombre_completo is not None:
            updates.append("nombre_normalizado = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.nombre_completo)

        if data.codigo_ean is not None:
            updates.append("codigo_ean = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.codigo_ean)

        if data.marca is not None:
            updates.append("marca = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.marca)

        if data.categoria is not None:
            updates.append("categoria = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.categoria)

        if data.subcategoria is not None:
            updates.append("subcategoria = " + ("%s" if database_type == "postgresql" else "?"))
            values.append(data.subcategoria)

        if not updates:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=400, detail="No hay datos para actualizar")

        # Agregar timestamp
        updates.append("ultima_actualizacion = " + ("CURRENT_TIMESTAMP" if database_type == "postgresql" else "CURRENT_TIMESTAMP"))

        # Agregar producto_id al final
        values.append(producto_id)

        query = f"""
            UPDATE productos_maestros
            SET {', '.join(updates)}
            WHERE id = {'%s' if database_type == 'postgresql' else '?'}
        """

        cursor.execute(query, values)
        conn.commit()

        print(f"✅ Producto {producto_id} actualizado")
        print(f"   Nombre: {datos_anteriores[0]} → {data.nombre_completo or datos_anteriores[0]}")
        print(f"   EAN: {datos_anteriores[1]} → {data.codigo_ean or datos_anteriores[1]}")

        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": "Producto actualizado correctamente",
            "cambios_aplicados": {
                "nombre": data.nombre_completo,
                "ean": data.codigo_ean,
                "marca": data.marca,
                "categoria": data.categoria
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/productos/consultar-api")
async def consultar_api_producto(codigo_ean: str):
    """
    Consulta información de un producto en OpenFoodFacts
    """
    try:
        import requests

        print(f"🌐 Consultando OpenFoodFacts para EAN: {codigo_ean}")

        url = f"https://world.openfoodfacts.org/api/v2/product/{codigo_ean}.json"

        response = requests.get(url, timeout=5)

        if response.status_code != 200:
            return {
                "success": False,
                "message": "Producto no encontrado en OpenFoodFacts",
                "sugerencia": None
            }

        data = response.json()

        if data.get("status") != 1:
            return {
                "success": False,
                "message": "Producto no encontrado en OpenFoodFacts",
                "sugerencia": None
            }

        product = data.get("product", {})

        nombre = product.get("product_name_es") or product.get("product_name") or "No disponible"
        marca = product.get("brands") or "No disponible"
        categorias = product.get("categories_tags", [])
        imagen_url = product.get("image_url")

        print(f"✅ Producto encontrado: {nombre}")

        return {
            "success": True,
            "sugerencia": {
                "nombre_completo": nombre,
                "marca": marca,
                "categorias": categorias,
                "imagen_url": imagen_url,
                "fuente": "OpenFoodFacts"
            }
        }

    except requests.Timeout:
        return {
            "success": False,
            "message": "Timeout al consultar API",
            "sugerencia": None
        }
    except Exception as e:
        print(f"❌ Error consultando API: {e}")
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "sugerencia": None
        }


@router.post("/productos/consolidar")
async def consolidar_productos(data: ConsolidacionRequest):
    """
    Consolidar productos duplicados en el sistema canónico

    Proceso:
    1. Crea/actualiza producto_canonico con datos del producto maestro
    2. Crea productos_variantes para cada producto (maestro + duplicados)
    3. Migra precios_productos manteniendo establecimiento/fecha/precio
    4. Actualiza items_factura para apuntar al canónico
    5. NO elimina productos_maestros originales (para historial)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        producto_maestro_id = data.producto_maestro_id
        productos_duplicados_ids = data.productos_duplicados_ids

        print(f"🔧 Consolidando productos...")
        print(f"   Maestro: {producto_maestro_id}")
        print(f"   Duplicados: {productos_duplicados_ids}")

        # =====================================
        # 1. OBTENER INFO DEL PRODUCTO MAESTRO
        # =====================================
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT nombre_normalizado, codigo_ean, marca, categoria
                FROM productos_maestros
                WHERE id = %s
            """,
                (producto_maestro_id,),
            )
        else:
            cursor.execute(
                """
                SELECT nombre_normalizado, codigo_ean, marca, categoria
                FROM productos_maestros
                WHERE id = ?
            """,
                (producto_maestro_id,),
            )

        maestro = cursor.fetchone()

        if not maestro:
            raise HTTPException(status_code=404, detail="Producto maestro no encontrado")

        nombre_oficial = maestro[0]
        ean_principal = maestro[1]
        marca = maestro[2]
        categoria = maestro[3]

        # =====================================
        # 2. CREAR/ACTUALIZAR PRODUCTO CANÓNICO
        # =====================================
        if database_type == "postgresql":
            cursor.execute(
                """
                INSERT INTO productos_canonicos
                    (nombre_oficial, ean_principal, marca, categoria, nombre_normalizado)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """,
                (nombre_oficial, ean_principal, marca, categoria, nombre_oficial),
            )
            producto_canonico_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """
                INSERT INTO productos_canonicos
                    (nombre_oficial, ean_principal, marca, categoria, nombre_normalizado)
                VALUES (?, ?, ?, ?, ?)
            """,
                (nombre_oficial, ean_principal, marca, categoria, nombre_oficial),
            )
            producto_canonico_id = cursor.lastrowid

        print(f"✅ Producto canónico creado: ID {producto_canonico_id}")

        # =====================================
        # 3. CREAR VARIANTES PARA TODOS LOS PRODUCTOS
        # =====================================
        todos_los_productos = [producto_maestro_id] + productos_duplicados_ids
        variantes_creadas = 0

        for prod_id in todos_los_productos:
            # 🔧 FIX: Query corregido aquí + códigos internos
            if database_type == "postgresql":
                cursor.execute(
                    """
                    SELECT DISTINCT
                        pm.nombre_normalizado,
                        pm.codigo_ean,
                        COALESCE(e.nombre_normalizado, f.establecimiento, 'Varios') as establecimiento,
                        pp.precio,
                        pp.fecha_registro,
                        items.codigo_leido
                    FROM productos_maestros pm
                    LEFT JOIN items_factura items ON items.producto_maestro_id = pm.id
                    LEFT JOIN precios_productos pp ON pp.producto_maestro_id = pm.id
                    LEFT JOIN establecimientos e ON pp.establecimiento_id = e.id
                    LEFT JOIN facturas f ON f.id = items.factura_id
                    WHERE pm.id = %s
                """,
                    (prod_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT DISTINCT
                        pm.nombre_normalizado,
                        pm.codigo_ean,
                        e.nombre as establecimiento,
                        pp.precio,
                        pp.fecha,
                        items.codigo_leido
                    FROM productos_maestros pm
                    LEFT JOIN items_factura items ON items.producto_maestro_id = pm.id
                    LEFT JOIN precios_productos pp ON pp.producto_maestro_id = pm.id
                    LEFT JOIN establecimientos e ON pp.establecimiento_id = e.id
                    LEFT JOIN facturas f ON f.id = items.factura_id
                    WHERE pm.id = ?
                """,
                    (prod_id,),
                )

            registros = cursor.fetchall()

            if not registros or len(registros) == 0:
                # Producto sin precios registrados, crear variante genérica
                if database_type == "postgresql":
                    cursor.execute(
                        """
                        SELECT pm.nombre_normalizado, pm.codigo_ean, items.codigo_leido
                        FROM productos_maestros pm
                        LEFT JOIN items_factura items ON items.producto_maestro_id = pm.id
                        WHERE pm.id = %s
                        LIMIT 1
                    """,
                        (prod_id,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT pm.nombre_normalizado, pm.codigo_ean, items.codigo_leido
                        FROM productos_maestros pm
                        LEFT JOIN items_factura items ON items.producto_maestro_id = pm.id
                        WHERE pm.id = ?
                        LIMIT 1
                    """,
                        (prod_id,),
                    )

                prod_info = cursor.fetchone()

                if prod_info:
                    nombre_prod = prod_info[0]
                    codigo_ean = prod_info[1]
                    codigo_leido = prod_info[2]

                    # ✅ LÓGICA MEJORADA: Priorizar códigos reales
                    if codigo_ean and len(codigo_ean) >= 8:
                        # Tiene EAN válido
                        codigo_prod = codigo_ean
                        tipo_codigo = "EAN"
                    elif codigo_leido and len(codigo_leido) >= 3:
                        # Tiene código PLU/interno del supermercado
                        codigo_prod = codigo_leido
                        tipo_codigo = "PLU" if len(codigo_leido) <= 7 else "INTERNO"
                    else:
                        # No tiene ningún código, generar temporal
                        codigo_prod = f"TEMP_{prod_id}"
                        tipo_codigo = "INTERNO"

                    print(f"   📦 Producto {prod_id}: código={codigo_prod} tipo={tipo_codigo}")

                    if database_type == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO productos_variantes
                                (producto_canonico_id, codigo, tipo_codigo, nombre_en_recibo,
                                 establecimiento, veces_reportado)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (codigo, establecimiento) DO NOTHING
                        """,
                            (producto_canonico_id, codigo_prod, tipo_codigo,
                             nombre_prod, "Varios", 1),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO productos_variantes
                                (producto_canonico_id, codigo, tipo_codigo, nombre_en_recibo,
                                 establecimiento, veces_reportado)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (producto_canonico_id, codigo_prod, tipo_codigo,
                             nombre_prod, "Varios", 1),
                        )

                    variantes_creadas += 1
            else:
                # Crear variante por cada establecimiento
                for registro in registros:
                    nombre_variante = registro[0]
                    codigo_ean = registro[1]
                    establecimiento = registro[2] or "Varios"
                    precio = registro[3]
                    fecha = registro[4]
                    codigo_leido = registro[5] if len(registro) > 5 else None

                    # ✅ LÓGICA MEJORADA: Priorizar códigos reales
                    if codigo_ean and len(codigo_ean) >= 8:
                        # Tiene EAN válido
                        codigo = codigo_ean
                        tipo_codigo = "EAN"
                    elif codigo_leido and len(codigo_leido) >= 3:
                        # Tiene código PLU/interno del supermercado
                        codigo = codigo_leido
                        tipo_codigo = "PLU" if len(codigo_leido) <= 7 else "INTERNO"
                    else:
                        # No tiene ningún código, generar temporal
                        codigo = f"TEMP_{prod_id}"
                        tipo_codigo = "INTERNO"

                    # Crear variante
                    if database_type == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO productos_variantes
                                (producto_canonico_id, codigo, tipo_codigo, nombre_en_recibo,
                                 establecimiento, veces_reportado)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (codigo, establecimiento) DO UPDATE
                            SET veces_reportado = productos_variantes.veces_reportado + 1
                            RETURNING id
                        """,
                            (producto_canonico_id, codigo, tipo_codigo,
                             nombre_variante, establecimiento, 1),
                        )
                        variante_id = cursor.fetchone()[0]
                    else:
                        cursor.execute(
                            """
                            INSERT OR IGNORE INTO productos_variantes
                                (producto_canonico_id, codigo, tipo_codigo, nombre_en_recibo,
                                 establecimiento, veces_reportado)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            (producto_canonico_id, codigo, tipo_codigo,
                             nombre_variante, establecimiento, 1),
                        )
                        variante_id = cursor.lastrowid

                    variantes_creadas += 1

                    # Migrar precio (manteniendo fecha/establecimiento/precio originales)
                    if precio and fecha:
                        # Obtener establecimiento_id
                        if database_type == "postgresql":
                            cursor.execute(
                                """
                                SELECT id FROM establecimientos WHERE nombre_normalizado = %s
                            """,
                                (establecimiento,),
                            )
                        else:
                            cursor.execute(
                                """
                                SELECT id FROM establecimientos WHERE nombre = ?
                            """,
                                (establecimiento,),
                            )

                        est_row = cursor.fetchone()
                        establecimiento_id = est_row[0] if est_row else None

                        if establecimiento_id:
                            if database_type == "postgresql":
                                cursor.execute(
                                    """
                                    INSERT INTO precios_productos
                                        (producto_canonico_id, variante_id, establecimiento_id,
                                         precio, fecha_registro, usuario_id)
                                    VALUES (%s, %s, %s, %s, %s,
                                        (SELECT usuario_id FROM precios_productos
                                         WHERE producto_maestro_id = %s LIMIT 1))
                                    ON CONFLICT (producto_canonico_id, establecimiento_id, fecha_registro)
                                    DO NOTHING
                                """,
                                    (producto_canonico_id, variante_id, establecimiento_id,
                                     precio, fecha, prod_id),
                                )
                            else:
                                cursor.execute(
                                    """
                                    INSERT OR IGNORE INTO precios_productos
                                        (producto_canonico_id, variante_id, establecimiento_id,
                                         precio, fecha, usuario_id)
                                    VALUES (?, ?, ?, ?, ?,
                                        (SELECT usuario_id FROM precios_productos
                                         WHERE producto_maestro_id = ? LIMIT 1))
                                """,
                                    (producto_canonico_id, variante_id, establecimiento_id,
                                     precio, fecha, prod_id),
                                )

        print(f"✅ {variantes_creadas} variantes creadas")

        # =====================================
        # 4. MARCAR PRODUCTOS MAESTROS COMO CONSOLIDADOS
        # =====================================
        print("\n📌 Marcando productos como consolidados...")

        for prod_id in todos_los_productos:
            if database_type == "postgresql":
                cursor.execute(
                    """
                    UPDATE productos_maestros
                    SET producto_canonico_id = %s
                    WHERE id = %s
                """,
                    (producto_canonico_id, prod_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE productos_maestros
                    SET producto_canonico_id = ?
                    WHERE id = ?
                """,
                    (producto_canonico_id, prod_id),
                )

        print(f"✅ {len(todos_los_productos)} productos marcados como consolidados")

        # =====================================
        # 5. ACTUALIZAR ITEMS_FACTURA
        # =====================================
        for prod_id in todos_los_productos:
            if database_type == "postgresql":
                cursor.execute(
                    """
                    UPDATE items_factura
                    SET producto_canonico_id = %s
                    WHERE producto_maestro_id = %s
                """,
                    (producto_canonico_id, prod_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE items_factura
                    SET producto_canonico_id = ?
                    WHERE producto_maestro_id = ?
                """,
                    (producto_canonico_id, prod_id),
                )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Consolidación completada")

        return {
            "success": True,
            "message": "Productos consolidados exitosamente",
            "producto_canonico_id": producto_canonico_id,
            "variantes_creadas": variantes_creadas,
            "productos_procesados": len(todos_los_productos)
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/productos/detalle-completo")
async def obtener_productos_detalle_completo():
    """
    Retorna información COMPLETA de todos los productos con precios por supermercado
    VERSIÓN CORREGIDA - Compatible con tu estructura de BD
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")

        print("📊 Obteniendo productos con precios...")

        # Query adaptado a tu estructura
        if database_type == "postgresql":
            query = """
                WITH ultimos_precios AS (
                    SELECT
                        pp.producto_maestro_id as producto_id,
                        pp.establecimiento_id,
                        pp.precio,
                        pp.fecha_registro,
                        ROW_NUMBER() OVER (
                            PARTITION BY pp.producto_maestro_id, pp.establecimiento_id
                            ORDER BY pp.fecha_registro DESC
                        ) as rn
                    FROM precios_productos pp
                    WHERE pp.producto_maestro_id IS NOT NULL
                ),
                productos_info AS (
                    SELECT
                        pm.id as producto_id,
                        pm.nombre_normalizado as nombre_producto,
                        pm.codigo_ean,
                        (
                            SELECT items.codigo_leido
                            FROM items_factura items
                            WHERE items.producto_maestro_id = pm.id
                            AND items.codigo_leido IS NOT NULL
                            AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                            AND items.codigo_leido NOT LIKE '77%'
                            LIMIT 1
                        ) as codigo_plu
                    FROM productos_maestros pm
                )
                SELECT
                    pi.producto_id,
                    pi.nombre_producto as nombre,
                    pi.codigo_ean,
                    pi.codigo_plu,
                    e.id as establecimiento_id,
                    e.nombre_normalizado as establecimiento_nombre,
                    up.precio,
                    up.fecha_registro as ultima_actualizacion
                FROM productos_info pi
                LEFT JOIN ultimos_precios up ON up.producto_id = pi.producto_id AND up.rn = 1
                LEFT JOIN establecimientos e ON e.id = up.establecimiento_id
                WHERE pi.nombre_producto IS NOT NULL
                ORDER BY pi.nombre_producto, e.nombre_normalizado
                LIMIT 1000
            """
        else:
            query = """
                SELECT DISTINCT
                    pm.id as producto_id,
                    pm.nombre_normalizado as nombre,
                    pm.codigo_ean,
                    (
                        SELECT items.codigo_leido
                        FROM items_factura items
                        WHERE items.producto_maestro_id = pm.id
                        AND items.codigo_leido IS NOT NULL
                        AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                        AND items.codigo_leido NOT LIKE '77%'
                        LIMIT 1
                    ) as codigo_plu,
                    NULL as establecimiento_id,
                    f.establecimiento as establecimiento_nombre,
                    pp.precio,
                    pp.fecha_registro as ultima_actualizacion
                FROM productos_maestros pm
                LEFT JOIN precios_productos pp ON pp.producto_maestro_id = pm.id
                LEFT JOIN items_factura items ON items.producto_maestro_id = pm.id
                LEFT JOIN facturas f ON f.id = items.factura_id
                WHERE pm.nombre_normalizado IS NOT NULL
                ORDER BY pm.nombre_normalizado, f.establecimiento
                LIMIT 1000
            """

        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"   ✅ {len(rows)} registros obtenidos")

        # Agrupar por producto
        productos_map = {}

        for row in rows:
            producto_id = row[0]

            if producto_id not in productos_map:
                productos_map[producto_id] = {
                    "producto_id": producto_id,
                    "nombre": row[1] or "Sin nombre",
                    "codigo_ean": row[2] or "Sin EAN",
                    "codigo_plu": row[3] or "Sin PLU",
                    "total_supermercados": 0,
                    "precios_por_super": []
                }

            # Agregar precio si existe
            if row[5] and row[6]:  # establecimiento_nombre y precio
                establecimiento = row[5]
                precio = row[6]
                fecha = row[7]

                # Verificar que no esté duplicado
                existe = any(
                    p["establecimiento"] == establecimiento
                    for p in productos_map[producto_id]["precios_por_super"]
                )

                if not existe:
                    from datetime import datetime

                    # Calcular días desde última actualización
                    dias = None
                    if fecha:
                        try:
                            if isinstance(fecha, str):
                                fecha_dt = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                            else:
                                fecha_dt = fecha
                            dias = (datetime.now() - fecha_dt).days
                        except:
                            dias = None

                    productos_map[producto_id]["precios_por_super"].append({
                        "establecimiento_id": row[4],
                        "establecimiento": establecimiento,
                        "precio": float(precio) if precio else 0,
                        "ultima_actualizacion": str(fecha) if fecha else None,
                        "dias_desde_actualizacion": dias
                    })

        # Actualizar contadores
        for prod in productos_map.values():
            prod["total_supermercados"] = len(prod["precios_por_super"])

        cursor.close()
        conn.close()

        print(f"✅ {len(productos_map)} productos únicos procesados")

        return {
            "total_productos": len(productos_map),
            "productos": list(productos_map.values())
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/productos-detalle-page", response_class=HTMLResponse)
async def productos_detalle_page():
    """
    Sirve la página HTML de visualización completa de productos
    """
    try:
        file_path = os.path.join("static", "productos_detalle.html")

        # Verificar si el archivo existe
        if not os.path.exists(file_path):
            return HTMLResponse(
                content="""
                <html>
                <head>
                    <style>
                        body {
                            font-family: Arial, sans-serif;
                            padding: 50px;
                            text-align: center;
                            background: #f5f5f5;
                        }
                        .error-box {
                            background: white;
                            border-radius: 10px;
                            padding: 40px;
                            max-width: 600px;
                            margin: 0 auto;
                            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                        }
                        h1 { color: #c62828; }
                        code {
                            background: #f0f0f0;
                            padding: 2px 8px;
                            border-radius: 4px;
                            font-family: monospace;
                        }
                        ol {
                            text-align: left;
                            margin: 20px auto;
                            max-width: 500px;
                        }
                        li { margin: 10px 0; }
                    </style>
                </head>
                <body>
                    <div class="error-box">
                        <h1>❌ Error 404</h1>
                        <h2>Archivo productos_detalle.html no encontrado</h2>
                        <p>El archivo debe estar en: <code>static/productos_detalle.html</code></p>

                        <h3>📋 Pasos para resolver:</h3>
                        <ol>
                            <li>Crear carpeta <code>static</code> en la raíz del proyecto</li>
                            <li>Copiar el archivo <code>productos_detalle.html</code> descargado</li>
                            <li>Colocarlo en <code>static/productos_detalle.html</code></li>
                            <li>Reiniciar el servidor</li>
                        </ol>

                        <p style="margin-top: 30px;">
                            <a href="/admin" style="color: #667eea; text-decoration: none;">← Volver al Dashboard</a>
                        </p>
                    </div>
                </body>
                </html>
                """,
                status_code=404
            )

        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    except Exception as e:
        import traceback
        traceback.print_exc()
        return HTMLResponse(
            content=f"""
            <html>
            <body style="font-family: Arial; padding: 50px; text-align: center;">
                <h1 style="color: #c62828;">❌ Error del Servidor</h1>
                <p>{str(e)}</p>
                <p><a href="/admin">← Volver al Dashboard</a></p>
            </body>
            </html>
            """,
            status_code=500
        )


print("✅ Endpoints de productos detalle agregados correctamente")
