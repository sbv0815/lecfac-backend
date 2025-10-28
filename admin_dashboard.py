# admin_dashboard.py - VERSIÓN ACTUALIZADA CON NUEVA ARQUITECTURA
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import List, Optional
from difflib import SequenceMatcher
from database import get_db_connection
import os

router = APIRouter(prefix="/admin", tags=["admin"])


@router.get("/stats")
async def estadisticas():
    """Obtener estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Total de facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]

        # 2. Productos únicos en catálogo NUEVO
        cursor.execute(
            """
            SELECT COUNT(DISTINCT id)
            FROM productos_maestros
        """
        )
        productos_unicos = cursor.fetchone()[0]

        # 3. Facturas pendientes de revisión
        cursor.execute(
            """
            SELECT COUNT(*) FROM facturas
            WHERE COALESCE(estado_validacion, 'pendiente') NOT IN ('revisada', 'validada')
        """
        )
        facturas_pendientes = cursor.fetchone()[0]

        # 4. Alertas activas (productos con variación de precio)
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

        cursor.close()
        conn.close()

        return {
            "total_facturas": total_facturas,
            "productos_unicos": productos_unicos,
            "alertas_activas": alertas_activas,
            "pendientes_revision": facturas_pendientes,
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
                    "precio_promedio": int(row[5]) if row[5] else 0,  # ✅ Pesos enteros
                    "precio_min": int(row[6]) if row[6] else 0,        # ✅ Pesos enteros
                    "precio_max": int(row[7]) if row[7] else 0,        # ✅ Pesos enteros
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
                    "total": int(row[2]) if row[2] else 0,  # ✅ Pesos enteros
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
                    "total": int(row[2]) if row[2] else 0,  # ✅ Pesos enteros
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
                        "cantidad": int(prod_row[1]) if prod_row[1] else 0,  # ✅ Entero para cantidades
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
                    "cantidad_actual": int(row[4]) if row[4] else 0,  # ✅ Entero para cantidades
                    "nivel_alerta": int(row[5]) if row[5] else 0,      # ✅ Entero para cantidades
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
            "total": total_pesos,  # ✅ Pesos enteros
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
                    "precio": precio_pesos,  # ✅ Pesos enteros
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
