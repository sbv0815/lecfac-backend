"""
API Admin - Endpoints para el Dashboard de Administración
LeFact - Sistema de auditoría y control de calidad

Este archivo contiene todos los endpoints necesarios para el dashboard admin:
- /api/admin/estadisticas - Estadísticas generales
- /api/admin/inventarios - Inventarios por usuario
- /api/admin/productos - Catálogo de productos
- /api/admin/duplicados/facturas - Detección de facturas duplicadas
- /api/admin/duplicados/productos - Detección de productos duplicados
- /api/auditoria/estadisticas - Estadísticas de auditoría
- /api/auditoria/ejecutar-completa - Ejecutar auditoría completa
- /api/auditoria/cola-revision - Cola de facturas pendientes
"""

from fastapi import APIRouter, HTTPException, Body
from database import get_db_connection
from typing import Dict, List
import traceback
from datetime import datetime, timedelta

router = APIRouter()

# ==========================================
# ENDPOINTS DE ESTADÍSTICAS GENERALES
# ==========================================


@router.get("/api/admin/estadisticas")
async def obtener_estadisticas():
    """
    Obtiene estadísticas generales del sistema

    Returns:
        - total_usuarios: Número de usuarios registrados
        - total_facturas: Número de facturas procesadas
        - total_productos: Número de productos únicos
        - calidad_promedio: Calidad promedio de las facturas
        - facturas_con_errores: Facturas con errores
        - productos_sin_categoria: Productos sin categoría
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total usuarios
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0] or 0

        # Total facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0] or 0

        # Total productos únicos
        cursor.execute("SELECT COUNT(DISTINCT nombre_producto) FROM items_factura")
        total_productos = cursor.fetchone()[0] or 0

        # Calidad promedio (si existe la columna puntaje_calidad)
        try:
            cursor.execute(
                """
                SELECT AVG(CAST(puntaje_calidad AS FLOAT))
                FROM facturas
                WHERE puntaje_calidad IS NOT NULL AND puntaje_calidad != ''
            """
            )
            calidad_promedio = cursor.fetchone()[0] or 0
        except:
            calidad_promedio = 0

        # Facturas con errores (estado_validacion = 'rechazada' o 'pendiente')
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM facturas
                WHERE estado_validacion IN ('rechazada', 'pendiente')
            """
            )
            facturas_con_errores = cursor.fetchone()[0] or 0
        except:
            facturas_con_errores = 0

        # Productos sin categoría
        cursor.execute(
            """
            SELECT COUNT(DISTINCT nombre_producto)
            FROM items_factura
            WHERE categoria IS NULL OR categoria = ''
        """
        )
        productos_sin_categoria = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "total_productos": total_productos,
            "calidad_promedio": round(calidad_promedio, 2),
            "facturas_con_errores": facturas_con_errores,
            "productos_sin_categoria": productos_sin_categoria,
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE INVENTARIOS
# ==========================================


@router.get("/api/admin/inventarios")
async def obtener_inventarios():
    """
    Obtiene inventarios por usuario
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                i.usuario_id,
                u.nombre as nombre_usuario,
                i.nombre_producto,
                i.cantidad_actual,
                i.categoria,
                i.ultima_actualizacion
            FROM inventario i
            LEFT JOIN usuarios u ON i.usuario_id = u.id
            ORDER BY i.ultima_actualizacion DESC
            LIMIT 100
        """
        )

        inventarios = []
        for row in cursor.fetchall():
            inventarios.append(
                {
                    "usuario_id": row[0],
                    "nombre_usuario": row[1],
                    "nombre_producto": row[2],
                    "cantidad_actual": row[3],
                    "categoria": row[4],
                    "ultima_actualizacion": row[5],
                }
            )

        conn.close()

        return {"inventarios": inventarios}

    except Exception as e:
        print(f"❌ Error obteniendo inventarios: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE PRODUCTOS
# ==========================================


@router.get("/api/admin/productos")
async def obtener_productos():
    """
    Obtiene catálogo de productos con estadísticas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                nombre_producto,
                codigo_ean,
                categoria,
                AVG(precio_unitario) as precio_promedio,
                COUNT(*) as veces_comprado
            FROM items_factura
            GROUP BY nombre_producto, codigo_ean, categoria
            ORDER BY veces_comprado DESC
            LIMIT 200
        """
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "nombre_producto": row[0],
                    "codigo_ean": row[1],
                    "categoria": row[2],
                    "precio_promedio": round(float(row[3]) if row[3] else 0, 2),
                    "veces_comprado": row[4],
                }
            )

        conn.close()

        return {"productos": productos}

    except Exception as e:
        print(f"❌ Error obteniendo productos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE DUPLICADOS
# ==========================================


@router.get("/api/admin/duplicados/facturas")
async def detectar_facturas_duplicadas():
    """
    Detecta facturas duplicadas basándose en establecimiento, total y fecha
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar facturas con mismo establecimiento, total y fecha cercana
        cursor.execute(
            """
            SELECT
                f1.id, f1.establecimiento, f1.total_factura, f1.fecha_compra,
                f2.id, f2.establecimiento, f2.total_factura, f2.fecha_compra
            FROM facturas f1
            JOIN facturas f2 ON
                f1.establecimiento = f2.establecimiento
                AND ABS(f1.total_factura - f2.total_factura) < 100
                AND ABS(julianday(f1.fecha_compra) - julianday(f2.fecha_compra)) <= 1
                AND f1.id < f2.id
            ORDER BY f1.fecha_compra DESC
            LIMIT 50
        """
        )

        duplicados_dict = {}
        for row in cursor.fetchall():
            key = f"{row[1]}_{row[2]}"
            if key not in duplicados_dict:
                duplicados_dict[key] = []

            # Agregar primera factura
            if not any(d["id"] == row[0] for d in duplicados_dict[key]):
                duplicados_dict[key].append(
                    {
                        "id": row[0],
                        "establecimiento": row[1],
                        "total_factura": row[2],
                        "fecha_compra": row[3],
                    }
                )

            # Agregar segunda factura
            if not any(d["id"] == row[4] for d in duplicados_dict[key]):
                duplicados_dict[key].append(
                    {
                        "id": row[4],
                        "establecimiento": row[5],
                        "total_factura": row[6],
                        "fecha_compra": row[7],
                    }
                )

        conn.close()

        # Convertir a lista de grupos
        duplicados = [grupo for grupo in duplicados_dict.values() if len(grupo) > 1]

        return {"duplicados": duplicados}

    except Exception as e:
        print(f"❌ Error detectando duplicados de facturas: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/admin/duplicados/productos")
async def detectar_productos_duplicados():
    """
    Detecta productos duplicados basándose en similitud de nombres
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar productos con nombres similares
        cursor.execute(
            """
            SELECT DISTINCT
                nombre_producto,
                codigo_ean,
                categoria,
                MIN(id) as id
            FROM items_factura
            GROUP BY LOWER(TRIM(nombre_producto))
            HAVING COUNT(*) > 1
            LIMIT 50
        """
        )

        productos_similares = {}
        for row in cursor.fetchall():
            nombre_base = row[0].lower().strip()

            if nombre_base not in productos_similares:
                productos_similares[nombre_base] = []

            productos_similares[nombre_base].append(
                {
                    "id": row[3],
                    "nombre_producto": row[0],
                    "codigo_ean": row[1],
                    "categoria": row[2],
                }
            )

        conn.close()

        # Filtrar solo grupos con más de 1 producto
        duplicados = [grupo for grupo in productos_similares.values() if len(grupo) > 1]

        return {"duplicados": duplicados}

    except Exception as e:
        print(f"❌ Error detectando duplicados de productos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE AUDITORÍA
# ==========================================


@router.get("/api/auditoria/estadisticas")
async def obtener_estadisticas_auditoria():
    """
    Obtiene estadísticas del sistema de auditoría
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Total procesadas con puntaje
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM facturas
                WHERE puntaje_calidad IS NOT NULL AND puntaje_calidad != ''
            """
            )
            total_procesadas = cursor.fetchone()[0] or 0
        except:
            total_procesadas = 0

        # Calidad promedio
        try:
            cursor.execute(
                """
                SELECT AVG(CAST(puntaje_calidad AS FLOAT))
                FROM facturas
                WHERE puntaje_calidad IS NOT NULL AND puntaje_calidad != ''
            """
            )
            calidad_promedio = cursor.fetchone()[0] or 0
        except:
            calidad_promedio = 0

        # Con errores (puntaje < 50)
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM facturas
                WHERE CAST(puntaje_calidad AS FLOAT) < 50
            """
            )
            con_errores = cursor.fetchone()[0] or 0
        except:
            con_errores = 0

        # En revisión (puntaje entre 50 y 80)
        try:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM facturas
                WHERE CAST(puntaje_calidad AS FLOAT) BETWEEN 50 AND 80
            """
            )
            en_revision = cursor.fetchone()[0] or 0
        except:
            en_revision = 0

        conn.close()

        return {
            "total_procesadas": total_procesadas,
            "calidad_promedio": round(calidad_promedio, 2),
            "con_errores": con_errores,
            "en_revision": en_revision,
        }

    except Exception as e:
        print(f"❌ Error obteniendo estadísticas de auditoría: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/auditoria/ejecutar-completa")
async def ejecutar_auditoria_completa():
    """
    Ejecuta auditoría completa de todas las facturas

    NOTA: Este endpoint necesita la integración con auditoria_automatica.py
    Por ahora retorna datos de ejemplo
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener todas las facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0] or 0

        # Por ahora, retornar estadísticas básicas
        # TODO: Integrar con auditoria_automatica.py para auditoría real

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM facturas
            WHERE estado_validacion = 'rechazada'
        """
        )
        con_errores = cursor.fetchone()[0] or 0

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM facturas
            WHERE estado_validacion = 'pendiente'
        """
        )
        en_revision = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "success": True,
            "total_procesadas": total_facturas,
            "calidad_promedio": 85,  # Valor de ejemplo
            "con_errores": con_errores,
            "en_revision": en_revision,
        }

    except Exception as e:
        print(f"❌ Error ejecutando auditoría: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/auditoria/cola-revision")
async def obtener_cola_revision():
    """
    Obtiene facturas pendientes de revisión (puntaje < 80%)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(
                """
                SELECT
                    id,
                    establecimiento,
                    total_factura,
                    fecha_compra,
                    puntaje_calidad,
                    motivo_rechazo
                FROM facturas
                WHERE CAST(puntaje_calidad AS FLOAT) < 80
                ORDER BY fecha_compra DESC
                LIMIT 50
            """
            )

            facturas = []
            for row in cursor.fetchall():
                facturas.append(
                    {
                        "id": row[0],
                        "establecimiento": row[1],
                        "total_factura": row[2],
                        "fecha_compra": row[3],
                        "puntaje_calidad": int(float(row[4])) if row[4] else 0,
                        "problemas": row[5] or "Sin detalles",
                    }
                )
        except:
            # Si no existe la columna puntaje_calidad, retornar facturas pendientes
            cursor.execute(
                """
                SELECT
                    id,
                    establecimiento,
                    total_factura,
                    fecha_compra,
                    estado_validacion
                FROM facturas
                WHERE estado_validacion = 'pendiente'
                ORDER BY fecha_compra DESC
                LIMIT 50
            """
            )

            facturas = []
            for row in cursor.fetchall():
                facturas.append(
                    {
                        "id": row[0],
                        "establecimiento": row[1],
                        "total_factura": row[2],
                        "fecha_compra": row[3],
                        "puntaje_calidad": 0,
                        "problemas": "Pendiente de auditoría",
                    }
                )

        conn.close()

        return {"facturas": facturas}

    except Exception as e:
        print(f"❌ Error obteniendo cola de revisión: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE ACCIONES ADMIN
# ==========================================


@router.delete("/api/admin/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """
    Elimina una factura y sus items asociados
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Eliminar items de la factura
        cursor.execute("DELETE FROM items_factura WHERE factura_id = ?", (factura_id,))

        # Eliminar factura
        cursor.execute("DELETE FROM facturas WHERE id = ?", (factura_id,))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Factura eliminada"}

    except Exception as e:
        print(f"❌ Error eliminando factura: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/admin/productos/fusionar")
async def fusionar_productos(datos: Dict = Body(...)):
    """
    Fusiona múltiples productos en uno solo
    """
    try:
        producto_ids = datos.get("producto_ids", [])

        if len(producto_ids) < 2:
            raise HTTPException(
                status_code=400, detail="Se necesitan al menos 2 productos"
            )

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener el producto principal (el primero)
        producto_principal_id = producto_ids[0]

        # Actualizar referencias a los otros productos
        for pid in producto_ids[1:]:
            cursor.execute(
                """
                UPDATE items_factura
                SET nombre_producto = (
                    SELECT nombre_producto
                    FROM items_factura
                    WHERE id = ?
                    LIMIT 1
                )
                WHERE id = ?
            """,
                (producto_principal_id, pid),
            )

        conn.commit()
        conn.close()

        return {"success": True, "message": "Productos fusionados"}

    except Exception as e:
        print(f"❌ Error fusionando productos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/admin/limpiar-datos")
async def limpiar_datos_antiguos(datos: Dict = Body(...)):
    """
    Elimina facturas antiguas con baja calidad
    """
    try:
        dias = datos.get("dias", 90)
        puntaje_minimo = datos.get("puntaje_minimo", 50)

        conn = get_db_connection()
        cursor = conn.cursor()

        fecha_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

        # Eliminar facturas antiguas con baja calidad
        cursor.execute(
            """
            DELETE FROM facturas
            WHERE fecha_compra < ?
            AND CAST(puntaje_calidad AS FLOAT) < ?
        """,
            (fecha_limite, puntaje_minimo),
        )

        facturas_eliminadas = cursor.rowcount

        conn.commit()
        conn.close()

        return {"success": True, "facturas_eliminadas": facturas_eliminadas}

    except Exception as e:
        print(f"❌ Error limpiando datos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/facturas/{factura_id}")
async def obtener_factura(factura_id: int):
    """
    Obtiene detalles de una factura específica
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener factura
        cursor.execute(
            """
            SELECT
                id, usuario_id, establecimiento, total_factura,
                fecha_compra, estado_validacion
            FROM facturas
            WHERE id = ?
        """,
            (factura_id,),
        )

        factura_row = cursor.fetchone()

        if not factura_row:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        factura = {
            "id": factura_row[0],
            "usuario_id": factura_row[1],
            "establecimiento": factura_row[2],
            "total_factura": factura_row[3],
            "fecha_compra": factura_row[4],
            "estado_validacion": factura_row[5],
        }

        # Obtener items
        cursor.execute(
            """
            SELECT nombre_producto, cantidad, precio_unitario, precio_total
            FROM items_factura
            WHERE factura_id = ?
        """,
            (factura_id,),
        )

        items = []
        for row in cursor.fetchall():
            items.append(
                {
                    "nombre_producto": row[0],
                    "cantidad": row[1],
                    "precio_unitario": row[2],
                    "precio_total": row[3],
                }
            )

        factura["items"] = items

        conn.close()

        return factura

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error obteniendo factura: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API Admin cargada correctamente")
