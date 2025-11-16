"""
api_inventario.py - APIs REST para la App Flutter
ACTUALIZADO: Usa productos_maestros_v2 correctamente
CORREGIDO: JOINs y campos correctos para V2
"""

from fastapi import APIRouter, HTTPException, Header
from typing import Optional
from datetime import datetime, timedelta
from database import get_db_connection
import os

router = APIRouter(prefix="/api/inventario", tags=["inventario_mobile"])


# ========================================
# AUTENTICACIÓN SIMPLE (mejorar en producción)
# ========================================
def verificar_token(authorization: str = Header(None)):
    """Verificar token de autorización"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="No autorizado")

    token = authorization.replace("Bearer ", "")

    try:
        user_id = int(token)
        return user_id
    except:
        raise HTTPException(status_code=401, detail="Token inválido")


# ========================================
# 1. OBTENER INVENTARIO DEL USUARIO
# ========================================
@router.get("/usuario/{user_id}")
async def get_inventario_usuario(user_id: int):
    """
    GET /api/inventario/usuario/{user_id}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.codigo_ean,
                    pm.nombre_consolidado,
                    pm.marca,
                    COALESCE(c.nombre, 'Sin categoría') as categoria,
                    NULL as imagen_url,
                    iu.cantidad_actual,
                    iu.unidad_medida,
                    iu.nivel_alerta,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    iu.alerta_activa,
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 'bajo'
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 'medio'
                        ELSE 'normal'
                    END as estado_stock,
                    iu.precio_ultima_compra as precio_promedio_global,
                    iu.precio_ultima_compra,
                    iu.precio_promedio,
                    iu.precio_minimo,
                    iu.precio_maximo,
                    iu.establecimiento,
                    iu.establecimiento_id,
                    iu.ubicacion,
                    iu.numero_compras,
                    iu.cantidad_total_comprada,
                    iu.total_gastado,
                    iu.ultima_factura_id,
                    iu.cantidad_por_unidad,
                    iu.dias_desde_ultima_compra,
                    iu.marca
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                LEFT JOIN categorias c ON pm.categoria_id = c.id
                WHERE iu.usuario_id = %s
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    iu.fecha_ultima_actualizacion DESC
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.codigo_ean,
                    pm.nombre_consolidado,
                    pm.marca,
                    COALESCE(c.nombre, 'Sin categoría') as categoria,
                    NULL as imagen_url,
                    iu.cantidad_actual,
                    iu.unidad_medida,
                    iu.nivel_alerta,
                    iu.fecha_ultima_compra,
                    iu.frecuencia_compra_dias,
                    iu.fecha_estimada_agotamiento,
                    iu.alerta_activa,
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 'bajo'
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 'medio'
                        ELSE 'normal'
                    END as estado_stock,
                    iu.precio_ultima_compra as precio_promedio_global,
                    iu.precio_ultima_compra,
                    iu.precio_promedio,
                    iu.precio_minimo,
                    iu.precio_maximo,
                    iu.establecimiento,
                    iu.establecimiento_id,
                    iu.ubicacion,
                    iu.numero_compras,
                    iu.cantidad_total_comprada,
                    iu.total_gastado,
                    iu.ultima_factura_id,
                    iu.cantidad_por_unidad,
                    iu.dias_desde_ultima_compra,
                    iu.marca
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                LEFT JOIN categorias c ON pm.categoria_id = c.id
                WHERE iu.usuario_id = ?
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    iu.fecha_ultima_actualizacion DESC
            """,
                (user_id,),
            )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3] or row[28] or "",
                    "categoria": row[4] or "",
                    "imagen_url": row[5],
                    "cantidad_actual": float(row[6]) if row[6] else 0.0,
                    "unidad_medida": row[7] or "unidades",
                    "nivel_alerta": float(row[8]) if row[8] else 0.0,
                    "fecha_ultima_compra": str(row[9]) if row[9] else None,
                    "frecuencia_dias": row[10],
                    "fecha_agotamiento_estimada": str(row[11]) if row[11] else None,
                    "alerta_activa": bool(row[12]),
                    "estado": row[13],
                    "precio_promedio_global": float(row[14]) if row[14] else 0.0,
                    "precio_ultima_compra": float(row[15]) if row[15] else 0.0,
                    "precio_promedio": float(row[16]) if row[16] else 0.0,
                    "precio_minimo": float(row[17]) if row[17] else 0.0,
                    "precio_maximo": float(row[18]) if row[18] else 0.0,
                    "establecimiento_nombre": row[19] or "",
                    "establecimiento_id": row[20],
                    "establecimiento_ubicacion": row[21] or "",
                    "numero_compras": int(row[22]) if row[22] else 0,
                    "cantidad_total_comprada": float(row[23]) if row[23] else 0.0,
                    "total_gastado": float(row[24]) if row[24] else 0.0,
                    "ultima_factura_id": row[25],
                    "cantidad_por_unidad": float(row[26]) if row[26] else 1.0,
                    "dias_desde_ultima_compra": int(row[27]) if row[27] else 0,
                }
            )

        total = len(productos)
        bajos = len([p for p in productos if p["estado"] == "bajo"])
        medios = len([p for p in productos if p["estado"] == "medio"])

        conn.close()

        return {
            "success": True,
            "productos": productos,
            "estadisticas": {
                "total": total,
                "stock_bajo": bajos,
                "stock_medio": medios,
                "stock_normal": total - bajos - medios,
            },
        }

    except Exception as e:
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=500, detail=f"Error al obtener inventario: {str(e)}"
        )


# ========================================
# 2. ACTUALIZAR CANTIDAD DE PRODUCTO
# ========================================
@router.put("/producto/{inventario_id}")
async def actualizar_cantidad(inventario_id: int, data: dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        nueva_cantidad = data.get("cantidad")
        usuario_id = data.get("usuario_id")

        if nueva_cantidad is None or usuario_id is None:
            raise HTTPException(status_code=400, detail="Faltan datos requeridos")

        if database_type == "postgresql":
            cursor.execute(
                "SELECT usuario_id FROM inventario_usuario WHERE id = %s",
                (inventario_id,),
            )
        else:
            cursor.execute(
                "SELECT usuario_id FROM inventario_usuario WHERE id = ?",
                (inventario_id,),
            )

        result = cursor.fetchone()
        if not result or result[0] != usuario_id:
            conn.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET cantidad_actual = %s,
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (nueva_cantidad, inventario_id),
            )
        else:
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET cantidad_actual = ?,
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (nueva_cantidad, inventario_id),
            )

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Cantidad actualizada correctamente",
            "nueva_cantidad": nueva_cantidad,
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 3. OBTENER ALERTAS DEL USUARIO
# ========================================
@router.get("/alertas/{user_id}")
async def get_alertas_usuario(user_id: int, solo_activas: bool = True):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        query = """
            SELECT
                a.id,
                a.tipo_alerta,
                a.mensaje_personalizado,
                a.prioridad,
                a.fecha_creacion,
                a.fecha_expiracion,
                pm.nombre_consolidado as producto_nombre,
                pm.codigo_ean,
                e.nombre_normalizado as establecimiento_nombre
            FROM alertas_usuario a
            LEFT JOIN productos_maestros_v2 pm ON a.producto_maestro_id = pm.id
            LEFT JOIN establecimientos e ON a.establecimiento_id = e.id
            WHERE a.usuario_id = {}
        """.format(
            "%s" if database_type == "postgresql" else "?"
        )

        if solo_activas:
            if database_type == "postgresql":
                query += " AND a.activa = TRUE AND a.enviada = FALSE"
            else:
                query += " AND a.activa = 1 AND a.enviada = 0"

        query += " ORDER BY a.prioridad DESC, a.fecha_creacion DESC"

        cursor.execute(query, (user_id,))

        alertas = []
        for row in cursor.fetchall():
            alertas.append(
                {
                    "id": row[0],
                    "tipo": row[1],
                    "mensaje": row[2],
                    "prioridad": row[3],
                    "fecha_creacion": str(row[4]),
                    "fecha_expiracion": str(row[5]) if row[5] else None,
                    "producto_nombre": row[6],
                    "producto_codigo": row[7],
                    "establecimiento": row[8],
                }
            )

        conn.close()

        return {"success": True, "alertas": alertas, "total": len(alertas)}

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 4. MARCAR ALERTA COMO LEÍDA
# ========================================
@router.put("/alertas/{alerta_id}/marcar-leida")
async def marcar_alerta_leida(alerta_id: int, data: dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        usuario_id = data.get("usuario_id")

        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE alertas_usuario
                SET enviada = TRUE,
                    fecha_envio = CURRENT_TIMESTAMP
                WHERE id = %s AND usuario_id = %s
                """,
                (alerta_id, usuario_id),
            )
        else:
            cursor.execute(
                """
                UPDATE alertas_usuario
                SET enviada = 1,
                    fecha_envio = CURRENT_TIMESTAMP
                WHERE id = ? AND usuario_id = ?
                """,
                (alerta_id, usuario_id),
            )

        conn.commit()
        conn.close()

        return {"success": True, "message": "Alerta marcada como leída"}

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 5. OBTENER PRESUPUESTO DEL USUARIO
# ========================================
@router.get("/presupuesto/{user_id}")
async def get_presupuesto_usuario(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    id, monto_mensual, monto_semanal,
                    fecha_inicio, fecha_fin, anio, mes, gasto_actual, gasto_semanal_actual
                FROM presupuesto_usuario
                WHERE usuario_id = %s AND activo = TRUE
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    id, monto_mensual, monto_semanal,
                    fecha_inicio, fecha_fin, anio, mes, gasto_actual, gasto_semanal_actual
                FROM presupuesto_usuario
                WHERE usuario_id = ? AND activo = 1
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )

        presupuesto = cursor.fetchone()

        if not presupuesto:
            conn.close()
            return {
                "success": True,
                "data": {"tiene_presupuesto": False, "presupuesto": None},
            }

        presupuesto_id = presupuesto[0]
        monto_mensual = float(presupuesto[1]) if presupuesto[1] else 0
        monto_semanal = float(presupuesto[2]) if presupuesto[2] else 0
        fecha_inicio = presupuesto[3]
        fecha_fin = presupuesto[4]

        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
                  AND fecha_cargue >= %s
                  AND fecha_cargue <= %s
                """,
                (user_id, fecha_inicio, fecha_fin),
            )
        else:
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                  AND date(fecha_cargue) >= date(?)
                  AND date(fecha_cargue) <= date(?)
                """,
                (user_id, fecha_inicio, fecha_fin),
            )

        gasto_mensual_actual = float(cursor.fetchone()[0])

        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
                  AND fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                  AND date(fecha_cargue) >= date('now', '-7 days')
                """,
                (user_id,),
            )

        gasto_semanal_actual = float(cursor.fetchone()[0])

        if database_type == "postgresql":
            cursor.execute(
                """
                UPDATE presupuesto_usuario
                SET gasto_actual = %s,
                    gasto_semanal_actual = %s
                WHERE id = %s
                """,
                (gasto_mensual_actual, gasto_semanal_actual, presupuesto_id),
            )
        else:
            cursor.execute(
                """
                UPDATE presupuesto_usuario
                SET gasto_actual = ?,
                    gasto_semanal_actual = ?
                WHERE id = ?
                """,
                (gasto_mensual_actual, gasto_semanal_actual, presupuesto_id),
            )

        conn.commit()
        conn.close()

        porcentaje_usado_mensual = (
            (gasto_mensual_actual / monto_mensual * 100) if monto_mensual > 0 else 0
        )
        porcentaje_usado_semanal = (
            (gasto_semanal_actual / monto_semanal * 100) if monto_semanal > 0 else 0
        )

        return {
            "success": True,
            "data": {
                "tiene_presupuesto": True,
                "presupuesto": {
                    "id": presupuesto_id,
                    "usuario_id": user_id,
                    "monto_mensual": monto_mensual,
                    "monto_semanal": monto_semanal,
                    "gasto_mensual_actual": gasto_mensual_actual,
                    "gasto_semanal_actual": gasto_semanal_actual,
                    "disponible_mensual": max(0, monto_mensual - gasto_mensual_actual),
                    "disponible_semanal": max(0, monto_semanal - gasto_semanal_actual),
                    "porcentaje_usado_mensual": round(porcentaje_usado_mensual, 1),
                    "porcentaje_usado_semanal": round(porcentaje_usado_semanal, 1),
                    "fecha_inicio": str(fecha_inicio),
                    "fecha_fin": str(fecha_fin),
                    "periodo": f"{presupuesto[6]}/{presupuesto[5]}",
                },
            },
        }

    except Exception as e:
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 6. CREAR/ACTUALIZAR PRESUPUESTO
# ========================================
@router.post("/presupuesto")
async def crear_presupuesto(data: dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        usuario_id = data.get("user_id") or data.get("usuario_id")
        monto_mensual = data.get("monto_mensual") or data.get("limite_mensual")
        monto_semanal = data.get("monto_semanal") or data.get("limite_semanal")
        anio = data.get("anio", datetime.now().year)
        mes = data.get("mes", datetime.now().month)

        if not usuario_id or not monto_mensual:
            raise HTTPException(status_code=400, detail="Faltan datos requeridos")

        if monto_mensual <= 0 or (monto_semanal and monto_semanal <= 0):
            raise HTTPException(
                status_code=400, detail="Los montos deben ser mayores a 0"
            )

        fecha_inicio = datetime(anio, mes, 1).date()
        if mes == 12:
            fecha_fin = datetime(anio + 1, 1, 1).date() - timedelta(days=1)
        else:
            fecha_fin = datetime(anio, mes + 1, 1).date() - timedelta(days=1)

        if database_type == "postgresql":
            cursor.execute(
                "UPDATE presupuesto_usuario SET activo = FALSE WHERE usuario_id = %s AND anio = %s AND mes = %s",
                (usuario_id, anio, mes),
            )
        else:
            cursor.execute(
                "UPDATE presupuesto_usuario SET activo = 0 WHERE usuario_id = ? AND anio = ? AND mes = ?",
                (usuario_id, anio, mes),
            )

        if database_type == "postgresql":
            cursor.execute(
                """
                INSERT INTO presupuesto_usuario
                (usuario_id, monto_mensual, monto_semanal, anio, mes, fecha_inicio, fecha_fin, activo)
                VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
                RETURNING id
                """,
                (
                    usuario_id,
                    monto_mensual,
                    monto_semanal,
                    anio,
                    mes,
                    fecha_inicio,
                    fecha_fin,
                ),
            )
            presupuesto_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """
                INSERT INTO presupuesto_usuario
                (usuario_id, monto_mensual, monto_semanal, anio, mes, fecha_inicio, fecha_fin, activo)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    usuario_id,
                    monto_mensual,
                    monto_semanal,
                    anio,
                    mes,
                    fecha_inicio,
                    fecha_fin,
                ),
            )
            presupuesto_id = cursor.lastrowid

        conn.commit()
        conn.close()

        return {
            "success": True,
            "message": "Presupuesto creado correctamente",
            "presupuesto_id": presupuesto_id,
            "periodo": f"{mes}/{anio}",
            "data": {
                "monto_mensual": monto_mensual,
                "monto_semanal": monto_semanal,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 7. ESTADÍSTICAS COMPLETAS DEL USUARIO
# ========================================
@router.get("/estadisticas/{user_id}")
async def get_estadisticas_usuario(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        resultado = {
            "success": True,
            "inventario": {},
            "gastos": {},
            "comunitarias": {},
        }

        # 1.1 Resumen de stock
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta THEN 1 ELSE 0 END) as bajos,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta * 2 AND cantidad_actual > nivel_alerta THEN 1 ELSE 0 END) as medios,
                    COALESCE(SUM(cantidad_actual * precio_ultima_compra), 0) as valor_total
                FROM inventario_usuario
                WHERE usuario_id = %s
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta THEN 1 ELSE 0 END) as bajos,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta * 2 AND cantidad_actual > nivel_alerta THEN 1 ELSE 0 END) as medios,
                    COALESCE(SUM(cantidad_actual * precio_ultima_compra), 0) as valor_total
                FROM inventario_usuario
                WHERE usuario_id = ?
                """,
                (user_id,),
            )

        inv = cursor.fetchone()
        total_productos = inv[0] or 0
        stock_bajo = inv[1] or 0
        stock_medio = inv[2] or 0
        valor_total = float(inv[3]) if inv[3] else 0

        # 1.2 Productos por categoría - CORREGIDO
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    COALESCE(c.nombre, 'Sin categoría') as categoria,
                    COUNT(*) as cantidad,
                    COALESCE(SUM(iu.cantidad_actual * iu.precio_ultima_compra), 0) as valor_total
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                LEFT JOIN categorias c ON pm.categoria_id = c.id
                WHERE iu.usuario_id = %s
                GROUP BY c.nombre
                ORDER BY cantidad DESC
                LIMIT 10
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    COALESCE(c.nombre, 'Sin categoría') as categoria,
                    COUNT(*) as cantidad,
                    COALESCE(SUM(iu.cantidad_actual * iu.precio_ultima_compra), 0) as valor_total
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                LEFT JOIN categorias c ON pm.categoria_id = c.id
                WHERE iu.usuario_id = ?
                GROUP BY c.nombre
                ORDER BY cantidad DESC
                LIMIT 10
                """,
                (user_id,),
            )

        por_categoria = []
        for row in cursor.fetchall():
            por_categoria.append(
                {
                    "categoria": row[0],
                    "cantidad": row[1],
                    "valor_total": float(row[2]) if row[2] else 0,
                }
            )

        # 1.3 Productos próximos a agotarse - CORREGIDO
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    COALESCE(
                        (iu.fecha_estimada_agotamiento::date - CURRENT_DATE),
                        0
                     )::INTEGER as dias_estimados,
                    iu.establecimiento
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                  AND iu.cantidad_actual <= iu.nivel_alerta * 2
                  AND iu.fecha_estimada_agotamiento IS NOT NULL
                ORDER BY iu.fecha_estimada_agotamiento ASC
                LIMIT 10
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    COALESCE(
                        CAST((julianday(iu.fecha_estimada_agotamiento) - julianday('now')) AS INTEGER),
                        0
                    ) as dias_estimados,
                    iu.establecimiento
                FROM inventario_usuario iu
                JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                  AND iu.cantidad_actual <= iu.nivel_alerta * 2
                  AND iu.fecha_estimada_agotamiento IS NOT NULL
                ORDER BY iu.fecha_estimada_agotamiento ASC
                LIMIT 10
                """,
                (user_id,),
            )

        proximos_agotar = []
        for row in cursor.fetchall():
            proximos_agotar.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "cantidad_actual": float(row[2]) if row[2] else 0,
                    "dias_estimados": max(0, row[3] or 0),
                    "establecimiento": row[4],
                }
            )

        resultado["inventario"] = {
            "total": total_productos,
            "stock_bajo": stock_bajo,
            "stock_medio": stock_medio,
            "stock_normal": total_productos - stock_bajo - stock_medio,
            "valor_total": valor_total,
            "por_categoria": por_categoria,
            "proximos_agotar": proximos_agotar,
        }

        # 2.1 Gasto mensual
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
                  AND EXTRACT(YEAR FROM fecha_cargue) = EXTRACT(YEAR FROM CURRENT_DATE)
                  AND EXTRACT(MONTH FROM fecha_cargue) = EXTRACT(MONTH FROM CURRENT_DATE)
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                  AND strftime('%Y-%m', fecha_cargue) = strftime('%Y-%m', 'now')
                """,
                (user_id,),
            )

        gasto_mensual = float(cursor.fetchone()[0])

        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT monto_mensual, gasto_actual
                FROM presupuesto_usuario
                WHERE usuario_id = %s AND activo = TRUE
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT monto_mensual, gasto_actual
                FROM presupuesto_usuario
                WHERE usuario_id = ? AND activo = 1
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )

        presupuesto_row = cursor.fetchone()
        presupuesto_mensual = (
            float(presupuesto_row[0]) if presupuesto_row and presupuesto_row[0] else 0
        )
        ahorro_mensual = (
            presupuesto_mensual - gasto_mensual if presupuesto_mensual > 0 else 0
        )

        # 2.2 Gastos por mes
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    TO_CHAR(fecha_cargue, 'MM') as mes,
                    EXTRACT(YEAR FROM fecha_cargue)::INTEGER as anio,
                    COALESCE(SUM(total_factura), 0) as total
                FROM facturas
                WHERE usuario_id = %s
                  AND fecha_cargue >= CURRENT_DATE - INTERVAL '6 months'
                GROUP BY TO_CHAR(fecha_cargue, 'MM'), EXTRACT(YEAR FROM fecha_cargue)
                ORDER BY anio DESC, mes DESC
                LIMIT 6
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    strftime('%m', fecha_cargue) as mes,
                    CAST(strftime('%Y', fecha_cargue) AS INTEGER) as anio,
                    COALESCE(SUM(total_factura), 0) as total
                FROM facturas
                WHERE usuario_id = ?
                  AND date(fecha_cargue) >= date('now', '-6 months')
                GROUP BY strftime('%Y-%m', fecha_cargue)
                ORDER BY fecha_cargue DESC
                LIMIT 6
                """,
                (user_id,),
            )

        gastos_por_mes = []
        for row in cursor.fetchall():
            gastos_por_mes.append(
                {"mes": row[0], "anio": row[1], "total": float(row[2]) if row[2] else 0}
            )

        # 2.3 Top productos
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    pm.nombre_consolidado,
                    COALESCE(SUM(itf.cantidad), 0) as cantidad_comprada,
                    COALESCE(SUM(itf.precio_pagado * itf.cantidad), 0) as total_gastado,
                    COALESCE(AVG(itf.precio_pagado), 0) as precio_promedio
                FROM items_factura itf
                JOIN productos_maestros_v2 pm ON itf.producto_maestro_id = pm.id
                WHERE itf.usuario_id = %s
                GROUP BY pm.id, pm.nombre_consolidado
                ORDER BY cantidad_comprada DESC
                LIMIT 5
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    pm.nombre_consolidado,
                    COALESCE(SUM(itf.cantidad), 0) as cantidad_comprada,
                    COALESCE(SUM(itf.precio_pagado * itf.cantidad), 0) as total_gastado,
                    COALESCE(AVG(itf.precio_pagado), 0) as precio_promedio
                FROM items_factura itf
                JOIN productos_maestros_v2 pm ON itf.producto_maestro_id = pm.id
                WHERE itf.usuario_id = ?
                GROUP BY pm.id, pm.nombre_consolidado
                ORDER BY cantidad_comprada DESC
                LIMIT 5
                """,
                (user_id,),
            )

        top_productos = []
        for row in cursor.fetchall():
            top_productos.append(
                {
                    "nombre": row[0],
                    "cantidad_comprada": int(row[1]) if row[1] else 0,
                    "total_gastado": float(row[2]) if row[2] else 0,
                    "precio_promedio": float(row[3]) if row[3] else 0,
                }
            )

        # 2.4 Top establecimientos
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    f.establecimiento,
                    COUNT(*) as numero_compras,
                    COALESCE(SUM(f.total_factura), 0) as total_gastado,
                    COALESCE(AVG(f.total_factura), 0) as promedio_compra
                FROM facturas f
                WHERE f.usuario_id = %s
                GROUP BY f.establecimiento
                ORDER BY total_gastado DESC
                LIMIT 3
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    f.establecimiento,
                    COUNT(*) as numero_compras,
                    COALESCE(SUM(f.total_factura), 0) as total_gastado,
                    COALESCE(AVG(f.total_factura), 0) as promedio_compra
                FROM facturas f
                WHERE f.usuario_id = ?
                GROUP BY f.establecimiento
                ORDER BY total_gastado DESC
                LIMIT 3
                """,
                (user_id,),
            )

        top_establecimientos = []
        for row in cursor.fetchall():
            top_establecimientos.append(
                {
                    "nombre": row[0],
                    "numero_compras": row[1],
                    "total_gastado": float(row[2]) if row[2] else 0,
                    "promedio_compra": float(row[3]) if row[3] else 0,
                }
            )

        resultado["gastos"] = {
            "gasto_mensual": gasto_mensual,
            "presupuesto_mensual": presupuesto_mensual,
            "ahorro_mensual": ahorro_mensual,
            "gastos_por_mes": gastos_por_mes,
            "top_productos": top_productos,
            "top_establecimientos": top_establecimientos,
        }

        # 3.1 Comparativa (simplificada porque V2 no tiene precio_promedio_global)
        resultado["comunitarias"] = {
            "ahorro_vs_promedio": 0,
            "productos_baratos": 0,
            "productos_caros": 0,
            "productos_comparativa": [],
            "mejores_establecimientos": [],
        }

        conn.close()

        return resultado

    except Exception as e:
        conn.close()
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
