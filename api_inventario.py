"""
api_inventario.py - APIs REST para la App Flutter
ACTUALIZADO: Incluye todos los campos nuevos del inventario
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
# 1. OBTENER INVENTARIO DEL USUARIO ⭐ ACTUALIZADO
# ========================================
@router.get("/usuario/{user_id}")
async def get_inventario_usuario(user_id: int):
    """
    GET /api/inventario/usuario/{user_id}

    Obtiene el inventario completo con TODOS los campos nuevos:
    - Precios (última compra, promedio, mínimo, máximo)
    - Información del establecimiento
    - Estadísticas de compra
    - Relación con facturas
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
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pm.imagen_url,
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
                    pm.precio_promedio_global,
                    -- ⭐ CAMPOS NUEVOS
                    iu.precio_ultima_compra,
                    iu.precio_promedio,
                    iu.precio_minimo,
                    iu.precio_maximo,
                    iu.establecimiento_nombre,
                    iu.establecimiento_id,
                    iu.establecimiento_ubicacion,
                    iu.numero_compras,
                    iu.cantidad_total_comprada,
                    iu.total_gastado,
                    iu.ultima_factura_id,
                    iu.cantidad_por_unidad,
                    iu.dias_desde_ultima_compra
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    pm.nombre_normalizado
            """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pm.imagen_url,
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
                    pm.precio_promedio_global,
                    iu.precio_ultima_compra,
                    iu.precio_promedio,
                    iu.precio_minimo,
                    iu.precio_maximo,
                    iu.establecimiento_nombre,
                    iu.establecimiento_id,
                    iu.establecimiento_ubicacion,
                    iu.numero_compras,
                    iu.cantidad_total_comprada,
                    iu.total_gastado,
                    iu.ultima_factura_id,
                    iu.cantidad_por_unidad,
                    iu.dias_desde_ultima_compra
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY
                    CASE
                        WHEN iu.cantidad_actual <= iu.nivel_alerta THEN 1
                        WHEN iu.cantidad_actual <= (iu.nivel_alerta * 2) THEN 2
                        ELSE 3
                    END,
                    pm.nombre_normalizado
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
                    "marca": row[3] or "",
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
                    # ⭐ CAMPOS NUEVOS
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

        # Calcular estadísticas
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
        raise HTTPException(
            status_code=500, detail=f"Error al obtener inventario: {str(e)}"
        )


# ========================================
# 2. ACTUALIZAR CANTIDAD DE PRODUCTO
# ========================================
@router.put("/producto/{inventario_id}")
async def actualizar_cantidad(inventario_id: int, data: dict):
    """
    PUT /api/inventario/producto/{inventario_id}
    Body: {"cantidad": 5.0, "usuario_id": 1}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        nueva_cantidad = data.get("cantidad")
        usuario_id = data.get("usuario_id")

        if nueva_cantidad is None or usuario_id is None:
            raise HTTPException(status_code=400, detail="Faltan datos requeridos")

        # Verificar que el inventario pertenece al usuario
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

        # Actualizar cantidad
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
    """
    GET /api/inventario/alertas/{user_id}?solo_activas=true
    """
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
                pm.nombre_normalizado as producto_nombre,
                pm.codigo_ean,
                e.nombre_normalizado as establecimiento_nombre
            FROM alertas_usuario a
            LEFT JOIN productos_maestros pm ON a.producto_maestro_id = pm.id
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
    """
    PUT /api/inventario/alertas/{alerta_id}/marcar-leida
    Body: {"usuario_id": 1}
    """
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
    """
    GET /api/inventario/presupuesto/{user_id}
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    id, monto_mensual, monto_semanal, gasto_actual,
                    gasto_semanal_actual, fecha_inicio, fecha_fin, anio, mes
                FROM presupuesto_usuario
                WHERE usuario_id = %s AND activo = TRUE
                  AND CURRENT_DATE BETWEEN fecha_inicio AND fecha_fin
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    id, monto_mensual, monto_semanal, gasto_actual,
                    gasto_semanal_actual, fecha_inicio, fecha_fin, anio, mes
                FROM presupuesto_usuario
                WHERE usuario_id = ? AND activo = 1
                  AND date('now') BETWEEN fecha_inicio AND fecha_fin
                ORDER BY fecha_inicio DESC LIMIT 1
                """,
                (user_id,),
            )

        presupuesto = cursor.fetchone()

        if not presupuesto:
            conn.close()
            return {"success": True, "tiene_presupuesto": False, "presupuesto": None}

        monto_mensual = float(presupuesto[1])
        gasto_actual = float(presupuesto[3]) if presupuesto[3] else 0
        porcentaje_usado = (
            (gasto_actual / monto_mensual * 100) if monto_mensual > 0 else 0
        )

        conn.close()

        return {
            "success": True,
            "tiene_presupuesto": True,
            "presupuesto": {
                "id": presupuesto[0],
                "monto_mensual": monto_mensual,
                "monto_semanal": float(presupuesto[2]) if presupuesto[2] else 0,
                "gasto_actual": gasto_actual,
                "gasto_semanal_actual": float(presupuesto[4]) if presupuesto[4] else 0,
                "disponible": monto_mensual - gasto_actual,
                "porcentaje_usado": round(porcentaje_usado, 1),
                "fecha_inicio": str(presupuesto[5]),
                "fecha_fin": str(presupuesto[6]),
                "periodo": f"{presupuesto[8]}/{presupuesto[7]}",
            },
        }

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 6. CREAR/ACTUALIZAR PRESUPUESTO
# ========================================
@router.post("/presupuesto")
async def crear_presupuesto(data: dict):
    """
    POST /api/inventario/presupuesto
    Body: {
        "usuario_id": 1,
        "monto_mensual": 500000,
        "monto_semanal": 125000,
        "anio": 2025,
        "mes": 10
    }
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        usuario_id = data.get("usuario_id")
        monto_mensual = data.get("monto_mensual")
        monto_semanal = data.get("monto_semanal")
        anio = data.get("anio", datetime.now().year)
        mes = data.get("mes", datetime.now().month)

        if not usuario_id or not monto_mensual:
            raise HTTPException(status_code=400, detail="Faltan datos requeridos")

        # Calcular fechas del periodo
        fecha_inicio = datetime(anio, mes, 1).date()
        if mes == 12:
            fecha_fin = datetime(anio + 1, 1, 1).date() - timedelta(days=1)
        else:
            fecha_fin = datetime(anio, mes + 1, 1).date() - timedelta(days=1)

        # Desactivar presupuestos anteriores
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

        # Crear nuevo presupuesto
        if database_type == "postgresql":
            cursor.execute(
                """
                INSERT INTO presupuesto_usuario
                (usuario_id, monto_mensual, monto_semanal, anio, mes, fecha_inicio, fecha_fin)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                (usuario_id, monto_mensual, monto_semanal, anio, mes, fecha_inicio, fecha_fin)
                VALUES (?, ?, ?, ?, ?, ?, ?)
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
        }

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ========================================
# 7. ESTADÍSTICAS DEL USUARIO
# ========================================
@router.get("/estadisticas/{user_id}")
async def get_estadisticas_usuario(user_id: int):
    """
    GET /api/inventario/estadisticas/{user_id}
    Dashboard completo con todas las métricas del usuario
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        stats = {}

        # 1. Productos en inventario
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta THEN 1 ELSE 0 END) as bajos,
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta * 2 AND cantidad_actual > nivel_alerta THEN 1 ELSE 0 END) as medios
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
                    SUM(CASE WHEN cantidad_actual <= nivel_alerta * 2 AND cantidad_actual > nivel_alerta THEN 1 ELSE 0 END) as medios
                FROM inventario_usuario
                WHERE usuario_id = ?
                """,
                (user_id,),
            )

        inv = cursor.fetchone()
        stats["inventario"] = {
            "total": inv[0] or 0,
            "stock_bajo": inv[1] or 0,
            "stock_medio": inv[2] or 0,
        }

        # 2. Alertas activas
        if database_type == "postgresql":
            cursor.execute(
                "SELECT COUNT(*) FROM alertas_usuario WHERE usuario_id = %s AND activa = TRUE AND enviada = FALSE",
                (user_id,),
            )
        else:
            cursor.execute(
                "SELECT COUNT(*) FROM alertas_usuario WHERE usuario_id = ? AND activa = 1 AND enviada = 0",
                (user_id,),
            )

        stats["alertas_pendientes"] = cursor.fetchone()[0] or 0

        # 3. Facturas del mes actual
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*), COALESCE(SUM(total_factura), 0)
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
                SELECT COUNT(*), COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
                  AND strftime('%Y-%m', fecha_cargue) = strftime('%Y-%m', 'now')
                """,
                (user_id,),
            )

        fac = cursor.fetchone()
        stats["facturas_mes"] = {
            "cantidad": fac[0] or 0,
            "total_gastado": float(fac[1]) if fac[1] else 0,
        }

        conn.close()

        return {"success": True, "estadisticas": stats}

    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ API Inventario para Flutter cargado correctamente con campos nuevos")
