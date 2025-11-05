"""
comparacion_precios.py
Endpoint para comparar precios de un producto entre establecimientos
"""

from fastapi import APIRouter, HTTPException, Header
from typing import Optional
from datetime import datetime, timedelta
from database import get_db_connection
import os

router = APIRouter(prefix="/api/productos", tags=["comparacion-precios"])


def get_user_id_from_token(authorization: Optional[str] = None) -> int:
    """Extraer usuario_id del token JWT"""
    if not authorization:
        return 1  # Usuario por defecto

    try:
        import jwt
        token = authorization.replace("Bearer ", "")
        payload = jwt.decode(token, options={"verify_signature": False})
        usuario_id = payload.get("user_id") or payload.get("sub") or payload.get("id")
        return int(usuario_id) if usuario_id else 1
    except:
        return 1


@router.get("/{producto_id}/comparar-precios")
async def comparar_precios_producto(
    producto_id: int,
    authorization: Optional[str] = Header(None)
):
    """
    📊 Comparar precios de un producto entre establecimientos

    Retorna:
    - Precio actual en cada establecimiento
    - Fecha del último reporte
    - Estadísticas (promedio, mínimo, máximo)
    - Mejor precio disponible
    - Último precio pagado por el usuario
    """
    usuario_id = get_user_id_from_token(authorization)

    print(f"📊 [COMPARACIÓN] Producto {producto_id} - Usuario {usuario_id}")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Obtener información del producto
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    id,
                    nombre_normalizado,
                    codigo_ean,
                    marca,
                    categoria
                FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    id,
                    nombre_normalizado,
                    codigo_ean,
                    marca,
                    categoria
                FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

        producto_row = cursor.fetchone()

        if not producto_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        producto_info = {
            "id": producto_row[0],
            "nombre": producto_row[1] or "Producto sin nombre",
            "codigo_ean": producto_row[2] or "",
            "marca": producto_row[3] or "",
            "categoria": producto_row[4] or "Sin categoría"
        }

        # 2. Obtener precios por establecimiento (últimos 90 días)
        fecha_limite = (datetime.now() - timedelta(days=90)).isoformat()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    f.establecimiento,
                    f.cadena,
                    i.precio_pagado,
                    f.fecha_cargue as fecha_compra,
                    COUNT(*) OVER (PARTITION BY f.establecimiento) as num_reportes,
                    AVG(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_promedio,
                    MIN(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_minimo,
                    MAX(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_maximo,
                    MAX(f.fecha_cargue) OVER (PARTITION BY f.establecimiento) as fecha_ultimo_reporte
                FROM items_factura i
                INNER JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = %s
                AND f.fecha_cargue >= %s
                AND i.precio_pagado > 0
                ORDER BY f.establecimiento, f.fecha_cargue DESC
            """, (producto_id, fecha_limite))
        else:
            cursor.execute("""
                SELECT
                    f.establecimiento,
                    f.cadena,
                    i.precio_pagado,
                    f.fecha_cargue as fecha_compra,
                    COUNT(*) OVER (PARTITION BY f.establecimiento) as num_reportes,
                    AVG(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_promedio,
                    MIN(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_minimo,
                    MAX(i.precio_pagado) OVER (PARTITION BY f.establecimiento) as precio_maximo,
                    MAX(f.fecha_cargue) OVER (PARTITION BY f.establecimiento) as fecha_ultimo_reporte
                FROM items_factura i
                INNER JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = ?
                AND f.fecha_cargue >= ?
                AND i.precio_pagado > 0
                ORDER BY f.establecimiento, f.fecha_cargue DESC
            """, (producto_id, fecha_limite))

        # Procesar resultados agrupados por establecimiento
        establecimientos_dict = {}

        for row in cursor.fetchall():
            establecimiento = row[0]
            if establecimiento not in establecimientos_dict:
                establecimientos_dict[establecimiento] = {
                    "establecimiento": establecimiento,
                    "cadena": row[1],
                    "precio_actual": float(row[2]) if row[2] else 0,
                    "fecha_ultimo_reporte": str(row[8]) if row[8] else None,
                    "numero_reportes": row[4] or 0,
                    "precio_promedio": float(row[5]) if row[5] else 0,
                    "precio_minimo": float(row[6]) if row[6] else 0,
                    "precio_maximo": float(row[7]) if row[7] else 0
                }

        comparacion = list(establecimientos_dict.values())

        # Ordenar por precio actual (menor a mayor)
        comparacion.sort(key=lambda x: x["precio_actual"])

        # 3. Identificar mejor precio
        mejor_precio = None
        if comparacion:
            mejor = min(comparacion, key=lambda x: x["precio_minimo"])
            peor = max(comparacion, key=lambda x: x["precio_maximo"])

            mejor_precio = {
                "establecimiento": mejor["establecimiento"],
                "precio": mejor["precio_minimo"],
                "fecha": mejor["fecha_ultimo_reporte"],
                "ahorro_vs_mas_caro": peor["precio_maximo"] - mejor["precio_minimo"]
            }

        # 4. Obtener último precio del usuario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    i.precio_pagado,
                    f.establecimiento,
                    f.fecha_cargue as fecha_compra
                FROM items_factura i
                INNER JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = %s
                AND i.usuario_id = %s
                ORDER BY f.fecha_cargue DESC
                LIMIT 1
            """, (producto_id, usuario_id))
        else:
            cursor.execute("""
                SELECT
                    i.precio_pagado,
                    f.establecimiento,
                    f.fecha_cargue as fecha_compra
                FROM items_factura i
                INNER JOIN facturas f ON i.factura_id = f.id
                WHERE i.producto_maestro_id = ?
                AND i.usuario_id = ?
                ORDER BY f.fecha_cargue DESC
                LIMIT 1
            """, (producto_id, usuario_id))

        mi_precio_row = cursor.fetchone()
        mi_ultimo_precio = None

        if mi_precio_row:
            mi_ultimo_precio = {
                "precio": float(mi_precio_row[0]) if mi_precio_row[0] else 0,
                "establecimiento": mi_precio_row[1],
                "fecha": str(mi_precio_row[2]) if mi_precio_row[2] else None
            }

        conn.close()

        print(f"✅ [COMPARACIÓN] {len(comparacion)} establecimientos encontrados")

        return {
            "success": True,
            "producto": producto_info,
            "comparacion": comparacion,
            "mejor_precio": mejor_precio,
            "mi_ultimo_precio": mi_ultimo_precio,
            "total_establecimientos": len(comparacion)
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [COMPARACIÓN] Error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{producto_id}/historial-precios")
async def historial_precios_producto(
    producto_id: int,
    establecimiento: Optional[str] = None,
    dias: int = 90
):
    """
    📈 Obtener historial de precios de un producto

    Query params:
    - establecimiento: Filtrar por establecimiento específico
    - dias: Días hacia atrás (default: 90)
    """
    print(f"📈 [HISTORIAL] Producto {producto_id} - Últimos {dias} días")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        fecha_limite = (datetime.now() - timedelta(days=dias)).isoformat()

        query = """
            SELECT
                DATE(f.fecha_cargue) as fecha,
                f.establecimiento,
                AVG(i.precio_pagado) as precio_promedio,
                MIN(i.precio_pagado) as precio_minimo,
                MAX(i.precio_pagado) as precio_maximo,
                COUNT(*) as num_reportes
            FROM items_factura i
            INNER JOIN facturas f ON i.factura_id = f.id
            WHERE i.producto_maestro_id = {}
            AND f.fecha_cargue >= '{}'
            AND i.precio_pagado > 0
        """.format(producto_id, fecha_limite)

        if establecimiento:
            query += " AND f.establecimiento = '{}'".format(establecimiento)

        query += """
            GROUP BY DATE(f.fecha_cargue), f.establecimiento
            ORDER BY fecha DESC, f.establecimiento
        """

        cursor.execute(query)

        historial = []
        for row in cursor.fetchall():
            historial.append({
                "fecha": str(row[0]),
                "establecimiento": row[1],
                "precio_promedio": float(row[2]) if row[2] else 0,
                "precio_minimo": float(row[3]) if row[3] else 0,
                "precio_maximo": float(row[4]) if row[4] else 0,
                "numero_reportes": row[5] or 0
            })

        conn.close()

        print(f"✅ [HISTORIAL] {len(historial)} registros encontrados")

        return {
            "success": True,
            "producto_id": producto_id,
            "dias": dias,
            "establecimiento_filtro": establecimiento,
            "historial": historial,
            "total_registros": len(historial)
        }

    except Exception as e:
        print(f"❌ [HISTORIAL] Error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de comparación de precios cargados")
