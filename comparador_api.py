# ============================================================================
# NUEVO ARCHIVO: comparador_api.py
# ============================================================================
# PROPÓSITO: Comparar precios del mismo producto (por EAN) en diferentes supermercados
# ============================================================================

from fastapi import APIRouter, HTTPException
import logging
from database import get_db_connection
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/comparador/precios")
async def obtener_productos_comparables():
    """
    Obtiene productos con el MISMO EAN en DIFERENTES establecimientos
    para comparación de precios.

    Criterios:
    - Debe tener código EAN válido (13 dígitos)
    - Debe estar en al menos 2 establecimientos diferentes
    - Precios actualizados en últimos 90 días

    Returns:
        Lista de productos con sus precios por establecimiento
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar productos con EAN que estén en múltiples establecimientos
        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                c.nombre as categoria,
                COUNT(DISTINCT ppe.establecimiento_id) as num_establecimientos
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            INNER JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
            WHERE
                pm.codigo_ean IS NOT NULL
                AND LENGTH(pm.codigo_ean) = 13
                AND ppe.precio_unitario > 0
                AND ppe.ultima_actualizacion >= NOW() - INTERVAL '90 days'
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.marca, c.nombre
            HAVING COUNT(DISTINCT ppe.establecimiento_id) >= 2
            ORDER BY pm.veces_visto DESC
            LIMIT 100
        """

        cursor.execute(query)
        productos = cursor.fetchall()

        resultado = []
        total_precios = 0
        establecimientos_set = set()

        for prod in productos:
            producto_id = prod[0]
            codigo_ean = prod[1]

            # Obtener precios de este producto en diferentes establecimientos
            cursor.execute(
                """
                SELECT
                    e.nombre_normalizado as establecimiento,
                    ppe.codigo_plu,
                    ppe.precio_unitario,
                    ppe.ultima_actualizacion,
                    ppe.total_reportes
                FROM productos_por_establecimiento ppe
                JOIN establecimientos e ON ppe.establecimiento_id = e.id
                WHERE
                    ppe.producto_maestro_id = %s
                    AND ppe.precio_unitario > 0
                    AND ppe.ultima_actualizacion >= NOW() - INTERVAL '90 days'
                ORDER BY ppe.precio_unitario ASC
            """,
                (producto_id,),
            )

            precios_rows = cursor.fetchall()

            if len(precios_rows) < 2:
                continue  # Saltar si no tiene precios en al menos 2 lugares

            precios = []
            for precio_row in precios_rows:
                establecimientos_set.add(precio_row[0])
                total_precios += 1

                # Formatear fecha
                fecha_actualizacion = precio_row[3]
                if fecha_actualizacion:
                    dias = (datetime.now() - fecha_actualizacion).days
                    if dias == 0:
                        fecha_str = "Hoy"
                    elif dias == 1:
                        fecha_str = "Ayer"
                    elif dias <= 7:
                        fecha_str = f"Hace {dias} días"
                    elif dias <= 30:
                        fecha_str = f"Hace {dias//7} semanas"
                    else:
                        fecha_str = fecha_actualizacion.strftime("%d/%m/%Y")
                else:
                    fecha_str = "Fecha desconocida"

                precios.append(
                    {
                        "establecimiento": precio_row[0],
                        "plu": precio_row[1],
                        "precio": float(precio_row[2]),
                        "fecha": fecha_str,
                        "fecha_raw": (
                            fecha_actualizacion.isoformat()
                            if fecha_actualizacion
                            else None
                        ),
                        "veces_visto": precio_row[4] or 1,
                    }
                )

            # Calcular diferencias
            precio_min = min(p["precio"] for p in precios)
            precio_max = max(p["precio"] for p in precios)
            diferencia = precio_max - precio_min
            diferencia_porcentaje = (
                round((diferencia / precio_min) * 100, 1) if precio_min > 0 else 0
            )

            resultado.append(
                {
                    "id": producto_id,
                    "codigo_ean": codigo_ean,
                    "nombre": prod[2],
                    "marca": prod[3],
                    "categoria": prod[4] or "Sin categoría",
                    "precios": precios,
                    "precio_min": precio_min,
                    "precio_max": precio_max,
                    "diferencia": diferencia,
                    "diferencia_porcentaje": diferencia_porcentaje,
                    "num_establecimientos": len(precios),
                }
            )

        # Calcular ahorro promedio
        ahorro_promedio = 0
        if resultado:
            ahorros = [
                p["diferencia_porcentaje"]
                for p in resultado
                if p["diferencia_porcentaje"] > 0
            ]
            ahorro_promedio = round(sum(ahorros) / len(ahorros), 1) if ahorros else 0

        cursor.close()
        conn.close()

        logger.info(
            f"✅ Comparador: {len(resultado)} productos comparables encontrados"
        )

        return {
            "success": True,
            "productos": resultado,
            "estadisticas": {
                "total_productos": len(resultado),
                "total_establecimientos": len(establecimientos_set),
                "total_precios": total_precios,
                "ahorro_promedio": ahorro_promedio,
            },
        }

    except Exception as e:
        logger.error(f"❌ Error en comparador de precios: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/comparador/producto/{producto_id}")
async def comparar_producto_especifico(producto_id: int):
    """
    Compara precios de UN producto específico en todos los establecimientos
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener info del producto
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                c.nombre as categoria
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
            WHERE pm.id = %s
        """,
            (producto_id,),
        )

        producto = cursor.fetchone()

        if not producto:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener precios
        cursor.execute(
            """
            SELECT
                e.nombre_normalizado,
                ppe.codigo_plu,
                ppe.precio_unitario,
                ppe.ultima_actualizacion,
                ppe.total_reportes
            FROM productos_por_establecimiento ppe
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
            ORDER BY ppe.precio_unitario ASC
        """,
            (producto_id,),
        )

        precios_rows = cursor.fetchall()
        cursor.close()
        conn.close()

        precios = []
        for row in precios_rows:
            fecha = row[3]
            fecha_str = fecha.strftime("%d/%m/%Y") if fecha else "Sin fecha"

            precios.append(
                {
                    "establecimiento": row[0],
                    "plu": row[1],
                    "precio": float(row[2]),
                    "fecha": fecha_str,
                    "veces_visto": row[4] or 1,
                }
            )

        # Calcular diferencias
        precio_min = min(p["precio"] for p in precios) if precios else 0
        precio_max = max(p["precio"] for p in precios) if precios else 0

        return {
            "success": True,
            "producto": {
                "id": producto[0],
                "codigo_ean": producto[1],
                "nombre": producto[2],
                "marca": producto[3],
                "categoria": producto[4],
            },
            "precios": precios,
            "precio_min": precio_min,
            "precio_max": precio_max,
            "diferencia": precio_max - precio_min,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error comparando producto: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# INSTRUCCIONES PARA REGISTRAR EN main.py
# ============================================================================
# Agrega estas líneas en main.py:
#
# from comparador_api import router as comparador_router
# app.include_router(comparador_router)
# ============================================================================
