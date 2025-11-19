# ============================================================================
# comparador_api.py - VERSION 2024-11-19-23:00
# ============================================================================
# AGRUPA POR CODIGO_LECFAC (no por EAN)
# ============================================================================

from fastapi import APIRouter, HTTPException
import logging
from database import get_db_connection
from datetime import datetime

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/api/comparador/precios")
async def obtener_productos_comparables():
    """
    Obtiene productos agrupados por codigo_lecfac con precios en múltiples establecimientos.
    VERSION: 2024-11-19-23:00 - AGRUPA POR CODIGO_LECFAC
    """
    print("\n" + "=" * 80)
    print("🔍 COMPARADOR API: Iniciando consulta")
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Query que trae TODOS los productos con codigo_lecfac y precios
        query = """
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                COALESCE(pm.codigo_lecfac, CONCAT('prod-', pm.id)) as codigo_lecfac,
                COALESCE(c.nombre, 'Sin categoría') as categoria,
                ppe.codigo_plu,
                e.nombre_normalizado as establecimiento,
                ppe.precio_unitario,
                ppe.fecha_actualizacion,
                COALESCE(ppe.total_reportes, 1) as total_reportes
            FROM productos_maestros_v2 pm
            INNER JOIN productos_por_establecimiento ppe
                ON pm.id = ppe.producto_maestro_id
            INNER JOIN establecimientos e
                ON ppe.establecimiento_id = e.id
            LEFT JOIN categorias c
                ON pm.categoria_id = c.id
            WHERE ppe.precio_unitario > 0
            ORDER BY codigo_lecfac, ppe.precio_unitario ASC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        print(f"📊 Total filas obtenidas: {len(rows)}")

        if rows:
            print("\n📋 Primeras 5 filas:")
            for i, row in enumerate(rows[:5]):
                print(
                    f"   {i+1}. ID:{row[0]} | LecFac:{row[4]} | {row[2]} | {row[7]} | ${row[8]}"
                )

        if len(rows) == 0:
            cursor.close()
            conn.close()
            print("⚠️ No se encontraron productos con precios")
            logger.info("✅ Comparador: 0 productos comparables encontrados")
            return {
                "success": True,
                "productos": [],
                "estadisticas": {
                    "total_productos": 0,
                    "total_establecimientos": 0,
                    "ahorro_promedio": 0,
                },
            }

        # Agrupar por codigo_lecfac
        print("\n📦 Agrupando por codigo_lecfac...")
        productos_dict = {}
        establecimientos_set = set()

        for row in rows:
            codigo_lecfac = row[4]
            establecimiento = row[7]

            establecimientos_set.add(establecimiento)

            # Crear grupo si no existe
            if codigo_lecfac not in productos_dict:
                productos_dict[codigo_lecfac] = {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "codigo_lecfac": codigo_lecfac,
                    "categoria": row[5],
                    "precios": [],
                }
                print(f"   ✅ Nuevo grupo: {codigo_lecfac} ({row[2]})")

            # Formatear fecha
            fecha_actualizacion = row[9]
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

            # Agregar precio al grupo
            productos_dict[codigo_lecfac]["precios"].append(
                {
                    "plu": row[6],
                    "establecimiento": establecimiento,
                    "precio": float(row[8]),
                    "fecha": fecha_str,
                    "fecha_raw": (
                        fecha_actualizacion.isoformat() if fecha_actualizacion else None
                    ),
                    "veces_visto": row[10],
                }
            )
            print(f"      → Precio agregado: {establecimiento} ${row[8]}")

        print(f"\n📦 Total grupos: {len(productos_dict)}")

        # Filtrar solo productos con 2+ precios
        print("\n🔍 Filtrando productos comparables (2+ precios)...")
        productos_comparables = []
        total_ahorro = 0
        productos_con_diferencia = 0

        for codigo_lecfac, prod in productos_dict.items():
            num_precios = len(prod["precios"])

            print(f"   {prod['nombre']}: {num_precios} precio(s)")

            if num_precios >= 2:
                precios_valores = [p["precio"] for p in prod["precios"]]
                precio_min = min(precios_valores)
                precio_max = max(precios_valores)
                diferencia = precio_max - precio_min
                porcentaje = (diferencia / precio_max * 100) if precio_max > 0 else 0

                prod["precio_min"] = precio_min
                prod["precio_max"] = precio_max
                prod["diferencia"] = diferencia
                prod["diferencia_porcentaje"] = round(porcentaje, 1)
                prod["num_establecimientos"] = num_precios

                if porcentaje > 0:
                    total_ahorro += porcentaje
                    productos_con_diferencia += 1

                productos_comparables.append(prod)
                print(f"      ✅ COMPARABLE: {porcentaje:.1f}% diferencia")
            else:
                print(f"      ❌ Solo tiene 1 precio, no es comparable")

        # Ordenar por mayor ahorro
        productos_comparables.sort(
            key=lambda x: x.get("diferencia_porcentaje", 0), reverse=True
        )

        ahorro_promedio = (
            round(total_ahorro / productos_con_diferencia, 1)
            if productos_con_diferencia > 0
            else 0
        )

        cursor.close()
        conn.close()

        print(f"\n📊 RESUMEN:")
        print(f"   Productos comparables: {len(productos_comparables)}")
        print(f"   Establecimientos: {len(establecimientos_set)}")
        print(f"   Ahorro promedio: {ahorro_promedio}%")
        print("=" * 80 + "\n")

        logger.info(
            f"✅ Comparador: {len(productos_comparables)} productos comparables encontrados"
        )

        return {
            "success": True,
            "productos": productos_comparables,
            "estadisticas": {
                "total_productos": len(productos_comparables),
                "total_establecimientos": len(establecimientos_set),
                "ahorro_promedio": ahorro_promedio,
            },
        }

    except Exception as e:
        logger.error(f"❌ Error en comparador: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.close()
        return {
            "success": False,
            "error": str(e),
            "productos": [],
            "estadisticas": {},
        }


@router.get("/api/comparador/producto/{producto_id}")
async def comparar_producto_especifico(producto_id: int):
    """
    Compara precios de UN producto específico por codigo_lecfac
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener codigo_lecfac del producto
        cursor.execute(
            """
            SELECT codigo_lecfac, nombre_consolidado
            FROM productos_maestros_v2
            WHERE id = %s
        """,
            (producto_id,),
        )

        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        codigo_lecfac = result[0]
        nombre = result[1]

        # Obtener todos los productos con ese codigo_lecfac
        cursor.execute(
            """
            SELECT
                pm.id,
                e.nombre_normalizado,
                ppe.codigo_plu,
                ppe.precio_unitario,
                ppe.fecha_actualizacion,
                ppe.total_reportes
            FROM productos_maestros_v2 pm
            JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE pm.codigo_lecfac = %s
            ORDER BY ppe.precio_unitario ASC
        """,
            (codigo_lecfac,),
        )

        precios_rows = cursor.fetchall()
        cursor.close()
        conn.close()

        precios = []
        for row in precios_rows:
            fecha = row[4]
            fecha_str = fecha.strftime("%d/%m/%Y") if fecha else "Sin fecha"

            precios.append(
                {
                    "establecimiento": row[1],
                    "plu": row[2],
                    "precio": float(row[3]),
                    "fecha": fecha_str,
                    "veces_visto": row[5] or 1,
                }
            )

        precio_min = min(p["precio"] for p in precios) if precios else 0
        precio_max = max(p["precio"] for p in precios) if precios else 0

        return {
            "success": True,
            "producto": {
                "nombre": nombre,
                "codigo_lecfac": codigo_lecfac,
            },
            "precios": precios,
            "precio_min": precio_min,
            "precio_max": precio_max,
            "diferencia": precio_max - precio_min,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))
