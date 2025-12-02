# ============================================================================
# comparador_api.py - VERSION 2024-12-02
# ============================================================================
# MODOS DE COMPARACIÓN:
# 1. MODO TIENDA: ¿Qué marca es más barata en MI supermercado?
# 2. MODO PRODUCTO: ¿En qué supermercado está más barato ESTE producto?
# ============================================================================

from fastapi import APIRouter, HTTPException, Query
import logging
from database import get_db_connection
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)
router = APIRouter()


# ============================================================================
# 🆕 MODO 1: MEJOR MARCA/PRODUCTO DENTRO DE UN SUPERMERCADO
# ============================================================================
@router.get("/api/comparador/mejor-en-tienda")
async def buscar_mejor_en_tienda(
    busqueda: str = Query(
        ..., min_length=2, description="Palabra clave (ej: empanadas, leche, arroz)"
    ),
    establecimiento_id: Optional[int] = Query(
        None, description="ID del supermercado (si no se envía, busca en todos)"
    ),
):
    """
    🏪 MODO 1: Buscar productos por palabra clave y ver cuál es más barato

    Ejemplo: Buscar "empanadas" en Jumbo → Ver todas las marcas ordenadas por precio

    Query params:
    - busqueda: Palabra clave (mínimo 2 caracteres)
    - establecimiento_id: ID del supermercado (opcional, si no se envía busca en todos)

    Retorna productos ordenados por precio con indicador de "mejor precio"
    """
    print(f"\n{'='*60}")
    print(f"🏪 MODO TIENDA: Buscando '{busqueda}'")
    if establecimiento_id:
        print(f"   📍 En establecimiento ID: {establecimiento_id}")
    else:
        print(f"   📍 En TODOS los establecimientos")
    print(f"{'='*60}")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Limpiar búsqueda
        busqueda_limpia = busqueda.strip().upper()
        busqueda_pattern = f"%{busqueda_limpia}%"

        # Query base
        query = """
            SELECT
                pm.id,
                pm.nombre_consolidado,
                pm.marca,
                pm.codigo_ean,
                e.id as establecimiento_id,
                e.nombre_normalizado as establecimiento,
                ppe.precio_unitario,
                ppe.fecha_actualizacion,
                COALESCE(ppe.total_reportes, 1) as veces_visto
            FROM productos_maestros_v2 pm
            INNER JOIN productos_por_establecimiento ppe
                ON pm.id = ppe.producto_maestro_id
            INNER JOIN establecimientos e
                ON ppe.establecimiento_id = e.id
            WHERE UPPER(pm.nombre_consolidado) LIKE %s
              AND ppe.precio_unitario > 0
        """

        params = [busqueda_pattern]

        # Filtrar por establecimiento si se especifica
        if establecimiento_id:
            query += " AND e.id = %s"
            params.append(establecimiento_id)

        query += " ORDER BY ppe.precio_unitario ASC"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        print(f"📊 Productos encontrados: {len(rows)}")

        if not rows:
            # Buscar establecimientos disponibles para sugerir
            cursor.execute(
                """
                SELECT id, nombre_normalizado
                FROM establecimientos
                ORDER BY nombre_normalizado
            """
            )
            establecimientos = [{"id": r[0], "nombre": r[1]} for r in cursor.fetchall()]

            conn.close()
            return {
                "success": True,
                "mensaje": f"No encontramos productos con '{busqueda}'",
                "sugerencia": "Intenta con otra palabra clave como: leche, pan, arroz, pollo",
                "productos": [],
                "establecimientos_disponibles": establecimientos,
                "total": 0,
            }

        # Procesar resultados
        productos = []
        precio_minimo = rows[0][6]  # El primero es el más barato (ORDER BY precio ASC)

        for row in rows:
            precio = float(row[6])
            diferencia = precio - precio_minimo

            # Formatear fecha
            fecha = row[7]
            if fecha:
                dias = (datetime.now() - fecha).days
                if dias == 0:
                    fecha_str = "Hoy"
                elif dias == 1:
                    fecha_str = "Ayer"
                elif dias <= 7:
                    fecha_str = f"Hace {dias} días"
                else:
                    fecha_str = fecha.strftime("%d/%m/%Y")
            else:
                fecha_str = ""

            productos.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "marca": row[2] or "Sin marca",
                    "codigo_ean": row[3],
                    "establecimiento_id": row[4],
                    "establecimiento": row[5],
                    "precio": precio,
                    "precio_formateado": f"${precio:,.0f}".replace(",", "."),
                    "es_mejor_precio": diferencia == 0,
                    "diferencia": diferencia,
                    "diferencia_formateada": (
                        f"+${diferencia:,.0f}".replace(",", ".")
                        if diferencia > 0
                        else "⭐ Mejor precio"
                    ),
                    "fecha_actualizado": fecha_str,
                    "veces_visto": row[8],
                }
            )

        # Obtener nombre del establecimiento si se filtró
        nombre_establecimiento = None
        if establecimiento_id and productos:
            nombre_establecimiento = productos[0]["establecimiento"]

        conn.close()

        print(f"✅ Retornando {len(productos)} productos")
        print(f"   💰 Mejor precio: ${precio_minimo:,.0f}")

        return {
            "success": True,
            "busqueda": busqueda,
            "establecimiento_id": establecimiento_id,
            "establecimiento_nombre": nombre_establecimiento,
            "productos": productos,
            "mejor_precio": precio_minimo,
            "total": len(productos),
            "mensaje_usuario": f"Encontramos {len(productos)} productos con '{busqueda}'"
            + (f" en {nombre_establecimiento}" if nombre_establecimiento else ""),
        }

    except Exception as e:
        logger.error(f"❌ Error en mejor-en-tienda: {e}")
        import traceback

        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 🆕 ENDPOINT AUXILIAR: Listar establecimientos disponibles
# ============================================================================
@router.get("/api/comparador/establecimientos")
async def listar_establecimientos():
    """
    📍 Lista todos los establecimientos disponibles para el dropdown

    Retorna lista ordenada de establecimientos con conteo de productos
    """
    print("📍 Obteniendo lista de establecimientos...")

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                e.id,
                e.nombre_normalizado,
                e.cadena,
                COUNT(DISTINCT ppe.producto_maestro_id) as total_productos
            FROM establecimientos e
            LEFT JOIN productos_por_establecimiento ppe
                ON e.id = ppe.establecimiento_id
            GROUP BY e.id, e.nombre_normalizado, e.cadena
            HAVING COUNT(DISTINCT ppe.producto_maestro_id) > 0
            ORDER BY e.cadena, e.nombre_normalizado
        """
        )

        rows = cursor.fetchall()
        conn.close()

        establecimientos = []
        for row in rows:
            establecimientos.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "cadena": row[2] or "Otro",
                    "total_productos": row[3],
                    "nombre_display": f"{row[1]} ({row[3]} productos)",
                }
            )

        print(f"✅ {len(establecimientos)} establecimientos con productos")

        return {
            "success": True,
            "establecimientos": establecimientos,
            "total": len(establecimientos),
        }

    except Exception as e:
        logger.error(f"❌ Error listando establecimientos: {e}")
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# 🔄 MODO 2 (EXISTENTE): MISMO PRODUCTO EN DIFERENTES TIENDAS
# ============================================================================
@router.get("/api/comparador/precios")
async def obtener_productos_comparables():
    """
    🛒 MODO 2: Obtiene productos con precios en múltiples establecimientos

    Agrupa por codigo_lecfac para comparar el MISMO producto entre tiendas
    """
    print("\n" + "=" * 80)
    print("🛒 MODO PRODUCTO: Comparando mismo producto entre tiendas")
    print("=" * 80)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

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

        if len(rows) == 0:
            cursor.close()
            conn.close()
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
        productos_dict = {}
        establecimientos_set = set()

        for row in rows:
            codigo_lecfac = row[4]
            establecimiento = row[7]
            establecimientos_set.add(establecimiento)

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

        # Filtrar solo productos con 2+ precios (comparables)
        productos_comparables = []
        total_ahorro = 0
        productos_con_diferencia = 0

        for codigo_lecfac, prod in productos_dict.items():
            num_precios = len(prod["precios"])

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

        print(f"✅ {len(productos_comparables)} productos comparables")

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
    🛒 MODO 2 (detalle): Compara precios de UN producto específico entre tiendas
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

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


# ============================================================================
# 🔍 NUEVO: Búsqueda rápida de productos (para autocompletado)
# ============================================================================
@router.get("/api/comparador/buscar")
async def buscar_productos_rapido(
    q: str = Query(..., min_length=2, description="Texto a buscar")
):
    """
    🔍 Búsqueda rápida para autocompletado

    Retorna hasta 10 sugerencias de productos que coincidan
    """
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        busqueda_pattern = f"%{q.strip().upper()}%"

        cursor.execute(
            """
            SELECT DISTINCT
                pm.id,
                pm.nombre_consolidado,
                pm.marca,
                COUNT(DISTINCT ppe.establecimiento_id) as num_tiendas
            FROM productos_maestros_v2 pm
            INNER JOIN productos_por_establecimiento ppe
                ON pm.id = ppe.producto_maestro_id
            WHERE UPPER(pm.nombre_consolidado) LIKE %s
              AND ppe.precio_unitario > 0
            GROUP BY pm.id, pm.nombre_consolidado, pm.marca
            ORDER BY num_tiendas DESC, pm.nombre_consolidado
            LIMIT 10
        """,
            (busqueda_pattern,),
        )

        rows = cursor.fetchall()
        conn.close()

        sugerencias = []
        for row in rows:
            sugerencias.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "marca": row[2] or "",
                    "num_tiendas": row[3],
                    "display": f"{row[1]}"
                    + (f" - {row[2]}" if row[2] else "")
                    + f" ({row[3]} tiendas)",
                }
            )

        return {"success": True, "sugerencias": sugerencias}

    except Exception as e:
        logger.error(f"❌ Error en búsqueda rápida: {e}")
        if conn:
            conn.close()
        return {"success": False, "sugerencias": []}


print("✅ Comparador API v2 cargado - Modos: TIENDA y PRODUCTO")
