"""
=============================================================================
LEFACT - PRODUCTOS_MEJORAS.PY
=============================================================================
Endpoints para gestión avanzada de productos:
- Detección de duplicados (EAN, PLU, nombres similares)
- Fusión de productos
- Análisis de calidad de datos
- Estadísticas de estandarización

Compatible con database.py de LecFac (sin SQLAlchemy - conexiones directas)

✅ VERSIÓN CORREGIDA - Sin referencias a codigo_plu en productos_maestros
=============================================================================
"""

from fastapi import APIRouter, HTTPException
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from difflib import SequenceMatcher
import re
import os
import json

# Importar funciones de database.py
from database import get_db_connection

router = APIRouter(prefix="/api/productos", tags=["productos-mejoras"])


# ============================================================================
# MODELOS PYDANTIC
# ============================================================================

class FusionarRequest(BaseModel):
    producto_principal_id: int
    productos_duplicados_ids: List[int]
    mantener_datos_de: str = "mas_completo"  # "principal", "mas_completo"


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================

def similitud_nombres(nombre1: str, nombre2: str) -> float:
    """Calcula similitud entre dos nombres usando SequenceMatcher"""
    if not nombre1 or not nombre2:
        return 0.0

    # Normalizar: minúsculas, sin acentos, sin espacios extra
    def normalizar(texto: str) -> str:
        texto = texto.lower().strip()
        texto = re.sub(r'\s+', ' ', texto)
        # Quitar acentos
        acentos = {'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u', 'ñ': 'n'}
        for acento, sin_acento in acentos.items():
            texto = texto.replace(acento, sin_acento)
        return texto

    n1 = normalizar(nombre1)
    n2 = normalizar(nombre2)

    return SequenceMatcher(None, n1, n2).ratio()


def producto_mas_completo(productos: List[Dict]) -> Dict:
    """Determina cuál producto tiene más datos completos"""
    def puntaje_completitud(p: Dict) -> int:
        puntos = 0
        if p.get('codigo_ean'): puntos += 3
        if p.get('marca'): puntos += 2
        if p.get('categoria'): puntos += 1
        if p.get('subcategoria'): puntos += 1
        if p.get('presentacion'): puntos += 1
        return puntos

    return max(productos, key=puntaje_completitud)


# ============================================================================
# ENDPOINTS - DETECCIÓN DE DUPLICADOS
# ============================================================================

@router.get("/duplicados/ean")
async def detectar_duplicados_ean():
    """
    Detecta productos con el mismo código EAN pero diferentes IDs
    ❌ SEVERIDAD ALTA - Esto NO debería pasar
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Query para encontrar EANs duplicados
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    codigo_ean,
                    COUNT(*) as total,
                    ARRAY_AGG(id) as producto_ids
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                  AND codigo_ean != ''
                  AND LENGTH(codigo_ean) >= 8
                GROUP BY codigo_ean
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
            """)
        else:
            cursor.execute("""
                SELECT
                    codigo_ean,
                    COUNT(*) as total,
                    GROUP_CONCAT(id) as producto_ids
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                  AND codigo_ean != ''
                  AND LENGTH(codigo_ean) >= 8
                GROUP BY codigo_ean
                HAVING COUNT(*) > 1
                ORDER BY COUNT(*) DESC
            """)

        duplicados_ean = cursor.fetchall()

        resultados = []
        for dup in duplicados_ean:
            codigo_ean = dup[0]
            total = dup[1]
            producto_ids = dup[2]

            # Convertir string de IDs a lista
            if isinstance(producto_ids, str):
                ids_list = [int(x) for x in producto_ids.split(',')]
            else:
                ids_list = producto_ids

            # Obtener detalles de cada producto
            productos_info = []
            for prod_id in ids_list:
                if database_type == "postgresql":
                    cursor.execute("""
                        SELECT
                            pm.id, pm.nombre_normalizado, pm.nombre_comercial, pm.marca,
                            pm.categoria, pm.subcategoria, pm.codigo_ean,
                            pm.primera_vez_reportado,
                            (SELECT COUNT(*) FROM items_factura WHERE producto_maestro_id = pm.id) as total_compras
                        FROM productos_maestros pm
                        WHERE pm.id = %s
                    """, (prod_id,))
                else:
                    cursor.execute("""
                        SELECT
                            pm.id, pm.nombre_normalizado, pm.nombre_comercial, pm.marca,
                            pm.categoria, pm.subcategoria, pm.codigo_ean
                        FROM productos_maestros pm
                        WHERE pm.id = ?
                    """, (prod_id,))

                p = cursor.fetchone()
                if not p:
                    continue

                # Contar compras (si no se obtuvo en la query anterior)
                if database_type == "sqlite":
                    cursor.execute("""
                        SELECT COUNT(*) FROM items_factura
                        WHERE producto_maestro_id = ?
                    """, (prod_id,))
                    total_compras = cursor.fetchone()[0]
                else:
                    total_compras = p[8] if len(p) > 8 else 0

                productos_info.append({
                    'id': p[0],
                    'nombre_normalizado': p[1],
                    'nombre_comercial': p[2],
                    'marca': p[3],
                    'categoria': p[4],
                    'subcategoria': p[5],
                    'codigo_ean': p[6],
                    'total_compras': total_compras,
                    'fecha_creacion': str(p[7]) if len(p) > 7 and p[7] else None
                })

            resultados.append({
                'tipo': 'ean_duplicado',
                'codigo_ean': codigo_ean,
                'total_productos': total,
                'productos': productos_info,
                'razon': f'El código EAN {codigo_ean} está registrado {total} veces',
                'severidad': 'alta'
            })

        cursor.close()
        conn.close()

        return {
            'total': len(resultados),
            'duplicados': resultados
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/duplicados/plu-establecimiento")
async def detectar_duplicados_plu_establecimiento():
    """
    Detecta productos con el mismo PLU en el mismo establecimiento
    ❌ SEVERIDAD ALTA - Cada PLU es único por tienda

    NOTA: Los PLUs están en codigos_locales o items_factura.codigo_leido
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Query para encontrar PLUs duplicados por establecimiento
        # Usar items_factura + facturas para obtener PLUs con establecimiento
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    items.codigo_leido as codigo_plu,
                    f.establecimiento,
                    ARRAY_AGG(DISTINCT items.producto_maestro_id) as producto_ids,
                    COUNT(DISTINCT items.producto_maestro_id) as total_productos
                FROM items_factura items
                INNER JOIN facturas f ON f.id = items.factura_id
                WHERE items.codigo_leido IS NOT NULL
                  AND items.codigo_leido != ''
                  AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                  AND f.establecimiento IS NOT NULL
                  AND items.producto_maestro_id IS NOT NULL
                GROUP BY items.codigo_leido, f.establecimiento
                HAVING COUNT(DISTINCT items.producto_maestro_id) > 1
                ORDER BY COUNT(DISTINCT items.producto_maestro_id) DESC
                LIMIT 100
            """)
        else:
            cursor.execute("""
                SELECT
                    items.codigo_leido as codigo_plu,
                    f.establecimiento,
                    GROUP_CONCAT(DISTINCT items.producto_maestro_id) as producto_ids,
                    COUNT(DISTINCT items.producto_maestro_id) as total_productos
                FROM items_factura items
                INNER JOIN facturas f ON f.id = items.factura_id
                WHERE items.codigo_leido IS NOT NULL
                  AND items.codigo_leido != ''
                  AND LENGTH(items.codigo_leido) BETWEEN 3 AND 7
                  AND f.establecimiento IS NOT NULL
                  AND items.producto_maestro_id IS NOT NULL
                GROUP BY items.codigo_leido, f.establecimiento
                HAVING COUNT(DISTINCT items.producto_maestro_id) > 1
                ORDER BY COUNT(DISTINCT items.producto_maestro_id) DESC
                LIMIT 100
            """)

        duplicados_plu = cursor.fetchall()

        resultados = []
        for dup in duplicados_plu:
            codigo_plu = dup[0]
            establecimiento = dup[1]
            producto_ids = dup[2]
            total_productos = dup[3]

            # Convertir IDs
            if isinstance(producto_ids, str):
                ids_list = [int(x) for x in producto_ids.split(',')]
            else:
                ids_list = producto_ids

            # Obtener detalles
            productos_info = []
            for prod_id in ids_list:
                if database_type == "postgresql":
                    cursor.execute("""
                        SELECT
                            id, nombre_normalizado, nombre_comercial, marca,
                            codigo_ean
                        FROM productos_maestros
                        WHERE id = %s
                    """, (prod_id,))
                else:
                    cursor.execute("""
                        SELECT
                            id, nombre_normalizado, nombre_comercial, marca,
                            codigo_ean
                        FROM productos_maestros
                        WHERE id = ?
                    """, (prod_id,))

                p = cursor.fetchone()
                if not p:
                    continue

                # Contar compras en ese establecimiento
                if database_type == "postgresql":
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM items_factura i
                        INNER JOIN facturas f ON f.id = i.factura_id
                        WHERE i.producto_maestro_id = %s
                          AND f.establecimiento = %s
                    """, (prod_id, establecimiento))
                else:
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM items_factura i
                        INNER JOIN facturas f ON f.id = i.factura_id
                        WHERE i.producto_maestro_id = ?
                          AND f.establecimiento = ?
                    """, (prod_id, establecimiento))

                total_compras = cursor.fetchone()[0]

                productos_info.append({
                    'id': p[0],
                    'nombre_normalizado': p[1],
                    'nombre_comercial': p[2],
                    'marca': p[3],
                    'codigo_ean': p[4],
                    'codigo_plu': codigo_plu,
                    'total_compras_establecimiento': total_compras
                })

            resultados.append({
                'tipo': 'plu_establecimiento_duplicado',
                'codigo_plu': codigo_plu,
                'establecimiento': establecimiento,
                'total_productos': total_productos,
                'productos': productos_info,
                'razon': f'PLU {codigo_plu} tiene {total_productos} productos diferentes en {establecimiento}',
                'severidad': 'alta'
            })

        cursor.close()
        conn.close()

        return {
            'total': len(resultados),
            'duplicados': resultados
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/duplicados/nombres-similares")
async def detectar_nombres_similares(
    umbral_similitud: float = 0.85,
    limite: int = 200
):
    """
    Detecta productos con nombres muy similares
    ⚠️ SEVERIDAD MEDIA - Posibles duplicados

    Ejemplo: "LECHE ENTERA 1L" vs "Leche Entera 1L"
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Obtener productos más reportados
        if database_type == "postgresql":
            cursor.execute("""
                SELECT id, nombre_normalizado, nombre_comercial, marca,
                       codigo_ean, total_reportes
                FROM productos_maestros
                WHERE nombre_normalizado IS NOT NULL
                ORDER BY total_reportes DESC
                LIMIT %s
            """, (limite,))
        else:
            cursor.execute("""
                SELECT id, nombre_normalizado, nombre_comercial, marca,
                       codigo_ean, total_reportes
                FROM productos_maestros
                WHERE nombre_normalizado IS NOT NULL
                ORDER BY total_reportes DESC
                LIMIT ?
            """, (limite,))

        productos = cursor.fetchall()

        duplicados_encontrados = []
        productos_procesados = set()

        for i, p1 in enumerate(productos):
            if p1[0] in productos_procesados:
                continue

            grupo_similares = [p1]

            for p2 in productos[i+1:]:
                if p2[0] in productos_procesados:
                    continue

                # Calcular similitud
                nombre1 = p1[1] or p1[2] or ''
                nombre2 = p2[1] or p2[2] or ''

                if not nombre1 or not nombre2:
                    continue

                similitud = similitud_nombres(nombre1, nombre2)

                if similitud >= umbral_similitud:
                    grupo_similares.append(p2)
                    productos_procesados.add(p2[0])

            # Si encontramos un grupo de similares
            if len(grupo_similares) > 1:
                productos_procesados.add(p1[0])

                productos_info = []
                for p in grupo_similares:
                    # Contar compras
                    if database_type == "postgresql":
                        cursor.execute("""
                            SELECT COUNT(*) FROM items_factura
                            WHERE producto_maestro_id = %s
                        """, (p[0],))
                    else:
                        cursor.execute("""
                            SELECT COUNT(*) FROM items_factura
                            WHERE producto_maestro_id = ?
                        """, (p[0],))

                    total_compras = cursor.fetchone()[0]

                    productos_info.append({
                        'id': p[0],
                        'nombre_normalizado': p[1],
                        'nombre_comercial': p[2],
                        'marca': p[3],
                        'codigo_ean': p[4],
                        'total_compras': total_compras,
                        'total_reportes': p[5]
                    })

                duplicados_encontrados.append({
                    'tipo': 'nombre_similar',
                    'total_productos': len(grupo_similares),
                    'productos': productos_info,
                    'razon': f'Nombres con similitud >= {int(umbral_similitud * 100)}%',
                    'severidad': 'media' if umbral_similitud >= 0.9 else 'baja'
                })

                # Limitar a 50 duplicados
                if len(duplicados_encontrados) >= 50:
                    break

            if len(duplicados_encontrados) >= 50:
                break

        cursor.close()
        conn.close()

        return {
            'total': len(duplicados_encontrados),
            'umbral_usado': umbral_similitud,
            'productos_analizados': len(productos),
            'duplicados': duplicados_encontrados
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/duplicados/resumen")
async def resumen_duplicados():
    """
    Dashboard con resumen de todos los tipos de duplicados
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Duplicados por EAN
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                ) subquery
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                )
            """)

        ean_duplicados = cursor.fetchone()[0]

        # Total de productos
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        total_productos = cursor.fetchone()[0]

        # Productos sin compras (huérfanos)
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros pm
                LEFT JOIN items_factura i ON i.producto_maestro_id = pm.id
                WHERE i.id IS NULL
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros pm
                LEFT JOIN items_factura i ON i.producto_maestro_id = pm.id
                WHERE i.id IS NULL
            """)

        productos_huerfanos = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            'total_productos': total_productos,
            'duplicados_ean': ean_duplicados,
            'productos_huerfanos': productos_huerfanos,
            'alerta': 'alta' if ean_duplicados > 0 else 'normal'
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINTS - FUSIÓN DE PRODUCTOS
# ============================================================================

@router.post("/fusionar")
async def fusionar_productos(request: FusionarRequest):
    """
    Fusiona varios productos duplicados en uno solo

    Proceso:
    1. Valida que todos los productos existan
    2. Determina qué datos mantener
    3. Actualiza items_factura
    4. Actualiza inventario_usuario (consolida cantidades)
    5. Elimina productos duplicados
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Validar producto principal
        if database_type == "postgresql":
            cursor.execute("""
                SELECT id, codigo_ean, marca, categoria,
                       subcategoria, presentacion, nombre_normalizado
                FROM productos_maestros WHERE id = %s
            """, (request.producto_principal_id,))
        else:
            cursor.execute("""
                SELECT id, codigo_ean, marca, categoria,
                       subcategoria, presentacion, nombre_normalizado
                FROM productos_maestros WHERE id = ?
            """, (request.producto_principal_id,))

        producto_principal = cursor.fetchone()

        if not producto_principal:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Producto principal no encontrado")

        # Obtener productos duplicados
        productos_duplicados = []
        for dup_id in request.productos_duplicados_ids:
            if database_type == "postgresql":
                cursor.execute("""
                    SELECT id, codigo_ean, marca, categoria,
                           subcategoria, presentacion, nombre_normalizado
                    FROM productos_maestros WHERE id = %s
                """, (dup_id,))
            else:
                cursor.execute("""
                    SELECT id, codigo_ean, marca, categoria,
                           subcategoria, presentacion, nombre_normalizado
                    FROM productos_maestros WHERE id = ?
                """, (dup_id,))

            dup = cursor.fetchone()
            if dup:
                productos_duplicados.append(dup)

        if len(productos_duplicados) != len(request.productos_duplicados_ids):
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Algunos productos duplicados no existen")

        # Determinar mejores datos si se solicita
        if request.mantener_datos_de == "mas_completo":
            todos_productos = [
                {
                    'id': producto_principal[0],
                    'codigo_ean': producto_principal[1],
                    'marca': producto_principal[2],
                    'categoria': producto_principal[3],
                    'subcategoria': producto_principal[4],
                    'presentacion': producto_principal[5]
                }
            ]

            for dup in productos_duplicados:
                todos_productos.append({
                    'id': dup[0],
                    'codigo_ean': dup[1],
                    'marca': dup[2],
                    'categoria': dup[3],
                    'subcategoria': dup[4],
                    'presentacion': dup[5]
                })

            mejor_producto = producto_mas_completo(todos_productos)

            # Actualizar producto principal con mejores datos
            if mejor_producto['id'] != producto_principal[0]:
                if database_type == "postgresql":
                    cursor.execute("""
                        UPDATE productos_maestros
                        SET codigo_ean = COALESCE(%s, codigo_ean),
                            marca = COALESCE(%s, marca),
                            categoria = COALESCE(%s, categoria),
                            subcategoria = COALESCE(%s, subcategoria),
                            presentacion = COALESCE(%s, presentacion)
                        WHERE id = %s
                    """, (
                        mejor_producto.get('codigo_ean'),
                        mejor_producto.get('marca'),
                        mejor_producto.get('categoria'),
                        mejor_producto.get('subcategoria'),
                        mejor_producto.get('presentacion'),
                        request.producto_principal_id
                    ))
                else:
                    cursor.execute("""
                        UPDATE productos_maestros
                        SET codigo_ean = COALESCE(?, codigo_ean),
                            marca = COALESCE(?, marca),
                            categoria = COALESCE(?, categoria),
                            subcategoria = COALESCE(?, subcategoria),
                            presentacion = COALESCE(?, presentacion)
                        WHERE id = ?
                    """, (
                        mejor_producto.get('codigo_ean'),
                        mejor_producto.get('marca'),
                        mejor_producto.get('categoria'),
                        mejor_producto.get('subcategoria'),
                        mejor_producto.get('presentacion'),
                        request.producto_principal_id
                    ))

        # REEMPLAZAR DESDE "# Actualizar items_factura" HASTA "return {"
# En productos_mejoras.py, función fusionar_productos()

        # ====================================================================
        # PASO 1: Actualizar referencias en items_factura
        # ====================================================================
        items_actualizados = 0
        for dup in productos_duplicados:
            dup_id = dup[0]

            if database_type == "postgresql":
                cursor.execute("""
                    UPDATE items_factura
                    SET producto_maestro_id = %s
                    WHERE producto_maestro_id = %s
                """, (request.producto_principal_id, dup_id))
            else:
                cursor.execute("""
                    UPDATE items_factura
                    SET producto_maestro_id = ?
                    WHERE producto_maestro_id = ?
                """, (request.producto_principal_id, dup_id))

            items_actualizados += cursor.rowcount

        # ====================================================================
        # PASO 2: Actualizar referencias en precios_productos
        # ====================================================================
        precios_actualizados = 0
        for dup in productos_duplicados:
            dup_id = dup[0]

            if database_type == "postgresql":
                cursor.execute("""
                    UPDATE precios_productos
                    SET producto_maestro_id = %s
                    WHERE producto_maestro_id = %s
                """, (request.producto_principal_id, dup_id))
            else:
                cursor.execute("""
                    UPDATE precios_productos
                    SET producto_maestro_id = ?
                    WHERE producto_maestro_id = ?
                """, (request.producto_principal_id, dup_id))

            precios_actualizados += cursor.rowcount

        # ====================================================================
        # PASO 3: Consolidar inventario_usuario
        # ====================================================================
        inventarios_actualizados = 0
        for dup in productos_duplicados:
            dup_id = dup[0]

            # Buscar inventarios del producto duplicado
            if database_type == "postgresql":
                cursor.execute("""
                    SELECT id, usuario_id, cantidad_actual,
                           cantidad_total_comprada
                    FROM inventario_usuario
                    WHERE producto_maestro_id = %s
                """, (dup_id,))
            else:
                cursor.execute("""
                    SELECT id, usuario_id, cantidad_actual,
                           cantidad_total_comprada
                    FROM inventario_usuario
                    WHERE producto_maestro_id = ?
                """, (dup_id,))

            inventarios_dup = cursor.fetchall()

            for inv_dup in inventarios_dup:
                inv_id = inv_dup[0]
                usuario_id = inv_dup[1]
                cantidad = inv_dup[2]
                cantidad_total = inv_dup[3]

                # Ver si ya existe inventario del producto principal
                if database_type == "postgresql":
                    cursor.execute("""
                        SELECT id, cantidad_actual, cantidad_total_comprada
                        FROM inventario_usuario
                        WHERE usuario_id = %s AND producto_maestro_id = %s
                    """, (usuario_id, request.producto_principal_id))
                else:
                    cursor.execute("""
                        SELECT id, cantidad_actual, cantidad_total_comprada
                        FROM inventario_usuario
                        WHERE usuario_id = ? AND producto_maestro_id = ?
                    """, (usuario_id, request.producto_principal_id))

                inv_principal = cursor.fetchone()

                if inv_principal:
                    # Consolidar cantidades
                    nueva_cantidad = float(inv_principal[1]) + float(cantidad)
                    nueva_cantidad_total = float(inv_principal[2]) + float(cantidad_total)

                    if database_type == "postgresql":
                        cursor.execute("""
                            UPDATE inventario_usuario
                            SET cantidad_actual = %s,
                                cantidad_total_comprada = %s
                            WHERE id = %s
                        """, (nueva_cantidad, nueva_cantidad_total, inv_principal[0]))

                        cursor.execute("""
                            DELETE FROM inventario_usuario WHERE id = %s
                        """, (inv_id,))
                    else:
                        cursor.execute("""
                            UPDATE inventario_usuario
                            SET cantidad_actual = ?,
                                cantidad_total_comprada = ?
                            WHERE id = ?
                        """, (nueva_cantidad, nueva_cantidad_total, inv_principal[0]))

                        cursor.execute("""
                            DELETE FROM inventario_usuario WHERE id = ?
                        """, (inv_id,))
                else:
                    # Cambiar producto_maestro_id
                    if database_type == "postgresql":
                        cursor.execute("""
                            UPDATE inventario_usuario
                            SET producto_maestro_id = %s
                            WHERE id = %s
                        """, (request.producto_principal_id, inv_id))
                    else:
                        cursor.execute("""
                            UPDATE inventario_usuario
                            SET producto_maestro_id = ?
                            WHERE id = ?
                        """, (request.producto_principal_id, inv_id))

                inventarios_actualizados += 1

        # ====================================================================
        # PASO 4: AHORA SÍ, eliminar productos duplicados
        # ====================================================================
        for dup in productos_duplicados:
            dup_id = dup[0]

            if database_type == "postgresql":
                cursor.execute("""
                    DELETE FROM productos_maestros WHERE id = %s
                """, (dup_id,))
            else:
                cursor.execute("""
                    DELETE FROM productos_maestros WHERE id = ?
                """, (dup_id,))

        # Commit
        conn.commit()
        cursor.close()
        conn.close()

        return {
            'success': True,
            'mensaje': 'Productos fusionados exitosamente',
            'producto_resultante_id': request.producto_principal_id,
            'items_factura_actualizados': items_actualizados,
            'precios_actualizados': precios_actualizados,
            'inventarios_consolidados': inventarios_actualizados,
            'productos_eliminados': len(productos_duplicados)
        }

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/{producto_id}/fusionar-con/{producto_duplicado_id}")
async def fusionar_dos_productos(producto_id: int, producto_duplicado_id: int):
    """
    Versión simplificada para fusionar solo 2 productos
    """
    request = FusionarRequest(
        producto_principal_id=producto_id,
        productos_duplicados_ids=[producto_duplicado_id],
        mantener_datos_de="mas_completo"
    )

    return await fusionar_productos(request)


# ============================================================================
# ENDPOINTS - ESTADÍSTICAS Y CALIDAD DE DATOS
# ============================================================================

@router.get("/estadisticas/calidad")
async def estadisticas_calidad():
    """
    Dashboard completo de calidad de datos
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Total productos
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        total = cursor.fetchone()[0]

        # Con EAN
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
            """)
        con_ean = cursor.fetchone()[0]

        # Con marca
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE marca IS NOT NULL AND marca != ''
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE marca IS NOT NULL AND marca != ''
            """)
        con_marca = cursor.fetchone()[0]

        # Con categoría
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE categoria IS NOT NULL AND categoria != ''
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros
                WHERE categoria IS NOT NULL AND categoria != ''
            """)
        con_categoria = cursor.fetchone()[0]

        # Productos huérfanos
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros pm
                LEFT JOIN items_factura i ON i.producto_maestro_id = pm.id
                WHERE i.id IS NULL
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM productos_maestros pm
                LEFT JOIN items_factura i ON i.producto_maestro_id = pm.id
                WHERE i.id IS NULL
            """)
        huerfanos = cursor.fetchone()[0]

        # Duplicados EAN
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                ) subquery
            """)
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM (
                    SELECT codigo_ean
                    FROM productos_maestros
                    WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
                    GROUP BY codigo_ean
                    HAVING COUNT(*) > 1
                )
            """)
        duplicados_ean = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        # Calcular porcentajes
        pct_ean = round((con_ean / total * 100) if total > 0 else 0, 1)
        pct_marca = round((con_marca / total * 100) if total > 0 else 0, 1)
        pct_categoria = round((con_categoria / total * 100) if total > 0 else 0, 1)

        # Calidad general
        calidad = 'buena' if pct_ean > 80 else 'regular' if pct_ean > 50 else 'baja'

        return {
            'total_productos': total,
            'con_ean': con_ean,
            'sin_ean': total - con_ean,
            'porcentaje_ean': pct_ean,
            'con_marca': con_marca,
            'sin_marca': total - con_marca,
            'porcentaje_marca': pct_marca,
            'con_categoria': con_categoria,
            'sin_categoria': total - con_categoria,
            'porcentaje_categoria': pct_categoria,
            'productos_huerfanos': huerfanos,
            'duplicados_potenciales': duplicados_ean,
            'calidad_general': calidad
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/{producto_id}/historial-compras")
async def historial_compras_producto(producto_id: int):
    """
    Ver todas las compras de un producto (útil antes de fusionar)
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    i.id, i.cantidad, i.precio_pagado,
                    f.establecimiento, f.fecha_factura, f.usuario_id
                FROM items_factura i
                INNER JOIN facturas f ON f.id = i.factura_id
                WHERE i.producto_maestro_id = %s
                ORDER BY f.fecha_factura DESC
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    i.id, i.cantidad, i.precio_pagado,
                    f.establecimiento, f.fecha_factura, f.usuario_id
                FROM items_factura i
                INNER JOIN facturas f ON f.id = i.factura_id
                WHERE i.producto_maestro_id = ?
                ORDER BY f.fecha_factura DESC
            """, (producto_id,))

        compras = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            'producto_id': producto_id,
            'total_compras': len(compras),
            'compras': [
                {
                    'id': c[0],
                    'cantidad': float(c[1]) if c[1] else 0,
                    'precio': int(c[2]) if c[2] else 0,
                    'establecimiento': c[3],
                    'fecha': str(c[4]) if c[4] else None,
                    'usuario_id': c[5]
                }
                for c in compras
            ]
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ============================================================================
# ENDPOINTS - LISTADO Y EDICIÓN
# ============================================================================

@router.get("")
async def listar_productos(
    pagina: int = 1,
    limite: int = 50,
    busqueda: Optional[str] = None,
    filtro: Optional[str] = "todos"
):
    """
    Lista productos con paginación, búsqueda y filtros
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        offset = (pagina - 1) * limite

        # Construir WHERE clause
        where_clauses = []
        params = []

        if busqueda:
            if database_type == "postgresql":
                where_clauses.append("""
                    (nombre_normalizado ILIKE %s
                     OR nombre_comercial ILIKE %s
                     OR codigo_ean ILIKE %s)
                """)
                search_param = f"%{busqueda}%"
                params.extend([search_param, search_param, search_param])
            else:
                where_clauses.append("""
                    (nombre_normalizado LIKE ?
                     OR nombre_comercial LIKE ?
                     OR codigo_ean LIKE ?)
                """)
                search_param = f"%{busqueda}%"
                params.extend([search_param, search_param, search_param])

        if filtro == "sin_ean":
            where_clauses.append("(codigo_ean IS NULL OR codigo_ean = '')")
        elif filtro == "sin_marca":
            where_clauses.append("(marca IS NULL OR marca = '')")
        elif filtro == "sin_categoria":
            where_clauses.append("(categoria IS NULL OR categoria = '')")

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Query principal
        if database_type == "postgresql":
            query = f"""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, total_reportes,
                    precio_promedio_global
                FROM productos_maestros
                WHERE {where_sql}
                ORDER BY total_reportes DESC
                LIMIT %s OFFSET %s
            """
            params.extend([limite, offset])
        else:
            query = f"""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, total_reportes,
                    precio_promedio_global
                FROM productos_maestros
                WHERE {where_sql}
                ORDER BY total_reportes DESC
                LIMIT ? OFFSET ?
            """
            params.extend([limite, offset])

        cursor.execute(query, params)
        productos = cursor.fetchall()

        # Contar total
        count_query = f"""
            SELECT COUNT(*) FROM productos_maestros
            WHERE {where_sql}
        """
        cursor.execute(count_query, params[:-2])  # Sin limit y offset
        total = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        # Formatear resultados
        productos_list = []
        for p in productos:
            # Detectar problemas
            problemas = []
            if not p[1]:  # sin EAN
                problemas.append('sin_ean')
            if not p[4]:  # sin marca
                problemas.append('sin_marca')
            if not p[5]:  # sin categoría
                problemas.append('sin_categoria')

            productos_list.append({
                'id': p[0],
                'codigo_ean': p[1],
                'nombre_normalizado': p[2],
                'nombre_comercial': p[3],
                'marca': p[4],
                'categoria': p[5],
                'subcategoria': p[6],
                'total_reportes': p[7],
                'precio_promedio': p[8],
                'problemas': problemas
            })

        paginas_totales = (total + limite - 1) // limite

        return {
            'productos': productos_list,
            'paginacion': {
                'pagina': pagina,
                'limite': limite,
                'total': total,
                'paginas': paginas_totales
            }
        }

    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/{producto_id}")
async def obtener_producto(producto_id: int):
    """
    Obtiene detalles completos de un producto
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, presentacion, total_reportes,
                    precio_promedio_global, precio_minimo_historico, precio_maximo_historico
                FROM productos_maestros
                WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT
                    id, codigo_ean, nombre_normalizado, nombre_comercial,
                    marca, categoria, subcategoria, presentacion, total_reportes,
                    precio_promedio_global, precio_minimo_historico, precio_maximo_historico
                FROM productos_maestros
                WHERE id = ?
            """, (producto_id,))

        producto = cursor.fetchone()

        cursor.close()
        conn.close()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        return {
            'id': producto[0],
            'codigo_ean': producto[1],
            'nombre_normalizado': producto[2],
            'nombre_comercial': producto[3],
            'marca': producto[4],
            'categoria': producto[5],
            'subcategoria': producto[6],
            'presentacion': producto[7],
            'total_reportes': producto[8],
            'precio_promedio': producto[9],
            'precio_minimo': producto[10],
            'precio_maximo': producto[11]
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.put("/{producto_id}")
async def actualizar_producto(producto_id: int, datos: Dict[str, Any]):
    """
    Actualiza los datos de un producto
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Construir SET clause dinámicamente
        campos_permitidos = [
            'codigo_ean', 'nombre_normalizado', 'nombre_comercial',
            'marca', 'categoria', 'subcategoria', 'presentacion'
        ]

        sets = []
        params = []

        for campo in campos_permitidos:
            if campo in datos:
                if database_type == "postgresql":
                    sets.append(f"{campo} = %s")
                else:
                    sets.append(f"{campo} = ?")
                params.append(datos[campo])

        if not sets:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(producto_id)

        if database_type == "postgresql":
            query = f"""
                UPDATE productos_maestros
                SET {', '.join(sets)}, ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """
        else:
            query = f"""
                UPDATE productos_maestros
                SET {', '.join(sets)}
                WHERE id = ?
            """

        cursor.execute(query, params)
        conn.commit()

        cursor.close()
        conn.close()

        return {'success': True, 'mensaje': 'Producto actualizado'}

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.delete("/{producto_id}")
async def eliminar_producto(producto_id: int):
    """
    Elimina un producto (solo si no tiene compras asociadas)
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error conectando a base de datos")

    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

        # Verificar que no tenga compras
        if database_type == "postgresql":
            cursor.execute("""
                SELECT COUNT(*) FROM items_factura
                WHERE producto_maestro_id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*) FROM items_factura
                WHERE producto_maestro_id = ?
            """, (producto_id,))

        total_compras = cursor.fetchone()[0]

        if total_compras > 0:
            cursor.close()
            conn.close()
            raise HTTPException(
                status_code=400,
                detail=f"No se puede eliminar: tiene {total_compras} compras asociadas"
            )

        # Eliminar
        if database_type == "postgresql":
            cursor.execute("""
                DELETE FROM productos_maestros WHERE id = %s
            """, (producto_id,))
        else:
            cursor.execute("""
                DELETE FROM productos_maestros WHERE id = ?
            """, (producto_id,))

        conn.commit()
        cursor.close()
        conn.close()

        return {'success': True, 'mensaje': 'Producto eliminado'}

    except HTTPException:
        if conn:
            conn.rollback()
            conn.close()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")
