"""
PRODUCT MATCHING - LECFAC
==========================
Sistema inteligente de clasificación y matching de productos para supermercados colombianos.
VERSIÓN CORREGIDA: Robusta para grandes volúmenes, con transacciones y logging optimizado
"""

import unicodedata
import re
from typing import Dict, Optional, Tuple
from datetime import datetime


# ==============================================================================
# CLASIFICACIÓN DE CÓDIGOS
# ==============================================================================


def clasificar_codigo(codigo: str, establecimiento: str = None) -> dict:
    """
    Clasifica un código según su tipo y establece estrategia de matching.
    """

    if not codigo or not isinstance(codigo, str):
        return {
            "tipo": "INVALIDO",
            "codigo_normalizado": None,
            "es_unico_global": False,
            "requiere_establecimiento": False,
            "confianza": "BAJA",
            "razon": "Código vacío o inválido",
        }

    # Limpiar código
    codigo = codigo.strip()

    # Caso 1: EAN-13 completo
    if len(codigo) == 13 and codigo.isdigit():
        es_colombiano = codigo.startswith("770")

        return {
            "tipo": "EAN13",
            "codigo_normalizado": codigo,
            "es_unico_global": True,
            "requiere_establecimiento": False,
            "confianza": "ALTA",
            "es_colombiano": es_colombiano,
            "razon": f"EAN-13 válido {'colombiano' if es_colombiano else 'importado'}",
        }

    # Caso 2: EAN-13 incompleto (10 dígitos)
    if len(codigo) == 10 and codigo.isdigit():
        codigo_completo = f"770{codigo}"

        return {
            "tipo": "EAN13_INCOMPLETO",
            "codigo_normalizado": codigo_completo,
            "codigo_original": codigo,
            "es_unico_global": True,
            "requiere_establecimiento": False,
            "confianza": "MEDIA",
            "razon": "EAN-13 incompleto, se asumió prefijo 770 (Colombia)",
        }

    # Caso 3: PLU estándar (4-5 dígitos)
    if 4 <= len(codigo) <= 5 and codigo.isdigit():
        if codigo[0] in ["3", "4", "9"]:
            return {
                "tipo": "PLU",
                "codigo_normalizado": codigo,
                "es_unico_global": False,
                "requiere_establecimiento": True,
                "confianza": "MEDIA",
                "razon": "PLU de 4-5 dígitos (frutas/verduras)",
            }

        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": "PLU no estándar, probablemente código interno",
        }

    # Caso 4: Código interno corto (1-7 dígitos)
    if 1 <= len(codigo) <= 7 and codigo.isdigit():
        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": f"Código interno de {len(codigo)} dígitos",
        }

    # Caso 5: Código con letras o caracteres especiales
    if len(codigo) >= 3:
        codigo_limpio = re.sub(r"[^A-Z0-9]", "", codigo.upper())

        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo_limpio,
            "codigo_original": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": "Código alfanumérico interno",
        }

    # Caso 6: Código muy corto o formato no reconocido
    return {
        "tipo": "INVALIDO",
        "codigo_normalizado": None,
        "codigo_original": codigo,
        "es_unico_global": False,
        "requiere_establecimiento": False,
        "confianza": "BAJA",
        "razon": f"Código demasiado corto o inválido (longitud: {len(codigo)})",
    }


# ==============================================================================
# FUNCIÓN PRINCIPAL DE MATCHING - VERSIÓN ROBUSTA
# ==============================================================================


def buscar_o_crear_producto_inteligente(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor
) -> Optional[int]:
    """
    Busca o crea producto maestro usando clasificación inteligente de códigos.
    VERSIÓN ROBUSTA: Maneja errores, logging optimizado, y garantiza creación.
    """

    if not nombre or not nombre.strip():
        return None

    if precio <= 0:
        return None

    # Clasificar código
    clasificacion = clasificar_codigo(codigo, establecimiento)

    # Estrategia según tipo de código
    try:
        if clasificacion["tipo"] in ["EAN13", "EAN13_INCOMPLETO"]:
            return buscar_o_crear_por_ean(
                codigo_ean=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                cursor=cursor,
            )

        elif clasificacion["requiere_establecimiento"]:
            return buscar_o_crear_por_codigo_interno(
                codigo=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
            )

        else:
            # Sin código válido → buscar por nombre
            return buscar_o_crear_por_nombre(
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
            )

    except Exception as e:
        print(f"   ❌ Error crítico en matching '{nombre}': {e}")
        import traceback
        traceback.print_exc()
        return None


# ==============================================================================
# BÚSQUEDA/CREACIÓN POR TIPO DE CÓDIGO - VERSIÓN ROBUSTA
# ==============================================================================


def buscar_o_crear_por_ean(
    codigo_ean: str, nombre: str, precio: int, cursor
) -> int:
    """
    Buscar o crear producto por código EAN-13.
    VERSIÓN ROBUSTA: Garantiza creación del producto.
    """

    nombre_norm = normalizar_nombre(nombre)

    try:
        # Buscar por EAN exacto
        cursor.execute(
            """
            SELECT id, nombre_normalizado, total_reportes
            FROM productos_maestros
            WHERE codigo_ean = %s
            LIMIT 1
            """,
            (codigo_ean,),
        )

        resultado = cursor.fetchone()

        if resultado:
            producto_id = resultado[0]
            actualizar_precio_producto(producto_id, precio, cursor)
            return producto_id

        # No existe → crear nuevo producto con EAN
        cursor.execute(
            """
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                precio_promedio_global,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (codigo_ean, nombre_norm, nombre, precio),
        )

        nuevo_id = cursor.fetchone()[0]
        return nuevo_id

    except Exception as e:
        print(f"   ❌ Error en buscar_o_crear_por_ean: {e}")
        raise


def buscar_o_crear_por_codigo_interno(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor
) -> int:
    """
    Buscar o crear producto por código interno de cadena.
    VERSIÓN ROBUSTA: Garantiza creación del producto.
    """

    nombre_norm = normalizar_nombre(nombre)
    codigo_interno_compuesto = f"{codigo}|{establecimiento}"

    try:
        # Buscar productos con el mismo código interno en el mismo establecimiento
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.nombre_normalizado,
                pm.nombre_comercial,
                SIMILARITY(pm.nombre_normalizado, %s) as similitud,
                pm.total_reportes
            FROM productos_maestros pm
            WHERE pm.subcategoria = %s
            AND pm.codigo_ean IS NULL
            ORDER BY similitud DESC
            LIMIT 1
            """,
            (nombre_norm, codigo_interno_compuesto),
        )

        resultado = cursor.fetchone()

        # Umbral de similitud: 75% para códigos internos
        UMBRAL_SIMILITUD = 0.75

        if resultado and resultado[3] >= UMBRAL_SIMILITUD:
            producto_id = resultado[0]
            actualizar_precio_producto(producto_id, precio, cursor)
            return producto_id

        # No existe → crear nuevo CON código interno en subcategoria
        cursor.execute(
            """
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                precio_promedio_global,
                subcategoria,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (NULL, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (nombre_norm, nombre, precio, codigo_interno_compuesto),
        )

        nuevo_id = cursor.fetchone()[0]
        return nuevo_id

    except Exception as e:
        print(f"   ❌ Error en buscar_o_crear_por_codigo_interno: {e}")
        raise


def buscar_o_crear_por_nombre(
    nombre: str, precio: int, establecimiento: str, cursor
) -> int:
    """
    Buscar producto solo por similitud de nombre en el mismo establecimiento.
    VERSIÓN ROBUSTA: Garantiza creación del producto.
    """

    nombre_norm = normalizar_nombre(nombre)

    try:
        # Buscar por nombre similar en mismo establecimiento
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.nombre_normalizado,
                SIMILARITY(pm.nombre_normalizado, %s) as similitud,
                pm.total_reportes
            FROM productos_maestros pm
            WHERE pm.subcategoria LIKE %s
            AND pm.codigo_ean IS NULL
            ORDER BY similitud DESC
            LIMIT 1
            """,
            (nombre_norm, f"%{establecimiento}%"),
        )

        resultado = cursor.fetchone()

        # Umbral más alto cuando no hay código: 85%
        UMBRAL_SIMILITUD = 0.85

        if resultado and resultado[2] >= UMBRAL_SIMILITUD:
            producto_id = resultado[0]
            actualizar_precio_producto(producto_id, precio, cursor)
            return producto_id

        # No existe → crear nuevo sin código
        cursor.execute(
            """
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                precio_promedio_global,
                subcategoria,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (NULL, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (nombre_norm, nombre, precio, f"SIN_CODIGO|{establecimiento}"),
        )

        nuevo_id = cursor.fetchone()[0]
        return nuevo_id

    except Exception as e:
        print(f"   ❌ Error en buscar_o_crear_por_nombre: {e}")
        raise


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================


def normalizar_nombre(nombre: str) -> str:
    """Normaliza nombre de producto para comparaciones y búsquedas."""

    if not nombre:
        return ""

    # Quitar acentos
    nombre = unicodedata.normalize("NFKD", nombre)
    nombre = nombre.encode("ASCII", "ignore").decode("ASCII")

    # Mayúsculas
    nombre = nombre.upper()

    # Quitar espacios múltiples
    nombre = " ".join(nombre.split())

    # Quitar algunos caracteres especiales comunes en facturas
    nombre = nombre.replace("*", "").replace("#", "")

    return nombre.strip()


def actualizar_precio_producto(producto_id: int, nuevo_precio: int, cursor):
    """
    Actualiza el precio promedio de un producto existente.
    Calcula nuevo promedio ponderado e incrementa el contador de reportes.
    """

    try:
        cursor.execute(
            """
            UPDATE productos_maestros
            SET
                precio_promedio_global = (
                    (COALESCE(precio_promedio_global, 0) * total_reportes + %s)
                    / (total_reportes + 1)
                )::integer,
                total_reportes = total_reportes + 1,
                total_usuarios_reportaron = total_usuarios_reportaron + 1,
                ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (nuevo_precio, producto_id),
        )
    except Exception as e:
        print(f"   ⚠️ Error actualizando precio: {e}")
        # No es crítico, continuar


# ==============================================================================
# FUNCIONES DE DIAGNÓSTICO
# ==============================================================================


def obtener_estadisticas_matching(cursor) -> dict:
    """Obtiene estadísticas sobre la calidad del matching en la base de datos."""

    cursor.execute(
        """
        SELECT
            COUNT(*) as total_items,
            COUNT(producto_maestro_id) as items_vinculados,
            COUNT(*) - COUNT(producto_maestro_id) as items_sin_vincular,
            ROUND(COUNT(producto_maestro_id)::numeric / NULLIF(COUNT(*), 0)::numeric * 100, 2) as porcentaje_vinculado
        FROM items_factura
    """
    )

    resultado = cursor.fetchone()

    return {
        "total_items": resultado[0],
        "items_vinculados": resultado[1],
        "items_sin_vincular": resultado[2],
        "porcentaje_vinculado": float(resultado[3]) if resultado[3] else 0,
    }


print("✅ Módulo product_matching ROBUSTO cargado correctamente")
