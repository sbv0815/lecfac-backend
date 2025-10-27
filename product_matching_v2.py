"""
PRODUCT MATCHING - LECFAC
==========================
VERSIÓN: 2025-10-27-19:15 - CON LOGGING EXTENSIVO
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
            "tipo": "ALFANUMERICO",
            "codigo_normalizado": codigo_limpio,
            "codigo_original": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": "Código alfanumérico, probablemente interno",
        }

    # Caso 6: No se pudo clasificar
    return {
        "tipo": "DESCONOCIDO",
        "codigo_normalizado": codigo,
        "es_unico_global": False,
        "requiere_establecimiento": True,
        "confianza": "MUY_BAJA",
        "razon": f"No se pudo clasificar código de longitud {len(codigo)}",
    }


# ==============================================================================
# FUNCIONES DE BÚSQUEDA Y CREACIÓN
# ==============================================================================


def buscar_o_crear_producto_inteligente(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor, conn
) -> Optional[int]:
    """
    Busca o crea producto maestro usando clasificación inteligente de códigos.
    VERSIÓN CON LOGGING EXTENSIVO PARA DEBUGGING.
    """

    print(f"\n🔍 buscar_o_crear_producto_inteligente() llamada:")
    print(f"   - codigo: {codigo}")
    print(f"   - nombre: {nombre}")
    print(f"   - precio: {precio}")
    print(f"   - establecimiento: {establecimiento}")
    print(f"   - cursor: {cursor}")
    print(f"   - conn: {conn}")

    if not nombre or not nombre.strip():
        print(f"   ❌ Nombre vacío, retornando None")
        return None

    if precio <= 0:
        print(f"   ❌ Precio inválido ({precio}), retornando None")
        return None

    # Clasificar código
    clasificacion = clasificar_codigo(codigo, establecimiento)
    print(f"   📊 Clasificación: {clasificacion['tipo']}")

    # Estrategia según tipo de código
    try:
        if clasificacion["tipo"] in ["EAN13", "EAN13_INCOMPLETO"]:
            print(f"   ➡️ Usando estrategia EAN")
            resultado = buscar_o_crear_por_ean(
                codigo_ean=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                cursor=cursor,
                conn=conn,
            )
            print(f"   ✅ Resultado EAN: {resultado}")
            return resultado

        elif clasificacion["requiere_establecimiento"]:
            print(f"   ➡️ Usando estrategia código interno")
            resultado = buscar_o_crear_por_codigo_interno(
                codigo=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
                conn=conn,
            )
            print(f"   ✅ Resultado interno: {resultado}")
            return resultado

        else:
            print(f"   ⚠️ Tipo de código no manejado: {clasificacion['tipo']}")
            return None

    except Exception as e:
        print(f"   ❌ EXCEPCIÓN en buscar_o_crear_producto_inteligente: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return None


def normalizar_nombre(nombre: str) -> str:
    """Normaliza nombre de producto para comparación"""
    if not nombre:
        return ""

    # Convertir a mayúsculas
    texto = nombre.upper()

    # Remover acentos
    texto = ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )

    # Remover caracteres especiales
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)

    # Normalizar espacios
    texto = ' '.join(texto.split())

    return texto


def buscar_o_crear_por_ean(
    codigo_ean: str, nombre: str, precio: int, cursor, conn
) -> int:
    """
    Buscar o crear producto por código EAN (único globalmente).
    """
    nombre_norm = normalizar_nombre(nombre)

    try:
        print(f"      🔎 Buscando EAN: {codigo_ean}")

        # Buscar por EAN
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
            print(f"      ✅ Producto encontrado por EAN: ID={producto_id}")
            actualizar_precio_producto(producto_id, precio, cursor, conn)
            return producto_id

        # No existe → crear nuevo producto con EAN
        print(f"      ➕ Creando nuevo producto con EAN")
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
        conn.commit()
        print(f"      ✅ Producto creado con EAN: ID={nuevo_id}")
        return nuevo_id

    except Exception as e:
        print(f"      ❌ Error en buscar_o_crear_por_ean: {e}")
        conn.rollback()
        raise


def buscar_o_crear_por_codigo_interno(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor, conn
) -> int:
    """
    Buscar o crear producto por código interno de cadena.
    """
    nombre_norm = normalizar_nombre(nombre)
    codigo_interno_compuesto = f"{codigo}|{establecimiento}"

    try:
        print(f"      🔎 Buscando código interno: {codigo_interno_compuesto}")

        # Buscar por nombre exacto en el mismo establecimiento
        cursor.execute(
            """
            SELECT id, nombre_normalizado, nombre_comercial, total_reportes
            FROM productos_maestros
            WHERE subcategoria = %s
            AND nombre_normalizado = %s
            AND codigo_ean IS NULL
            LIMIT 1
            """,
            (codigo_interno_compuesto, nombre_norm),
        )
        resultado = cursor.fetchone()

        if resultado:
            producto_id = resultado[0]
            print(f"      ✅ Producto encontrado por código interno: ID={producto_id}")
            actualizar_precio_producto(producto_id, precio, cursor, conn)
            return producto_id

        # No existe → crear nuevo CON código interno en subcategoria
        print(f"      ➕ Creando nuevo producto con código interno")
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
        conn.commit()
        print(f"      ✅ Producto creado con código interno: ID={nuevo_id}")
        return nuevo_id

    except Exception as e:
        print(f"      ❌ Error en buscar_o_crear_por_codigo_interno: {e}")
        conn.rollback()
        raise


def actualizar_precio_producto(producto_id: int, precio_nuevo: int, cursor, conn):
    """
    Actualiza el precio promedio de un producto existente.
    """
    try:
        # Obtener datos actuales
        cursor.execute(
            """
            SELECT precio_promedio_global, total_reportes
            FROM productos_maestros
            WHERE id = %s
            """,
            (producto_id,),
        )
        resultado = cursor.fetchone()

        if not resultado:
            return

        precio_actual, reportes_actuales = resultado

        # Calcular nuevo promedio
        nuevo_total_reportes = reportes_actuales + 1
        nuevo_precio_promedio = (
            (precio_actual * reportes_actuales + precio_nuevo) / nuevo_total_reportes
        )

        # Actualizar
        cursor.execute(
            """
            UPDATE productos_maestros
            SET precio_promedio_global = %s,
                total_reportes = %s,
                ultima_actualizacion = CURRENT_TIMESTAMP
            WHERE id = %s
            """,
            (nuevo_precio_promedio, nuevo_total_reportes, producto_id),
        )
        conn.commit()

        print(f"      📊 Precio actualizado: {precio_actual} → {nuevo_precio_promedio:.0f}")

    except Exception as e:
        print(f"      ⚠️ Error actualizando precio: {e}")
        conn.rollback()


print("✅ product_matching_v2 cargado (versión 2025-10-27-19:15 con logging)")
