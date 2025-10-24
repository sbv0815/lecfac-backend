"""
PRODUCT MATCHING - LECFAC
==========================
Sistema inteligente de clasificación y matching de productos para supermercados colombianos.

Maneja:
- Códigos EAN-13 (770XXXXXXXXXX)
- Códigos EAN-13 incompletos (10 dígitos)
- Códigos PLU (4-5 dígitos para frutas/verduras)
- Códigos internos de cadenas (1-7 dígitos)
- Productos sin código (matching por nombre)

Autor: LecFac Team
Versión: 1.0
"""

import unicodedata
import re
from typing import Dict, Optional, Tuple


# ==============================================================================
# CLASIFICACIÓN DE CÓDIGOS
# ==============================================================================


def clasificar_codigo(codigo: str, establecimiento: str = None) -> dict:
    """
    Clasifica un código según su tipo y establece estrategia de matching.

    Args:
        codigo: Código leído del producto
        establecimiento: Nombre del supermercado (opcional)

    Returns:
        dict con:
            - tipo: "EAN13" | "EAN13_INCOMPLETO" | "PLU" | "INTERNO" | "INVALIDO"
            - codigo_normalizado: Código limpio y normalizado
            - es_unico_global: True si es único mundialmente (EAN-13)
            - requiere_establecimiento: True si necesita contexto de cadena
            - confianza: "ALTA" | "MEDIA" | "BAJA"

    Examples:
        >>> clasificar_codigo("7702001058917")
        {"tipo": "EAN13", "confianza": "ALTA", "es_unico_global": True}

        >>> clasificar_codigo("1220")
        {"tipo": "PLU", "confianza": "MEDIA", "requiere_establecimiento": True}

        >>> clasificar_codigo("625", "JUMBO")
        {"tipo": "INTERNO", "requiere_establecimiento": True}
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

    # Caso 1: EAN-13 completo (código de barras estándar)
    if len(codigo) == 13 and codigo.isdigit():
        # Verificar si es código colombiano (770)
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
    # Algunos supermercados imprimen solo los últimos 10 dígitos
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
            "nota": "Verificar si el prefijo 770 es correcto",
        }

    # Caso 3: PLU estándar (4-5 dígitos)
    # Códigos para frutas, verduras y productos a granel
    if 4 <= len(codigo) <= 5 and codigo.isdigit():
        # PLU internacional estándar: empiezan con 3, 4 o 9
        # Ejemplo: 4011 (banano), 4590 (mango), 94011 (banano orgánico)
        if codigo[0] in ["3", "4", "9"]:
            return {
                "tipo": "PLU",
                "codigo_normalizado": codigo,
                "es_unico_global": False,  # PLU puede variar por cadena
                "requiere_establecimiento": True,
                "confianza": "MEDIA",
                "razon": "PLU de 4-5 dígitos (frutas/verduras)",
                "nota": "Puede ser estándar internacional o interno de cadena",
            }

        # PLU que no empieza con 3, 4 o 9 → probablemente código interno
        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": "PLU no estándar, probablemente código interno",
        }

    # Caso 4: Código interno corto (1-7 dígitos)
    # Cada cadena tiene su propio sistema
    if 1 <= len(codigo) <= 7 and codigo.isdigit():
        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True,
            "confianza": "BAJA",
            "razon": f"Código interno de {len(codigo)} dígitos",
            "nota": "CRÍTICO: Debe incluir establecimiento en búsqueda",
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
# FUNCIÓN PRINCIPAL DE MATCHING
# ==============================================================================


def buscar_o_crear_producto_inteligente(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor
) -> Optional[int]:
    """
    Busca o crea producto maestro usando clasificación inteligente de códigos.

    Esta es la función principal que se debe llamar desde main.py.

    Args:
        codigo: Código del producto (puede ser EAN, PLU, o interno)
        nombre: Nombre del producto leído de la factura
        precio: Precio en pesos colombianos (entero)
        establecimiento: Nombre del supermercado (ej: "JUMBO", "EXITO")
        cursor: Cursor de PostgreSQL

    Returns:
        ID del producto maestro (int) o None si falla

    Estrategia:
        - EAN-13: Búsqueda global, crear si no existe
        - PLU/INTERNO: Buscar por código + establecimiento + similitud de nombre
        - Sin código: Buscar solo por nombre similar en mismo establecimiento
    """

    if not nombre or not nombre.strip():
        print("   ⚠️ Producto sin nombre, saltando")
        return None

    if precio <= 0:
        print(f"   ⚠️ Precio inválido para '{nombre}': {precio}")
        return None

    # Clasificar código
    clasificacion = clasificar_codigo(codigo, establecimiento)

    print(
        f"   📋 '{nombre}' → Tipo: {clasificacion['tipo']} | Confianza: {clasificacion['confianza']}"
    )

    if clasificacion.get("nota"):
        print(f"      💡 {clasificacion['nota']}")

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
        print(f"   ❌ Error en matching: {e}")
        return None


# ==============================================================================
# ESTRATEGIAS DE BÚSQUEDA
# ==============================================================================


def buscar_o_crear_por_ean(codigo_ean: str, nombre: str, precio: int, cursor) -> int:
    """
    Buscar producto por EAN-13 (búsqueda global sin considerar establecimiento).

    Los códigos EAN son únicos mundialmente, por lo que no importa
    en qué supermercado se compró.
    """

    # Buscar existente
    cursor.execute(
        """
        SELECT id, nombre_comercial, total_reportes, precio_promedio_global
        FROM productos_maestros
        WHERE codigo_ean = %s
        LIMIT 1
        """,
        (codigo_ean,),
    )

    resultado = cursor.fetchone()

    if resultado:
        producto_id = resultado[0]
        print(
            f"   ✅ Producto existente (ID: {producto_id}) | Reportes: {resultado[2]}"
        )

        # Actualizar precio promedio
        actualizar_precio_producto(producto_id, precio, cursor)

        # Si el nombre comercial estaba vacío, actualizarlo
        if not resultado[1]:
            cursor.execute(
                "UPDATE productos_maestros SET nombre_comercial = %s WHERE id = %s",
                (nombre, producto_id),
            )

        return producto_id

    # No existe → crear nuevo
    print(f"   🆕 Creando producto con EAN: {codigo_ean}")

    nombre_norm = normalizar_nombre(nombre)

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
    print(f"   ✅ Producto creado (ID: {nuevo_id})")

    return nuevo_id


def buscar_o_crear_por_codigo_interno(
    codigo: str, nombre: str, precio: int, establecimiento: str, cursor
) -> int:
    """
    Buscar producto por código interno + establecimiento + similitud de nombre.

    CRÍTICO: Los códigos internos NO son únicos globalmente.
    Ejemplo: "625" en Jumbo puede ser pan, pero "625" en Éxito puede ser leche.

    Por eso buscamos: mismo código + mismo establecimiento + nombre similar.
    """

    nombre_norm = normalizar_nombre(nombre)

    # Buscar productos con el mismo código en el mismo establecimiento
    cursor.execute(
        """
        SELECT
            pm.id,
            pm.nombre_normalizado,
            pm.nombre_comercial,
            SIMILARITY(pm.nombre_normalizado, %s) as similitud,
            pm.total_reportes
        FROM productos_maestros pm
        WHERE pm.codigo_ean = %s
        AND pm.subcategoria = %s
        ORDER BY similitud DESC
        LIMIT 1
        """,
        (nombre_norm, codigo, establecimiento),
    )

    resultado = cursor.fetchone()

    # Umbral de similitud: 75% para códigos internos
    UMBRAL_SIMILITUD = 0.75

    if resultado and resultado[3] >= UMBRAL_SIMILITUD:
        producto_id = resultado[0]
        similitud_pct = resultado[3] * 100

        print(
            f"   ✅ Match por código interno (ID: {producto_id}) | Similitud: {similitud_pct:.0f}% | Reportes: {resultado[4]}"
        )

        actualizar_precio_producto(producto_id, precio, cursor)
        return producto_id

    # No existe → crear nuevo CON establecimiento en subcategoria
    print(f"   🆕 Creando producto con código interno: {codigo} ({establecimiento})")

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
        ) VALUES (%s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id
        """,
        (codigo, nombre_norm, nombre, precio, establecimiento),
    )

    nuevo_id = cursor.fetchone()[0]
    print(f"   ✅ Producto creado (ID: {nuevo_id})")

    return nuevo_id


def buscar_o_crear_por_nombre(
    nombre: str, precio: int, establecimiento: str, cursor
) -> int:
    """
    Buscar producto solo por similitud de nombre en el mismo establecimiento.

    Se usa cuando el producto no tiene código válido.
    Umbral de similitud más alto (85%) para evitar falsos positivos.
    """

    nombre_norm = normalizar_nombre(nombre)

    # Buscar por nombre similar en mismo establecimiento
    cursor.execute(
        """
        SELECT
            pm.id,
            pm.nombre_normalizado,
            SIMILARITY(pm.nombre_normalizado, %s) as similitud,
            pm.total_reportes
        FROM productos_maestros pm
        WHERE pm.subcategoria = %s
        AND pm.codigo_ean IS NULL
        ORDER BY similitud DESC
        LIMIT 1
        """,
        (nombre_norm, establecimiento),
    )

    resultado = cursor.fetchone()

    # Umbral más alto cuando no hay código: 85%
    UMBRAL_SIMILITUD = 0.85

    if resultado and resultado[2] >= UMBRAL_SIMILITUD:
        producto_id = resultado[0]
        similitud_pct = resultado[2] * 100

        print(
            f"   ✅ Match por nombre (ID: {producto_id}) | Similitud: {similitud_pct:.0f}% | Reportes: {resultado[3]}"
        )

        actualizar_precio_producto(producto_id, precio, cursor)
        return producto_id

    # No existe → crear nuevo sin código
    print(f"   🆕 Creando producto sin código: {nombre} ({establecimiento})")

    cursor.execute(
        """
        INSERT INTO productos_maestros (
            nombre_normalizado,
            nombre_comercial,
            precio_promedio_global,
            subcategoria,
            total_reportes,
            primera_vez_reportado,
            ultima_actualizacion
        ) VALUES (%s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        RETURNING id
        """,
        (nombre_norm, nombre, precio, establecimiento),
    )

    nuevo_id = cursor.fetchone()[0]
    print(f"   ✅ Producto creado (ID: {nuevo_id})")

    return nuevo_id


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================


def normalizar_nombre(nombre: str) -> str:
    """
    Normaliza nombre de producto para comparaciones y búsquedas.

    - Quita acentos y diacríticos
    - Convierte a mayúsculas
    - Elimina espacios extra
    - Elimina caracteres especiales comunes

    Examples:
        >>> normalizar_nombre("Leche Entera Colanta 1L")
        "LECHE ENTERA COLANTA 1L"

        >>> normalizar_nombre("  Café  con  Azúcar  ")
        "CAFE CON AZUCAR"
    """

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

    Calcula nuevo promedio ponderado:
        nuevo_promedio = (promedio_anterior * reportes + nuevo_precio) / (reportes + 1)

    También incrementa el contador de reportes.
    """

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


# ==============================================================================
# FUNCIONES DE DIAGNÓSTICO
# ==============================================================================


def obtener_estadisticas_matching(cursor) -> dict:
    """
    Obtiene estadísticas sobre la calidad del matching en la base de datos.

    Returns:
        dict con métricas de calidad
    """

    cursor.execute(
        """
        SELECT
            COUNT(*) as total_items,
            COUNT(producto_maestro_id) as items_vinculados,
            COUNT(*) - COUNT(producto_maestro_id) as items_sin_vincular,
            ROUND(COUNT(producto_maestro_id)::numeric / COUNT(*)::numeric * 100, 2) as porcentaje_vinculado
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


print("✅ Módulo product_matching cargado correctamente")
print("📦 Funciones disponibles:")
print("   - clasificar_codigo()")
print("   - buscar_o_crear_producto_inteligente()")
print("   - obtener_estadisticas_matching()")
