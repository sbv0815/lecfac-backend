"""
establishments.py
Módulo para gestión y validación de establecimientos comerciales
"""

import logging
from typing import List, Dict, Tuple

logger = logging.getLogger(__name__)


# ========================================
# CONFIGURACIÓN DE ESTABLECIMIENTOS
# ========================================

ESTABLECIMIENTOS_CONOCIDOS = {
    # Cadenas principales
    "OLIMPICA": "OLÍMPICA",
    "ÉXITO": "ÉXITO",
    "EXITO": "ÉXITO",
    "CARREFOUR": "CARREFOUR",
    "JUMBO": "JUMBO",
    "MAKRO": "MAKRO",
    "METRO": "METRO",
    "ALKOSTO": "ALKOSTO",
    "PRICESMART": "PRICEMART",
    "PRICE SMART": "PRICEMART",
    # Hard Discount
    "D1": "D1",
    "ARA": "ARA",
    # Supermercados regionales
    "SUPERINTER": "SUPERINTER",
    "SURTIMAX": "SURTIMAX",
    "COLSUBSIDIO": "COLSUBSIDIO",
    "CAFAM": "CAFAM",
    "COORATIENDAS": "COORATIENDAS",
    "MERCACENTRO": "MERCACENTRO",
    "SUPERMERCADO CENTRAL": "SUPERMERCADO CENTRAL",
    # Farmacias
    "FARMATODO": "FARMATODO",
    "CRUZ VERDE": "CRUZ VERDE",
    "CAFAM DROGUERIA": "CAFAM DROGUERÍA",
    "DROGAS LA REBAJA": "DROGAS LA REBAJA",
    "LOCATEL": "LOCATEL",
    # Tiendas especializadas
    "FALABELLA": "FALABELLA",
    "HOMECENTER": "HOMECENTER",
    "SODIMAC": "SODIMAC",
    "DECATHLON": "DECATHLON",
}

CADENAS_COMERCIALES = {
    # Grupo Éxito
    "ÉXITO": "Grupo Éxito",
    "EXITO": "Grupo Éxito",
    "CARREFOUR": "Grupo Éxito",
    "ARA": "Grupo Éxito",
    "SURTIMAX": "Grupo Éxito",
    # Olímpica
    "OLÍMPICA": "Olímpica",
    "OLIMPICA": "Olímpica",
    # Cencosud
    "JUMBO": "Cencosud",
    "METRO": "Cencosud",
    # Alkosto
    "ALKOSTO": "Alkosto",
    # PriceSmart
    "PRICEMART": "PriceSmart",
    "PRICE SMART": "PriceSmart",
    # Koba (D1)
    "D1": "Koba Colombia",
    # Makro
    "MAKRO": "Makro",
    # Farmacias
    "FARMATODO": "Farmatodo",
    "CRUZ VERDE": "Cruz Verde",
    "DROGAS LA REBAJA": "Drogas La Rebaja",
    "LOCATEL": "Locatel",
    # Tiendas especializadas
    "FALABELLA": "Falabella",
    "HOMECENTER": "Sodimac",
    "SODIMAC": "Sodimac",
    "DECATHLON": "Decathlon",
}


# ========================================
# FUNCIONES PRINCIPALES
# ========================================


def normalizar_establecimiento(nombre: str) -> str:
    """
    Normaliza el nombre del establecimiento detectado

    Args:
        nombre: Nombre detectado por OCR

    Returns:
        Nombre normalizado del establecimiento

    Examples:
        >>> normalizar_establecimiento("olimpica")
        'OLÍMPICA'
        >>> normalizar_establecimiento("exito")
        'ÉXITO'
    """
    if not nombre or not isinstance(nombre, str):
        return "DESCONOCIDO"

    # Normalizar a mayúsculas y limpiar
    nombre = nombre.upper().strip()

    # Remover caracteres especiales comunes
    nombre = nombre.replace(".", "").replace(",", "").replace("-", " ")

    # Remover espacios múltiples
    import re

    nombre = re.sub(r"\s+", " ", nombre)

    # Buscar coincidencia exacta
    if nombre in ESTABLECIMIENTOS_CONOCIDOS:
        return ESTABLECIMIENTOS_CONOCIDOS[nombre]

    # Buscar coincidencia parcial (contiene)
    for key, value in ESTABLECIMIENTOS_CONOCIDOS.items():
        if key in nombre or nombre in key:
            return value

    # Buscar similitud con fuzzy matching
    from difflib import SequenceMatcher

    mejor_match = None
    mejor_similitud = 0.0

    for key, value in ESTABLECIMIENTOS_CONOCIDOS.items():
        similitud = SequenceMatcher(None, nombre, key).ratio()
        if similitud > mejor_similitud and similitud >= 0.7:  # 70% de similitud mínima
            mejor_similitud = similitud
            mejor_match = value

    if mejor_match:
        logger.info(
            f"📍 Match por similitud: '{nombre}' → '{mejor_match}' ({mejor_similitud:.0%})"
        )
        return mejor_match

    # Si no se encuentra, devolver el nombre normalizado
    logger.warning(f"⚠️ Establecimiento desconocido: '{nombre}'")
    return nombre


def detectar_establecimiento_desde_productos(productos: List[Dict]) -> str:
    """
    Intenta detectar el establecimiento a partir de pistas en los productos

    Args:
        productos: Lista de productos detectados

    Returns:
        Nombre del establecimiento o "DESCONOCIDO"
    """
    if not productos:
        return "DESCONOCIDO"

    # Buscar pistas en nombres de productos
    for prod in productos:
        nombre = str(prod.get("nombre", "")).upper()

        # Algunas cadenas tienen productos con su marca
        if "OLIMPICA" in nombre:
            return "OLÍMPICA"
        elif "EXITO" in nombre or "ÉXITO" in nombre:
            return "ÉXITO"
        elif "CARREFOUR" in nombre:
            return "CARREFOUR"
        elif "JUMBO" in nombre:
            return "JUMBO"

    return "DESCONOCIDO"


def obtener_cadena(establecimiento: str) -> str:
    """
    Determina la cadena comercial a la que pertenece un establecimiento

    Args:
        establecimiento: Nombre del establecimiento

    Returns:
        Nombre de la cadena comercial

    Examples:
        >>> obtener_cadena("OLÍMPICA")
        'Olímpica'
        >>> obtener_cadena("CARREFOUR")
        'Grupo Éxito'
    """
    if not establecimiento:
        return "Independiente"

    establecimiento = establecimiento.upper()

    # Buscar en el diccionario de cadenas
    for key, value in CADENAS_COMERCIALES.items():
        if key in establecimiento:
            return value

    return "Independiente"


def validar_y_corregir_establecimiento(
    establecimiento_detectado: str, productos: List[Dict] = None, total: float = 0
) -> str:
    """
    Valida y corrige el establecimiento detectado usando múltiples señales

    Args:
        establecimiento_detectado: Nombre detectado por OCR
        productos: Lista de productos (opcional, para buscar pistas)
        total: Total de la factura (opcional, para validaciones futuras)

    Returns:
        Nombre corregido y normalizado del establecimiento
    """

    # Paso 1: Normalizar el nombre detectado
    establecimiento = normalizar_establecimiento(establecimiento_detectado)

    # Paso 2: Si es desconocido, intentar detectar por productos
    if establecimiento == "DESCONOCIDO" and productos:
        establecimiento_desde_productos = detectar_establecimiento_desde_productos(
            productos
        )
        if establecimiento_desde_productos != "DESCONOCIDO":
            logger.info(
                f"📍 Establecimiento detectado desde productos: '{establecimiento_desde_productos}'"
            )
            establecimiento = establecimiento_desde_productos

    return establecimiento


def procesar_establecimiento(
    establecimiento_raw: str, productos: List[Dict] = None, total: float = 0
) -> Tuple[str, str]:
    """
    Función principal para procesar el establecimiento detectado

    Args:
        establecimiento_raw: Nombre crudo detectado por OCR
        productos: Lista de productos (opcional)
        total: Total de la factura (opcional)

    Returns:
        Tupla (establecimiento_normalizado, cadena_comercial)

    Examples:
        >>> procesar_establecimiento("olimpica")
        ('OLÍMPICA', 'Olímpica')
        >>> procesar_establecimiento("EXITO")
        ('ÉXITO', 'Grupo Éxito')
    """

    # 1. Validar y corregir
    establecimiento = validar_y_corregir_establecimiento(
        establecimiento_raw, productos, total
    )

    # 2. Obtener cadena comercial
    cadena = obtener_cadena(establecimiento)

    # 3. Log para debugging
    if establecimiento_raw and establecimiento_raw.upper() != establecimiento:
        logger.info(
            f"📍 Establecimiento corregido: '{establecimiento_raw}' → '{establecimiento}'"
        )
    else:
        logger.info(f"📍 Establecimiento detectado: '{establecimiento}'")

    logger.info(f"🏢 Cadena identificada: {cadena}")

    return establecimiento, cadena


def obtener_o_crear_establecimiento_id(
    conn, establecimiento: str, cadena: str = None
) -> int:
    """
    Obtiene o crea un establecimiento en la base de datos

    Args:
        conn: Conexión a la base de datos
        establecimiento: Nombre del establecimiento
        cadena: Nombre de la cadena (opcional, se detecta automáticamente)

    Returns:
        ID del establecimiento
    """
    import os

    cursor = conn.cursor()

    # Si no se proporciona cadena, detectarla
    if not cadena:
        cadena = obtener_cadena(establecimiento)

    # Buscar establecimiento existente
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute(
            "SELECT id FROM establecimientos WHERE UPPER(nombre) = UPPER(%s)",
            (establecimiento,),
        )
    else:
        cursor.execute(
            "SELECT id FROM establecimientos WHERE UPPER(nombre) = UPPER(?)",
            (establecimiento,),
        )

    result = cursor.fetchone()

    if result:
        return result[0]

    # Crear nuevo establecimiento
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute(
            "INSERT INTO establecimientos (nombre, cadena) VALUES (%s, %s) RETURNING id",
            (establecimiento, cadena),
        )
        establecimiento_id = cursor.fetchone()[0]
    else:
        cursor.execute(
            "INSERT INTO establecimientos (nombre, cadena) VALUES (?, ?)",
            (establecimiento, cadena),
        )
        establecimiento_id = cursor.lastrowid

    conn.commit()
    logger.info(
        f"✅ Nuevo establecimiento creado: {establecimiento} (ID: {establecimiento_id})"
    )

    return establecimiento_id


# ========================================
# UTILIDADES ADICIONALES
# ========================================


def listar_establecimientos_conocidos() -> List[str]:
    """
    Retorna lista de todos los establecimientos conocidos

    Returns:
        Lista de nombres de establecimientos
    """
    return sorted(set(ESTABLECIMIENTOS_CONOCIDOS.values()))


def listar_cadenas_comerciales() -> List[str]:
    """
    Retorna lista de todas las cadenas comerciales

    Returns:
        Lista de nombres de cadenas
    """
    return sorted(set(CADENAS_COMERCIALES.values()))


def es_establecimiento_conocido(nombre: str) -> bool:
    """
    Verifica si un establecimiento es conocido

    Args:
        nombre: Nombre del establecimiento

    Returns:
        True si es conocido, False en caso contrario
    """
    nombre_normalizado = normalizar_establecimiento(nombre)
    return (
        nombre_normalizado != "DESCONOCIDO"
        and nombre_normalizado in ESTABLECIMIENTOS_CONOCIDOS.values()
    )
