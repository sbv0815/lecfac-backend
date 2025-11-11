"""
============================================================================
VALIDADOR Y FILTRO DE PRODUCTOS - LECFAC
============================================================================
Filtros inteligentes para eliminar texto basura del OCR

FUNCIONES:
1. Detectar texto promocional/basura
2. Validar nombres de productos
3. Corregir nombres mal escritos comunes
4. Detectar productos incompletos

AUTOR: LecFac Team
FECHA: 2025-11-11
============================================================================
"""

import re
from typing import Tuple, Optional, List


# ============================================================================
# LISTAS DE DETECCIÓN
# ============================================================================

# Palabras que indican texto basura/promocional
PALABRAS_BASURA = [
    # Promociones
    'ahorra', 'ahorro', 'descuento', 'oferta', 'promocion', 'promo',
    '2x1', '3x2', 'lleva', 'paga', 'gratis',

    # Textos de factura
    'subtotal', 'total', 'iva', 'propina', 'cambio', 'efectivo',
    'tarjeta', 'credito', 'debito', 'pago', 'recibido',
    'devuelta', 'vuelto', 'recaudo',

    # Textos generales
    'precio final', 'precio', 'display', 'exhibicion',
    'espaci', 'espaciador', 'separador',
    'canastusio', 'caramuso',  # OCR mal leído

    # Instrucciones
    'guardar', 'refrigerar', 'congelar', 'temperatura',
]

# Palabras que si están SOLAS son basura
PALABRAS_SOLAS_INVALIDAS = [
    'de', 'del', 'la', 'el', 'y', 'o', 'con', 'sin',
    'para', 'por', 'en', 'a', 'un', 'una',
]

# Patrones regex de texto basura
PATRONES_BASURA = [
    r'^\d+x\d+$',  # 2x1, 3x2, etc
    r'^[0-9]+\s*%',  # 40%, 50% OFF
    r'^\$\s*\d+',  # Solo precio
    r'^precio\s+final',  # Precio final
    r'ahorra?\s+\d+',  # Ahorra 40
    r'descuento\s+\d+',  # Descuento 20
]

# Correcciones de nombres mal escritos comunes
CORRECCIONES_NOMBRES = {
    # Chocolates
    'choctinga': 'chocolatina',
    'chocting': 'chocolatina',
    'chocitina': 'chocolatina',
    'choclatina': 'chocolatina',
    'chocolatna': 'chocolatina',

    # Margarina
    'esparci': 'margarina',
    'esparcir': 'margarina',

    # Ponqué
    'pono': 'ponqué',
    'ponque': 'ponqué',
    'ggns': '',  # Ruido OCR

    # Atún
    'medall': 'medalla',

    # Leche
    'alqueria': 'alpina',  # Corrección según tu nota

    # Queso
    'mostaza': 'mozzarella',
    'mozarella': 'mozzarella',
}

# Marcas conocidas para validación
MARCAS_CONOCIDAS = {
    'olimpica': ['medalla', 'medalla de oro'],  # Según tu nota
    'alpina': ['alpina', 'regeneris', 'yox', 'bon yurt'],
    'colanta': ['colanta'],
    'bimbo': ['bimbo', 'artesano'],
    'ramo': ['ramo', 'chocorramo', 'ponqué ramo'],
    'nacional': ['chocolates', 'jet'],
    'nestle': ['maggi', 'nescafe', 'milo'],
}


# ============================================================================
# FUNCIONES DE VALIDACIÓN
# ============================================================================

def es_texto_basura(nombre: str) -> Tuple[bool, Optional[str]]:
    """
    Detecta si un texto es basura promocional o no es un producto

    Args:
        nombre: Nombre del producto a validar

    Returns:
        Tuple[bool, str]: (es_basura, razon)
    """
    if not nombre or not isinstance(nombre, str):
        return True, "Nombre vacío o inválido"

    nombre_lower = nombre.lower().strip()

    # 1. Validar longitud mínima
    if len(nombre_lower) < 3:
        return True, f"Nombre muy corto: '{nombre}'"

    # 2. Verificar palabras basura
    for palabra in PALABRAS_BASURA:
        if palabra in nombre_lower:
            return True, f"Contiene palabra basura: '{palabra}'"

    # 3. Verificar si es solo una palabra inválida
    palabras = nombre_lower.split()
    if len(palabras) == 1 and palabras[0] in PALABRAS_SOLAS_INVALIDAS:
        return True, f"Palabra sola inválida: '{palabras[0]}'"

    # 4. Verificar patrones regex
    for patron in PATRONES_BASURA:
        if re.search(patron, nombre_lower, re.IGNORECASE):
            return True, f"Coincide con patrón basura: {patron}"

    # 5. Verificar si es solo números
    if nombre.replace(' ', '').isdigit():
        return True, "Solo contiene números"

    # 6. Verificar nombres demasiado cortos o incompletos
    if len(nombre_lower) < 5:
        palabras_validas = [p for p in palabras if len(p) >= 3]
        if len(palabras_validas) == 0:
            return True, f"Nombre muy corto sin palabras válidas: '{nombre}'"

    return False, None


def corregir_nombre_producto(nombre: str) -> str:
    """
    Corrige nombres mal escritos comunes del OCR

    Args:
        nombre: Nombre del producto

    Returns:
        str: Nombre corregido
    """
    if not nombre:
        return nombre

    nombre_corregido = nombre.lower()

    # Aplicar correcciones
    for error, correccion in CORRECCIONES_NOMBRES.items():
        nombre_corregido = nombre_corregido.replace(error, correccion)

    # Limpiar espacios múltiples
    nombre_corregido = ' '.join(nombre_corregido.split())

    # Capitalizar primera letra de cada palabra
    nombre_corregido = nombre_corregido.title()

    return nombre_corregido


def validar_nombre_minimo(nombre: str) -> Tuple[bool, Optional[str]]:
    """
    Valida que un nombre tenga información mínima útil

    Args:
        nombre: Nombre del producto

    Returns:
        Tuple[bool, str]: (es_valido, razon_rechazo)
    """
    if not nombre or not isinstance(nombre, str):
        return False, "Nombre vacío"

    nombre = nombre.strip()

    # Longitud mínima absoluta
    if len(nombre) < 3:
        return False, f"Nombre muy corto: '{nombre}' ({len(nombre)} caracteres)"

    # Debe tener al menos una palabra de 3+ letras
    palabras = nombre.split()
    palabras_validas = [p for p in palabras if len(p) >= 3 and not p.isdigit()]

    if len(palabras_validas) == 0:
        return False, f"Sin palabras válidas: '{nombre}'"

    # No puede ser solo números
    if nombre.replace(' ', '').isdigit():
        return False, f"Solo números: '{nombre}'"

    return True, None


def validar_producto_completo(
    nombre: str,
    precio: int,
    codigo: str = ""
) -> Tuple[bool, Optional[str]]:
    """
    Validación completa de un producto

    Args:
        nombre: Nombre del producto
        precio: Precio en pesos
        codigo: Código del producto (opcional)

    Returns:
        Tuple[bool, str]: (es_valido, razon_rechazo)
    """
    # 1. Verificar texto basura
    es_basura, razon = es_texto_basura(nombre)
    if es_basura:
        return False, f"BASURA: {razon}"

    # 2. Validar nombre mínimo
    nombre_valido, razon = validar_nombre_minimo(nombre)
    if not nombre_valido:
        return False, f"NOMBRE INVÁLIDO: {razon}"

    # 3. Validar precio
    if precio <= 0:
        return False, f"Precio inválido: ${precio:,}"

    if precio < 100:
        return False, f"Precio muy bajo: ${precio:,} (posible error OCR)"

    if precio > 10_000_000:
        return False, f"Precio sospechosamente alto: ${precio:,}"

    # 4. Si tiene código, validar que no sea basura
    if codigo:
        codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))
        if len(codigo_limpio) < 3:
            return False, f"Código muy corto: '{codigo}'"

    return True, None


def enriquecer_nombre_producto(nombre: str, establecimiento: str = "") -> str:
    """
    Enriquece el nombre del producto con información adicional

    Args:
        nombre: Nombre del producto
        establecimiento: Establecimiento donde se compró

    Returns:
        str: Nombre enriquecido
    """
    # 1. Corregir errores comunes
    nombre_corregido = corregir_nombre_producto(nombre)

    # 2. Agregar marca si es Olímpica y tiene "Medalla"
    if establecimiento.upper() == "OLIMPICA" and "medalla" in nombre_corregido.lower():
        if "medalla de oro" not in nombre_corregido.lower():
            # Solo agregar si no está ya
            pass  # Por ahora no modificar, solo registrar

    return nombre_corregido


def filtrar_productos_validos(
    productos: List[dict],
    establecimiento: str = ""
) -> Tuple[List[dict], List[dict]]:
    """
    Filtra una lista de productos eliminando basura

    Args:
        productos: Lista de productos del OCR
        establecimiento: Establecimiento origen

    Returns:
        Tuple[List, List]: (productos_validos, productos_rechazados)
    """
    productos_validos = []
    productos_rechazados = []

    for producto in productos:
        nombre = producto.get('nombre', '')
        precio = producto.get('precio', 0)
        codigo = producto.get('codigo', '')

        # Convertir precio a int si es necesario
        if isinstance(precio, str):
            precio = int(float(precio.replace(',', '').replace('.', '')))

        # Validar
        es_valido, razon = validar_producto_completo(nombre, precio, codigo)

        if es_valido:
            # Enriquecer nombre
            nombre_mejorado = enriquecer_nombre_producto(nombre, establecimiento)

            producto_valido = producto.copy()
            producto_valido['nombre'] = nombre_mejorado
            producto_valido['nombre_original'] = nombre
            productos_validos.append(producto_valido)
        else:
            producto_rechazado = producto.copy()
            producto_rechazado['razon_rechazo'] = razon
            productos_rechazados.append(producto_rechazado)

    return productos_validos, productos_rechazados


# ============================================================================
# FUNCIONES DE TESTING
# ============================================================================

def test_validador():
    """Prueba el validador con casos reales"""
    print("\n" + "="*80)
    print("🧪 TESTING: Validador de Productos")
    print("="*80)

    casos_test = [
        # (nombre, precio, deberia_pasar)
        ("CHOCOLATINA JET LECHE", 15450, True),
        ("AHORRA 40X ESPARCI", 6400, False),
        ("PRECIO FINAL CREMA LECHE", 9600, False),
        ("KIWI", 9290, True),  # Corto pero válido
        ("PERA", 5000, True),  # Corto pero válido
        ("CHOCTINGA JET LECHE", 15450, True),  # Se corregirá
        ("2X1", 3000, False),
        ("TOTAL", 296900, False),
        ("LECHE ALPINA ENTERA 1100ML", 8900, True),
        ("", 5000, False),
        ("DE", 1000, False),
    ]

    pasaron = 0
    fallaron = 0

    for nombre, precio, esperado in casos_test:
        es_valido, razon = validar_producto_completo(nombre, precio)

        if es_valido == esperado:
            resultado = "✅ PASS"
            pasaron += 1
        else:
            resultado = "❌ FAIL"
            fallaron += 1

        print(f"\n{resultado}")
        print(f"   Nombre: {nombre}")
        print(f"   Precio: ${precio:,}")
        print(f"   Esperado: {'VÁLIDO' if esperado else 'RECHAZAR'}")
        print(f"   Resultado: {'VÁLIDO' if es_valido else 'RECHAZADO'}")
        if not es_valido:
            print(f"   Razón: {razon}")

    print(f"\n{'='*80}")
    print(f"RESULTADOS: {pasaron} pasaron, {fallaron} fallaron")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    test_validador()


print("✅ validador_productos.py cargado")
print("   📦 Funciones disponibles:")
print("      • validar_producto_completo(nombre, precio, codigo)")
print("      • filtrar_productos_validos(productos, establecimiento)")
print("      • corregir_nombre_producto(nombre)")
print("      • es_texto_basura(nombre)")
