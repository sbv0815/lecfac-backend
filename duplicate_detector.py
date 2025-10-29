"""
Detector Inteligente de Productos Duplicados
=============================================

Detecta y elimina productos duplicados en facturas basándose en:
1. Código EAN idéntico
2. Nombres muy similares
3. Precios idénticos o muy cercanos
4. Validación contra total de factura

Autor: LecFac
Versión: 1.0.0
Fecha: 2025-01-18
"""

import os
from typing import List, Dict, Tuple
from difflib import SequenceMatcher


def normalizar_nombre(nombre: str) -> str:
    """
    Normaliza un nombre de producto para comparación

    Args:
        nombre: Nombre del producto

    Returns:
        Nombre normalizado (lowercase, sin espacios extras)
    """
    if not nombre:
        return ""

    # Convertir a minúsculas
    nombre = nombre.lower()

    # Eliminar espacios múltiples
    nombre = " ".join(nombre.split())

    # Eliminar caracteres especiales comunes
    caracteres_eliminar = [".", ",", "-", "_", "(", ")", "[", "]"]
    for char in caracteres_eliminar:
        nombre = nombre.replace(char, " ")

    # Volver a normalizar espacios
    nombre = " ".join(nombre.split())

    return nombre.strip()


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres usando SequenceMatcher

    Args:
        nombre1: Primer nombre
        nombre2: Segundo nombre

    Returns:
        Porcentaje de similitud (0.0 a 1.0)
    """
    nombre1_norm = normalizar_nombre(nombre1)
    nombre2_norm = normalizar_nombre(nombre2)

    if not nombre1_norm or not nombre2_norm:
        return 0.0

    return SequenceMatcher(None, nombre1_norm, nombre2_norm).ratio()


def son_duplicados(prod1: Dict, prod2: Dict, umbral_similitud: float = 0.85) -> bool:
    """
    Determina si dos productos son duplicados

    Args:
        prod1: Primer producto
        prod2: Segundo producto
        umbral_similitud: Umbral de similitud para considerar duplicados (default: 0.85)

    Returns:
        True si son duplicados, False en caso contrario
    """
    # Criterio 1: Código EAN idéntico (si ambos lo tienen)
    codigo1 = prod1.get("codigo", "").strip()
    codigo2 = prod2.get("codigo", "").strip()

    if codigo1 and codigo2 and len(codigo1) >= 8 and len(codigo2) >= 8:
        if codigo1 == codigo2:
            return True

    # Criterio 2: Nombres muy similares
    nombre1 = prod1.get("nombre", "")
    nombre2 = prod2.get("nombre", "")

    similitud = calcular_similitud(nombre1, nombre2)

    if similitud >= umbral_similitud:
        # Si nombres muy similares, verificar precios
        precio1 = float(prod1.get("valor", 0))
        precio2 = float(prod2.get("valor", 0))

        # Si precio idéntico o muy cercano (±5%), son duplicados
        if precio1 > 0 and precio2 > 0:
            diferencia_porcentual = abs(precio1 - precio2) / max(precio1, precio2)
            if diferencia_porcentual <= 0.05:  # 5% de diferencia
                return True

    return False


def detectar_duplicados_automaticamente(
    productos: List[Dict],
    total_factura: float,
    umbral_similitud: float = 0.85,
    tolerancia_total: float = 0.15
) -> Dict:
    """
    Detecta y elimina productos duplicados automáticamente

    Args:
        productos: Lista de productos de la factura
        total_factura: Total declarado en la factura
        umbral_similitud: Umbral para considerar nombres similares (0.0-1.0)
        tolerancia_total: Tolerancia para diferencia con total (default: 15%)

    Returns:
        Dict con:
            - productos_limpios: Lista sin duplicados
            - duplicados_detectados: True si se encontraron duplicados
            - productos_eliminados: Lista de productos eliminados
            - metricas: Estadísticas del proceso
    """
    print(f"\n{'='*60}")
    print(f"🔍 DETECTOR DE DUPLICADOS")
    print(f"{'='*60}")
    print(f"📦 Productos entrada: {len(productos)}")
    print(f"💰 Total factura: ${total_factura:,.0f}")

    if not productos:
        return {
            "success": True,
            "productos_limpios": [],
            "duplicados_detectados": False,
            "productos_eliminados": [],
            "metricas": {
                "total_productos_entrada": 0,
                "total_productos_salida": 0,
                "productos_eliminados": 0,
                "suma_entrada": 0,
                "suma_salida": 0,
                "total_declarado": total_factura,
            }
        }

    # Calcular suma inicial
    suma_entrada = sum(float(p.get("valor", 0)) for p in productos)
    print(f"💵 Suma productos: ${suma_entrada:,.0f}")

    # Verificar si hay diferencia significativa
    diferencia = abs(suma_entrada - total_factura)
    porcentaje_diferencia = (diferencia / total_factura * 100) if total_factura > 0 else 0

    print(f"📊 Diferencia: ${diferencia:,.0f} ({porcentaje_diferencia:.1f}%)")

    # Si la diferencia es menor al 5%, no hacer nada
    if porcentaje_diferencia < 5:
        print(f"✅ Diferencia menor al 5% - No se requiere limpieza")
        return {
            "success": True,
            "productos_limpios": productos,
            "duplicados_detectados": False,
            "productos_eliminados": [],
            "metricas": {
                "total_productos_entrada": len(productos),
                "total_productos_salida": len(productos),
                "productos_eliminados": 0,
                "suma_entrada": suma_entrada,
                "suma_salida": suma_entrada,
                "total_declarado": total_factura,
                "porcentaje_diferencia": porcentaje_diferencia,
            }
        }

    # Detectar duplicados
    productos_limpios = []
    productos_eliminados = []
    indices_eliminados = set()

    for i, prod1 in enumerate(productos):
        if i in indices_eliminados:
            continue

        # Buscar duplicados de este producto
        duplicados_encontrados = []

        for j, prod2 in enumerate(productos):
            if i >= j or j in indices_eliminados:
                continue

            if son_duplicados(prod1, prod2, umbral_similitud):
                duplicados_encontrados.append(j)

        if duplicados_encontrados:
            # Marcar como eliminados
            for idx in duplicados_encontrados:
                indices_eliminados.add(idx)
                productos_eliminados.append({
                    "indice": idx,
                    "nombre": productos[idx].get("nombre", ""),
                    "codigo": productos[idx].get("codigo", ""),
                    "valor": float(productos[idx].get("valor", 0)),
                    "razon": f"Duplicado de '{prod1.get('nombre', '')}'"
                })

            print(f"   ⚠️ Duplicado: {prod1.get('nombre', '')} (eliminados: {len(duplicados_encontrados)})")

        # Agregar producto original a lista limpia
        productos_limpios.append(prod1)

    # Calcular suma después de limpieza
    suma_salida = sum(float(p.get("valor", 0)) for p in productos_limpios)
    diferencia_final = abs(suma_salida - total_factura)
    porcentaje_final = (diferencia_final / total_factura * 100) if total_factura > 0 else 0

    print(f"\n📊 RESULTADOS:")
    print(f"   Entrada: {len(productos)} productos (${suma_entrada:,.0f})")
    print(f"   Salida: {len(productos_limpios)} productos (${suma_salida:,.0f})")
    print(f"   Eliminados: {len(productos_eliminados)} productos")
    print(f"   Diferencia final: ${diferencia_final:,.0f} ({porcentaje_final:.1f}%)")

    # Si después de eliminar duplicados todavía hay diferencia > 15%
    if porcentaje_final > tolerancia_total * 100:
        print(f"   ⚠️ Diferencia aún significativa después de limpieza")
    else:
        print(f"   ✅ Diferencia dentro del rango aceptable")

    print(f"{'='*60}\n")

    return {
        "success": True,
        "productos_limpios": productos_limpios,
        "duplicados_detectados": len(productos_eliminados) > 0,
        "productos_eliminados": productos_eliminados,
        "metricas": {
            "total_productos_entrada": len(productos),
            "total_productos_salida": len(productos_limpios),
            "productos_eliminados": len(productos_eliminados),
            "suma_entrada": suma_entrada,
            "suma_salida": suma_salida,
            "total_declarado": total_factura,
            "diferencia_inicial": diferencia,
            "porcentaje_inicial": porcentaje_diferencia,
            "diferencia_final": diferencia_final,
            "porcentaje_final": porcentaje_final,
        }
    }


def detectar_duplicados_por_codigo(productos: List[Dict]) -> List[Tuple[int, int]]:
    """
    Detecta duplicados basándose solo en código EAN

    Args:
        productos: Lista de productos

    Returns:
        Lista de tuplas (indice1, indice2) de productos duplicados
    """
    duplicados = []
    codigos_vistos = {}

    for i, producto in enumerate(productos):
        codigo = producto.get("codigo", "").strip()

        if codigo and len(codigo) >= 8:
            if codigo in codigos_vistos:
                duplicados.append((codigos_vistos[codigo], i))
            else:
                codigos_vistos[codigo] = i

    return duplicados


def detectar_duplicados_por_nombre(
    productos: List[Dict],
    umbral_similitud: float = 0.90
) -> List[Tuple[int, int]]:
    """
    Detecta duplicados basándose solo en similitud de nombres

    Args:
        productos: Lista de productos
        umbral_similitud: Umbral de similitud (default: 0.90)

    Returns:
        Lista de tuplas (indice1, indice2) de productos duplicados
    """
    duplicados = []

    for i in range(len(productos)):
        for j in range(i + 1, len(productos)):
            similitud = calcular_similitud(
                productos[i].get("nombre", ""),
                productos[j].get("nombre", "")
            )

            if similitud >= umbral_similitud:
                duplicados.append((i, j))

    return duplicados


# ==========================================
# TESTING (ejecutar con: python duplicate_detector.py)
# ==========================================
if __name__ == "__main__":
    print("🧪 Testing duplicate_detector.py")
    print("=" * 60)

    # Test 1: Sin duplicados
    print("\n📋 Test 1: Sin duplicados")
    productos_test1 = [
        {"codigo": "7702001030644", "nombre": "Arroz Diana 500gr", "valor": 3500},
        {"codigo": "7707232560012", "nombre": "Aceite Gourmet 900ml", "valor": 12000},
        {"codigo": "7702001019939", "nombre": "Atún Van Camps 170gr", "valor": 4500},
    ]

    resultado1 = detectar_duplicados_automaticamente(productos_test1, 20000)
    print(f"✅ Productos salida: {len(resultado1['productos_limpios'])}")
    print(f"   Duplicados detectados: {resultado1['duplicados_detectados']}")

    # Test 2: Con duplicados por código
    print("\n📋 Test 2: Con duplicados por código")
    productos_test2 = [
        {"codigo": "7702001030644", "nombre": "Arroz Diana 500gr", "valor": 3500},
        {"codigo": "7702001030644", "nombre": "Arroz Diana 500g", "valor": 3500},
        {"codigo": "7707232560012", "nombre": "Aceite Gourmet 900ml", "valor": 12000},
    ]

    resultado2 = detectar_duplicados_automaticamente(productos_test2, 19000)
    print(f"✅ Productos salida: {len(resultado2['productos_limpios'])}")
    print(f"   Duplicados detectados: {resultado2['duplicados_detectados']}")
    print(f"   Productos eliminados: {len(resultado2['productos_eliminados'])}")

    # Test 3: Con duplicados por nombre similar
    print("\n📋 Test 3: Con duplicados por nombre similar")
    productos_test3 = [
        {"codigo": "", "nombre": "COCA COLA 400ML", "valor": 2500},
        {"codigo": "", "nombre": "Coca Cola 400ml", "valor": 2500},
        {"codigo": "", "nombre": "PEPSI 400ML", "valor": 2300},
    ]

    resultado3 = detectar_duplicados_automaticamente(productos_test3, 7300)
    print(f"✅ Productos salida: {len(resultado3['productos_limpios'])}")
    print(f"   Duplicados detectados: {resultado3['duplicados_detectados']}")
    print(f"   Productos eliminados: {len(resultado3['productos_eliminados'])}")

    # Test 4: Diferencia dentro del rango (no hace nada)
    print("\n📋 Test 4: Diferencia menor al 5% (no limpia)")
    productos_test4 = [
        {"codigo": "7702001030644", "nombre": "Arroz Diana 500gr", "valor": 3500},
        {"codigo": "7707232560012", "nombre": "Aceite Gourmet 900ml", "valor": 12000},
    ]

    resultado4 = detectar_duplicados_automaticamente(productos_test4, 15700)
    print(f"✅ Productos salida: {len(resultado4['productos_limpios'])}")
    print(f"   Duplicados detectados: {resultado4['duplicados_detectados']}")

    print("\n" + "=" * 60)
    print("✅ Tests completados")
