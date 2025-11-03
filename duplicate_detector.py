"""
Detector Inteligente de Productos Duplicados - V2
==================================================
✅ VERSIÓN MEJORADA: Consolida cantidades en lugar de eliminar

Detecta y consolida productos duplicados en facturas basándose en:
1. Código EAN idéntico
2. Nombres muy similares
3. Precios idénticos o muy cercanos
4. Validación contra total de factura

IMPORTANTE: Cuando detecta duplicados, SUMA las cantidades
Ejemplo: 3 aguacates (1+1+1) = 1 producto con cantidad 3

Autor: LecFac
Versión: 2.0.0
Fecha: 2025-11-03
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
    Detecta y CONSOLIDA productos duplicados automáticamente

    ✅ MEJORA: Suma cantidades en lugar de eliminar productos

    Args:
        productos: Lista de productos de la factura
        total_factura: Total declarado en la factura
        umbral_similitud: Umbral para considerar nombres similares (0.0-1.0)
        tolerancia_total: Tolerancia para diferencia con total (default: 15%)

    Returns:
        Dict con:
            - productos_limpios: Lista consolidada (con cantidades sumadas)
            - duplicados_detectados: True si se encontraron duplicados
            - productos_eliminados: Lista de productos consolidados
            - metricas: Estadísticas del proceso
    """
    print(f"\n{'='*60}")
    print(f"🔍 DETECTOR DE DUPLICADOS V2 (Consolida cantidades)")
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
    suma_entrada = sum(float(p.get("valor", 0)) * float(p.get("cantidad", 1)) for p in productos)
    print(f"💵 Suma productos: ${suma_entrada:,.0f}")

    # Verificar si hay diferencia significativa
    diferencia = abs(suma_entrada - total_factura)
    porcentaje_diferencia = (diferencia / total_factura * 100) if total_factura > 0 else 0

    print(f"📊 Diferencia: ${diferencia:,.0f} ({porcentaje_diferencia:.1f}%)")

    # ========================================
    # ✅ SIEMPRE BUSCAR Y CONSOLIDAR DUPLICADOS
    # (Ya no saltamos este paso aunque la suma cuadre)
    # ========================================

    # ========================================
    # ✅ CONSOLIDAR DUPLICADOS (SUMAR CANTIDADES)
    # ========================================
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
            # ✅ CONSOLIDAR: Sumar cantidades en lugar de eliminar
            cantidad_total = float(prod1.get("cantidad", 1))

            for idx in duplicados_encontrados:
                indices_eliminados.add(idx)
                cantidad_duplicado = float(productos[idx].get("cantidad", 1))
                cantidad_total += cantidad_duplicado

                productos_eliminados.append({
                    "indice": idx,
                    "nombre": productos[idx].get("nombre", ""),
                    "codigo": productos[idx].get("codigo", ""),
                    "valor": float(productos[idx].get("valor", 0)),
                    "cantidad": cantidad_duplicado,
                    "razon": f"Consolidado en '{prod1.get('nombre', '')}'"
                })

            print(f"   ⚠️ Duplicado: {prod1.get('nombre', '')} (consolidados: {len(duplicados_encontrados)})")
            print(f"      Cantidad original: {prod1.get('cantidad', 1)}")
            print(f"      Cantidad consolidada: {cantidad_total} unidades")

            # ✅ Crear producto consolidado con cantidad total
            producto_consolidado = prod1.copy()
            producto_consolidado["cantidad"] = cantidad_total
            productos_limpios.append(producto_consolidado)
        else:
            # Agregar producto sin duplicados
            productos_limpios.append(prod1)

    # Calcular suma después de consolidación
    suma_salida = sum(float(p.get("valor", 0)) * float(p.get("cantidad", 1)) for p in productos_limpios)
    diferencia_final = abs(suma_salida - total_factura)
    porcentaje_final = (diferencia_final / total_factura * 100) if total_factura > 0 else 0

    print(f"\n📊 RESULTADOS:")
    print(f"   Entrada: {len(productos)} registros (${suma_entrada:,.0f})")
    print(f"   Salida: {len(productos_limpios)} productos únicos (${suma_salida:,.0f})")

    # Si no se encontraron duplicados
    if len(productos_eliminados) == 0:
        print(f"   ✅ No se detectaron duplicados")
    else:
        print(f"   Consolidados: {len(productos_eliminados)} registros duplicados")

    print(f"   Diferencia final: ${diferencia_final:,.0f} ({porcentaje_final:.1f}%)")

    # Si después de consolidar todavía hay diferencia > 15%
    if porcentaje_final > tolerancia_total * 100:
        print(f"   ⚠️ Diferencia aún significativa después de consolidación")
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


# ==========================================
# TESTING
# ==========================================
if __name__ == "__main__":
    print("🧪 Testing duplicate_detector V2 (Consolida cantidades)")
    print("=" * 60)

    # Test: Duplicados con cantidades
    print("\n📋 Test: Aguacates duplicados")
    productos_test = [
        {"codigo": "", "nombre": "AGUACATE HASS", "valor": 2000, "cantidad": 1},
        {"codigo": "", "nombre": "Aguacate Hass", "valor": 2000, "cantidad": 1},
        {"codigo": "", "nombre": "AGUACATE HASS", "valor": 2000, "cantidad": 1},
    ]

    resultado = detectar_duplicados_automaticamente(productos_test, 6000)

    print(f"\n✅ Resultado:")
    print(f"   Productos de entrada: {len(productos_test)}")
    print(f"   Productos consolidados: {len(resultado['productos_limpios'])}")

    if resultado['productos_limpios']:
        prod = resultado['productos_limpios'][0]
        print(f"   Producto final: {prod['nombre']}")
        print(f"   Cantidad total: {prod['cantidad']} unidades")
        print(f"   Precio unitario: ${prod['valor']:,}")
        print(f"   Total: ${prod['valor'] * prod['cantidad']:,}")

    print("\n" + "=" * 60)
    print("✅ Tests completados")
