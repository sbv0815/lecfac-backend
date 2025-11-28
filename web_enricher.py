"""
============================================================================
BÚSQUEDA VTEX MEJORADA - CON AUTOCOMPLETADO
============================================================================
Versión: 2.0
Fecha: 2025-11-28

Este archivo contiene funciones mejoradas para buscar en VTEX.
Copia estas funciones a tu productos_api_v2.py

MEJORAS:
- Usa endpoint de autocompletado cuando búsqueda normal falla
- Búsqueda por términos parciales (como el buscador web)
- Mejor manejo de resultados vacíos
============================================================================
"""

import requests
import urllib.parse
from typing import List, Dict, Any, Optional


# Configuración VTEX
VTEX_CONFIG = {
    "OLIMPICA": "https://www.olimpica.com",
    "EXITO": "https://www.exito.com",
    "CARULLA": "https://www.carulla.com",
    "JUMBO": "https://www.tiendasjumbo.co",
    "ALKOSTO": "https://www.alkosto.com",
    "MAKRO": "https://www.makro.com.co",
    "COLSUBSIDIO": "https://www.mercadocolsubsidio.com",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-CO,es;q=0.9",
}


def buscar_productos_vtex_mejorado(
    termino: str, establecimiento: str, limite: int = 15
) -> Dict[str, Any]:
    """
    🆕 Búsqueda mejorada en VTEX con múltiples estrategias.

    Estrategias (en orden):
    1. Búsqueda directa por término
    2. Búsqueda por autocompletado (como el buscador web)
    3. Búsqueda por palabras individuales

    Args:
        termino: Término de búsqueda (PLU o nombre)
        establecimiento: Nombre del supermercado
        limite: Máximo de resultados

    Returns:
        Dict con success, resultados, total, etc.
    """
    establecimiento_upper = establecimiento.upper()
    base_url = None

    for key, url in VTEX_CONFIG.items():
        if key in establecimiento_upper:
            base_url = url
            break

    if not base_url:
        return {
            "success": False,
            "error": f"Supermercado {establecimiento} no soportado",
            "supermercados_disponibles": list(VTEX_CONFIG.keys()),
        }

    termino_limpio = termino.strip()
    resultados = []

    # ========================================
    # ESTRATEGIA 1: Búsqueda por PLU directo
    # ========================================
    if termino_limpio.isdigit() and len(termino_limpio) >= 3:
        print(f"   🔍 Estrategia 1: Búsqueda por PLU {termino_limpio}")

        url = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_RefId:{termino_limpio}"

        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            if resp.status_code in [200, 206]:
                data = resp.json()
                if data:
                    resultados = parsear_resultados_vtex(data, base_url, limite)
                    if resultados:
                        return {
                            "success": True,
                            "query": termino,
                            "establecimiento": establecimiento,
                            "estrategia": "plu_directo",
                            "total": len(resultados),
                            "resultados": resultados,
                        }
        except Exception as e:
            print(f"   ⚠️ Error búsqueda PLU: {e}")

    # ========================================
    # ESTRATEGIA 2: Búsqueda por texto normal
    # ========================================
    print(f"   🔍 Estrategia 2: Búsqueda por texto '{termino_limpio}'")

    url = f"{base_url}/api/catalog_system/pub/products/search/{urllib.parse.quote(termino_limpio)}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code in [200, 206]:
            data = resp.json()
            if data:
                resultados = parsear_resultados_vtex(data, base_url, limite)
                if resultados:
                    return {
                        "success": True,
                        "query": termino,
                        "establecimiento": establecimiento,
                        "estrategia": "texto_normal",
                        "total": len(resultados),
                        "resultados": resultados,
                    }
    except Exception as e:
        print(f"   ⚠️ Error búsqueda texto: {e}")

    # ========================================
    # ESTRATEGIA 3: Búsqueda con fullText
    # ========================================
    print(f"   🔍 Estrategia 3: Búsqueda fullText '{termino_limpio}'")

    url = f"{base_url}/api/catalog_system/pub/products/search?ft={urllib.parse.quote(termino_limpio)}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code in [200, 206]:
            data = resp.json()
            if data:
                resultados = parsear_resultados_vtex(data, base_url, limite)
                if resultados:
                    return {
                        "success": True,
                        "query": termino,
                        "establecimiento": establecimiento,
                        "estrategia": "fulltext",
                        "total": len(resultados),
                        "resultados": resultados,
                    }
    except Exception as e:
        print(f"   ⚠️ Error búsqueda fullText: {e}")

    # ========================================
    # ESTRATEGIA 4: Búsqueda por palabras individuales
    # ========================================
    palabras = [p for p in termino_limpio.split() if len(p) >= 3]

    if len(palabras) > 1:
        print(f"   🔍 Estrategia 4: Búsqueda por palabras {palabras}")

        for palabra in palabras:
            url = f"{base_url}/api/catalog_system/pub/products/search?ft={urllib.parse.quote(palabra)}"

            try:
                resp = requests.get(url, headers=HEADERS, timeout=10)
                if resp.status_code in [200, 206]:
                    data = resp.json()
                    if data:
                        # Filtrar resultados que contengan TODAS las palabras
                        resultados_filtrados = []
                        for item in data:
                            nombre = item.get("productName", "").upper()
                            if all(p.upper() in nombre for p in palabras):
                                resultados_filtrados.append(item)

                        if resultados_filtrados:
                            resultados = parsear_resultados_vtex(
                                resultados_filtrados, base_url, limite
                            )
                            if resultados:
                                return {
                                    "success": True,
                                    "query": termino,
                                    "establecimiento": establecimiento,
                                    "estrategia": "palabras_individuales",
                                    "total": len(resultados),
                                    "resultados": resultados,
                                }
            except Exception as e:
                print(f"   ⚠️ Error búsqueda palabra '{palabra}': {e}")
                continue

    # ========================================
    # ESTRATEGIA 5: Autocompletado VTEX
    # ========================================
    print(f"   🔍 Estrategia 5: Autocompletado VTEX")

    # Primero obtener sugerencias
    url_autocomplete = f"{base_url}/buscaautocomplete?productNameContains={urllib.parse.quote(termino_limpio)}"

    try:
        resp = requests.get(url_autocomplete, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("itemsReturned", [])

            if items:
                # Extraer los nombres sugeridos y buscar cada uno
                for item in items[:5]:
                    nombre_sugerido = item.get("name", "")
                    if nombre_sugerido:
                        url = f"{base_url}/api/catalog_system/pub/products/search?ft={urllib.parse.quote(nombre_sugerido)}"

                        try:
                            resp2 = requests.get(url, headers=HEADERS, timeout=10)
                            if resp2.status_code in [200, 206]:
                                data2 = resp2.json()
                                if data2:
                                    nuevos = parsear_resultados_vtex(data2, base_url, 5)
                                    for nuevo in nuevos:
                                        # Evitar duplicados
                                        if not any(
                                            r["plu"] == nuevo["plu"] for r in resultados
                                        ):
                                            resultados.append(nuevo)
                        except:
                            continue

                if resultados:
                    return {
                        "success": True,
                        "query": termino,
                        "establecimiento": establecimiento,
                        "estrategia": "autocompletado",
                        "total": len(resultados[:limite]),
                        "resultados": resultados[:limite],
                    }
    except Exception as e:
        print(f"   ⚠️ Error autocompletado: {e}")

    # ========================================
    # ESTRATEGIA 6: Búsqueda en categorías
    # ========================================
    # Mapeo de términos comunes a categorías VTEX
    CATEGORIAS = {
        "queso": "lacteos",
        "leche": "lacteos",
        "yogurt": "lacteos",
        "carne": "carnes",
        "pollo": "carnes",
        "pan": "panaderia",
        "arroz": "granos",
        "aceite": "aceites",
        "jabon": "aseo",
    }

    for keyword, categoria in CATEGORIAS.items():
        if keyword in termino_limpio.lower():
            print(f"   🔍 Estrategia 6: Búsqueda en categoría '{categoria}'")

            url = f"{base_url}/api/catalog_system/pub/products/search?fq=C:/{categoria}/&ft={urllib.parse.quote(termino_limpio)}"

            try:
                resp = requests.get(url, headers=HEADERS, timeout=10)
                if resp.status_code in [200, 206]:
                    data = resp.json()
                    if data:
                        resultados = parsear_resultados_vtex(data, base_url, limite)
                        if resultados:
                            return {
                                "success": True,
                                "query": termino,
                                "establecimiento": establecimiento,
                                "estrategia": "categoria",
                                "total": len(resultados),
                                "resultados": resultados,
                            }
            except Exception as e:
                print(f"   ⚠️ Error búsqueda categoría: {e}")
            break

    # No se encontró nada
    return {
        "success": True,
        "query": termino,
        "establecimiento": establecimiento,
        "estrategia": "ninguna",
        "total": 0,
        "resultados": [],
        "sugerencias": [
            "Intenta con menos palabras",
            "Busca por marca (ej: 'alpina')",
            "Busca por categoría (ej: 'queso')",
            f"Verifica que {establecimiento} tenga el producto",
        ],
    }


def parsear_resultados_vtex(
    data: List[Dict], base_url: str, limite: int
) -> List[Dict[str, Any]]:
    """
    Parsea los resultados de VTEX a formato estándar.
    """
    resultados = []

    for item in data[:limite]:
        try:
            nombre = item.get("productName", "")
            if not nombre:
                continue

            link = item.get("link", "")
            plu = None
            ean = None
            precio = None
            imagen = None
            marca = item.get("brand", "")

            # Extraer datos del SKU
            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                ean = sku.get("ean", "")

                # PLU desde referenceId
                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict) and ref.get("Value"):
                            plu = ref["Value"]
                            break

                if not plu:
                    plu = item.get("productReference", "") or item.get("productId", "")

                # Precio
                sellers = sku.get("sellers", [])
                if sellers:
                    oferta = sellers[0].get("commertialOffer", {})
                    precio = oferta.get("Price", 0)

                # Imagen
                images = sku.get("images", [])
                if images and len(images) > 0:
                    imagen = images[0].get("imageUrl", "")

            # URL completa
            if link and not link.startswith("http"):
                link = f"{base_url}{link}"

            resultados.append(
                {
                    "plu": str(plu) if plu else "",
                    "ean": str(ean) if ean else "",
                    "nombre": nombre,
                    "marca": marca,
                    "precio": precio or 0,
                    "imagen": imagen or "",
                    "url": link,
                    "establecimiento": base_url.split("//")[1].split(".")[0].upper(),
                }
            )

        except Exception as e:
            print(f"   ⚠️ Error parseando producto: {e}")
            continue

    return resultados


# ============================================================================
# ENDPOINT PARA productos_api_v2.py
# ============================================================================
# Copia este endpoint a tu archivo productos_api_v2.py

"""
@router.get("/buscar-productos/{establecimiento}")
async def buscar_productos_en_vtex(
    establecimiento: str,
    q: str,
    limite: int = 15
):
    '''
    Busca productos en el catálogo web VTEX.
    Usa múltiples estrategias de búsqueda.

    Ejemplos:
    - /api/v2/buscar-productos/CARULLA?q=queso%20tajadas
    - /api/v2/buscar-productos/OLIMPICA?q=632967
    '''
    try:
        resultado = buscar_productos_vtex_mejorado(
            termino=q,
            establecimiento=establecimiento,
            limite=limite
        )
        return resultado

    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
"""


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    # Test de búsqueda
    print("\n" + "=" * 60)
    print("TEST: Búsqueda 'queso tajadas' en CARULLA")
    print("=" * 60)

    resultado = buscar_productos_vtex_mejorado(
        termino="queso tajadas", establecimiento="CARULLA", limite=10
    )

    print(f"\nÉxito: {resultado['success']}")
    print(f"Estrategia: {resultado.get('estrategia', 'N/A')}")
    print(f"Total: {resultado.get('total', 0)}")

    if resultado.get("resultados"):
        print("\nResultados:")
        for i, prod in enumerate(resultado["resultados"][:5], 1):
            print(f"  {i}. {prod['nombre'][:50]}")
            print(f"     PLU: {prod['plu']} | Precio: ${prod['precio']:,}")
