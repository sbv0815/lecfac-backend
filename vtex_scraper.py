"""
Scraper Genérico para Supermercados VTEX - LecFac
Versión: 7.0
Soporta: Carulla, Éxito, Jumbo (todos usan VTEX)

Uso:
    from vtex_scraper import enriquecer_producto, SUPERMERCADOS

    # Buscar en Carulla
    resultado = await enriquecer_producto("AREPA SARY", plu="237373", supermercado="carulla")

    # Buscar en Éxito
    resultado = await enriquecer_producto("LECHE ALQUERIA", supermercado="exito")

    # Buscar en todos los supermercados
    resultados = await buscar_en_todos("LECHE ALQUERIA 1LT")
"""

import asyncio
import aiohttp
import re
from typing import Optional, Dict, List
import urllib.parse


# ============================================
# CONFIGURACIÓN DE SUPERMERCADOS VTEX
# ============================================

SUPERMERCADOS = {
    "carulla": {
        "nombre": "Carulla",
        "base_url": "https://www.carulla.com",
        "api_search": "/api/catalog_system/pub/products/search",
        "activo": True,
    },
    "exito": {
        "nombre": "Éxito",
        "base_url": "https://www.exito.com",
        "api_search": "/api/catalog_system/pub/products/search",
        "activo": True,
    },
    "jumbo": {
        "nombre": "Jumbo",
        "base_url": "https://www.tiendasjumbo.co",
        "api_search": "/api/catalog_system/pub/products/search",
        "activo": True,
    },
    "olimpica": {
        "nombre": "Olímpica",
        "base_url": "https://www.olimpica.com",
        "api_search": "/api/catalog_system/pub/products/search",
        "activo": True,
    },
}


class VTEXScraper:
    """Scraper genérico para tiendas VTEX"""

    def __init__(self, supermercado: str = "carulla"):
        supermercado = supermercado.lower()
        if supermercado not in SUPERMERCADOS:
            raise ValueError(
                f"Supermercado '{supermercado}' no soportado. Opciones: {list(SUPERMERCADOS.keys())}"
            )

        self.config = SUPERMERCADOS[supermercado]
        self.supermercado = self.config["nombre"]
        self.base_url = self.config["base_url"]
        self.api_search = self.config["api_search"]

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            "Referer": self.base_url + "/",
            "Origin": self.base_url,
        }

        # Abreviaciones colombianas
        self.abreviaciones = {
            "QSO": "queso",
            "QESO": "queso",
            "LCH": "leche",
            "LCHE": "leche",
            "MOZAR": "mozarella",
            "ALPI": "alpina",
            "ALPIN": "alpina",
            "ALQER": "alqueria",
            "ALQUER": "alqueria",
            "COLANT": "colanta",
            "EXTRADELGA": "extradelgada",
            "EXTRADEL": "extradelgada",
            "DESLAC": "deslactosada",
            "DESLACT": "deslactosada",
            "SEMIDES": "semidescremada",
            "DESCREM": "descremada",
            "TRAD": "tradicional",
            "TAJAD": "tajadas",
            "TAJ": "tajadas",
            "PAQ": "paquete",
            "UND": "unidades",
            "UNDS": "unidades",
            "GR": "gramos",
            "KG": "kilo",
            "LT": "litro",
            "ML": "mililitros",
            "MARG": "margarina",
            "MANTE": "mantequilla",
            "YOG": "yogurt",
            "HUEV": "huevos",
            "JAM": "jamon",
            "SALCH": "salchicha",
            "POLL": "pollo",
            "CARN": "carne",
            "ARR": "arroz",
            "AZUC": "azucar",
            "ACEIT": "aceite",
            "FRIJ": "frijol",
            "SARY": "sary",
            "ZENU": "zenu",
            "PIETR": "pietran",
        }

    def limpiar_nombre(self, nombre_ocr: str) -> str:
        """Expande abreviaciones del tiquete"""
        palabras = nombre_ocr.upper().split()
        resultado = []

        for palabra in palabras:
            limpia = re.sub(r"[^A-Z0-9]", "", palabra)
            if limpia in self.abreviaciones:
                resultado.append(self.abreviaciones[limpia])
            else:
                resultado.append(limpia.lower())

        return " ".join(resultado)

    def _parsear_producto(self, item: Dict) -> Optional[Dict]:
        """Parsea un producto del JSON de VTEX"""
        try:
            nombre = item.get("productName", "")
            link = item.get("link", "")

            # Extraer PLU (referenceId es el PLU del tiquete)
            plu = None
            sku_id = None
            ean = None

            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                sku_id = sku.get("itemId", "")
                ean = sku.get("ean", "")

                # El PLU está en referenceId
                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict) and ref.get("Value"):
                            plu = ref["Value"]
                            break

            if not plu:
                plu = item.get("productReference", "") or item.get("productId", "")

            # Precio
            precio = None
            precio_lista = None

            if item.get("items") and len(item["items"]) > 0:
                sellers = item["items"][0].get("sellers", [])
                if sellers:
                    oferta = sellers[0].get("commertialOffer", {})
                    precio = int(oferta.get("Price", 0)) or None
                    precio_lista = int(oferta.get("ListPrice", 0)) or None

            # URL
            if link and not link.startswith("http"):
                link = f"{self.base_url}{link}"

            if nombre:
                return {
                    "nombre": nombre,
                    "plu": str(plu) if plu else None,
                    "sku_id": str(sku_id) if sku_id else None,
                    "ean": str(ean) if ean else None,
                    "precio": precio,
                    "precio_lista": precio_lista,
                    "supermercado": self.supermercado,
                    "url": link,
                }
        except Exception as e:
            print(f"   ⚠️ Error parseando: {e}")

        return None

    async def buscar_por_nombre(self, nombre: str, max_results: int = 10) -> List[Dict]:
        """Busca productos por nombre"""
        productos = []
        url = f"{self.base_url}{self.api_search}/{urllib.parse.quote(nombre)}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=20)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        for item in data[:max_results]:
                            producto = self._parsear_producto(item)
                            if producto:
                                productos.append(producto)
            except Exception as e:
                print(f"   ❌ Error API {self.supermercado}: {str(e)[:50]}")

        return productos

    async def buscar_por_plu(self, plu: str) -> Optional[Dict]:
        """Busca por PLU (Reference ID)"""
        url = f"{self.base_url}{self.api_search}?fq=alternateIds_RefId:{plu}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        if data:
                            return self._parsear_producto(data[0])
            except Exception as e:
                print(f"   ❌ Error buscando PLU: {str(e)[:50]}")

        return None

    async def buscar_por_ean(self, ean: str) -> Optional[Dict]:
        """Busca por código de barras EAN"""
        url = f"{self.base_url}{self.api_search}?fq=alternateIds_Ean:{ean}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(
                    url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        if data:
                            return self._parsear_producto(data[0])
            except Exception as e:
                print(f"   ❌ Error buscando EAN: {str(e)[:50]}")

        return None

    async def enriquecer(
        self,
        nombre_ocr: str,
        plu_ocr: str = None,
        ean_ocr: str = None,
        precio_ocr: int = None,
    ) -> Dict:
        """
        Función principal: enriquece un producto del tiquete

        Estrategia:
        1. Si hay EAN → buscar por EAN
        2. Si hay PLU → buscar por PLU
        3. Buscar por nombre
        """
        resultado = {
            "nombre_ocr": nombre_ocr,
            "nombre_completo": None,
            "plu_ocr": plu_ocr,
            "plu_web": None,
            "ean_ocr": ean_ocr,
            "ean_web": None,
            "precio_ocr": precio_ocr,
            "precio_web": None,
            "supermercado": self.supermercado,
            "url": None,
            "verificado": False,
            "enriquecido": False,
            "encontrado": False,
            "candidatos": [],
            "mensaje": "",
        }

        nombre_limpio = self.limpiar_nombre(nombre_ocr)
        print(f"\n🔍 [{self.supermercado}] Buscando: '{nombre_ocr}'")

        producto = None

        # 1. Buscar por EAN
        if ean_ocr and len(ean_ocr) >= 8:
            print(f"   📊 Buscando por EAN: {ean_ocr}")
            producto = await self.buscar_por_ean(ean_ocr)
            if producto:
                resultado["verificado"] = True

        # 2. Buscar por PLU
        if not producto and plu_ocr and len(plu_ocr) >= 4:
            print(f"   🏷️ Buscando por PLU: {plu_ocr}")
            producto = await self.buscar_por_plu(plu_ocr)
            if producto and str(producto.get("plu")) == str(plu_ocr):
                resultado["verificado"] = True

        # 3. Buscar por nombre
        if not producto:
            print(f"   📝 Buscando por nombre: '{nombre_limpio}'")
            candidatos = await self.buscar_por_nombre(nombre_limpio, max_results=5)
            resultado["candidatos"] = candidatos

            if candidatos:
                # Verificar si alguno coincide con el PLU
                for c in candidatos:
                    if plu_ocr and str(c.get("plu")) == str(plu_ocr):
                        producto = c
                        resultado["verificado"] = True
                        break

                if not producto:
                    producto = candidatos[0]  # Usar el primero como mejor match

        # Procesar resultado
        if producto:
            resultado["nombre_completo"] = producto["nombre"]
            resultado["plu_web"] = producto["plu"]
            resultado["ean_web"] = producto.get("ean")
            resultado["precio_web"] = producto["precio"]
            resultado["url"] = producto["url"]
            resultado["enriquecido"] = True
            resultado["encontrado"] = True

            if resultado["verificado"]:
                resultado["mensaje"] = "✅ Producto verificado"
                print(f"   ✅ VERIFICADO: {producto['nombre'][:50]}...")
            else:
                resultado["mensaje"] = f"⚠️ Encontrado pero no verificado"
                print(f"   ⚠️ Encontrado: {producto['nombre'][:50]}...")
        else:
            resultado["mensaje"] = "❌ No encontrado"
            print(f"   ❌ No encontrado")

        return resultado


# ============================================
# FUNCIONES DE ALTO NIVEL
# ============================================


async def enriquecer_producto(
    nombre_ocr: str,
    plu_ocr: str = None,
    ean_ocr: str = None,
    precio_ocr: int = None,
    supermercado: str = "carulla",
) -> Dict:
    """
    Enriquece un producto de un supermercado específico
    """
    scraper = VTEXScraper(supermercado)
    return await scraper.enriquecer(nombre_ocr, plu_ocr, ean_ocr, precio_ocr)


async def buscar_en_todos(
    nombre_ocr: str, plu_ocr: str = None, ean_ocr: str = None
) -> Dict[str, Dict]:
    """
    Busca un producto en TODOS los supermercados soportados
    Útil para comparar precios
    """
    resultados = {}

    for key, config in SUPERMERCADOS.items():
        if config["activo"]:
            try:
                scraper = VTEXScraper(key)
                resultado = await scraper.enriquecer(nombre_ocr, plu_ocr, ean_ocr)
                resultados[key] = resultado
            except Exception as e:
                print(f"   ❌ Error en {config['nombre']}: {e}")

    return resultados


async def comparar_precios(
    nombre_ocr: str, plu_ocr: str = None, ean_ocr: str = None
) -> List[Dict]:
    """
    Compara precios del mismo producto en diferentes supermercados
    Retorna lista ordenada de menor a mayor precio
    """
    resultados = await buscar_en_todos(nombre_ocr, plu_ocr, ean_ocr)

    comparacion = []
    for supermercado, resultado in resultados.items():
        if resultado.get("enriquecido") and resultado.get("precio_web"):
            comparacion.append(
                {
                    "supermercado": resultado["supermercado"],
                    "nombre": resultado["nombre_completo"],
                    "precio": resultado["precio_web"],
                    "url": resultado["url"],
                    "verificado": resultado["verificado"],
                }
            )

    # Ordenar por precio
    comparacion.sort(key=lambda x: x["precio"])

    return comparacion


# ============================================
# INTEGRACIÓN CON BASE DE DATOS
# ============================================


async def enriquecer_y_guardar(
    nombre_ocr: str,
    plu_ocr: str = None,
    ean_ocr: str = None,
    precio_ocr: int = None,
    supermercado: str = "carulla",
    guardar_candidatos: bool = True,
) -> Dict:
    """
    Enriquece Y guarda en la base de datos
    """
    resultado = await enriquecer_producto(
        nombre_ocr, plu_ocr, ean_ocr, precio_ocr, supermercado
    )

    try:
        from db_productos_enriched import (
            guardar_producto_enriched,
            guardar_candidatos_enriched,
            crear_tabla_enriched,
        )

        crear_tabla_enriched()

        if resultado["enriquecido"]:
            producto_id = guardar_producto_enriched(resultado)
            resultado["db_id"] = producto_id
            print(f"   💾 Guardado en BD: ID={producto_id}")

        if guardar_candidatos and resultado.get("candidatos"):
            guardados = guardar_candidatos_enriched(
                resultado["candidatos"], supermercado=resultado["supermercado"]
            )
            resultado["candidatos_guardados"] = guardados

    except ImportError:
        print("   ⚠️ db_productos_enriched no disponible")
    except Exception as e:
        print(f"   ❌ Error guardando: {e}")

    return resultado


# ============================================
# PRUEBAS
# ============================================


async def main():
    print("=" * 70)
    print("🧪 PRUEBA: Scraper VTEX Multi-Supermercado v7.0")
    print("   Soporta: Carulla, Éxito, Jumbo, Olímpica")
    print("=" * 70)

    # Test 1: Carulla con PLU
    print("\n" + "=" * 70)
    print("📋 TEST 1: Carulla - AREPA SARY con PLU")
    print("=" * 70)

    resultado = await enriquecer_producto(
        nombre_ocr="AREPA EXTRADELGA SARY",
        plu_ocr="237373",
        precio_ocr=7800,
        supermercado="carulla",
    )

    if resultado["enriquecido"]:
        print(f"\n📊 RESULTADO:")
        print(f"   Nombre: {resultado['nombre_completo']}")
        print(f"   PLU: {resultado['plu_web']}")
        print(f"   EAN: {resultado['ean_web']}")
        print(f"   Precio: ${resultado['precio_web']:,}")
        print(f"   Verificado: {'✅' if resultado['verificado'] else '⚠️'}")

    # Test 2: Éxito
    print("\n" + "=" * 70)
    print("📋 TEST 2: Éxito - LECHE ALQUERIA")
    print("=" * 70)

    resultado2 = await enriquecer_producto(
        nombre_ocr="LECHE ALQUERIA DESLACTOSADA", supermercado="exito"
    )

    if resultado2["enriquecido"]:
        print(f"\n📊 RESULTADO:")
        print(f"   Nombre: {resultado2['nombre_completo']}")
        print(f"   Precio: ${resultado2['precio_web']:,}")

    # Test 3: Olímpica
    print("\n" + "=" * 70)
    print("📋 TEST 3: Olímpica - LECHE ALQUERIA")
    print("=" * 70)

    resultado3 = await enriquecer_producto(
        nombre_ocr="LECHE ALQUERIA DESLACTOSADA 1 LITRO", supermercado="olimpica"
    )

    if resultado3["enriquecido"]:
        print(f"\n📊 RESULTADO:")
        print(f"   Nombre: {resultado3['nombre_completo']}")
        print(f"   Precio: ${resultado3['precio_web']:,}")

    # Test 4: Comparar precios en TODOS
    print("\n" + "=" * 70)
    print("📋 TEST 4: Comparar precios en TODOS los supermercados")
    print("=" * 70)

    comparacion = await comparar_precios("LECHE ALQUERIA DESLACTOSADA 1 LITRO")

    if comparacion:
        print(f"\n💰 COMPARACIÓN DE PRECIOS:")
        for i, item in enumerate(comparacion, 1):
            verificado = "✅" if item["verificado"] else "⚠️"
            print(f"   {i}. {item['supermercado']}: ${item['precio']:,} {verificado}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
