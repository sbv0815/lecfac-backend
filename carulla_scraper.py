"""
Scraper Carulla para LecFac
Versión: 6.0 - Usa API VTEX + Playwright mejorado
Autor: Claude + Santiago

CAMBIOS v6.0:
- Añadido soporte para API de búsqueda VTEX (más rápido y confiable)
- Regex de URLs corregido para capturar slugs con números
- Selectores de Playwright actualizados
- Mejor extracción de PLU (formato "MARCA-PLU: NUMERO")
- Fallback inteligente: API -> Búsqueda Google -> Scraping directo
"""

import asyncio
import aiohttp
from playwright.async_api import async_playwright
import re
from typing import Optional, Dict, List
import urllib.parse
import json


class CarullaScraper:
    def __init__(self):
        self.supermercado = "Carulla"
        self.rate_limit_delay = 2
        self.vtex_search_url = (
            "https://www.carulla.com/api/catalog_system/pub/products/search"
        )
        self.vtex_autocomplete_url = "https://www.carulla.com/buscaautocomplete"

    # ============================================
    # MÉTODO 1: API VTEX (más rápido y confiable)
    # ============================================

    async def buscar_api_vtex(
        self, nombre_busqueda: str, max_results: int = 10
    ) -> List[Dict]:
        """
        Busca productos usando la API pública de VTEX
        Endpoint: /api/catalog_system/pub/products/search/
        Retorna lista de productos con nombre, plu, precio, url
        """
        productos = []

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
            "Referer": "https://www.carulla.com/",
            "Origin": "https://www.carulla.com",
        }

        # URL del endpoint de búsqueda VTEX
        # Formato: /api/catalog_system/pub/products/search/{termino}
        search_url = f"https://www.carulla.com/api/catalog_system/pub/products/search/{urllib.parse.quote(nombre_busqueda)}"

        async with aiohttp.ClientSession() as session:
            try:
                print(f"🔎 API VTEX: {nombre_busqueda}")
                print(f"   URL: {search_url}")

                async with session.get(
                    search_url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                    ssl=True,
                ) as response:
                    print(f"   Status: {response.status}")

                    # VTEX retorna 200 o 206 (Partial Content) cuando hay resultados
                    if response.status in [200, 206]:
                        data = await response.json()

                        for item in data[:max_results]:
                            producto = self._parsear_producto_vtex(item)
                            if producto:
                                productos.append(producto)
                                print(
                                    f"   ✓ {producto['nombre'][:50]}... PLU:{producto['plu']}"
                                )

                        if productos:
                            print(f"📦 API encontró {len(productos)} productos")
                    else:
                        print(f"   ⚠️ API respondió {response.status}")
                        # Intentar leer el error
                        try:
                            error_text = await response.text()
                            print(f"   Error: {error_text[:100]}")
                        except:
                            pass

            except asyncio.TimeoutError:
                print("   ⏱️ Timeout en API VTEX")
            except aiohttp.ClientError as e:
                print(f"   ❌ Error de conexión: {str(e)[:80]}")
            except Exception as e:
                print(f"   ❌ Error API: {type(e).__name__}: {str(e)[:80]}")

        return productos

    async def buscar_por_ean(self, ean: str) -> Optional[Dict]:
        """
        Busca un producto por su código EAN (código de barras)
        Endpoint: /api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}
        Muy útil para productos con código de barras del tiquete
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.carulla.com/",
        }

        search_url = f"https://www.carulla.com/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"

        async with aiohttp.ClientSession() as session:
            try:
                print(f"🔎 Buscando EAN: {ean}")

                async with session.get(
                    search_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        if data and len(data) > 0:
                            producto = self._parsear_producto_vtex(data[0])
                            if producto:
                                print(f"   ✅ Encontrado: {producto['nombre'][:50]}...")
                                return producto

                    print(f"   ⚠️ EAN no encontrado (status {response.status})")

            except Exception as e:
                print(f"   ❌ Error buscando EAN: {str(e)[:50]}")

        return None

    async def buscar_por_sku_id(self, sku_id: str) -> Optional[Dict]:
        """
        Busca un producto por su SKU ID
        Endpoint: /api/catalog_system/pub/products/search?fq=skuId:{id}
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.carulla.com/",
        }

        search_url = f"https://www.carulla.com/api/catalog_system/pub/products/search?fq=skuId:{sku_id}"

        async with aiohttp.ClientSession() as session:
            try:
                print(f"🔎 Buscando SKU ID: {sku_id}")

                async with session.get(
                    search_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        if data and len(data) > 0:
                            producto = self._parsear_producto_vtex(data[0])
                            if producto:
                                print(f"   ✅ Encontrado: {producto['nombre'][:50]}...")
                                return producto

                    print(f"   ⚠️ SKU ID no encontrado (status {response.status})")

            except Exception as e:
                print(f"   ❌ Error buscando SKU: {str(e)[:50]}")

        return None

    async def buscar_por_reference_id(self, ref_id: str) -> Optional[Dict]:
        """
        Busca un producto por su Reference ID (PLU del tiquete en Carulla)
        Endpoint: /api/catalog_system/pub/products/search?fq=alternateIds_RefId:{id}

        Este es el campo que Carulla usa como PLU en los tiquetes.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://www.carulla.com/",
        }

        # VTEX usa alternateIds_RefId para buscar por referenceId
        search_url = f"https://www.carulla.com/api/catalog_system/pub/products/search?fq=alternateIds_RefId:{ref_id}"

        async with aiohttp.ClientSession() as session:
            try:
                print(f"🔎 Buscando PLU/RefId: {ref_id}")

                async with session.get(
                    search_url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
                ) as response:
                    if response.status in [200, 206]:
                        data = await response.json()
                        if data and len(data) > 0:
                            producto = self._parsear_producto_vtex(data[0])
                            if producto:
                                print(f"   ✅ Encontrado: {producto['nombre'][:50]}...")
                                return producto

                    print(f"   ⚠️ RefId no encontrado (status {response.status})")

            except Exception as e:
                print(f"   ❌ Error buscando RefId: {str(e)[:50]}")

        return None

    def _parsear_producto_vtex(self, item: Dict) -> Optional[Dict]:
        """
        Parsea un producto del JSON de VTEX

        Estructura típica de respuesta VTEX para Carulla:
        {
            "productId": "115721",           # ID interno VTEX (NO es el PLU)
            "productName": "Nombre del producto",
            "productReference": "REF123",    # Referencia producto (a veces vacío)
            "link": "/slug-producto/p",
            "items": [{
                "itemId": "115721",          # SKU ID (NO es el PLU)
                "name": "Variante",
                "referenceId": [
                    {"Key": "RefId", "Value": "237373"}  # ← ¡ESTE ES EL PLU!
                ],
                "ean": "7701234567890",
                "sellers": [{
                    "commertialOffer": {
                        "Price": 12900,
                        "ListPrice": 15000
                    }
                }]
            }]
        }

        El PLU que aparece en el tiquete de Carulla es el "referenceId.Value"
        """
        try:
            nombre = item.get("productName", "")
            link = item.get("link", "")

            # ============================================
            # EXTRACCIÓN DE PLU (¡El orden es importante!)
            # ============================================
            plu = None
            sku_id = None  # Guardar el itemId por separado

            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                sku_id = sku.get("itemId", "")

                # 1. PRIORIDAD: referenceId.Value (este es el PLU del tiquete)
                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict):
                            val = ref.get("Value", "")
                            if val:
                                plu = val
                                break

            # 2. Si no hay referenceId, usar productReference
            if not plu:
                plu = item.get("productReference", "")

            # 3. Si aún no hay PLU, usar el productId (último recurso)
            if not plu:
                plu = item.get("productId", "")

            # ============================================
            # EXTRACCIÓN DE EAN (código de barras)
            # ============================================
            ean = None
            if item.get("items") and len(item["items"]) > 0:
                ean = item["items"][0].get("ean", "")

            # ============================================
            # EXTRACCIÓN DE PRECIO
            # ============================================
            precio = None
            precio_lista = None

            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                sellers = sku.get("sellers", [])
                if sellers and len(sellers) > 0:
                    # Buscar el seller con mejor precio
                    for seller in sellers:
                        oferta = seller.get("commertialOffer", {})
                        precio_actual = oferta.get("Price", 0)
                        if precio_actual and precio_actual > 0:
                            if precio is None or precio_actual < precio:
                                precio = int(precio_actual)
                                precio_lista = int(oferta.get("ListPrice", 0)) or None

            # ============================================
            # URL COMPLETA
            # ============================================
            if link:
                if not link.startswith("http"):
                    link = f"https://www.carulla.com{link}"
                # Asegurar que termina en /p
                if not link.endswith("/p"):
                    link = f"{link}/p" if not link.endswith("/") else f"{link}p"

            # ============================================
            # CONSTRUIR RESULTADO
            # ============================================
            if nombre:
                return {
                    "nombre": nombre,
                    "plu": str(plu) if plu else None,
                    "sku_id": (
                        str(sku_id) if sku_id else None
                    ),  # Guardar también el SKU ID
                    "ean": str(ean) if ean else None,
                    "precio": precio,
                    "precio_lista": precio_lista,
                    "supermercado": self.supermercado,
                    "url": link,
                    "product_id": item.get("productId", ""),
                }

        except Exception as e:
            print(f"   ⚠️ Error parseando producto: {e}")

        return None

    # ============================================
    # MÉTODO 2: Scraping con Playwright (fallback)
    # ============================================

    async def scrape_producto(self, url: str) -> Optional[Dict]:
        """Extrae datos de un producto de Carulla dado su URL"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )
            page = await context.new_page()

            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)

                # Esperar a que cargue el contenido principal
                await page.wait_for_timeout(3000)

                # ============================================
                # EXTRACCIÓN DE PLU (varios métodos)
                # ============================================
                plu = None

                # Método 1: Buscar texto con formato "MARCA-PLU: NUMERO"
                try:
                    plu_elements = await page.locator("text=/-PLU:/i").all()
                    for elem in plu_elements:
                        text = await elem.text_content()
                        if text:
                            # Formato: "SARY-PLU: 237373" o similar
                            match = re.search(r"PLU:\s*(\d+)", text, re.IGNORECASE)
                            if match:
                                plu = match.group(1)
                                break
                except:
                    pass

                # Método 2: Buscar en el contenido completo
                if not plu:
                    try:
                        content = await page.content()
                        # Buscar patrones como "PLU: 237373" o "PLU:237373"
                        matches = re.findall(
                            r"PLU[:\s]+(\d{5,7})", content, re.IGNORECASE
                        )
                        if matches:
                            plu = matches[0]
                    except:
                        pass

                # Método 3: Buscar en atributos data-*
                if not plu:
                    try:
                        # VTEX suele poner el SKU/referencia en data attributes
                        sku_elem = await page.locator("[data-product-id]").first
                        if sku_elem:
                            plu = await sku_elem.get_attribute("data-product-id")
                    except:
                        pass

                # Método 4: Buscar "Referencia" en la página
                if not plu:
                    try:
                        ref_text = await page.locator(
                            "text=/Referencia/i"
                        ).first.text_content(timeout=2000)
                        if ref_text:
                            match = re.search(r"(\d{5,7})", ref_text)
                            if match:
                                plu = match.group(1)
                    except:
                        pass

                # ============================================
                # EXTRACCIÓN DE NOMBRE
                # ============================================
                nombre = None

                # Método 1: Selector específico de VTEX para título
                try:
                    nombre_elem = await page.locator(
                        ".vtex-store-components-3-x-productNameContainer, .vtex-product-summary-2-x-productBrand, h1.vtex-store-components-3-x-productBrand"
                    ).first
                    nombre = await nombre_elem.text_content(timeout=3000)
                except:
                    pass

                # Método 2: Cualquier H1 en la página
                if not nombre:
                    try:
                        nombre = await page.locator("h1").first.text_content(
                            timeout=2000
                        )
                    except:
                        pass

                # Método 3: Título de la página
                if not nombre:
                    nombre = await page.title()
                    if nombre:
                        # Limpiar sufijos comunes
                        for suffix in [
                            " - Carulla",
                            " | Carulla",
                            "| carulla.com",
                            "carulla.com",
                        ]:
                            if suffix.lower() in nombre.lower():
                                nombre = nombre.split(suffix)[0].strip()

                # ============================================
                # EXTRACCIÓN DE PRECIO
                # ============================================
                precio = None

                # Método 1: Selectores específicos de VTEX
                try:
                    precio_selectors = [
                        ".vtex-product-price-1-x-sellingPriceValue",
                        ".vtex-store-components-3-x-sellingPrice",
                        "[class*='sellingPrice']",
                        "[class*='Price'] span",
                    ]
                    for selector in precio_selectors:
                        try:
                            precio_elem = await page.locator(selector).first
                            precio_text = await precio_elem.text_content(timeout=1000)
                            if precio_text:
                                # Limpiar y convertir: "$7.800" -> 7800
                                precio_clean = re.sub(r"[^\d]", "", precio_text)
                                if precio_clean and int(precio_clean) > 100:
                                    precio = int(precio_clean)
                                    break
                        except:
                            continue
                except:
                    pass

                # Método 2: Buscar en JSON-LD (schema.org)
                if not precio:
                    try:
                        scripts = await page.locator(
                            "script[type='application/ld+json']"
                        ).all()
                        for script in scripts:
                            text = await script.text_content()
                            if text:
                                data = json.loads(text)
                                if isinstance(data, dict) and "offers" in data:
                                    precio_raw = data["offers"].get("price")
                                    if precio_raw:
                                        precio = int(float(precio_raw))
                                        break
                    except:
                        pass

                # Método 3: Regex en el contenido
                if not precio:
                    try:
                        content = await page.content()
                        # Buscar precios en formato colombiano: $7.800
                        precios = re.findall(r"\$\s*([\d\.]+)", content)
                        for p_str in precios:
                            p_clean = p_str.replace(".", "")
                            if p_clean.isdigit():
                                p_int = int(p_clean)
                                # Filtrar precios razonables (>1000 y <1000000)
                                if 1000 < p_int < 1000000:
                                    precio = p_int
                                    break
                    except:
                        pass

                if not nombre:
                    return None

                return {
                    "nombre": nombre.strip() if nombre else None,
                    "plu": plu,
                    "precio": precio,
                    "supermercado": self.supermercado,
                    "url": url,
                }

            except Exception as e:
                print(f"❌ Error scrapeando {url}: {str(e)}")
                return None
            finally:
                await browser.close()

    # ============================================
    # MÉTODO 3: Búsqueda en motores (último recurso)
    # ============================================

    def limpiar_nombre_para_busqueda(self, nombre_ocr: str) -> str:
        """Expande abreviaciones comunes de tiquetes colombianos"""
        abreviaciones = {
            "QSO": "queso",
            "QESO": "queso",
            "LCH": "leche",
            "LCHE": "leche",
            "MOZAR": "mozarella",
            "MOZARE": "mozarella",
            "ALPI": "alpina",
            "ALPIN": "alpina",
            "ALQER": "alqueria",
            "ALQUER": "alqueria",
            "COLANT": "colanta",
            "EXTRADELGA": "extradelgada",
            "EXTRADEL": "extradelgada",
            "DELGA": "delgada",
            "DELGAD": "delgada",
            "ENTR": "entera",
            "ENTER": "entera",
            "DESLAC": "deslactosada",
            "DESLACT": "deslactosada",
            "SEMIDES": "semidescremada",
            "DESCREM": "descremada",
            "TRAD": "tradicional",
            "TRADIC": "tradicional",
            "BLC": "blanca",
            "BLNC": "blanca",
            "BLANC": "blanca",
            "TAJAD": "tajadas",
            "TAJ": "tajadas",
            "PORCION": "porcionado",
            "PORC": "porcionado",
            "PAQ": "paquete",
            "UND": "unidades",
            "UNDS": "unidades",
            "GR": "gramos",
            "KG": "kilo",
            "LT": "litro",
            "ML": "mililitros",
            "AREPA": "arepa",
            "AREPAS": "arepas",
            "PAN": "pan",
            "MARG": "margarina",
            "MARGAR": "margarina",
            "MANTE": "mantequilla",
            "MANTEQ": "mantequilla",
            "YOGUR": "yogurt",
            "YOG": "yogurt",
            "HUEV": "huevos",
            "HUEVO": "huevos",
            "JAM": "jamon",
            "JAMON": "jamon",
            "SALCH": "salchicha",
            "SALCHIC": "salchicha",
            "POLLO": "pollo",
            "POLL": "pollo",
            "CARN": "carne",
            "RES": "res",
            "CERDO": "cerdo",
            "CERD": "cerdo",
            "ARROZ": "arroz",
            "ARR": "arroz",
            "AZUC": "azucar",
            "AZUCAR": "azucar",
            "ACEIT": "aceite",
            "SAL": "sal",
            "FRIJ": "frijol",
            "FRIJOL": "frijol",
            "SARY": "sary",
            "ZENÚ": "zenu",
            "ZENU": "zenu",
            "RANCHERA": "ranchera",
            "RANCH": "ranchera",
            "RICA": "rica",
            "PIETR": "pietran",
            "PIETRAN": "pietran",
            "DONA": "doña",
            "PAISA": "paisa",
            "MAIZ": "maiz",
        }

        palabras = nombre_ocr.upper().split()
        palabras_limpias = []

        for palabra in palabras:
            palabra_limpia = re.sub(r"[^A-Z0-9]", "", palabra)
            if palabra_limpia:
                if palabra_limpia in abreviaciones:
                    palabras_limpias.append(abreviaciones[palabra_limpia])
                else:
                    encontrado = False
                    for abrev, completo in abreviaciones.items():
                        if palabra_limpia.startswith(abrev) or abrev.startswith(
                            palabra_limpia
                        ):
                            palabras_limpias.append(completo)
                            encontrado = True
                            break
                    if not encontrado:
                        palabras_limpias.append(palabra_limpia.lower())

        return " ".join(palabras_limpias)

    async def buscar_urls_por_nombre(
        self, nombre_busqueda: str, max_urls: int = 5
    ) -> List[str]:
        """Busca URLs usando DuckDuckGo HTML"""
        query = f"site:carulla.com {nombre_busqueda}"
        duckduckgo_url = (
            f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
        )

        print(f"🔎 DuckDuckGo: {query}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
            )

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            page = await context.new_page()

            try:
                await page.goto(
                    duckduckgo_url, wait_until="domcontentloaded", timeout=30000
                )
                await page.wait_for_timeout(2000)

                content = await page.content()

                # CORREGIDO: Regex que captura slugs con números y guiones
                # Ejemplo: arepa-de-maiz-extradelgrada-tradicional-x-10-unds-115721
                urls = re.findall(
                    r"https://www\.carulla\.com/([a-zA-Z0-9\-]+)/p", content
                )

                # Reconstruir URLs completas
                urls_completas = [f"https://www.carulla.com/{slug}/p" for slug in urls]

                # También buscar en formato URL encoded
                urls_encoded = re.findall(
                    r"carulla\.com%2F([a-zA-Z0-9\-]+)%2Fp", content
                )
                for encoded in urls_encoded:
                    url = f"https://www.carulla.com/{encoded}/p"
                    if url not in urls_completas:
                        urls_completas.append(url)

                # Filtrar duplicadas y páginas genéricas
                urls_validas = []
                excluir = [
                    "preguntas",
                    "politicas",
                    "terminos",
                    "authentication",
                    "compra-y-recoge",
                ]

                for url in urls_completas:
                    if url not in urls_validas:
                        if not any(x in url.lower() for x in excluir):
                            urls_validas.append(url)
                            if len(urls_validas) >= max_urls:
                                break

                print(f"📍 Encontradas {len(urls_validas)} URLs")
                return urls_validas

            except Exception as e:
                print(f"❌ Error buscando: {str(e)}")
                return []
            finally:
                await browser.close()

    # ============================================
    # FUNCIÓN PRINCIPAL: Buscar y verificar PLU
    # ============================================

    async def buscar_y_verificar_plu(
        self,
        nombre_ocr: str,
        plu_ocr: str,
        ean_ocr: str = None,
        max_candidatos: int = 5,
    ) -> Dict:
        """
        🎯 FUNCIÓN PRINCIPAL: Busca por nombre y verifica PLU

        Estrategia de búsqueda (en orden):
        1. Si hay EAN, buscar directamente por EAN
        2. Si hay PLU, buscar directamente por SKU ID
        3. Buscar por nombre via API VTEX
        4. Fallback: Búsqueda web + Scraping individual
        """
        resultado = {
            "producto": None,
            "verificado": False,
            "candidatos": [],
            "mensaje": "",
        }

        nombre_busqueda = self.limpiar_nombre_para_busqueda(nombre_ocr)
        print(f"\n{'='*60}")
        print(f"🔍 Buscando: '{nombre_ocr}'")
        print(f"📝 Términos: '{nombre_busqueda}'")
        print(f"🏷️  PLU tiquete: {plu_ocr or '(no disponible)'}")
        if ean_ocr:
            print(f"📊 EAN tiquete: {ean_ocr}")
        print("=" * 60)

        # ============================================
        # PASO 1: Si hay EAN, buscar directamente
        # ============================================
        if ean_ocr and len(ean_ocr) >= 8:
            print("\n📊 Buscando por EAN...")
            producto = await self.buscar_por_ean(ean_ocr)
            if producto:
                resultado["producto"] = producto
                resultado["candidatos"].append(producto)
                resultado["verificado"] = True
                resultado["mensaje"] = "✅ Producto encontrado por EAN"
                return resultado

        # ============================================
        # PASO 2: Si hay PLU, buscar por Reference ID (PLU del tiquete)
        # ============================================
        if plu_ocr and len(plu_ocr) >= 4:
            print("\n🏷️ Buscando por PLU (Reference ID)...")

            # Primero intentar con alternateIds_RefId (PLU real del tiquete)
            producto = await self.buscar_por_reference_id(plu_ocr)

            if producto:
                resultado["candidatos"].append(producto)
                plu_web = str(producto.get("plu", ""))

                if plu_web == str(plu_ocr):
                    resultado["producto"] = producto
                    resultado["verificado"] = True
                    resultado["mensaje"] = "✅ Producto verificado por PLU directo"
                    return resultado
                else:
                    print(f"   ⚠️ PLU encontrado ({plu_web}) ≠ PLU buscado ({plu_ocr})")
                    resultado["producto"] = producto
                    resultado["verificado"] = False

            # Si no encontró por RefId, intentar por SKU ID
            if not producto:
                print("   Intentando por SKU ID...")
                producto = await self.buscar_por_sku_id(plu_ocr)

                if producto:
                    resultado["candidatos"].append(producto)
                    plu_web = str(producto.get("plu", ""))

                    if plu_web == str(plu_ocr):
                        resultado["producto"] = producto
                        resultado["verificado"] = True
                        resultado["mensaje"] = "✅ Producto verificado por SKU ID"
                        return resultado
                    else:
                        print(
                            f"   ⚠️ PLU encontrado ({plu_web}) ≠ PLU buscado ({plu_ocr})"
                        )
                        if not resultado["producto"]:
                            resultado["producto"] = producto
                        resultado["verificado"] = False
                        resultado["mensaje"] = (
                            f"⚠️ Producto encontrado, pero PLU web ({plu_web}) ≠ PLU tiquete ({plu_ocr})"
                        )

        # ============================================
        # PASO 3: Buscar por nombre via API VTEX
        # ============================================
        print("\n📡 Buscando por nombre via API VTEX...")
        productos_api = await self.buscar_api_vtex(nombre_busqueda, max_candidatos)

        if productos_api:
            for producto in productos_api:
                resultado["candidatos"].append(producto)

                # Verificar PLU
                if producto["plu"] and plu_ocr:
                    if str(producto["plu"]) == str(plu_ocr):
                        print(f"✅ ¡PLU COINCIDE via API!")
                        resultado["producto"] = producto
                        resultado["verificado"] = True
                        resultado["mensaje"] = (
                            "✅ Producto verificado via API - PLU coincide"
                        )
                        return resultado

            # Si no hubo match exacto pero hay candidatos
            if resultado["candidatos"]:
                resultado["producto"] = resultado["candidatos"][0]
                resultado["mensaje"] = (
                    f"⚠️ API encontró {len(resultado['candidatos'])} productos, PLU no verificado"
                )
                # No retornar aún - intentar verificar con scraping si es necesario

        # ============================================
        # PASO 4: Buscar URLs y scrapear (fallback)
        # ============================================
        if not resultado["verificado"]:
            print("\n🌐 Buscando URLs de productos (fallback)...")
            urls = await self.buscar_urls_por_nombre(nombre_busqueda, max_candidatos)

            if not urls and nombre_busqueda:
                # Reintentar con menos palabras
                palabras = nombre_busqueda.split()[:2]
                if len(palabras) >= 2:
                    nombre_corto = " ".join(palabras)
                    print(f"🔄 Reintentando con: '{nombre_corto}'")
                    urls = await self.buscar_urls_por_nombre(
                        nombre_corto, max_candidatos
                    )

            if urls:
                print(f"\n📦 Scrapeando {len(urls)} URLs...")

                for i, url in enumerate(urls, 1):
                    print(f"\n   [{i}/{len(urls)}] {url[:60]}...")

                    producto = await self.scrape_producto(url)

                    if producto:
                        # Evitar duplicados
                        ya_existe = any(
                            c.get("url") == producto["url"]
                            for c in resultado["candidatos"]
                        )

                        if not ya_existe:
                            resultado["candidatos"].append(producto)
                            print(f"   📦 {producto['nombre'][:40]}...")
                            print(f"   🏷️  PLU: {producto['plu']}")

                            # Verificar PLU
                            if producto["plu"] and plu_ocr:
                                if str(producto["plu"]) == str(plu_ocr):
                                    print(f"   ✅ ¡PLU COINCIDE!")
                                    resultado["producto"] = producto
                                    resultado["verificado"] = True
                                    resultado["mensaje"] = (
                                        "✅ Producto verificado via scraping - PLU coincide"
                                    )
                                    return resultado

                    if i < len(urls):
                        await asyncio.sleep(self.rate_limit_delay)

        # ============================================
        # RESULTADO FINAL
        # ============================================
        if not resultado["verificado"] and resultado["candidatos"]:
            resultado["producto"] = resultado["candidatos"][0]
            if not resultado["mensaje"]:
                resultado["mensaje"] = (
                    f"⚠️ {len(resultado['candidatos'])} productos encontrados, PLU no verificado"
                )
        elif not resultado["candidatos"]:
            resultado["mensaje"] = f"❌ No se encontraron productos para '{nombre_ocr}'"

        return resultado


# ============================================
# FUNCIONES PARA LECFAC
# ============================================


async def enriquecer_producto_ocr(
    nombre_ocr: str, plu_ocr: str = None, precio_ocr: int = None, ean_ocr: str = None
) -> Dict:
    """
    🎯 FUNCIÓN PRINCIPAL PARA LECFAC

    Parámetros:
    - nombre_ocr: Nombre del producto como aparece en el tiquete
    - plu_ocr: Código PLU del tiquete (si está disponible)
    - precio_ocr: Precio del tiquete en pesos colombianos
    - ean_ocr: Código de barras EAN si está disponible

    Retorna:
    - Dict con datos enriquecidos del producto
    """
    resultado = {
        "nombre_ocr": nombre_ocr,
        "nombre_completo": None,
        "plu_ocr": plu_ocr,
        "plu_web": None,
        "plu_verificado": False,
        "ean_ocr": ean_ocr,
        "ean_web": None,
        "precio_ocr": precio_ocr,
        "precio_web": None,
        "precio_lista_web": None,
        "enriquecido": False,
        "supermercado": "Carulla",
        "url": None,
        "candidatos": [],
        "mensaje": "",
    }

    if not nombre_ocr or len(nombre_ocr.strip()) < 3:
        resultado["mensaje"] = "❌ Nombre muy corto para buscar"
        return resultado

    scraper = CarullaScraper()
    busqueda = await scraper.buscar_y_verificar_plu(
        nombre_ocr=nombre_ocr, plu_ocr=plu_ocr or "", ean_ocr=ean_ocr
    )

    resultado["candidatos"] = busqueda["candidatos"]
    resultado["mensaje"] = busqueda["mensaje"]

    if busqueda["producto"]:
        producto = busqueda["producto"]
        resultado["nombre_completo"] = producto["nombre"]
        resultado["plu_web"] = producto["plu"]
        resultado["ean_web"] = producto.get("ean")
        resultado["precio_web"] = producto["precio"]
        resultado["precio_lista_web"] = producto.get("precio_lista")
        resultado["url"] = producto["url"]
        resultado["enriquecido"] = True
        resultado["plu_verificado"] = busqueda["verificado"]

        print(f"\n{'='*60}")
        print("📊 RESULTADO FINAL")
        print("=" * 60)
        print(f"📋 Tiquete:       '{nombre_ocr}'")
        print(f"📦 Web:           '{producto['nombre']}'")
        print(f"🏷️  PLU tiquete:   {plu_ocr or '(no disponible)'}")
        print(f"🏷️  PLU web:       {producto['plu']}")

        if producto.get("ean"):
            print(f"📊 EAN web:       {producto['ean']}")

        if busqueda["verificado"]:
            print(f"✅ PLU VERIFICADO")
        else:
            print(f"⚠️  NO VERIFICADO - PLUs diferentes")
            if plu_ocr and producto["plu"]:
                print(f"   (tiquete: {plu_ocr} vs web: {producto['plu']})")

        if precio_ocr or producto["precio"]:
            print(
                f"💰 Precio tiquete: ${precio_ocr:,}"
                if precio_ocr
                else "💰 Precio tiquete: (no disponible)"
            )
            print(
                f"💰 Precio web:     ${producto['precio']:,}"
                if producto["precio"]
                else "💰 Precio web: (no disponible)"
            )

            if precio_ocr and producto["precio"]:
                diff = producto["precio"] - precio_ocr
                if diff != 0:
                    print(
                        f"   Diferencia:     ${diff:+,} ({'+' if diff > 0 else ''}{diff*100/precio_ocr:.1f}%)"
                    )

        print(f"🔗 URL: {producto['url']}")

    return resultado


# ============================================
# PRUEBAS
# ============================================


async def main():
    print("=" * 70)
    print("🧪 PRUEBA: Scraper Carulla v6.0")
    print("   Con soporte para API VTEX + búsqueda por PLU/EAN")
    print("=" * 70)

    # Prueba 1: Producto conocido con PLU
    print("\n" + "=" * 70)
    print("📋 TEST 1: AREPA EXTRADELGA SARY | PLU: 237373")
    print("=" * 70)

    resultado = await enriquecer_producto_ocr(
        nombre_ocr="AREPA EXTRADELGA SARY", plu_ocr="237373", precio_ocr=7800
    )

    print("\n" + "=" * 70)
    if resultado["enriquecido"]:
        if resultado["plu_verificado"]:
            print("🎉 ÉXITO: Producto encontrado y PLU verificado")
        else:
            print("⚠️  PARCIAL: Producto encontrado pero PLU no coincide")
            if resultado["candidatos"]:
                print(f"   Candidatos encontrados: {len(resultado['candidatos'])}")
                for i, c in enumerate(resultado["candidatos"][:3], 1):
                    print(f"   {i}. PLU:{c.get('plu')} - {c.get('nombre', '')[:40]}...")
    else:
        print("❌ No se encontró el producto")
        print(f"   Mensaje: {resultado['mensaje']}")
    print("=" * 70)

    # Prueba 2: Solo nombre, sin PLU
    print("\n" + "=" * 70)
    print("📋 TEST 2: LECHE ALQUERIA DESLACTOSADA | Sin PLU")
    print("=" * 70)

    resultado2 = await enriquecer_producto_ocr(
        nombre_ocr="LCH ALQUER DESLACT", plu_ocr=None, precio_ocr=None
    )

    print(f"\n{'='*70}")
    print(
        f"Resultado: {'Encontrado' if resultado2['enriquecido'] else 'No encontrado'}"
    )
    if resultado2["candidatos"]:
        print(f"Candidatos: {len(resultado2['candidatos'])}")
        for i, c in enumerate(resultado2["candidatos"][:3], 1):
            print(
                f"   {i}. PLU:{c.get('plu')} ${c.get('precio', 0):,} - {c.get('nombre', '')[:40]}..."
            )
    print("=" * 70)

    # Retornar resultados para verificación
    return {"test1": resultado, "test2": resultado2}


# ============================================
# INTEGRACIÓN CON BASE DE DATOS
# ============================================


async def enriquecer_y_guardar(
    nombre_ocr: str,
    plu_ocr: str = None,
    precio_ocr: int = None,
    ean_ocr: str = None,
    guardar_candidatos: bool = True,
) -> Dict:
    """
    🎯 FUNCIÓN COMPLETA: Enriquece el producto Y lo guarda en la BD

    Esta función:
    1. Busca el producto en la web del supermercado
    2. Verifica el PLU
    3. Guarda el producto enriquecido en productos_web_enriched
    4. Opcionalmente guarda también los candidatos encontrados

    Uso:
        resultado = await enriquecer_y_guardar(
            nombre_ocr="AREPA EXTRADELGA SARY",
            plu_ocr="237373",
            precio_ocr=7800,
            guardar_candidatos=True
        )
    """
    # Primero enriquecer
    resultado = await enriquecer_producto_ocr(
        nombre_ocr=nombre_ocr, plu_ocr=plu_ocr, precio_ocr=precio_ocr, ean_ocr=ean_ocr
    )

    # Intentar guardar en BD
    try:
        from db_productos_enriched import (
            guardar_producto_enriched,
            guardar_candidatos_enriched,
            crear_tabla_enriched,
        )

        # Asegurar que la tabla existe
        crear_tabla_enriched()

        # Guardar producto principal si fue enriquecido
        if resultado["enriquecido"]:
            producto_id = guardar_producto_enriched(resultado)
            resultado["db_id"] = producto_id
            print(f"💾 Producto guardado en BD: ID={producto_id}")

        # Guardar candidatos si se solicita
        if guardar_candidatos and resultado.get("candidatos"):
            guardados = guardar_candidatos_enriched(
                resultado["candidatos"],
                supermercado=resultado.get("supermercado", "Carulla"),
            )
            resultado["candidatos_guardados"] = guardados

    except ImportError:
        print("⚠️ Módulo db_productos_enriched no disponible, no se guardará en BD")
    except Exception as e:
        print(f"❌ Error guardando en BD: {e}")

    return resultado


if __name__ == "__main__":
    asyncio.run(main())
