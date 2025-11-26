"""
Integración Scraper Carulla <> LecFac
Enriquece productos automáticamente cuando se detectan nuevos PLUs
Versión: 1.1 - Import opcional de scraper
"""

import asyncio
from typing import Optional, Dict
import logging

# Import opcional del scraper - no falla si falta playwright
try:
    from carulla_scraper import CarullaScraper

    SCRAPER_AVAILABLE = True
except ImportError:
    CarullaScraper = None
    SCRAPER_AVAILABLE = False
    print("⚠️ CarullaScraper no disponible - playwright no instalado")
    print("   El servidor funcionará sin enriquecimiento externo")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProductEnricher:
    """
    Enriquece productos de LecFac con datos scrapeados
    """

    def __init__(self):
        if SCRAPER_AVAILABLE:
            self.scraper = CarullaScraper()
        else:
            self.scraper = None
        self.cache = {}  # Cache simple en memoria

    async def enriquecer_por_plu(
        self, plu: str, supermercado: str = "Carulla"
    ) -> Optional[Dict]:
        """
        Busca información de un producto por PLU
        """
        if not SCRAPER_AVAILABLE:
            logger.warning("⚠️ Scraper no disponible - saltando enriquecimiento")
            return None

        cache_key = f"{supermercado}_{plu}"

        # Revisar cache
        if cache_key in self.cache:
            logger.info(f"📦 Producto {plu} encontrado en cache")
            return self.cache[cache_key]

        try:
            logger.info(f"🔍 Buscando PLU {plu} en {supermercado}...")

            productos = await self.scraper.scrape_busqueda(
                f"PLU {plu}", max_productos=5
            )

            for producto in productos:
                if producto["plu"] == plu:
                    self.cache[cache_key] = producto
                    logger.info(f"✅ Producto encontrado: {producto['nombre']}")
                    return producto

            logger.warning(f"⚠️ PLU {plu} no encontrado en scraping")
            return None

        except Exception as e:
            logger.error(f"❌ Error enriqueciendo PLU {plu}: {str(e)}")
            return None

    async def enriquecer_por_nombre(
        self, nombre: str, supermercado: str = "Carulla"
    ) -> Optional[Dict]:
        """
        Busca información de un producto por nombre
        """
        if not SCRAPER_AVAILABLE:
            logger.warning("⚠️ Scraper no disponible - saltando enriquecimiento")
            return None

        try:
            logger.info(f"🔍 Buscando '{nombre}' en {supermercado}...")

            productos = await self.scraper.scrape_busqueda(nombre, max_productos=3)

            if not productos:
                logger.warning(f"⚠️ No se encontraron resultados para '{nombre}'")
                return None

            mejor_match = productos[0]
            logger.info(f"✅ Mejor match: {mejor_match['nombre']}")

            cache_key = f"{supermercado}_{mejor_match['plu']}"
            self.cache[cache_key] = mejor_match

            return mejor_match

        except Exception as e:
            logger.error(f"❌ Error buscando '{nombre}': {str(e)}")
            return None

    async def enriquecer_producto_lecfac(self, producto_lecfac: Dict) -> Dict:
        """
        Enriquece un producto de LecFac con datos scrapeados
        """
        resultado = producto_lecfac.copy()

        if not SCRAPER_AVAILABLE:
            resultado["fuente_enriquecimiento"] = "scraper_no_disponible"
            resultado["confianza"] = "baja"
            return resultado

        # Intentar por PLU primero (más preciso)
        if "plu" in producto_lecfac and producto_lecfac["plu"]:
            datos_scrapeados = await self.enriquecer_por_plu(
                producto_lecfac["plu"], producto_lecfac.get("supermercado", "Carulla")
            )

            if datos_scrapeados:
                resultado["nombre_completo"] = datos_scrapeados["nombre"]
                resultado["nombre_original_ocr"] = producto_lecfac["nombre"]
                resultado["fuente_enriquecimiento"] = "scraping_carulla"
                resultado["confianza"] = "alta"

                logger.info(
                    f"✨ Enriquecido: '{producto_lecfac['nombre']}' -> '{datos_scrapeados['nombre']}'"
                )
                return resultado

        # Si no hay PLU o no se encontró, intentar por nombre
        if "nombre" in producto_lecfac and producto_lecfac["nombre"]:
            datos_scrapeados = await self.enriquecer_por_nombre(
                producto_lecfac["nombre"],
                producto_lecfac.get("supermercado", "Carulla"),
            )

            if datos_scrapeados:
                resultado["nombre_completo"] = datos_scrapeados["nombre"]
                resultado["nombre_original_ocr"] = producto_lecfac["nombre"]
                resultado["plu_sugerido"] = datos_scrapeados["plu"]
                resultado["fuente_enriquecimiento"] = "scraping_carulla"
                resultado["confianza"] = "media"

                logger.info(
                    f"✨ Enriquecido por nombre: '{producto_lecfac['nombre']}' -> '{datos_scrapeados['nombre']}'"
                )
                return resultado

        # No se pudo enriquecer
        logger.warning(
            f"⚠️ No se pudo enriquecer: {producto_lecfac.get('nombre', 'SIN NOMBRE')}"
        )
        resultado["fuente_enriquecimiento"] = "sin_enriquecer"
        resultado["confianza"] = "baja"

        return resultado


# === EJEMPLOS DE USO ===


async def ejemplo_enriquecer_lote():
    """
    Simula el enriquecimiento de productos de una factura completa
    """
    if not SCRAPER_AVAILABLE:
        print("⚠️ Scraper no disponible - ejemplo no puede ejecutarse")
        return []

    enricher = ProductEnricher()

    productos_ocr = [
        {
            "nombre": "MOZARELL FINESSE",
            "plu": "426036",
            "precio": 26100,
            "supermercado": "Carulla",
        },
        {
            "nombre": "QUESO ALPINA",
            "plu": "350092",
            "precio": 23700,
            "supermercado": "Carulla",
        },
        {
            "nombre": "LECHE ENTERA",
            "plu": None,
            "precio": 4500,
            "supermercado": "Carulla",
        },
    ]

    print("=" * 70)
    print("ENRIQUECIENDO PRODUCTOS DE FACTURA")
    print("=" * 70)

    productos_enriquecidos = []

    for i, producto in enumerate(productos_ocr, 1):
        print(f"\n{i}. Procesando: {producto['nombre']}")
        print("-" * 70)

        producto_enriquecido = await enricher.enriquecer_producto_lecfac(producto)
        productos_enriquecidos.append(producto_enriquecido)

        print(f"   OCR Original: {producto['nombre']}")
        print(
            f"   Nombre Completo: {producto_enriquecido.get('nombre_completo', 'N/A')}"
        )
        print(f"   Confianza: {producto_enriquecido.get('confianza', 'N/A')}")

        await asyncio.sleep(3)

    print("\n" + "=" * 70)
    print(
        f"TOTAL ENRIQUECIDOS: {len([p for p in productos_enriquecidos if p.get('nombre_completo')])}/{len(productos_ocr)}"
    )
    print("=" * 70)

    return productos_enriquecidos


async def ejemplo_individual():
    """
    Enriquece un solo producto
    """
    if not SCRAPER_AVAILABLE:
        print("⚠️ Scraper no disponible - ejemplo no puede ejecutarse")
        return None

    enricher = ProductEnricher()

    producto = {
        "nombre": "MOZARELL FINESSE",
        "plu": "426036",
        "precio": 26100,
        "supermercado": "Carulla",
    }

    print("\n🔍 ENRIQUECIENDO PRODUCTO INDIVIDUAL:")
    print("=" * 60)
    print(f"Entrada: {producto}")

    resultado = await enricher.enriquecer_producto_lecfac(producto)

    print("\n✨ Resultado:")
    print(f"Nombre Original: {resultado.get('nombre_original_ocr', 'N/A')}")
    print(f"Nombre Completo: {resultado.get('nombre_completo', 'N/A')}")
    print(f"Confianza: {resultado.get('confianza', 'N/A')}")

    return resultado


if __name__ == "__main__":
    asyncio.run(ejemplo_enriquecer_lote())
