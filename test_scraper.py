"""
Test Rápido - Scraper Carulla
Verifica que todo funcione correctamente
"""

import asyncio
import sys


def check_playwright():
    """Verificar que Playwright esté instalado"""
    try:
        from playwright.async_api import async_playwright

        print("✅ Playwright instalado correctamente")
        return True
    except ImportError:
        print("❌ Playwright NO instalado")
        print("   Ejecuta: pip install playwright --break-system-packages")
        print("   Luego: playwright install chromium")
        return False


async def test_scraping_simple():
    """Test básico de scraping"""
    print("\n" + "=" * 60)
    print("TEST 1: Scraping Simple")
    print("=" * 60)

    try:
        from carulla_scraper import CarullaScraper

        scraper = CarullaScraper()

        # URL de prueba (producto real)
        url_test = (
            "https://www.carulla.com/queso-mozarella-x-25-tajadas-417-gr-268748/p"
        )

        print(f"\n🔍 Scrapeando: {url_test}")
        print("⏳ Esto puede tomar 5-10 segundos...")

        producto = await scraper.scrape_producto(url_test)

        if producto:
            print("\n✅ SCRAPING EXITOSO!")
            print(f"📦 Nombre: {producto['nombre']}")
            print(f"🏷️  PLU: {producto['plu']}")
            print(f"💰 Precio: ${producto['precio']:,}")
            print(f"🏪 Supermercado: {producto['supermercado']}")
            return True
        else:
            print("\n❌ No se pudo extraer el producto")
            print("   Posibles causas:")
            print("   - Página cambió su estructura")
            print("   - Conexión a internet lenta")
            print("   - Carulla bloqueó la IP")
            return False

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        print("\nStack trace completo:")
        import traceback

        traceback.print_exc()
        return False


async def test_busqueda():
    """Test de búsqueda de productos"""
    print("\n" + "=" * 60)
    print("TEST 2: Búsqueda de Productos")
    print("=" * 60)

    try:
        from carulla_scraper import buscar_productos

        termino = "leche"
        print(f"\n🔍 Buscando: '{termino}'")
        print("⏳ Esto puede tomar 20-30 segundos...")

        productos = await buscar_productos(termino, max_productos=2)

        if productos:
            print(f"\n✅ BÚSQUEDA EXITOSA! ({len(productos)} productos)")
            for i, p in enumerate(productos, 1):
                print(f"\n{i}. {p['nombre']}")
                print(f"   PLU: {p['plu']} | Precio: ${p['precio']:,}")
            return True
        else:
            print("\n⚠️ No se encontraron productos")
            return False

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        return False


async def test_enricher():
    """Test de enriquecimiento de productos"""
    print("\n" + "=" * 60)
    print("TEST 3: Enriquecimiento de Productos")
    print("=" * 60)

    try:
        from lecfac_enricher import ProductEnricher

        enricher = ProductEnricher()

        # Producto simulado del OCR
        producto_ocr = {
            "nombre": "QUESO ALPIN",  # Nombre incompleto del OCR
            "plu": "350092",
            "precio": 23700,
            "supermercado": "Carulla",
        }

        print(f"\n📋 Producto OCR: {producto_ocr['nombre']}")
        print("⏳ Enriqueciendo...")

        resultado = await enricher.enriquecer_producto_lecfac(producto_ocr)

        if resultado.get("nombre_completo"):
            print("\n✅ ENRIQUECIMIENTO EXITOSO!")
            print(f"📝 Nombre Original: {resultado['nombre_original_ocr']}")
            print(f"📦 Nombre Completo: {resultado['nombre_completo']}")
            print(f"🎯 Confianza: {resultado['confianza']}")
            return True
        else:
            print("\n⚠️ No se pudo enriquecer")
            print(f"   Resultado: {resultado}")
            return False

    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback

        traceback.print_exc()
        return False


async def run_all_tests():
    """Ejecutar todos los tests"""
    print("\n" + "🧪" * 30)
    print("SUITE DE TESTS - SCRAPER CARULLA")
    print("🧪" * 30)

    # Check Playwright
    if not check_playwright():
        print("\n❌ Tests cancelados - Instala Playwright primero")
        sys.exit(1)

    # Ejecutar tests
    resultados = []

    # Test 1: Scraping simple
    resultado1 = await test_scraping_simple()
    resultados.append(("Scraping Simple", resultado1))

    if not resultado1:
        print("\n⚠️ Test 1 falló - saltando tests siguientes")
        print_resultados(resultados)
        return

    # Test 2: Búsqueda (opcional, toma más tiempo)
    print("\n¿Ejecutar Test 2 (Búsqueda)? Toma ~30 segundos")
    # resultado2 = await test_busqueda()
    # resultados.append(("Búsqueda", resultado2))

    # Test 3: Enricher
    resultado3 = await test_enricher()
    resultados.append(("Enriquecimiento", resultado3))

    # Resumen
    print_resultados(resultados)


def print_resultados(resultados):
    """Imprimir resumen de resultados"""
    print("\n" + "=" * 60)
    print("RESUMEN DE TESTS")
    print("=" * 60)

    exitosos = sum(1 for _, r in resultados if r)
    total = len(resultados)

    for nombre, resultado in resultados:
        status = "✅ PASS" if resultado else "❌ FAIL"
        print(f"{status} - {nombre}")

    print("\n" + "-" * 60)
    print(f"Total: {exitosos}/{total} tests exitosos")

    if exitosos == total:
        print("🎉 ¡Todos los tests pasaron!")
        print("\n📋 PRÓXIMOS PASOS:")
        print("   1. Revisar GUIA_SCRAPING.md")
        print("   2. Integrar con tu sistema LecFac")
        print("   3. Implementar rate limiting y cache")
    else:
        print("\n⚠️ Algunos tests fallaron")
        print("   Revisa los errores arriba y la guía")


if __name__ == "__main__":
    print("\n🚀 Iniciando tests del scraper...")
    asyncio.run(run_all_tests())
