"""
============================================================================
TEST PERPLEXITY INTEGRATION
Script de pruebas para validar integración con Perplexity
============================================================================

Este script prueba:
1. Conexión con Perplexity API
2. Validación de nombres de productos
3. Diferentes casos de uso (errores OCR, productos correctos, PLUs)

REQUISITOS:
- Variable 'lefact' configurada en entorno
- Conexión a internet

USO:
    python test_perplexity_integration.py
============================================================================
"""

import os
import sys
import json
from datetime import datetime


def test_configuracion():
    """Test 1: Verificar configuración"""
    print("\n" + "="*80)
    print("TEST 1: VERIFICACIÓN DE CONFIGURACIÓN")
    print("="*80)

    api_key = os.environ.get("lefact", "").strip()

    if not api_key:
        print("❌ ERROR: Variable 'lefact' no configurada")
        print("\n📋 SOLUCIÓN:")
        print("   En Railway, ve a Variables → Agregar variable:")
        print("   Nombre: lefact")
        print("   Valor: tu_api_key_de_perplexity")
        return False

    print(f"✅ Variable 'lefact' configurada")
    print(f"   Longitud: {len(api_key)} caracteres")
    print(f"   Prefijo: {api_key[:10]}...")

    return True


def test_importacion():
    """Test 2: Importar módulo de Perplexity"""
    print("\n" + "="*80)
    print("TEST 2: IMPORTACIÓN DE MÓDULOS")
    print("="*80)

    try:
        from perplexity_validator import validar_nombre_producto
        print("✅ perplexity_validator importado correctamente")
        return True
    except ImportError as e:
        print(f"❌ Error importando perplexity_validator: {e}")
        return False


def test_validacion_basica():
    """Test 3: Validación básica con Perplexity"""
    print("\n" + "="*80)
    print("TEST 3: VALIDACIÓN BÁSICA")
    print("="*80)

    try:
        from perplexity_validator import validar_nombre_producto

        # Caso simple: error OCR típico
        print("\n📝 Probando: QSO BLANCO en OLÍMPICA")
        resultado = validar_nombre_producto(
            nombre_ocr="QSO BLANCO",
            precio=8600,
            supermercado="OLIMPICA"
        )

        print(f"\n📊 RESULTADO:")
        print(json.dumps(resultado, indent=2, ensure_ascii=False))

        if resultado['fuente'] == 'perplexity':
            print("\n✅ Validación exitosa con Perplexity")
            return True
        else:
            print(f"\n⚠️  Usó fallback: {resultado.get('error', 'Sin error')}")
            return False

    except Exception as e:
        print(f"\n❌ Error en validación: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_casos_reales():
    """Test 4: Casos reales de facturas"""
    print("\n" + "="*80)
    print("TEST 4: CASOS REALES DE FACTURAS")
    print("="*80)

    try:
        from perplexity_validator import validar_nombre_producto

        casos_prueba = [
            {
                "nombre": "CREMA VECHE",
                "precio": 5240,
                "supermercado": "EXITO",
                "descripcion": "Error OCR típico en lácteos"
            },
            {
                "nombre": "ARROZ DIANA",
                "precio": 4500,
                "supermercado": "JUMBO",
                "codigo": "7702001023456",
                "descripcion": "Producto con nombre correcto y EAN"
            },
            {
                "nombre": "MANGO",
                "precio": 6280,
                "supermercado": "EXITO",
                "codigo": "1220",
                "descripcion": "Producto fresco con PLU"
            },
            {
                "nombre": "PONQ ARE",
                "precio": 14800,
                "supermercado": "OLIMPICA",
                "descripcion": "Nombre truncado por OCR"
            }
        ]

        resultados = []
        exitosos = 0

        for i, caso in enumerate(casos_prueba, 1):
            print(f"\n{'─'*70}")
            print(f"CASO {i}: {caso['descripcion']}")
            print(f"{'─'*70}")
            print(f"📝 Nombre OCR: {caso['nombre']}")
            print(f"💰 Precio: ${caso['precio']:,}")
            print(f"🏪 Supermercado: {caso['supermercado']}")

            resultado = validar_nombre_producto(
                nombre_ocr=caso['nombre'],
                precio=caso['precio'],
                supermercado=caso['supermercado'],
                codigo=caso.get('codigo', '')
            )

            resultados.append({
                'caso': i,
                'input': caso['nombre'],
                'output': resultado['nombre_validado'],
                'fuente': resultado['fuente'],
                'confianza': resultado.get('confianza', 'N/A')
            })

            if resultado['fuente'] == 'perplexity':
                exitosos += 1
                print(f"✅ Validado: {resultado['nombre_validado']}")
            else:
                print(f"⚠️  Fallback: {resultado['nombre_validado']}")

        # Resumen
        print(f"\n{'='*80}")
        print(f"📊 RESUMEN DE PRUEBAS")
        print(f"{'='*80}")
        print(f"Total de casos: {len(casos_prueba)}")
        print(f"✅ Validados con Perplexity: {exitosos}")
        print(f"⚠️  Fallback a OCR: {len(casos_prueba) - exitosos}")
        print(f"\nTasa de éxito: {(exitosos/len(casos_prueba)*100):.1f}%")

        # Tabla de resultados
        print(f"\n{'='*80}")
        print(f"TABLA DE RESULTADOS")
        print(f"{'='*80}")
        print(f"{'Caso':<6} {'Input':<20} {'Output':<30} {'Fuente':<15} {'Conf':<8}")
        print(f"{'-'*6} {'-'*20} {'-'*30} {'-'*15} {'-'*8}")

        for r in resultados:
            print(f"{r['caso']:<6} {r['input'][:20]:<20} {r['output'][:30]:<30} {r['fuente']:<15} {r['confianza']:<8}")

        print(f"{'='*80}\n")

        return exitosos > 0

    except Exception as e:
        print(f"\n❌ Error en casos reales: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_product_matcher_integration():
    """Test 5: Integración con product_matcher"""
    print("\n" + "="*80)
    print("TEST 5: INTEGRACIÓN CON PRODUCT_MATCHER")
    print("="*80)

    try:
        from product_matcher import buscar_o_crear_producto_inteligente
        print("✅ product_matcher importado correctamente")

        # Verificar que tiene la integración
        import inspect
        source = inspect.getsource(buscar_o_crear_producto_inteligente)

        if "validar_nombre_producto" in source:
            print("✅ product_matcher tiene integración con Perplexity")
            return True
        else:
            print("⚠️  product_matcher NO tiene integración con Perplexity")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Ejecutar todos los tests"""
    print("\n" + "="*80)
    print("🧪 SUITE DE PRUEBAS - INTEGRACIÓN PERPLEXITY")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python: {sys.version.split()[0]}")
    print("="*80)

    tests = [
        ("Configuración", test_configuracion),
        ("Importación", test_importacion),
        ("Validación Básica", test_validacion_basica),
        ("Casos Reales", test_casos_reales),
        ("Integración product_matcher", test_product_matcher_integration)
    ]

    resultados = []

    for nombre, test_func in tests:
        try:
            resultado = test_func()
            resultados.append((nombre, resultado))
        except Exception as e:
            print(f"\n❌ Error ejecutando test '{nombre}': {e}")
            resultados.append((nombre, False))

    # Resumen final
    print("\n" + "="*80)
    print("📊 RESUMEN FINAL")
    print("="*80)

    total = len(resultados)
    exitosos = sum(1 for _, r in resultados if r)
    fallidos = total - exitosos

    for nombre, resultado in resultados:
        icono = "✅" if resultado else "❌"
        print(f"{icono} {nombre}")

    print(f"\n{'─'*80}")
    print(f"Total: {total} tests")
    print(f"✅ Exitosos: {exitosos}")
    print(f"❌ Fallidos: {fallidos}")
    print(f"📊 Tasa de éxito: {(exitosos/total*100):.1f}%")
    print(f"{'─'*80}")

    if exitosos == total:
        print("\n🎉 ¡TODOS LOS TESTS PASARON!")
        print("✅ Sistema listo para deployment a Railway")
    elif exitosos > 0:
        print("\n⚠️  ALGUNOS TESTS FALLARON")
        print("📝 Revisa los errores arriba antes de deployar")
    else:
        print("\n❌ TODOS LOS TESTS FALLARON")
        print("🔧 Revisa la configuración antes de continuar")

    print("="*80 + "\n")

    return exitosos == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
