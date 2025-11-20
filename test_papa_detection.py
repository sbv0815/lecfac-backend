"""
test_papa_detection.py - Script para probar detección automática de PAPAS
========================================================================

Este script prueba la nueva funcionalidad de detección de PAPAS sin necesidad
de escanear facturas reales.

IMPORTANTE: Este script debe ejecutarse en producción (Railway) o con
acceso a la base de datos PostgreSQL donde están los PAPAS.
"""

import sys
import os

# FORZAR PostgreSQL
os.environ["DATABASE_TYPE"] = "postgresql"

# Agregar el directorio del backend al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db_connection


def test_buscar_papa_por_ean():
    """
    Test 1: Buscar PAPA por EAN (JUMBO)
    """
    print("\n" + "=" * 80)
    print("TEST 1: BUSCAR PAPA POR EAN (JUMBO)")
    print("=" * 80)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Buscar un PAPA existente con EAN
    cursor.execute(
        """
        SELECT id, codigo_ean, nombre_consolidado, marca
        FROM productos_maestros_v2
        WHERE es_producto_papa = TRUE
          AND codigo_ean IS NOT NULL
        LIMIT 1
    """
    )

    papa = cursor.fetchone()

    if not papa:
        print("❌ No hay PAPAS con EAN en la base de datos")
        print("   Primero marca un producto JUMBO como PAPA desde papa-dashboard.html")
        conn.close()
        return False

    papa_id = papa[0]
    codigo_ean = papa[1]
    nombre_papa = papa[2]
    marca_papa = papa[3]

    print(f"✅ PAPA encontrado:")
    print(f"   ID: {papa_id}")
    print(f"   EAN: {codigo_ean}")
    print(f"   Nombre: {nombre_papa}")
    print(f"   Marca: {marca_papa}")

    # Simular búsqueda
    print(f"\n🔍 Simulando búsqueda con EAN: {codigo_ean}")

    try:
        from product_matcher import buscar_papa_primero

        resultado = buscar_papa_primero(
            codigo=codigo_ean,
            establecimiento_id=3,  # JUMBO = ID 3
            cursor=cursor,
            conn=conn,
            precio=10000,
        )

        if resultado:
            print(f"✅ PAPA ENCONTRADO:")
            print(f"   Papa ID: {resultado['papa_id']}")
            print(f"   Nombre: {resultado['nombre_consolidado']}")
            print(f"   Marca: {resultado.get('marca', 'N/A')}")
            print(f"   Fuente: {resultado['fuente']}")
            return True
        else:
            print(f"❌ No se encontró PAPA (esto no debería pasar)")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        conn.close()


def test_buscar_papa_por_plu():
    """
    Test 2: Buscar PAPA por PLU (OLÍMPICA)
    """
    print("\n" + "=" * 80)
    print("TEST 2: BUSCAR PAPA POR PLU (OLÍMPICA)")
    print("=" * 80)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Buscar un PAPA existente con PLU
    cursor.execute(
        """
        SELECT pm.id, pm.nombre_consolidado, pm.marca, ppe.codigo_plu, ppe.establecimiento_id, e.nombre_normalizado
        FROM productos_maestros_v2 pm
        JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
        JOIN establecimientos e ON ppe.establecimiento_id = e.id
        WHERE pm.es_producto_papa = TRUE
          AND ppe.codigo_plu IS NOT NULL
        LIMIT 1
    """
    )

    papa = cursor.fetchone()

    if not papa:
        print("❌ No hay PAPAS con PLU en la base de datos")
        print(
            "   Primero marca un producto OLÍMPICA como PAPA desde papa-dashboard.html"
        )
        conn.close()
        return False

    papa_id = papa[0]
    nombre_papa = papa[1]
    marca_papa = papa[2]
    codigo_plu = papa[3]
    establecimiento_id = papa[4]
    establecimiento_nombre = papa[5]

    print(f"✅ PAPA encontrado:")
    print(f"   ID: {papa_id}")
    print(f"   Nombre: {nombre_papa}")
    print(f"   Marca: {marca_papa}")
    print(f"   PLU: {codigo_plu}")
    print(f"   Establecimiento: {establecimiento_nombre}")

    # Simular búsqueda
    print(f"\n🔍 Simulando búsqueda con PLU: {codigo_plu}")

    try:
        from product_matcher import buscar_papa_primero

        resultado = buscar_papa_primero(
            codigo=codigo_plu,
            establecimiento_id=establecimiento_id,
            cursor=cursor,
            conn=conn,
            precio=5000,
        )

        if resultado:
            print(f"✅ PAPA ENCONTRADO:")
            print(f"   Papa ID: {resultado['papa_id']}")
            print(f"   Nombre: {resultado['nombre_consolidado']}")
            print(f"   Marca: {resultado.get('marca', 'N/A')}")
            print(f"   PLU: {resultado.get('codigo_plu', 'N/A')}")
            print(f"   Fuente: {resultado['fuente']}")
            return True
        else:
            print(f"❌ No se encontró PAPA (esto no debería pasar)")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        conn.close()


def test_producto_sin_papa():
    """
    Test 3: Producto sin PAPA (debe seguir flujo normal)
    """
    print("\n" + "=" * 80)
    print("TEST 3: PRODUCTO SIN PAPA (flujo normal)")
    print("=" * 80)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Usar un código que NO existe como PAPA
    codigo_fake = "9999999999999"

    print(f"🔍 Buscando PAPA con código falso: {codigo_fake}")

    try:
        from product_matcher import buscar_papa_primero

        resultado = buscar_papa_primero(
            codigo=codigo_fake,
            establecimiento_id=3,
            cursor=cursor,
            conn=conn,
            precio=10000,
        )

        if resultado is None:
            print(f"✅ Correcto: No se encontró PAPA (como esperado)")
            print(f"   El sistema continuará con búsqueda normal")
            return True
        else:
            print(f"❌ Error: Se encontró un PAPA con código falso (no debería pasar)")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        conn.close()


def test_estadisticas_papa():
    """
    Test 4: Verificar que se actualizan estadísticas del PAPA
    """
    print("\n" + "=" * 80)
    print("TEST 4: ACTUALIZACIÓN DE ESTADÍSTICAS")
    print("=" * 80)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Buscar un PAPA cualquiera
    cursor.execute(
        """
        SELECT id, codigo_ean, nombre_consolidado, veces_visto
        FROM productos_maestros_v2
        WHERE es_producto_papa = TRUE
        LIMIT 1
    """
    )

    papa = cursor.fetchone()

    if not papa:
        print("❌ No hay PAPAS en la base de datos")
        conn.close()
        return False

    papa_id = papa[0]
    codigo_ean = papa[1]
    nombre_papa = papa[2]
    veces_visto_antes = papa[3]

    print(f"✅ PAPA seleccionado:")
    print(f"   ID: {papa_id}")
    print(f"   Nombre: {nombre_papa}")
    print(f"   Veces visto ANTES: {veces_visto_antes}")

    # Simular búsqueda (esto debería incrementar veces_visto)
    print(f"\n🔄 Simulando búsqueda...")

    try:
        from product_matcher import buscar_papa_primero

        resultado = buscar_papa_primero(
            codigo=codigo_ean,
            establecimiento_id=3,
            cursor=cursor,
            conn=conn,
            precio=10000,
        )

        # Verificar que se actualizó
        cursor.execute(
            """
            SELECT veces_visto, fecha_ultima_actualizacion
            FROM productos_maestros_v2
            WHERE id = %s
        """,
            (papa_id,),
        )

        nuevo_registro = cursor.fetchone()
        veces_visto_despues = nuevo_registro[0]
        fecha_actualizacion = nuevo_registro[1]

        print(f"\n📊 RESULTADOS:")
        print(f"   Veces visto DESPUÉS: {veces_visto_despues}")
        print(f"   Fecha actualización: {fecha_actualizacion}")

        if veces_visto_despues == veces_visto_antes + 1:
            print(f"✅ ¡Estadísticas actualizadas correctamente!")
            return True
        else:
            print(f"❌ Error: veces_visto no se incrementó")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        conn.close()


def main():
    """
    Ejecutar todos los tests
    """
    print("\n" + "=" * 80)
    print("🧪 TESTING: DETECCIÓN AUTOMÁTICA DE PAPAS")
    print("=" * 80)

    resultados = []

    # Test 1: EAN
    try:
        resultado1 = test_buscar_papa_por_ean()
        resultados.append(("Búsqueda por EAN", resultado1))
    except Exception as e:
        print(f"❌ Test 1 falló: {e}")
        resultados.append(("Búsqueda por EAN", False))

    # Test 2: PLU
    try:
        resultado2 = test_buscar_papa_por_plu()
        resultados.append(("Búsqueda por PLU", resultado2))
    except Exception as e:
        print(f"❌ Test 2 falló: {e}")
        resultados.append(("Búsqueda por PLU", False))

    # Test 3: Sin PAPA
    try:
        resultado3 = test_producto_sin_papa()
        resultados.append(("Producto sin PAPA", resultado3))
    except Exception as e:
        print(f"❌ Test 3 falló: {e}")
        resultados.append(("Producto sin PAPA", False))

    # Test 4: Estadísticas
    try:
        resultado4 = test_estadisticas_papa()
        resultados.append(("Actualización estadísticas", resultado4))
    except Exception as e:
        print(f"❌ Test 4 falló: {e}")
        resultados.append(("Actualización estadísticas", False))

    # Resumen
    print("\n" + "=" * 80)
    print("📊 RESUMEN DE TESTS")
    print("=" * 80)

    total = len(resultados)
    exitosos = sum(1 for _, resultado in resultados if resultado)

    for nombre, resultado in resultados:
        emoji = "✅" if resultado else "❌"
        print(f"{emoji} {nombre}")

    print(f"\n{'=' * 80}")
    print(f"TOTAL: {exitosos}/{total} tests exitosos")
    print(f"{'=' * 80}\n")

    if exitosos == total:
        print("🎉 ¡TODOS LOS TESTS PASARON!")
        return True
    else:
        print("⚠️  Algunos tests fallaron. Revisar logs arriba.")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⏹️  Tests interrumpidos por el usuario")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Error fatal: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
