"""
Script de Corrección: Mover códigos EAN de subcategoria a codigo_ean
Corrige la estructura de productos_maestros moviendo EANs mal ubicados
"""

import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Cargar variables de entorno
load_dotenv()

def conectar_db():
    """Conectar a la base de datos de Railway usando DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL no encontrada en las variables de entorno")

    # Parsear la URL como en database.py
    url = urlparse(database_url)

    return psycopg2.connect(
        host=url.hostname,
        database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port or 5432,
        connect_timeout=10,
        sslmode='prefer'
    )

def obtener_productos_a_corregir(cursor):
    """Obtener todos los productos con EAN en subcategoria"""
    query = r"""
        SELECT
            id,
            nombre_normalizado,
            codigo_ean,
            subcategoria,
            categoria
        FROM productos_maestros
        WHERE codigo_ean IS NULL
        AND subcategoria ~ '^[0-9]+\|'
        ORDER BY id
    """

    cursor.execute(query)
    return cursor.fetchall()

def extraer_ean_y_establecimiento(subcategoria):
    """Extrae el EAN y el establecimiento de la subcategoria"""
    if not subcategoria or '|' not in subcategoria:
        return None, None

    partes = subcategoria.split('|')
    ean = partes[0].strip()
    establecimiento = partes[1].strip() if len(partes) > 1 else None

    return ean, establecimiento

def validar_ean(ean):
    """Valida que el EAN sea numérico y tenga longitud válida"""
    if not ean:
        return False

    if not ean.isdigit():
        return False

    # EAN puede ser 3+ dígitos (PLU) o 8, 12, 13, 14 dígitos (EAN-8, UPC, EAN-13, EAN-14)
    longitud = len(ean)
    if longitud >= 3 and longitud <= 14:
        return True

    return False

def preview_cambios(productos):
    """Muestra un preview de los cambios a realizar"""
    print("=" * 80)
    print("🔍 PREVIEW DE CAMBIOS")
    print("=" * 80)
    print()

    cambios_validos = []
    cambios_invalidos = []

    for row in productos:
        id_prod, nombre, codigo_ean_actual, subcategoria, categoria = row
        ean_extraido, establecimiento = extraer_ean_y_establecimiento(subcategoria)

        if validar_ean(ean_extraido):
            cambios_validos.append({
                'id': id_prod,
                'nombre': nombre,
                'ean': ean_extraido,
                'establecimiento': establecimiento,
                'subcategoria_original': subcategoria
            })
        else:
            cambios_invalidos.append({
                'id': id_prod,
                'nombre': nombre,
                'subcategoria': subcategoria,
                'razon': 'EAN inválido o no numérico'
            })

    # Mostrar cambios válidos
    if cambios_validos:
        print(f"✅ CAMBIOS VÁLIDOS A APLICAR ({len(cambios_validos)} productos):")
        print("-" * 80)

        for i, cambio in enumerate(cambios_validos[:10], 1):  # Mostrar primeros 10
            print(f"{i}. ID {cambio['id']}: {cambio['nombre']}")
            print(f"   ANTES:")
            print(f"     codigo_ean: NULL")
            print(f"     subcategoria: {cambio['subcategoria_original']}")
            print(f"   DESPUÉS:")
            print(f"     codigo_ean: {cambio['ean']}")
            print(f"     subcategoria: {cambio['establecimiento']}")
            print()

        if len(cambios_validos) > 10:
            print(f"   ... y {len(cambios_validos) - 10} productos más")
            print()

    # Mostrar cambios inválidos
    if cambios_invalidos:
        print(f"⚠️  PRODUCTOS QUE NO SE PUEDEN CORREGIR ({len(cambios_invalidos)} productos):")
        print("-" * 80)

        for cambio in cambios_invalidos:
            print(f"ID {cambio['id']}: {cambio['nombre']}")
            print(f"  Subcategoría: {cambio['subcategoria']}")
            print(f"  Razón: {cambio['razon']}")
            print()

    print("=" * 80)
    print("📊 RESUMEN")
    print("=" * 80)
    print(f"Total de productos a corregir: {len(cambios_validos)}")
    print(f"Productos con problemas: {len(cambios_invalidos)}")
    print()

    return cambios_validos, cambios_invalidos

def aplicar_correcciones(cursor, cambios_validos):
    """Aplica las correcciones a la base de datos"""
    print("=" * 80)
    print("🔧 APLICANDO CORRECCIONES")
    print("=" * 80)
    print()

    exitosos = 0
    fallidos = 0

    for cambio in cambios_validos:
        try:
            # UPDATE del producto
            cursor.execute("""
                UPDATE productos_maestros
                SET codigo_ean = %s,
                    subcategoria = %s,
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (cambio['ean'], cambio['establecimiento'], cambio['id']))

            exitosos += 1
            print(f"✅ ID {cambio['id']}: {cambio['nombre'][:50]}")

        except Exception as e:
            fallidos += 1
            print(f"❌ ID {cambio['id']}: Error - {e}")

    print()
    print("=" * 80)
    print("📊 RESULTADO")
    print("=" * 80)
    print(f"✅ Correcciones exitosas: {exitosos}")
    print(f"❌ Correcciones fallidas: {fallidos}")
    print()

    return exitosos, fallidos

def main():
    """Función principal"""
    print("=" * 80)
    print("🔧 CORRECCIÓN AUTOMÁTICA DE CÓDIGOS EAN")
    print("=" * 80)
    print()

    try:
        # Conectar a la base de datos
        print("📡 Conectando a la base de datos...")
        conn = conectar_db()
        cursor = conn.cursor()
        print("✅ Conexión establecida")
        print()

        # Obtener productos a corregir
        print("🔍 Buscando productos con EAN en subcategoria...")
        productos = obtener_productos_a_corregir(cursor)
        print(f"✅ Encontrados {len(productos)} productos")
        print()

        if not productos:
            print("✅ No hay productos para corregir")
            cursor.close()
            conn.close()
            return

        # Mostrar preview
        cambios_validos, cambios_invalidos = preview_cambios(productos)

        if not cambios_validos:
            print("❌ No hay cambios válidos para aplicar")
            cursor.close()
            conn.close()
            return

        # Pedir confirmación
        print("=" * 80)
        print("⚠️  ADVERTENCIA")
        print("=" * 80)
        print("Esta operación modificará la base de datos de producción.")
        print(f"Se actualizarán {len(cambios_validos)} productos.")
        print()

        respuesta = input("¿Deseas continuar? (escribe 'SI' para confirmar): ").strip().upper()

        if respuesta != 'SI':
            print()
            print("❌ Operación cancelada por el usuario")
            cursor.close()
            conn.close()
            return

        print()

        # Aplicar correcciones
        exitosos, fallidos = aplicar_correcciones(cursor, cambios_validos)

        # Commit de los cambios
        if exitosos > 0:
            print("💾 Guardando cambios en la base de datos...")
            conn.commit()
            print("✅ Cambios guardados correctamente")
        else:
            print("⚠️  No se realizaron cambios")
            conn.rollback()

        print()
        print("=" * 80)
        print("✅ PROCESO COMPLETADO")
        print("=" * 80)
        print(f"Total de productos corregidos: {exitosos}")
        print()

        # Verificación final
        print("🔍 Verificación final...")
        cursor.execute(r"""
            SELECT COUNT(*)
            FROM productos_maestros
            WHERE codigo_ean IS NULL
            AND subcategoria ~ '^[0-9]+\|'
        """)
        productos_pendientes = cursor.fetchone()[0]

        print(f"Productos que aún tienen EAN en subcategoria: {productos_pendientes}")

        if productos_pendientes == 0:
            print("✅ Todos los productos fueron corregidos exitosamente")
        else:
            print(f"⚠️  Quedan {productos_pendientes} productos por revisar manualmente")

        print()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
