"""
Script de Diagnóstico: Productos con EAN en subcategoria
Analiza la tabla productos_maestros para identificar códigos EAN mal ubicados
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

def diagnosticar_ean_en_subcategoria():
    """Diagnosticar productos con EAN en subcategoria"""

    print("=" * 80)
    print("🔍 DIAGNÓSTICO: Códigos EAN en subcategoria")
    print("=" * 80)
    print()

    conn = conectar_db()
    cursor = conn.cursor()

    # 1. Total de productos
    cursor.execute("SELECT COUNT(*) FROM productos_maestros")
    total_productos = cursor.fetchone()[0]
    print(f"📊 Total de productos en productos_maestros: {total_productos}")
    print()

    # 2. Productos con codigo_ean NULL
    cursor.execute("SELECT COUNT(*) FROM productos_maestros WHERE codigo_ean IS NULL")
    sin_codigo = cursor.fetchone()[0]
    print(f"⚠️  Productos sin codigo_ean: {sin_codigo} ({sin_codigo*100/total_productos:.1f}%)")
    print()

    # 3. Productos con subcategoria que contiene '|'
    cursor.execute("""
        SELECT COUNT(*)
        FROM productos_maestros
        WHERE subcategoria LIKE '%|%'
    """)
    con_pipe = cursor.fetchone()[0]
    print(f"🔍 Productos con '|' en subcategoria: {con_pipe}")
    print()

    # 4. Productos con EAN en subcategoria (patrón: números|texto)
    query_problema = r"""
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
        LIMIT 20
    """

    cursor.execute(query_problema)
    productos_problema = cursor.fetchall()

    print(f"🎯 Productos con EAN en subcategoria (primeros 20):")
    print("-" * 80)

    if productos_problema:
        for row in productos_problema:
            id_prod, nombre_normalizado, codigo_ean, subcategoria, categoria = row
            # Extraer el posible EAN
            if subcategoria and '|' in subcategoria:
                partes = subcategoria.split('|')
                posible_ean = partes[0]
                establecimiento = partes[1] if len(partes) > 1 else ""

                print(f"ID: {id_prod}")
                print(f"  Nombre: {nombre_normalizado}")
                print(f"  Código EAN actual: {codigo_ean}")
                print(f"  Subcategoría: {subcategoria}")
                print(f"  → EAN detectado: {posible_ean}")
                print(f"  → Establecimiento: {establecimiento}")
                print()
    else:
        print("✅ No se encontraron productos con este problema")
        print()

    # 5. Contar total de productos con este patrón
    cursor.execute(r"""
        SELECT COUNT(*)
        FROM productos_maestros
        WHERE codigo_ean IS NULL
        AND subcategoria ~ '^[0-9]+\|'
    """)
    total_problema = cursor.fetchone()[0]

    print("=" * 80)
    print("📊 RESUMEN DEL DIAGNÓSTICO")
    print("=" * 80)
    print(f"Total de productos: {total_productos}")
    print(f"Productos sin código EAN: {sin_codigo}")
    print(f"Productos con EAN en subcategoria: {total_problema}")
    print(f"Porcentaje afectado: {total_problema*100/total_productos:.1f}%")
    print()

    if total_problema > 0:
        print("⚠️  ACCIÓN REQUERIDA:")
        print(f"   Necesitas corregir {total_problema} productos")
        print(f"   Estos productos no se pueden usar para matching automático")
        print()
    else:
        print("✅ No hay productos con este problema")
        print()

    # 6. Análisis de patrones en subcategoria
    print("=" * 80)
    print("🔍 ANÁLISIS DE PATRONES EN SUBCATEGORIA")
    print("=" * 80)

    cursor.execute("""
        SELECT
            subcategoria,
            COUNT(*) as cantidad
        FROM productos_maestros
        WHERE subcategoria IS NOT NULL
        AND subcategoria LIKE '%|%'
        GROUP BY subcategoria
        ORDER BY cantidad DESC
        LIMIT 10
    """)

    patrones = cursor.fetchall()
    if patrones:
        print("Top 10 patrones más comunes:")
        for subcategoria, cantidad in patrones:
            print(f"  {subcategoria}: {cantidad} productos")
    print()

    cursor.close()
    conn.close()

    print("=" * 80)
    print("✅ Diagnóstico completado")
    print("=" * 80)

if __name__ == "__main__":
    try:
        diagnosticar_ean_en_subcategoria()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
