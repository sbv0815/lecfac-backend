"""
Script para verificar los valores reales en la base de datos PostgreSQL de RAILWAY
FORZANDO conexión a PostgreSQL (ignorando SQLite local)
"""

import os
import sys

# Forzar PostgreSQL
os.environ['DATABASE_TYPE'] = 'postgresql'

try:
    import psycopg2
    from urllib.parse import urlparse

    print("✅ psycopg2 disponible")
except ImportError:
    print("❌ psycopg2 no disponible, instalando...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2
    from urllib.parse import urlparse


def get_postgresql_connection():
    """Conexión DIRECTA a PostgreSQL de Railway"""

    database_url = os.environ.get('DATABASE_URL')

    if not database_url:
        print("❌ ERROR: Variable DATABASE_URL no configurada")
        print()
        print("Para conectarte a Railway desde tu máquina local:")
        print()
        print("1. Ve a Railway → tu proyecto → PostgreSQL")
        print("2. Copia la DATABASE_URL")
        print("3. Ejecuta:")
        print()
        print('   set DATABASE_URL="postgresql://..."')
        print("   python verificar_precios_bd.py")
        print()
        sys.exit(1)

    # Parsear URL
    parsed = urlparse(database_url)

    print(f"🔗 Conectando a PostgreSQL:")
    print(f"   Host: {parsed.hostname}")
    print(f"   Port: {parsed.port}")
    print(f"   Database: {parsed.path[1:]}")
    print(f"   User: {parsed.username}")
    print()

    try:
        conn = psycopg2.connect(
            host=parsed.hostname,
            port=parsed.port,
            database=parsed.path[1:],
            user=parsed.username,
            password=parsed.password
        )

        print("✅ Conectado a PostgreSQL de Railway")
        print()
        return conn

    except Exception as e:
        print(f"❌ Error conectando: {e}")
        sys.exit(1)


def verificar_precios():
    print("=" * 60)
    print("🔍 VERIFICANDO PRECIOS EN BASE DE DATOS")
    print("=" * 60)
    print()

    try:
        conn = get_postgresql_connection()
        cursor = conn.cursor()

        # Verificar items_factura
        print("📦 ITEMS_FACTURA - Últimos 10 registros:")
        print("-" * 60)

        cursor.execute("""
            SELECT
                id,
                nombre_leido,
                precio_pagado,
                factura_id
            FROM items_factura
            ORDER BY id DESC
            LIMIT 10
        """)

        items_data = cursor.fetchall()

        if not items_data:
            print("⚠️  No hay items en la base de datos")
        else:
            for row in items_data:
                precio = int(row[2]) if row[2] else 0
                print(f"ID: {row[0]:3d} | {row[1][:30]:30s} | ${precio:>15,} | Factura: {row[3]}")

        print()
        print("📊 ESTADÍSTICAS DE PRECIOS:")
        print("-" * 60)

        cursor.execute("""
            SELECT
                MIN(precio_pagado) as min_precio,
                MAX(precio_pagado) as max_precio,
                AVG(precio_pagado) as avg_precio,
                COUNT(*) as total
            FROM items_factura
        """)

        row = cursor.fetchone()
        min_p = int(row[0]) if row[0] else 0
        max_p = int(row[1]) if row[1] else 0
        avg_p = int(row[2]) if row[2] else 0

        print(f"Mínimo:   ${min_p:>15,}")
        print(f"Máximo:   ${max_p:>15,}")
        print(f"Promedio: ${avg_p:>15,}")
        print(f"Total items: {row[3]}")

        # Análisis del problema
        print()
        if max_p > 10000000:  # 10 millones
            print("🚨 PROBLEMA DETECTADO:")
            print("   Los precios están MULTIPLICADOS por 10,000")
            print(f"   Ejemplo: ${max_p:,} debería ser ${int(max_p/10000):,}")
            print()
            print("✅ SOLUCIÓN: Ejecutar corregir_precios_bd.py")
        elif max_p > 1000000:  # 1 millón
            print("⚠️  POSIBLE PROBLEMA:")
            print("   Algunos precios parecen muy altos")
            print(f"   Precio máximo: ${max_p:,}")
        else:
            print("✅ Los precios parecen CORRECTOS")
            print(f"   Rango razonable: ${min_p:,} - ${max_p:,}")

        print()
        print("📄 FACTURAS - Últimas 5:")
        print("-" * 60)

        cursor.execute("""
            SELECT
                id,
                establecimiento,
                total_factura,
                fecha_factura
            FROM facturas
            ORDER BY id DESC
            LIMIT 5
        """)

        for row in cursor.fetchall():
            total = int(row[2]) if row[2] else 0
            fecha = str(row[3]) if row[3] else "Sin fecha"
            print(f"ID: {row[0]:3d} | {row[1][:20]:20s} | ${total:>15,} | {fecha[:10]}")

        print()
        print("🛒 PRODUCTOS_MAESTROS - Últimos 10:")
        print("-" * 60)

        cursor.execute("""
            SELECT
                id,
                nombre_normalizado,
                precio_promedio_global
            FROM productos_maestros
            WHERE precio_promedio_global IS NOT NULL
            ORDER BY id DESC
            LIMIT 10
        """)

        productos = cursor.fetchall()

        if not productos:
            print("⚠️  No hay productos con precios")
        else:
            for row in productos:
                precio = int(row[2]) if row[2] else 0
                print(f"ID: {row[0]:3d} | {row[1][:30]:30s} | ${precio:>15,}")

        cursor.close()
        conn.close()

        print()
        print("=" * 60)
        print("✅ VERIFICACIÓN COMPLETADA")
        print("=" * 60)
        print()

    except Exception as e:
        print()
        print("=" * 60)
        print("❌ ERROR")
        print("=" * 60)
        print(f"Error: {e}")
        print()
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    verificar_precios()
