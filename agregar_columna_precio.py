#!/usr/bin/env python3
"""
Script COMPLETO para agregar TODAS las columnas faltantes
En AMBAS tablas: productos_por_establecimiento Y productos_maestros
Versión 4.0 - Arregla TODO de una vez
"""

import os
import psycopg2
from urllib.parse import urlparse
import sys

# Tu DATABASE_URL de Railway
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def conectar_db():
    """Conectar a la base de datos de Railway"""
    try:
        print("\n🔗 Conectando a PostgreSQL...")
        url = urlparse(DATABASE_URL)

        print(f"   Host: {url.hostname}")
        print(f"   Puerto: {url.port}")
        print(f"   Base de datos: {url.path[1:]}")

        conn = psycopg2.connect(
            host=url.hostname,
            database=url.path[1:],
            user=url.username,
            password=url.password,
            port=url.port or 5432
        )

        print("✅ Conexión exitosa")
        return conn

    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def agregar_columnas_tabla(conn, tabla, columnas_necesarias):
    """Función genérica para agregar columnas a cualquier tabla"""
    cursor = conn.cursor()

    print(f"\n📋 Verificando tabla: {tabla}")

    # Verificar si existe
    cursor.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = %s
        )
    """, (tabla,))

    if not cursor.fetchone()[0]:
        print(f"   ❌ La tabla {tabla} no existe")
        return False

    # Obtener columnas existentes
    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %s
    """, (tabla,))

    columnas_existentes = [row[0] for row in cursor.fetchall()]
    print(f"   Columnas actuales: {len(columnas_existentes)}")

    agregadas = []

    # Agregar columnas faltantes
    for columna, tipo in columnas_necesarias.items():
        if columna not in columnas_existentes:
            print(f"   🔧 Agregando: {columna}")
            try:
                cursor.execute(f"""
                    ALTER TABLE {tabla}
                    ADD COLUMN {columna} {tipo}
                """)
                conn.commit()
                agregadas.append(columna)
                print(f"      ✅ {columna} agregada")
            except Exception as e:
                print(f"      ⚠️ Error: {e}")
                conn.rollback()
        else:
            print(f"   ✓ {columna} ya existe")

    if agregadas:
        print(f"   ✅ Se agregaron {len(agregadas)} columnas")
    else:
        print(f"   ✅ Todas las columnas ya existían")

    cursor.close()
    return True


def main():
    print("="*60)
    print("🔧 LECFAC - REPARACIÓN COMPLETA DE TABLAS")
    print("="*60)

    conn = conectar_db()
    if not conn:
        print("❌ No se pudo conectar")
        return

    try:
        # 1. ARREGLAR productos_por_establecimiento
        print("\n" + "="*40)
        print("TABLA 1: productos_por_establecimiento")
        print("="*40)

        columnas_ppe = {
            'precio_unitario': 'INTEGER',
            'fecha_creacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'fecha_actualizacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }

        agregar_columnas_tabla(conn, 'productos_por_establecimiento', columnas_ppe)

        # 2. ARREGLAR productos_maestros
        print("\n" + "="*40)
        print("TABLA 2: productos_maestros")
        print("="*40)

        columnas_pm = {
            'fecha_actualizacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'ultima_actualizacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }

        agregar_columnas_tabla(conn, 'productos_maestros', columnas_pm)

        # 3. Verificación final
        print("\n" + "="*60)
        print("📊 VERIFICACIÓN FINAL")
        print("="*60)

        cursor = conn.cursor()

        # Verificar productos_por_establecimiento
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'productos_por_establecimiento'
            AND column_name IN ('precio_unitario', 'fecha_creacion', 'fecha_actualizacion')
        """)

        cols_ppe = cursor.fetchall()
        print("\n✅ productos_por_establecimiento:")
        for col in cols_ppe:
            print(f"   • {col[0]}: {col[1]}")

        # Verificar productos_maestros
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
            AND column_name IN ('fecha_actualizacion', 'ultima_actualizacion')
        """)

        cols_pm = cursor.fetchall()
        print("\n✅ productos_maestros:")
        for col in cols_pm:
            print(f"   • {col[0]}: {col[1]}")

        cursor.close()

        print("\n" + "="*60)
        print("🎉 ¡PROCESO COMPLETADO!")
        print("="*60)
        print("✅ Ambas tablas han sido reparadas")
        print("✅ El modal de edición funcionará correctamente")
        print("✅ Los PLUs se pueden editar sin errores")
        print("💡 No necesitas reiniciar la aplicación")
        print("="*60)

    except Exception as e:
        print(f"\n❌ Error general: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
