"""
migrate_inventario.py
Script para agregar columnas faltantes a inventario_usuario en PostgreSQL
"""

import os
import psycopg2
from urllib.parse import urlparse


def migrate_inventario_columns():
    """Agregar columnas nuevas a inventario_usuario en PostgreSQL"""

    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("❌ DATABASE_URL no configurada")
        return False

    try:
        # Parsear URL de PostgreSQL
        result = urlparse(database_url)

        # Conectar a PostgreSQL
        conn = psycopg2.connect(
            dbname=result.path[1:],
            user=result.username,
            password=result.password,
            host=result.hostname,
            port=result.port,
        )

        cursor = conn.cursor()

        print("✅ Conectado a PostgreSQL")
        print("🔄 Iniciando migración de columnas...")

        # Lista de columnas a agregar
        columnas = [
            ("precio_ultima_compra", "DECIMAL(10,2) DEFAULT 0.0"),
            ("precio_promedio", "DECIMAL(10,2) DEFAULT 0.0"),
            ("precio_minimo", "DECIMAL(10,2) DEFAULT 0.0"),
            ("precio_maximo", "DECIMAL(10,2) DEFAULT 0.0"),
            ("establecimiento_nombre", "VARCHAR(255)"),
            ("establecimiento_id", "INTEGER"),
            ("establecimiento_ubicacion", "VARCHAR(255)"),
            ("numero_compras", "INTEGER DEFAULT 0"),
            ("cantidad_total_comprada", "DECIMAL(10,2) DEFAULT 0.0"),
            ("total_gastado", "DECIMAL(12,2) DEFAULT 0.0"),
            ("ultima_factura_id", "INTEGER"),
            ("cantidad_por_unidad", "DECIMAL(10,2) DEFAULT 1.0"),
            ("dias_desde_ultima_compra", "INTEGER DEFAULT 0"),
        ]

        columnas_agregadas = 0
        columnas_existentes = 0

        for nombre_columna, tipo_columna in columnas:
            try:
                # Intentar agregar columna
                sql = f"""
                ALTER TABLE inventario_usuario
                ADD COLUMN IF NOT EXISTS {nombre_columna} {tipo_columna}
                """

                cursor.execute(sql)
                conn.commit()

                print(f"   ✅ Columna '{nombre_columna}' agregada")
                columnas_agregadas += 1

            except psycopg2.errors.DuplicateColumn:
                print(f"   ⚠️  Columna '{nombre_columna}' ya existe")
                columnas_existentes += 1
                conn.rollback()

            except Exception as e:
                print(f"   ❌ Error con '{nombre_columna}': {e}")
                conn.rollback()

        # Crear índices para optimizar consultas
        print("\n🔧 Creando índices...")

        indices = [
            "CREATE INDEX IF NOT EXISTS idx_inventario_usuario_establecimiento ON inventario_usuario(establecimiento_id)",
            "CREATE INDEX IF NOT EXISTS idx_inventario_precio_promedio ON inventario_usuario(precio_promedio)",
            "CREATE INDEX IF NOT EXISTS idx_inventario_ultima_compra ON inventario_usuario(fecha_ultima_compra)",
        ]

        for idx_sql in indices:
            try:
                cursor.execute(idx_sql)
                conn.commit()
                print(f"   ✅ Índice creado")
            except Exception as e:
                print(f"   ⚠️  Índice ya existe o error: {e}")
                conn.rollback()

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✅ MIGRACIÓN COMPLETADA")
        print("=" * 60)
        print(f"📊 Columnas agregadas: {columnas_agregadas}")
        print(f"📊 Columnas existentes: {columnas_existentes}")
        print(f"📊 Total columnas: {len(columnas)}")

        return True

    except Exception as e:
        print(f"❌ Error en migración: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 MIGRACIÓN DE INVENTARIO_USUARIO")
    print("=" * 60)

    success = migrate_inventario_columns()

    if success:
        print("\n✅ Migración exitosa. Puedes usar el inventario ahora.")
    else:
        print("\n❌ Migración falló. Revisa los errores.")
