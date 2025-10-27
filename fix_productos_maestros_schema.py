"""
Script para arreglar el schema de productos_maestros en Railway
Permite NULL en codigo_ean para productos sin código de barras
"""

import os
import sys

# Intentar importar psycopg2
try:
    import psycopg2
    from urllib.parse import urlparse
except ImportError:
    print("❌ Error: psycopg2 no está instalado")
    print("💡 Instala con: pip install psycopg2-binary")
    sys.exit(1)


def fix_productos_maestros_schema():
    """Arregla el schema de productos_maestros para permitir NULL en codigo_ean"""

    # Obtener DATABASE_URL de variables de entorno
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("❌ ERROR: Variable de entorno DATABASE_URL no configurada")
        print("\n💡 Para Railway, ejecuta:")
        print("   railway run python fix_productos_maestros_schema.py")
        print("\n💡 O configura DATABASE_URL manualmente:")
        print('   set DATABASE_URL=postgresql://user:pass@host:port/database')
        return False

    print("🔗 Conectando a PostgreSQL...")
    print(f"   URL: {database_url[:30]}...")

    try:
        # Parsear URL
        url = urlparse(database_url)

        # Conectar
        conn = psycopg2.connect(
            host=url.hostname,
            database=url.path[1:],
            user=url.username,
            password=url.password,
            port=url.port or 5432,
            connect_timeout=10
        )

        cursor = conn.cursor()

        print("✅ Conectado exitosamente\n")

        # Paso 1: Ver schema actual
        print("📊 Schema actual de productos_maestros:")
        cursor.execute("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
            AND column_name = 'codigo_ean'
        """)

        result = cursor.fetchone()
        if result:
            col_name, data_type, max_length, is_nullable = result
            print(f"   Columna: {col_name}")
            print(f"   Tipo: {data_type}({max_length})")
            print(f"   Permite NULL: {is_nullable}")
        else:
            print("   ⚠️ Tabla productos_maestros no encontrada")
            conn.close()
            return False

        print("\n🔧 Aplicando correcciones...\n")

        # Paso 2: Eliminar constraint UNIQUE (si existe)
        print("1️⃣ Eliminando constraint UNIQUE de codigo_ean...")
        try:
            cursor.execute("""
                ALTER TABLE productos_maestros
                DROP CONSTRAINT IF EXISTS productos_maestros_codigo_ean_key
            """)
            conn.commit()
            print("   ✅ Constraint UNIQUE eliminado\n")
        except Exception as e:
            print(f"   ⚠️ Warning: {e}\n")
            conn.rollback()

        # Paso 3: Eliminar constraint CHECK viejo
        print("2️⃣ Eliminando constraint CHECK viejo...")
        try:
            cursor.execute("""
                ALTER TABLE productos_maestros
                DROP CONSTRAINT IF EXISTS productos_maestros_codigo_ean_check
            """)
            conn.commit()
            print("   ✅ Constraint CHECK viejo eliminado\n")
        except Exception as e:
            print(f"   ⚠️ Warning: {e}\n")
            conn.rollback()

        # Paso 4: Permitir NULL en codigo_ean
        print("3️⃣ Permitiendo NULL en codigo_ean...")
        try:
            cursor.execute("""
                ALTER TABLE productos_maestros
                ALTER COLUMN codigo_ean DROP NOT NULL
            """)
            conn.commit()
            print("   ✅ Columna codigo_ean ahora permite NULL\n")
        except Exception as e:
            print(f"   ⚠️ Warning: {e}\n")
            conn.rollback()

        # Paso 5: Agregar nuevo constraint CHECK
        print("4️⃣ Agregando nuevo constraint CHECK...")
        try:
            cursor.execute("""
                ALTER TABLE productos_maestros
                ADD CONSTRAINT productos_maestros_codigo_ean_check
                CHECK (codigo_ean IS NULL OR (LENGTH(codigo_ean) >= 3 AND LENGTH(codigo_ean) <= 14))
            """)
            conn.commit()
            print("   ✅ Nuevo constraint CHECK agregado\n")
        except Exception as e:
            print(f"   ❌ Error: {e}\n")
            conn.rollback()

        # Verificar resultado final
        print("📊 Schema final de productos_maestros:")
        cursor.execute("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
            AND column_name = 'codigo_ean'
        """)

        result = cursor.fetchone()
        if result:
            col_name, data_type, max_length, is_nullable = result
            print(f"   Columna: {col_name}")
            print(f"   Tipo: {data_type}({max_length})")
            print(f"   Permite NULL: {is_nullable}")

        # Verificar cuántos productos hay
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        count = cursor.fetchone()[0]
        print(f"\n📦 Productos en productos_maestros: {count}")

        conn.close()

        print("\n" + "="*60)
        print("✅ SCHEMA CORREGIDO EXITOSAMENTE")
        print("="*60)
        print("\n🎯 SIGUIENTE PASO:")
        print("   Escanea una factura nueva desde la app móvil")
        print("   Los productos ahora se crearán correctamente\n")

        return True

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("="*60)
    print("🔧 FIX SCHEMA productos_maestros - Railway PostgreSQL")
    print("="*60)
    print()

    success = fix_productos_maestros_schema()

    if success:
        sys.exit(0)
    else:
        sys.exit(1)
