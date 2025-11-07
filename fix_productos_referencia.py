#!/usr/bin/env python3
"""
Script para eliminar y recrear la tabla productos_referencia
"""
import os
import sys

# Intentar importar psycopg
try:
    import psycopg
    PSYCOPG_VERSION = 3
    print("✅ Usando psycopg3")
except ImportError:
    try:
        import psycopg2 as psycopg
        PSYCOPG_VERSION = 2
        print("✅ Usando psycopg2")
    except ImportError:
        print("❌ No se encontró psycopg2 ni psycopg3")
        sys.exit(1)

def main():
    print("=" * 60)
    print("🔧 FIX: Recreando tabla productos_referencia")
    print("=" * 60)

    # Obtener DATABASE_URL
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        print("❌ DATABASE_URL no configurada")
        print("💡 Configúrala con: export DATABASE_URL='...'")
        return 1

    print(f"📡 Conectando a base de datos...")

    try:
        # Conectar
        if PSYCOPG_VERSION == 3:
            conn = psycopg.connect(database_url)
        else:
            from urllib.parse import urlparse
            url = urlparse(database_url)
            conn = psycopg.connect(
                host=url.hostname,
                database=url.path[1:],
                user=url.username,
                password=url.password,
                port=url.port or 5432
            )

        print("✅ Conectado")

        cursor = conn.cursor()

        # Ver estructura actual
        print("\n📋 Verificando estructura actual...")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'productos_referencia'
            ORDER BY ordinal_position
        """)

        columnas = cursor.fetchall()

        if columnas:
            print("\n📊 Columnas actuales:")
            for col in columnas:
                print(f"   • {col[0]:20} ({col[1]})")
        else:
            print("   ⚠️ Tabla no existe")

        # Preguntar confirmación
        print("\n" + "=" * 60)
        print("⚠️  ADVERTENCIA: Se eliminará la tabla productos_referencia")
        print("    y se recreará con la estructura correcta.")
        print("=" * 60)

        respuesta = input("\n¿Continuar? (si/no): ").strip().lower()

        if respuesta != 'si':
            print("❌ Operación cancelada")
            conn.close()
            return 0

        # Eliminar tabla
        print("\n🗑️  Eliminando tabla productos_referencia...")
        cursor.execute("DROP TABLE IF EXISTS productos_referencia CASCADE")
        conn.commit()
        print("✅ Tabla eliminada")

        # Recrear tabla
        print("\n🏗️  Creando tabla con estructura correcta...")
        cursor.execute("""
            CREATE TABLE productos_referencia (
                id SERIAL PRIMARY KEY,
                codigo_ean VARCHAR(50) UNIQUE NOT NULL,
                nombre VARCHAR(500) NOT NULL,
                marca VARCHAR(200),
                categoria VARCHAR(200),
                presentacion VARCHAR(200),
                unidad_medida VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CHECK (LENGTH(nombre) >= 2),
                CHECK (LENGTH(codigo_ean) >= 8)
            )
        """)
        conn.commit()
        print("✅ Tabla creada")

        # Crear índices
        print("\n📑 Creando índices...")

        indices = [
            "CREATE INDEX idx_prod_ref_ean ON productos_referencia(codigo_ean)",
            "CREATE INDEX idx_prod_ref_nombre ON productos_referencia(nombre)",
            "CREATE INDEX idx_prod_ref_marca ON productos_referencia(marca)",
            "CREATE INDEX idx_prod_ref_categoria ON productos_referencia(categoria)",
        ]

        for idx_sql in indices:
            try:
                cursor.execute(idx_sql)
                conn.commit()
                print(f"   ✅ Índice creado")
            except Exception as e:
                print(f"   ⚠️ Error: {e}")

        # Verificar resultado
        print("\n✅ Verificando resultado...")
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'productos_referencia'
            ORDER BY ordinal_position
        """)

        columnas_nuevas = cursor.fetchall()

        print("\n📊 Estructura final:")
        for col in columnas_nuevas:
            print(f"   ✅ {col[0]:20} ({col[1]})")

        cursor.close()
        conn.close()

        print("\n" + "=" * 60)
        print("✅ TABLA RECREADA EXITOSAMENTE")
        print("=" * 60)
        print("\n💡 Ahora puedes reiniciar la app de Flutter:")
        print("   flutter run")

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

        if conn:
            conn.close()

        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
