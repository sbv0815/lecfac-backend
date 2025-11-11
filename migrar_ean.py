"""
Script de migración: Agregar columna codigo_ean a correcciones_aprendidas
Ejecutar UNA SOLA VEZ desde Railway o local
"""
import os
import psycopg2
from psycopg2 import sql

def migrate_add_codigo_ean():
    """Agrega columna codigo_ean a la tabla correcciones_aprendidas"""

    # Obtener DATABASE_URL desde variables de entorno
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        print("❌ ERROR: DATABASE_URL no está configurada")
        return False

    try:
        # Conectar a la base de datos
        print("🔄 Conectando a la base de datos...")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        # Verificar que la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'correcciones_aprendidas'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            print("❌ ERROR: La tabla correcciones_aprendidas no existe")
            return False

        print("✅ Tabla correcciones_aprendidas encontrada")

        # Verificar si la columna ya existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.columns
                WHERE table_name = 'correcciones_aprendidas'
                AND column_name = 'codigo_ean'
            );
        """)
        column_exists = cursor.fetchone()[0]

        if column_exists:
            print("⚠️  La columna codigo_ean ya existe, no se hace nada")
            return True

        # Agregar la columna
        print("🔄 Agregando columna codigo_ean...")
        cursor.execute("""
            ALTER TABLE correcciones_aprendidas
            ADD COLUMN codigo_ean VARCHAR(13);
        """)

        # Crear índice para búsquedas rápidas
        print("🔄 Creando índice en codigo_ean...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_correcciones_codigo_ean
            ON correcciones_aprendidas(codigo_ean);
        """)

        # Commit de los cambios
        conn.commit()
        print("✅ Migración completada exitosamente")

        # Mostrar estructura actualizada
        cursor.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_name = 'correcciones_aprendidas'
            ORDER BY ordinal_position;
        """)

        print("\n📋 Estructura actualizada de correcciones_aprendidas:")
        print("-" * 60)
        for row in cursor.fetchall():
            col_name, data_type, max_length = row
            length_info = f"({max_length})" if max_length else ""
            print(f"  • {col_name}: {data_type}{length_info}")
        print("-" * 60)

        cursor.close()
        conn.close()

        return True

    except psycopg2.Error as e:
        print(f"❌ Error en la base de datos: {e}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 MIGRACIÓN: Agregar codigo_ean a correcciones_aprendidas")
    print("=" * 60)
    print()

    success = migrate_add_codigo_ean()

    print()
    if success:
        print("✅ Migración ejecutada correctamente")
        print("📝 La columna codigo_ean está lista para usar")
    else:
        print("❌ La migración falló, revisa los errores arriba")
    print()
    print("=" * 60)
