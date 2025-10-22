"""
Script para verificar el estado actual de la base de datos
Muestra todas las tablas, columnas y cantidad de datos
"""

from database import get_db_connection


def verificar_database():
    """Muestra el estado completo de la BD"""

    conn = get_db_connection()
    cursor = conn.cursor()

    print("=" * 70)
    print("🔍 ESTADO ACTUAL DE LA BASE DE DATOS")
    print("=" * 70)

    try:
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()

        if not tables:
            print("\n⚠️  No hay tablas en la base de datos")
            print("💡 Ejecuta 'setup_database.py' para crearlas")
            return

        print(f"\n📊 Tablas encontradas: {len(tables)}\n")

        for table in tables:
            table_name = table[0]

            # Nombre de la tabla
            print(f"📋 Tabla: {table_name}")
            print("-" * 70)

            # Obtener estructura de la tabla
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()

            print("   Columnas:")
            for col in columns:
                col_id, col_name, col_type, not_null, default_val, is_pk = col
                pk_mark = " 🔑" if is_pk else ""
                null_mark = " NOT NULL" if not_null else ""
                default_mark = f" DEFAULT {default_val}" if default_val else ""
                print(
                    f"      - {col_name} ({col_type}){pk_mark}{null_mark}{default_mark}"
                )

            # Contar registros
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"   📦 Registros: {count}")

            # Mostrar primeros 3 registros si existen
            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                print(f"   📄 Primeros registros:")
                for i, row in enumerate(rows, 1):
                    print(f"      {i}. {row}")

            print()

        print("=" * 70)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        conn.close()


if __name__ == "__main__":
    verificar_database()
