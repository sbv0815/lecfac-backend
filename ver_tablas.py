"""
Script para ver TODAS las tablas en Railway PostgreSQL
"""
import psycopg2

def ver_tablas_railway():
    """Muestra todas las tablas y sus registros"""

    database_url = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        print("✅ Conexión exitosa a Railway")
        print("=" * 60)
        print("📋 TABLAS EN LA BASE DE DATOS:")
        print("=" * 60)

        # Obtener todas las tablas
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)

        tablas = cursor.fetchall()

        for (tabla,) in tablas:
            # Contar registros
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]

            print(f"📊 {tabla:30} - {count:5} registros")

        print("=" * 60)

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    ver_tablas_railway()
