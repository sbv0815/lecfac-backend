"""
Script para verificar que las tablas existan antes de migrar
"""
import sqlite3

def verificar_tablas():
    print("🔍 Verificando tablas en lecfac.db...\n")

    try:
        conn = sqlite3.connect("lecfac.db")
        cursor = conn.cursor()

        # Listar todas las tablas
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table'
            ORDER BY name
        """)

        tablas = cursor.fetchall()

        print(f"📊 Total de tablas: {len(tablas)}\n")

        for tabla in tablas:
            print(f"   ✓ {tabla[0]}")

        # Verificar específicamente 'establecimientos'
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='establecimientos'
        """)

        existe_establecimientos = cursor.fetchone()

        print("\n" + "="*60)

        if existe_establecimientos:
            print("✅ Tabla 'establecimientos' EXISTE")

            # Ver columnas
            cursor.execute("PRAGMA table_info(establecimientos)")
            columnas = cursor.fetchall()

            print(f"\n📋 Columnas de 'establecimientos' ({len(columnas)}):")
            for col in columnas:
                print(f"   - {col[1]:30} {col[2]:15} {'NULL' if col[3] == 0 else 'NOT NULL'}")

            # Verificar si ya tiene color_bg
            tiene_color_bg = any(col[1] == 'color_bg' for col in columnas)
            tiene_color_text = any(col[1] == 'color_text' for col in columnas)

            print("\n" + "="*60)
            if tiene_color_bg and tiene_color_text:
                print("✅ Las columnas de colores YA EXISTEN")
                print("   No necesitas ejecutar la migración")
            else:
                print("⚠️  Las columnas de colores NO EXISTEN")
                print("   ✓ Puedes ejecutar: python migrar_colores.py")
        else:
            print("❌ Tabla 'establecimientos' NO EXISTE")
            print("   ✓ Ejecuta primero: python database.py")

        print("="*60 + "\n")

        cursor.close()
        conn.close()

        return existe_establecimientos is not None

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    verificar_tablas()
