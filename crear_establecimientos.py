"""
Script mínimo: Crear SOLO tabla establecimientos en SQLite
"""
import sqlite3

def crear_tabla_establecimientos():
    print("🏗️  Creando tabla 'establecimientos' en SQLite...\n")

    try:
        conn = sqlite3.connect("lecfac.db")
        cursor = conn.cursor()

        # Crear tabla establecimientos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS establecimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre_normalizado TEXT UNIQUE NOT NULL,
                cadena TEXT,
                tipo TEXT,
                ciudad TEXT,
                direccion TEXT,
                latitud REAL,
                longitud REAL,
                total_facturas_reportadas INTEGER DEFAULT 0,
                calificacion_promedio REAL,
                activo INTEGER DEFAULT 1,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)

        conn.commit()

        # Verificar que se creó
        cursor.execute("""
            SELECT name FROM sqlite_master
            WHERE type='table' AND name='establecimientos'
        """)

        existe = cursor.fetchone()

        if existe:
            print("✅ Tabla 'establecimientos' creada exitosamente\n")

            # Insertar algunos ejemplos
            print("📝 Insertando ejemplos de establecimientos...\n")

            ejemplos = [
                ("Éxito", "Grupo Éxito"),
                ("Jumbo", "Grupo Éxito"),
                ("Carulla", "Grupo Éxito"),
                ("Olímpica", "Olímpica"),
                ("D1", "Koba Colombia"),
                ("Ara", "Grupo Éxito"),
            ]

            for nombre, cadena in ejemplos:
                try:
                    cursor.execute("""
                        INSERT OR IGNORE INTO establecimientos (nombre_normalizado, cadena)
                        VALUES (?, ?)
                    """, (nombre, cadena))

                    if cursor.rowcount > 0:
                        print(f"   ✓ {nombre:20} ({cadena})")
                except:
                    pass

            conn.commit()

            # Contar registros
            cursor.execute("SELECT COUNT(*) FROM establecimientos")
            total = cursor.fetchone()[0]

            print(f"\n✅ Total establecimientos: {total}\n")
        else:
            print("❌ Error: La tabla no se pudo crear\n")

        cursor.close()
        conn.close()

        print("="*60)
        print("✅ Ahora puedes ejecutar: python migrar_colores.py")
        print("="*60 + "\n")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    crear_tabla_establecimientos()
