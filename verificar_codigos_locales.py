"""
Verificar y Crear Tabla codigos_locales
========================================
"""

import psycopg

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def verificar_y_crear_tabla():
    """Verifica si existe tabla codigos_locales y la crea si no existe"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        # Verificar si existe la tabla
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'codigos_locales'
            )
        """)

        existe = cursor.fetchone()[0]

        if existe:
            print("✅ Tabla 'codigos_locales' ya existe")

            # Ver cuántos registros tiene
            cursor.execute("SELECT COUNT(*) FROM codigos_locales")
            total = cursor.fetchone()[0]
            print(f"   📊 Total de códigos locales registrados: {total}")

            if total > 0:
                # Mostrar algunos ejemplos
                cursor.execute("""
                    SELECT
                        cl.codigo_local,
                        e.nombre_normalizado,
                        pm.nombre_normalizado,
                        cl.veces_visto
                    FROM codigos_locales cl
                    JOIN establecimientos e ON cl.establecimiento_id = e.id
                    JOIN productos_maestros pm ON cl.producto_maestro_id = pm.id
                    ORDER BY cl.veces_visto DESC
                    LIMIT 5
                """)

                print("\n   📋 Ejemplos de códigos locales más usados:")
                for row in cursor.fetchall():
                    print(f"      {row[0]} en {row[1]} → {row[2]} (visto {row[3]} veces)")

        else:
            print("❌ Tabla 'codigos_locales' NO existe")
            print("   ➕ Creando tabla...")

            cursor.execute("""
                CREATE TABLE codigos_locales (
                    id SERIAL PRIMARY KEY,
                    producto_maestro_id INTEGER REFERENCES productos_maestros(id) ON DELETE CASCADE,
                    establecimiento_id INTEGER REFERENCES establecimientos(id) ON DELETE CASCADE,
                    codigo_local VARCHAR(50) NOT NULL,
                    descripcion_local TEXT,
                    veces_visto INTEGER DEFAULT 1,
                    primera_vez_visto TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ultima_vez_visto TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    activo BOOLEAN DEFAULT TRUE,
                    UNIQUE(establecimiento_id, codigo_local)
                )
            """)

            # Crear índices
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_codigos_locales_establecimiento
                ON codigos_locales(establecimiento_id, codigo_local)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_codigos_locales_producto
                ON codigos_locales(producto_maestro_id)
            """)

            conn.commit()
            print("   ✅ Tabla 'codigos_locales' creada exitosamente")
            print("   ✅ Índices creados")

        print("\n" + "=" * 70)
        print("✅ Sistema listo para usar códigos locales (PLU)")
        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verificar_y_crear_tabla()
