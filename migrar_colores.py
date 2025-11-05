"""
=============================================================================
SCRIPT DE MIGRACIÓN: Agregar colores a establecimientos
=============================================================================
Ejecutar con: python migrar_colores.py

Este script:
1. Agrega columnas color_bg y color_text a establecimientos
2. Asigna colores por defecto a supermercados conocidos
3. Es seguro ejecutar múltiples veces (idempotente)
=============================================================================
"""

import os
import sys

# Importar función de conexión desde database.py
try:
    from database import get_db_connection
except ImportError:
    print("❌ Error: No se pudo importar database.py")
    print("   Asegúrate de ejecutar este script desde la raíz del proyecto")
    sys.exit(1)


def ejecutar_migracion():
    """Ejecuta la migración de colores en establecimientos"""

    print("=" * 80)
    print("🎨 MIGRACIÓN: Agregar colores a establecimientos")
    print("=" * 80)

    conn = get_db_connection()

    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return False

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        print("\n📋 PASO 1: Verificando tabla 'establecimientos'...")

        # Verificar que la tabla existe
        if database_type == "postgresql":
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'establecimientos'
                )
            """)
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name='establecimientos'
            """)

        existe = cursor.fetchone()

        if not existe or (database_type == "postgresql" and not existe[0]):
            print("❌ La tabla 'establecimientos' no existe")
            print("   Ejecuta primero: python database.py")
            cursor.close()
            conn.close()
            return False

        print("✅ Tabla 'establecimientos' existe")

        # =====================================================
        # PASO 2: Agregar columnas
        # =====================================================
        print("\n📋 PASO 2: Agregando columnas de colores...")

        if database_type == "postgresql":
            # Verificar columnas existentes
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'establecimientos'
            """)
            columnas = [row[0] for row in cursor.fetchall()]

            # Agregar color_bg
            if 'color_bg' not in columnas:
                print("   ➕ Agregando columna 'color_bg'...")
                cursor.execute("""
                    ALTER TABLE establecimientos
                    ADD COLUMN color_bg VARCHAR(20) DEFAULT '#e9ecef'
                """)
                conn.commit()
                print("   ✅ Columna 'color_bg' agregada")
            else:
                print("   ✓ Columna 'color_bg' ya existe")

            # Agregar color_text
            if 'color_text' not in columnas:
                print("   ➕ Agregando columna 'color_text'...")
                cursor.execute("""
                    ALTER TABLE establecimientos
                    ADD COLUMN color_text VARCHAR(20) DEFAULT '#495057'
                """)
                conn.commit()
                print("   ✅ Columna 'color_text' agregada")
            else:
                print("   ✓ Columna 'color_text' ya existe")

        else:
            # SQLite
            cursor.execute("PRAGMA table_info(establecimientos)")
            columnas = [row[1] for row in cursor.fetchall()]

            if 'color_bg' not in columnas:
                print("   ➕ Agregando columna 'color_bg'...")
                cursor.execute("""
                    ALTER TABLE establecimientos
                    ADD COLUMN color_bg TEXT DEFAULT '#e9ecef'
                """)
                conn.commit()
                print("   ✅ Columna 'color_bg' agregada")
            else:
                print("   ✓ Columna 'color_bg' ya existe")

            if 'color_text' not in columnas:
                print("   ➕ Agregando columna 'color_text'...")
                cursor.execute("""
                    ALTER TABLE establecimientos
                    ADD COLUMN color_text TEXT DEFAULT '#495057'
                """)
                conn.commit()
                print("   ✅ Columna 'color_text' agregada")
            else:
                print("   ✓ Columna 'color_text' ya existe")

        # =====================================================
        # PASO 3: Asignar colores a supermercados conocidos
        # =====================================================
        print("\n📋 PASO 3: Asignando colores a supermercados conocidos...")

        # Definir colores para cada cadena
        colores = {
            'exito': ('#e3f2fd', '#1565c0', ['%exito%', '%éxito%']),
            'jumbo': ('#fff3e0', '#e65100', ['%jumbo%']),
            'carulla': ('#f3e5f5', '#7b1fa2', ['%carulla%']),
            'olimpica': ('#e8f5e9', '#2e7d32', ['%olimpica%', '%olímpica%']),
            'd1': ('#fff9c4', '#f57f17', ['%d1%', 'd1']),
            'ara': ('#ffe0b2', '#ef6c00', ['%ara%']),
            'pricesmart': ('#ffebee', '#c62828', ['%pricesmart%']),
            'makro': ('#fce4ec', '#ad1457', ['%makro%']),
        }

        actualizados = 0

        for cadena, (color_bg, color_text, patrones) in colores.items():
            for patron in patrones:
                try:
                    if database_type == "postgresql":
                        cursor.execute("""
                            UPDATE establecimientos
                            SET color_bg = %s, color_text = %s
                            WHERE LOWER(nombre_normalizado) LIKE %s
                              AND (color_bg IS NULL OR color_bg = '#e9ecef')
                        """, (color_bg, color_text, patron.lower()))
                    else:
                        cursor.execute("""
                            UPDATE establecimientos
                            SET color_bg = ?, color_text = ?
                            WHERE LOWER(nombre_normalizado) LIKE ?
                              AND (color_bg IS NULL OR color_bg = '#e9ecef')
                        """, (color_bg, color_text, patron.lower()))

                    if cursor.rowcount > 0:
                        actualizados += cursor.rowcount
                        print(f"   ✅ {cursor.rowcount} establecimiento(s) de {cadena.upper()} actualizado(s)")

                    conn.commit()

                except Exception as e:
                    print(f"   ⚠️ Error actualizando {cadena}: {e}")
                    conn.rollback()

        print(f"\n✅ Total: {actualizados} establecimientos actualizados con colores")

        # =====================================================
        # PASO 4: Mostrar resumen
        # =====================================================
        print("\n📋 PASO 4: Resumen de establecimientos...")

        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE color_bg IS NOT NULL
                ORDER BY nombre_normalizado
                LIMIT 20
            """)
        else:
            cursor.execute("""
                SELECT
                    nombre_normalizado,
                    color_bg,
                    color_text
                FROM establecimientos
                WHERE color_bg IS NOT NULL
                ORDER BY nombre_normalizado
                LIMIT 20
            """)

        establecimientos = cursor.fetchall()

        if establecimientos:
            print("\n📊 Establecimientos con colores asignados:")
            print("-" * 80)
            for est in establecimientos:
                nombre = est[0]
                bg = est[1]
                text = est[2]
                print(f"   {nombre:30} | BG: {bg:10} | TEXT: {text:10}")

            if len(establecimientos) >= 20:
                print(f"\n   ... y más ({len(establecimientos)} mostrados)")

        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 80)
        print("\n📝 PRÓXIMOS PASOS:")
        print("   1. Agregar establecimientos_api.py a tu proyecto")
        print("   2. Registrar el router en main.py")
        print("   3. Actualizar productos.html con código dinámico")
        print("   4. Probar: GET /api/establecimientos/colores")
        print("\n")

        return True

    except Exception as e:
        print(f"\n❌ ERROR durante la migración: {e}")
        import traceback
        traceback.print_exc()

        if conn:
            conn.rollback()
            cursor.close()
            conn.close()

        return False


if __name__ == "__main__":
    print("\n🚀 Iniciando migración de colores...")
    print()

    exito = ejecutar_migracion()

    if exito:
        sys.exit(0)
    else:
        print("\n❌ La migración falló. Revisa los errores arriba.")
        sys.exit(1)
