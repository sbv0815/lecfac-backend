"""
============================================================================
DETECTIVE_EAN.PY - Buscar dónde están los códigos EAN escaneados
============================================================================
Este script busca en TODAS las tablas de la BD para encontrar dónde
tu app de escaneo está guardando los códigos de barras
============================================================================
"""

import os
from dotenv import load_dotenv

load_dotenv()

from database import get_db_connection

def buscar_ean_en_todas_tablas(codigo_ean_ejemplo: str = None):
    """
    Busca un código EAN en todas las tablas de la base de datos

    Args:
        codigo_ean_ejemplo: Código EAN para buscar (opcional)
    """

    print("=" * 80)
    print("🔍 DETECTIVE EAN - Buscando datos de tu app de escaneo")
    print("=" * 80)

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    es_postgres = os.getenv('DATABASE_TYPE', 'sqlite').lower() == 'postgresql'

    try:
        # ============================================
        # 1. LISTAR TODAS LAS TABLAS
        # ============================================
        print("\n📋 PASO 1: Listando todas las tablas...")

        if es_postgres:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
        else:
            cursor.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                ORDER BY name
            """)

        tablas = [row[0] for row in cursor.fetchall()]
        print(f"   ✅ {len(tablas)} tablas encontradas")

        # ============================================
        # 2. BUSCAR COLUMNAS CON "EAN" O "CODIGO"
        # ============================================
        print("\n📋 PASO 2: Buscando columnas con códigos...")

        tablas_con_codigo = {}

        for tabla in tablas:
            if es_postgres:
                cursor.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = %s
                      AND (column_name ILIKE '%%ean%%'
                           OR column_name ILIKE '%%codigo%%'
                           OR column_name ILIKE '%%barr%%')
                    ORDER BY ordinal_position
                """, (tabla,))
            else:
                cursor.execute(f"PRAGMA table_info({tabla})")
                columnas = cursor.fetchall()
                # Filtrar columnas que contengan 'ean' o 'codigo'
                columnas = [
                    (col[1], col[2]) for col in columnas
                    if 'ean' in col[1].lower() or 'codigo' in col[1].lower()
                ]

            if es_postgres:
                columnas = cursor.fetchall()

            if columnas:
                tablas_con_codigo[tabla] = columnas
                print(f"\n   🎯 {tabla}:")
                for col_name, col_type in columnas:
                    print(f"      • {col_name} ({col_type})")

        if not tablas_con_codigo:
            print("\n   ⚠️ No se encontraron columnas con 'ean' o 'codigo'")

        # ============================================
        # 3. CONTAR REGISTROS EN CADA TABLA
        # ============================================
        print("\n📋 PASO 3: Contando registros en tablas relevantes...")

        tablas_con_datos = []

        for tabla, columnas in tablas_con_codigo.items():
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count = cursor.fetchone()[0]

                if count > 0:
                    tablas_con_datos.append((tabla, count))
                    print(f"\n   📦 {tabla}: {count:,} registros")

                    # Mostrar algunos ejemplos
                    col_codigo = columnas[0][0]  # Primera columna con código

                    cursor.execute(f"""
                        SELECT {col_codigo}, *
                        FROM {tabla}
                        LIMIT 3
                    """)

                    ejemplos = cursor.fetchall()
                    if ejemplos:
                        print(f"      Ejemplos:")
                        for ej in ejemplos:
                            print(f"         • Código: {ej[0]}")

            except Exception as e:
                print(f"   ⚠️ Error en {tabla}: {e}")

        # ============================================
        # 4. BUSCAR CÓDIGO ESPECÍFICO (si se proporcionó)
        # ============================================
        if codigo_ean_ejemplo:
            print(f"\n📋 PASO 4: Buscando código específico: {codigo_ean_ejemplo}")

            encontrado_en = []

            for tabla, columnas in tablas_con_codigo.items():
                for col_name, col_type in columnas:
                    try:
                        placeholder = "%s" if es_postgres else "?"
                        cursor.execute(f"""
                            SELECT *
                            FROM {tabla}
                            WHERE {col_name} = {placeholder}
                            LIMIT 1
                        """, (codigo_ean_ejemplo,))

                        resultado = cursor.fetchone()

                        if resultado:
                            encontrado_en.append(tabla)
                            print(f"\n   ✅ ENCONTRADO en {tabla}.{col_name}")
                            print(f"      Datos: {resultado}")

                    except Exception as e:
                        pass

            if not encontrado_en:
                print(f"\n   ⚠️ Código {codigo_ean_ejemplo} NO encontrado en ninguna tabla")

        # ============================================
        # 5. ANALIZAR productos_referencia
        # ============================================
        print("\n📋 PASO 5: Analizando productos_referencia...")

        try:
            if 'productos_referencia' in tablas:
                cursor.execute("SELECT COUNT(*) FROM productos_referencia")
                count = cursor.fetchone()[0]

                print(f"   📊 productos_referencia: {count} registros")

                if count == 0:
                    print(f"   ⚠️ Tabla VACÍA - Tu app NO está guardando aquí")
                else:
                    print(f"   ✅ Tabla tiene datos")

                    # Mostrar estructura
                    if es_postgres:
                        cursor.execute("""
                            SELECT column_name, data_type
                            FROM information_schema.columns
                            WHERE table_name = 'productos_referencia'
                            ORDER BY ordinal_position
                        """)
                    else:
                        cursor.execute("PRAGMA table_info(productos_referencia)")

                    columnas = cursor.fetchall()
                    print(f"   📋 Estructura:")
                    for col in columnas:
                        print(f"      • {col[0] if es_postgres else col[1]}")
            else:
                print(f"   ❌ Tabla productos_referencia NO EXISTE")

        except Exception as e:
            print(f"   ❌ Error: {e}")

        # ============================================
        # 6. RESUMEN Y RECOMENDACIONES
        # ============================================
        print("\n" + "=" * 80)
        print("📊 RESUMEN")
        print("=" * 80)

        if tablas_con_datos:
            print("\n✅ Tablas con códigos de barras:")
            for tabla, count in sorted(tablas_con_datos, key=lambda x: x[1], reverse=True):
                print(f"   • {tabla}: {count:,} registros")

            print("\n💡 RECOMENDACIÓN:")
            print(f"   La tabla con más datos es: {tablas_con_datos[0][0]}")
            print(f"   Probablemente tu app esté guardando ahí")
        else:
            print("\n⚠️ No se encontraron datos")

        print("\n" + "=" * 80)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

        try:
            cursor.close()
            conn.close()
        except:
            pass


def buscar_tablas_recientes():
    """
    Busca tablas que tengan registros recientes (últimos 7 días)
    para identificar dónde tu app está guardando activamente
    """

    print("\n" + "=" * 80)
    print("🔍 BUSCANDO TABLAS CON ACTIVIDAD RECIENTE (últimos 7 días)")
    print("=" * 80)

    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    es_postgres = os.getenv('DATABASE_TYPE', 'sqlite').lower() == 'postgresql'

    try:
        # Listar tablas
        if es_postgres:
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
            """)
        else:
            cursor.execute("""
                SELECT name
                FROM sqlite_master
                WHERE type='table'
            """)

        tablas = [row[0] for row in cursor.fetchall()]

        tablas_recientes = []

        for tabla in tablas:
            try:
                # Buscar columnas de fecha
                if es_postgres:
                    cursor.execute(f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = %s
                          AND (column_name ILIKE '%%fecha%%'
                               OR column_name ILIKE '%%created%%'
                               OR column_name ILIKE '%%updated%%')
                        LIMIT 1
                    """, (tabla,))
                else:
                    cursor.execute(f"PRAGMA table_info({tabla})")
                    cols = cursor.fetchall()
                    cols = [c[1] for c in cols if 'fecha' in c[1].lower() or 'created' in c[1].lower()]
                    if cols:
                        cursor.execute(f"SELECT '{cols[0]}'")

                col_fecha = cursor.fetchone()

                if col_fecha:
                    col_nombre = col_fecha[0]

                    # Contar registros recientes
                    if es_postgres:
                        cursor.execute(f"""
                            SELECT COUNT(*)
                            FROM {tabla}
                            WHERE {col_nombre} >= CURRENT_DATE - INTERVAL '7 days'
                        """)
                    else:
                        cursor.execute(f"""
                            SELECT COUNT(*)
                            FROM {tabla}
                            WHERE {col_nombre} >= date('now', '-7 days')
                        """)

                    count = cursor.fetchone()[0]

                    if count > 0:
                        tablas_recientes.append((tabla, count))

            except:
                pass

        if tablas_recientes:
            print("\n✅ Tablas con actividad reciente:")
            for tabla, count in sorted(tablas_recientes, key=lambda x: x[1], reverse=True):
                print(f"   • {tabla}: {count} registros nuevos")
        else:
            print("\n⚠️ No se encontraron tablas con actividad reciente")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        try:
            cursor.close()
            conn.close()
        except:
            pass


if __name__ == "__main__":
    # Ejecutar detective
    print("\n🕵️ Iniciando investigación...")

    # Puedes poner un código EAN de ejemplo aquí
    # Por ejemplo, uno que sabes que existe en tu app
    codigo_ejemplo = 7702007084542 # Cambia esto por un código real si quieres

    buscar_ean_en_todas_tablas(codigo_ejemplo)
    buscar_tablas_recientes()

    print("\n" + "=" * 80)
    print("✅ Investigación completada")
    print("=" * 80)
    print("\n💡 Próximos pasos:")
    print("   1. Revisa las tablas con más datos")
    print("   2. Si sabes un código EAN específico, ejecútalo de nuevo:")
    print("      python detective_ean.py")
    print("   3. Modifica codigo_ejemplo = 'TU_CODIGO' en el script")
    print("=" * 80 + "\n")
