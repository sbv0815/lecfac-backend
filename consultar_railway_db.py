"""
query_interactivo_railway.py - Consultas SQL interactivas a Railway
========================================================================
Ejecuta cualquier query SQL y ve los resultados formateados

INSTALACIÓN:
pip install psycopg2-binary tabulate

USO:
python query_interactivo_railway.py
"""

import psycopg2
from tabulate import tabulate

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═══════════════════════════════════════════════════════════════
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

# Parsear URL
url_parts = DATABASE_URL.replace("postgresql://", "").split("@")
user_pass = url_parts[0].split(":")
host_port_db = url_parts[1].split("/")
host_port = host_port_db[0].split(":")

USER = user_pass[0]
PASSWORD = user_pass[1]
HOST = host_port[0]
PORT = host_port[1]
DATABASE = host_port_db[1]


def conectar():
    """Conecta a la base de datos"""
    try:
        conn = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=DATABASE,
            user=USER,
            password=PASSWORD
        )
        conn.autocommit = True  # ← IMPORTANTE: Evita el error de transacción
        return conn
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        return None


def listar_tablas(cursor):
    """Lista todas las tablas de la base de datos"""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """
    cursor.execute(query)
    tablas = cursor.fetchall()

    print("\n" + "="*80)
    print("📋 TABLAS DISPONIBLES EN LA BASE DE DATOS")
    print("="*80)

    for idx, (tabla,) in enumerate(tablas, 1):
        print(f"{idx:2}. {tabla}")

    print("="*80)
    return [tabla[0] for tabla in tablas]


def ver_estructura_tabla(cursor, tabla):
    """Muestra las columnas de una tabla"""
    query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = '{tabla}'
        ORDER BY ordinal_position;
    """
    cursor.execute(query)
    columnas = cursor.fetchall()

    print(f"\n{'='*80}")
    print(f"📊 ESTRUCTURA DE LA TABLA: {tabla}")
    print("="*80)

    headers = ["Columna", "Tipo", "Nullable"]
    print(tabulate(columnas, headers=headers, tablefmt="grid"))


def ejecutar_query_personalizado(cursor, query):
    """Ejecuta un query personalizado y muestra resultados"""
    try:
        cursor.execute(query)

        # Si es un SELECT, mostrar resultados
        if query.strip().upper().startswith("SELECT"):
            resultados = cursor.fetchall()

            if not resultados:
                print("\n⚠️  Query ejecutado correctamente pero no retornó resultados")
                return

            # Obtener nombres de columnas
            columnas = [desc[0] for desc in cursor.description]

            # Mostrar resultados en tabla bonita
            print("\n" + "="*80)
            print(f"✅ RESULTADOS ({len(resultados)} filas)")
            print("="*80)
            print(tabulate(resultados, headers=columnas, tablefmt="grid"))

        else:
            print("\n✅ Query ejecutado correctamente")

    except Exception as e:
        print(f"\n❌ Error ejecutando query: {e}")


def menu_principal():
    """Menú principal interactivo"""
    print("\n" + "="*80)
    print("🗄️  CONSULTOR INTERACTIVO DE BASE DE DATOS RAILWAY")
    print("="*80)
    print("Host:", HOST)
    print("Database:", DATABASE)
    print("="*80)

    conn = conectar()
    if not conn:
        return

    cursor = conn.cursor()
    print("✅ Conexión exitosa\n")

    tablas = None

    while True:
        print("\n" + "="*80)
        print("MENÚ PRINCIPAL")
        print("="*80)
        print("1. Listar todas las tablas")
        print("2. Ver estructura de una tabla")
        print("3. Ejecutar query personalizado")
        print("4. Queries rápidos predefinidos")
        print("5. Salir")
        print("="*80)

        opcion = input("\n👉 Selecciona una opción (1-5): ").strip()

        if opcion == "1":
            tablas = listar_tablas(cursor)

        elif opcion == "2":
            if not tablas:
                tablas = listar_tablas(cursor)

            tabla = input("\n📋 Nombre de la tabla: ").strip()
            if tabla in tablas:
                ver_estructura_tabla(cursor, tabla)
            else:
                print(f"❌ Tabla '{tabla}' no existe")

        elif opcion == "3":
            print("\n" + "="*80)
            print("EJECUTAR QUERY PERSONALIZADO")
            print("="*80)
            print("Escribe tu query SQL (puede ser multilínea)")
            print("Cuando termines, escribe 'GO' en una línea nueva")
            print("Para cancelar, escribe 'CANCEL'")
            print("="*80)

            lineas = []
            while True:
                linea = input()
                if linea.strip().upper() == "GO":
                    break
                if linea.strip().upper() == "CANCEL":
                    print("❌ Cancelado")
                    lineas = []
                    break
                lineas.append(linea)

            if lineas:
                query = "\n".join(lineas)
                print(f"\n🔍 Ejecutando:\n{query}\n")
                ejecutar_query_personalizado(cursor, query)

        elif opcion == "4":
            menu_queries_rapidos(cursor)

        elif opcion == "5":
            print("\n👋 Cerrando conexión...")
            cursor.close()
            conn.close()
            print("✅ Adiós!")
            break

        else:
            print("❌ Opción inválida")


def menu_queries_rapidos(cursor):
    """Menú con queries predefinidos"""
    print("\n" + "="*80)
    print("⚡ QUERIES RÁPIDOS")
    print("="*80)
    print("1. Contar registros en una tabla")
    print("2. Ver primeros 10 registros de una tabla")
    print("3. Ver últimos 10 registros de una tabla (por ID)")
    print("4. Buscar en una tabla por columna")
    print("5. Volver al menú principal")
    print("="*80)

    opcion = input("\n👉 Selecciona una opción (1-5): ").strip()

    if opcion == "1":
        tabla = input("📋 Nombre de la tabla: ").strip()
        query = f"SELECT COUNT(*) as total FROM {tabla};"
        ejecutar_query_personalizado(cursor, query)

    elif opcion == "2":
        tabla = input("📋 Nombre de la tabla: ").strip()
        query = f"SELECT * FROM {tabla} LIMIT 10;"
        ejecutar_query_personalizado(cursor, query)

    elif opcion == "3":
        tabla = input("📋 Nombre de la tabla: ").strip()
        query = f"SELECT * FROM {tabla} ORDER BY id DESC LIMIT 10;"
        ejecutar_query_personalizado(cursor, query)

    elif opcion == "4":
        tabla = input("📋 Nombre de la tabla: ").strip()
        columna = input("📋 Nombre de la columna: ").strip()
        valor = input("🔍 Valor a buscar: ").strip()

        # Si es número, no usar comillas
        try:
            int(valor)
            query = f"SELECT * FROM {tabla} WHERE {columna} = {valor} LIMIT 20;"
        except:
            query = f"SELECT * FROM {tabla} WHERE {columna} ILIKE '%{valor}%' LIMIT 20;"

        ejecutar_query_personalizado(cursor, query)


if __name__ == "__main__":
    try:
        menu_principal()
    except KeyboardInterrupt:
        print("\n\n👋 Programa interrumpido. Adiós!")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback
        traceback.print_exc()
