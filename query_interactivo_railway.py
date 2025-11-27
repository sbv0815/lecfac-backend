"""
query_interactivo_railway.py - Consultas SQL interactivas a Railway
========================================================================
Ejecuta cualquier query SQL y ve los resultados formateados
Incluye opción para resetear datos de usuario

INSTALACIÓN:
pip install psycopg2-binary tabulate python-dotenv

USO:
python query_interactivo_railway.py
"""

import os
import psycopg2
from tabulate import tabulate

# ═══════════════════════════════════════════════════════════════
# CONFIGURACIÓN - Actualiza con tu URL de Railway
# ═══════════════════════════════════════════════════════════════
# Intenta cargar desde .env primero
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# Prioridad: Variable de entorno > URL hardcodeada
DATABASE_URL = (
    os.environ.get("DATABASE_URL")
    or "postgresql://postgres:HsErCQoBVoqanRbuotsyMBYYUtbtMULP@maglev.proxy.rlwy.net:45644/railway"
)
# ═══════════════════════════════════════════════════════════════


def parsear_url(url):
    """Parsea la DATABASE_URL para extraer componentes"""
    url_parts = url.replace("postgresql://", "").split("@")
    user_pass = url_parts[0].split(":")
    host_port_db = url_parts[1].split("/")
    host_port = host_port_db[0].split(":")

    return {
        "user": user_pass[0],
        "password": user_pass[1],
        "host": host_port[0],
        "port": host_port[1],
        "database": host_port_db[1],
    }


def conectar():
    """Conecta a la base de datos"""
    try:
        config = parsear_url(DATABASE_URL)
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )
        conn.autocommit = True
        return conn, config
    except Exception as e:
        print(f"❌ Error conectando: {e}")
        print("\n💡 Asegúrate de:")
        print("   1. Habilitar Public Networking en Railway")
        print("   2. Actualizar DATABASE_URL en este script o en .env")
        return None, None


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

    print("\n" + "=" * 80)
    print("📋 TABLAS DISPONIBLES EN LA BASE DE DATOS")
    print("=" * 80)

    for idx, (tabla,) in enumerate(tablas, 1):
        print(f"{idx:2}. {tabla}")

    print("=" * 80)
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
    print("=" * 80)

    headers = ["Columna", "Tipo", "Nullable"]
    print(tabulate(columnas, headers=headers, tablefmt="grid"))


def ejecutar_query_personalizado(cursor, query):
    """Ejecuta un query personalizado y muestra resultados"""
    try:
        cursor.execute(query)

        if query.strip().upper().startswith("SELECT"):
            resultados = cursor.fetchall()

            if not resultados:
                print("\n⚠️  Query ejecutado correctamente pero no retornó resultados")
                return

            columnas = [desc[0] for desc in cursor.description]

            print("\n" + "=" * 80)
            print(f"✅ RESULTADOS ({len(resultados)} filas)")
            print("=" * 80)
            print(tabulate(resultados, headers=columnas, tablefmt="grid"))

        else:
            print("\n✅ Query ejecutado correctamente")

    except Exception as e:
        print(f"\n❌ Error ejecutando query: {e}")


def reset_datos_usuario(cursor, user_id=None):
    """
    Resetea los datos de un usuario específico o de todos
    Mantiene: usuarios, establecimientos, productos maestros
    Borra: facturas, items, inventario, historial, precios
    """
    print("\n" + "=" * 80)
    print("🔥 RESET DE DATOS DE USUARIO")
    print("=" * 80)

    if user_id is None:
        opcion = input("¿Borrar datos de TODOS los usuarios? (s/n): ").strip().lower()
        if opcion != "s":
            user_id = input("ID del usuario a resetear: ").strip()
            try:
                user_id = int(user_id)
            except:
                print("❌ ID inválido")
                return

    # Mostrar qué se va a borrar
    print("\n⚠️  Se borrarán:")
    print("   - Facturas e items de factura")
    print("   - Inventario de usuario")
    print("   - Historial de compras")
    print("   - Uso de API")
    print("   - Límites de usuario")

    if user_id:
        print(f"\n   Solo para usuario ID: {user_id}")
    else:
        print("\n   ¡PARA TODOS LOS USUARIOS!")

    print("\n✅ Se mantendrán:")
    print("   - Usuarios")
    print("   - Establecimientos")
    print("   - Productos maestros (catálogo)")
    print("   - Categorías")

    confirmacion = input("\n⚠️  Escribe 'CONFIRMAR' para continuar: ").strip()
    if confirmacion != "CONFIRMAR":
        print("❌ Cancelado")
        return

    try:
        where_clause = f"WHERE usuario_id = {user_id}" if user_id else ""
        where_clause_user = f"WHERE user_id = {user_id}" if user_id else ""

        # 1. Items de factura (depende de facturas)
        if user_id:
            cursor.execute(
                f"""
                DELETE FROM items_factura
                WHERE factura_id IN (SELECT id FROM facturas WHERE usuario_id = {user_id})
            """
            )
        else:
            cursor.execute("TRUNCATE TABLE items_factura CASCADE")
        print("   ✓ Items de factura borrados")

        # 2. Facturas
        if user_id:
            cursor.execute(f"DELETE FROM facturas {where_clause}")
        else:
            cursor.execute("TRUNCATE TABLE facturas CASCADE")
        print("   ✓ Facturas borradas")

        # 3. Inventario
        if user_id:
            cursor.execute(f"DELETE FROM inventario_usuario {where_clause}")
        else:
            cursor.execute("TRUNCATE TABLE inventario_usuario CASCADE")
        print("   ✓ Inventario borrado")

        # 4. Historial de compras
        try:
            if user_id:
                cursor.execute(f"DELETE FROM historial_compras_usuario {where_clause}")
            else:
                cursor.execute("TRUNCATE TABLE historial_compras_usuario CASCADE")
            print("   ✓ Historial de compras borrado")
        except Exception as e:
            print(f"   ⚠️ historial_compras_usuario: {e}")

        # 5. Uso de API
        try:
            if user_id:
                cursor.execute(f"DELETE FROM uso_api {where_clause_user}")
            else:
                cursor.execute("TRUNCATE TABLE uso_api CASCADE")
            print("   ✓ Uso de API borrado")
        except Exception as e:
            print(f"   ⚠️ uso_api: {e}")

        # 6. Límites de usuario (reset, no borrar)
        try:
            if user_id:
                cursor.execute(
                    f"""
                    UPDATE limites_usuario SET
                        tokens_usados_mes = 0,
                        facturas_usadas_mes = 0,
                        menus_usados_mes = 0
                    {where_clause_user}
                """
                )
            else:
                cursor.execute(
                    """
                    UPDATE limites_usuario SET
                        tokens_usados_mes = 0,
                        facturas_usadas_mes = 0,
                        menus_usados_mes = 0
                """
                )
            print("   ✓ Límites de usuario reseteados")
        except Exception as e:
            print(f"   ⚠️ limites_usuario: {e}")

        print("\n" + "=" * 80)
        print("✅ RESET COMPLETADO")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error durante reset: {e}")


def reset_completo(cursor):
    """
    Reset completo: borra TODO incluyendo productos
    Solo mantiene usuarios y establecimientos
    """
    print("\n" + "=" * 80)
    print("🔥🔥🔥 RESET COMPLETO - BORRAR TODO 🔥🔥🔥")
    print("=" * 80)

    print("\n⚠️  Se borrarán:")
    print("   - TODAS las facturas e items")
    print("   - TODO el inventario")
    print("   - TODOS los productos maestros V2")
    print("   - TODOS los productos por establecimiento")
    print("   - TODO el historial de precios")
    print("   - TODAS las revisiones pendientes")
    print("   - TODAS las correcciones aprendidas")

    print("\n✅ Se mantendrán:")
    print("   - Usuarios")
    print("   - Establecimientos")
    print("   - Categorías")

    confirmacion = input("\n⚠️  Escribe 'SI BORRAR TODO' para continuar: ").strip()
    if confirmacion != "SI BORRAR TODO":
        print("❌ Cancelado")
        return

    try:
        tablas_a_truncar = [
            "items_factura",
            "facturas",
            "inventario_usuario",
            "historial_compras_usuario",
            "precios_productos",
            "precios_historicos",
            "productos_por_establecimiento",
            "productos_revision_admin",
            "correcciones_aprendidas",
            "productos_maestros_v2",
            "uso_api",
        ]

        for tabla in tablas_a_truncar:
            try:
                cursor.execute(f"TRUNCATE TABLE {tabla} CASCADE")
                print(f"   ✓ {tabla} borrada")
            except Exception as e:
                print(f"   ⚠️ {tabla}: {e}")

        # Reset secuencias
        secuencias = [
            "facturas_id_seq",
            "productos_maestros_v2_id_seq",
            "items_factura_id_seq",
            "inventario_usuario_id_seq",
        ]

        print("\n🔄 Reseteando secuencias...")
        for seq in secuencias:
            try:
                cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                print(f"   ✓ {seq} reseteada")
            except Exception as e:
                print(f"   ⚠️ {seq}: {e}")

        # Reset límites de usuario
        try:
            cursor.execute(
                """
                UPDATE limites_usuario SET
                    tokens_usados_mes = 0,
                    facturas_usadas_mes = 0,
                    menus_usados_mes = 0
            """
            )
            print("   ✓ Límites reseteados")
        except:
            pass

        print("\n" + "=" * 80)
        print("✅ RESET COMPLETO FINALIZADO")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error durante reset: {e}")


def ver_estadisticas(cursor):
    """Muestra estadísticas generales de la base de datos"""
    print("\n" + "=" * 80)
    print("📊 ESTADÍSTICAS DE LA BASE DE DATOS")
    print("=" * 80)

    consultas = [
        ("Usuarios", "SELECT COUNT(*) FROM usuarios"),
        ("Establecimientos", "SELECT COUNT(*) FROM establecimientos"),
        ("Facturas", "SELECT COUNT(*) FROM facturas"),
        ("Items de factura", "SELECT COUNT(*) FROM items_factura"),
        ("Productos Maestros V2", "SELECT COUNT(*) FROM productos_maestros_v2"),
        (
            "Productos por Establecimiento",
            "SELECT COUNT(*) FROM productos_por_establecimiento",
        ),
        ("Categorías", "SELECT COUNT(*) FROM categorias"),
        ("Inventario Usuario", "SELECT COUNT(*) FROM inventario_usuario"),
    ]

    for nombre, query in consultas:
        try:
            cursor.execute(query)
            resultado = cursor.fetchone()[0]
            print(f"   {nombre}: {resultado}")
        except Exception as e:
            print(f"   {nombre}: ⚠️ Error - {e}")

    print("=" * 80)


def menu_queries_rapidos(cursor):
    """Menú con queries predefinidos"""
    print("\n" + "=" * 80)
    print("⚡ QUERIES RÁPIDOS")
    print("=" * 80)
    print("1. Contar registros en una tabla")
    print("2. Ver primeros 10 registros de una tabla")
    print("3. Ver últimos 10 registros de una tabla (por ID)")
    print("4. Buscar en una tabla por columna")
    print("5. Volver al menú principal")
    print("=" * 80)

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

        try:
            int(valor)
            query = f"SELECT * FROM {tabla} WHERE {columna} = {valor} LIMIT 20;"
        except:
            query = f"SELECT * FROM {tabla} WHERE {columna} ILIKE '%{valor}%' LIMIT 20;"

        ejecutar_query_personalizado(cursor, query)


def menu_principal():
    """Menú principal interactivo"""
    print("\n" + "=" * 80)
    print("🗄️  CONSULTOR INTERACTIVO DE BASE DE DATOS RAILWAY")
    print("=" * 80)

    conn, config = conectar()
    if not conn:
        return

    print(f"Host: {config['host']}")
    print(f"Database: {config['database']}")
    print("=" * 80)

    cursor = conn.cursor()
    print("✅ Conexión exitosa\n")

    tablas = None

    while True:
        print("\n" + "=" * 80)
        print("MENÚ PRINCIPAL")
        print("=" * 80)
        print("1. 📋 Listar todas las tablas")
        print("2. 🔍 Ver estructura de una tabla")
        print("3. 💻 Ejecutar query personalizado")
        print("4. ⚡ Queries rápidos predefinidos")
        print("5. 📊 Ver estadísticas generales")
        print("6. 🔄 Reset datos de usuario")
        print("7. 🔥 Reset completo (borrar TODO)")
        print("8. 🚪 Salir")
        print("=" * 80)

        opcion = input("\n👉 Selecciona una opción (1-8): ").strip()

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
            print("\n" + "=" * 80)
            print("EJECUTAR QUERY PERSONALIZADO")
            print("=" * 80)
            print("Escribe tu query SQL (puede ser multilínea)")
            print("Cuando termines, escribe 'GO' en una línea nueva")
            print("Para cancelar, escribe 'CANCEL'")
            print("=" * 80)

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
            ver_estadisticas(cursor)

        elif opcion == "6":
            reset_datos_usuario(cursor)

        elif opcion == "7":
            reset_completo(cursor)

        elif opcion == "8":
            print("\n👋 Cerrando conexión...")
            cursor.close()
            conn.close()
            print("✅ Adiós!")
            break

        else:
            print("❌ Opción inválida")


if __name__ == "__main__":
    try:
        menu_principal()
    except KeyboardInterrupt:
        print("\n\n👋 Programa interrumpido. Adiós!")
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        import traceback

        traceback.print_exc()
