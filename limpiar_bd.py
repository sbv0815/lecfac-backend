"""
Script de Limpieza: Reset de tablas para empezar fresh
Elimina todos los datos de facturas, items y productos, pero MANTIENE los usuarios
"""

import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import urlparse

# Cargar variables de entorno
load_dotenv()

def conectar_db():
    """Conectar a la base de datos de Railway usando DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL no encontrada en las variables de entorno")

    # Parsear la URL como en database.py
    url = urlparse(database_url)

    return psycopg2.connect(
        host=url.hostname,
        database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port or 5432,
        connect_timeout=10,
        sslmode='prefer'
    )

def contar_registros(cursor):
    """Cuenta los registros en cada tabla"""
    tablas = [
        'usuarios',
        'facturas',
        'items_factura',
        'productos_maestros',
        'inventario_usuario',
        'processing_jobs',
        'precios_productos'
    ]

    conteos = {}

    for tabla in tablas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            conteos[tabla] = cursor.fetchone()[0]
        except Exception as e:
            conteos[tabla] = f"Error: {e}"

    return conteos

def mostrar_estado_actual(cursor):
    """Muestra el estado actual de la base de datos"""
    print("=" * 80)
    print("📊 ESTADO ACTUAL DE LA BASE DE DATOS")
    print("=" * 80)
    print()

    conteos = contar_registros(cursor)

    print("Registros por tabla:")
    print("-" * 80)
    for tabla, count in conteos.items():
        if isinstance(count, int):
            emoji = "👥" if tabla == "usuarios" else "📦"
            print(f"{emoji} {tabla:25} {count:>10} registros")
        else:
            print(f"⚠️  {tabla:25} {count}")
    print()

def limpiar_tablas(cursor):
    """Limpia todas las tablas EXCEPTO usuarios"""
    print("=" * 80)
    print("🧹 LIMPIANDO TABLAS")
    print("=" * 80)
    print()

    # Orden de limpieza (respetando foreign keys)
    tablas_a_limpiar = [
        ('processing_jobs', 'Jobs de procesamiento'),
        ('precios_productos', 'Historial de precios'),
        ('inventario_usuario', 'Inventarios personales'),
        ('items_factura', 'Items de facturas'),
        ('facturas', 'Facturas'),
        ('productos_maestros', 'Catálogo de productos')
    ]

    resultados = []

    for tabla, descripcion in tablas_a_limpiar:
        try:
            cursor.execute(f"DELETE FROM {tabla}")
            registros_eliminados = cursor.rowcount
            print(f"✅ {descripcion} ({tabla}): {registros_eliminados} registros eliminados")
            resultados.append((tabla, registros_eliminados, True))
        except Exception as e:
            print(f"❌ {descripcion} ({tabla}): Error - {e}")
            resultados.append((tabla, 0, False))

    print()
    return resultados

def resetear_secuencias(conn, cursor):
    """Resetea las secuencias (IDs) de las tablas limpiadas"""
    print("=" * 80)
    print("🔄 RESETEANDO SECUENCIAS DE IDs")
    print("=" * 80)
    print()

    tablas = [
        'facturas',
        'items_factura',
        'productos_maestros',
        'inventario_usuario'
    ]

    for tabla in tablas:
        try:
            # Verificar si la secuencia existe
            cursor.execute(f"""
                SELECT EXISTS (
                    SELECT 1 FROM pg_class
                    WHERE relname = '{tabla}_id_seq'
                )
            """)
            existe = cursor.fetchone()[0]

            if existe:
                cursor.execute(f"ALTER SEQUENCE {tabla}_id_seq RESTART WITH 1")
                conn.commit()  # Commit individual para cada secuencia
                print(f"✅ Secuencia {tabla}_id_seq reseteada a 1")
            else:
                print(f"⚠️  {tabla}: Secuencia no existe (probablemente sin ID autoincremental)")
        except Exception as e:
            print(f"⚠️  {tabla}: {e}")
            conn.rollback()  # Rollback solo de esta operación

    print()

def main():
    """Función principal"""
    print("=" * 80)
    print("🧹 LIMPIEZA COMPLETA DE BASE DE DATOS")
    print("=" * 80)
    print()

    try:
        # Conectar a la base de datos
        print("📡 Conectando a la base de datos...")
        conn = conectar_db()
        cursor = conn.cursor()
        print("✅ Conexión establecida")
        print()

        # Mostrar estado actual
        mostrar_estado_actual(cursor)

        # Advertencia
        print("=" * 80)
        print("⚠️  ADVERTENCIA IMPORTANTE")
        print("=" * 80)
        print("Esta operación eliminará PERMANENTEMENTE:")
        print()
        print("  ❌ Todas las facturas")
        print("  ❌ Todos los items de facturas")
        print("  ❌ Todo el catálogo de productos")
        print("  ❌ Todos los inventarios personales")
        print("  ❌ Todo el historial de precios")
        print("  ❌ Todos los jobs de procesamiento")
        print()
        print("PERO MANTENDRÁ:")
        print("  ✅ Todos los usuarios y sus contraseñas")
        print()
        print("Esto NO se puede deshacer.")
        print("=" * 80)
        print()

        # Pedir confirmación
        respuesta = input("¿Deseas continuar? (escribe 'LIMPIAR' para confirmar): ").strip()

        if respuesta != 'LIMPIAR':
            print()
            print("❌ Operación cancelada por el usuario")
            cursor.close()
            conn.close()
            return

        print()

        # Limpiar tablas
        resultados = limpiar_tablas(cursor)

        # Commit después de limpiar
        print("💾 Guardando limpieza...")
        conn.commit()
        print("✅ Tablas limpiadas")
        print()

        # Resetear secuencias
        resetear_secuencias(conn, cursor)

        # Verificar que usuarios NO fue afectado
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        usuarios_count = cursor.fetchone()[0]

        if usuarios_count > 0:
            print("=" * 80)
            print("✅ VERIFICACIÓN")
            print("=" * 80)
            print(f"Usuarios mantenidos: {usuarios_count}")
            print("✅ Los usuarios NO fueron afectados")
            print()

        # Estado final
        print("=" * 80)
        print("📊 ESTADO FINAL")
        print("=" * 80)
        print()
        mostrar_estado_actual(cursor)

        print("=" * 80)
        print("✅ LIMPIEZA COMPLETADA")
        print("=" * 80)
        print()
        print("La base de datos está lista para empezar fresh.")
        print("Ahora puedes escanear facturas nuevamente y ver cómo se comporta.")
        print()
        print("📝 Próximos pasos:")
        print("  1. Escanea una factura de JUMBO (tiene códigos EAN)")
        print("  2. Verifica que los productos se crean correctamente")
        print("  3. Verifica que los códigos EAN están en la columna correcta")
        print("  4. Verifica que el inventario se actualiza automáticamente")
        print()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
