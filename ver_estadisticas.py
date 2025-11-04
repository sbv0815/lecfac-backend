"""
Script para Ver Estadísticas - Conectado a Railway PostgreSQL
==============================================================
"""

import psycopg

def mostrar_estadisticas():
    """Muestra estadísticas actuales de Railway PostgreSQL"""

    # URL de conexión de Railway
    DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        print("=" * 70)
        print("📊 ESTADÍSTICAS ACTUALES DEL SISTEMA - RAILWAY")
        print("=" * 70)

        # Usuarios
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0]
        print(f"\n👥 Total Usuarios: {total_usuarios}")

        if total_usuarios > 0:
            cursor.execute("SELECT id, nombre, email FROM usuarios ORDER BY id")
            usuarios = cursor.fetchall()
            for user in usuarios:
                print(f"   - Usuario {user[0]}: {user[1]} ({user[2]})")

        # Facturas por usuario
        print(f"\n📄 Facturas por usuario:")
        cursor.execute("""
            SELECT u.nombre, COUNT(f.id)
            FROM usuarios u
            LEFT JOIN facturas f ON f.usuario_id = u.id
            GROUP BY u.id, u.nombre
            ORDER BY u.id
        """)
        total_facturas_sistema = 0
        for user, count in cursor.fetchall():
            print(f"   - {user}: {count} facturas")
            total_facturas_sistema += count

        # Items por usuario
        print(f"\n📦 Items de facturas por usuario:")
        cursor.execute("""
            SELECT u.nombre, COUNT(i.id)
            FROM usuarios u
            LEFT JOIN items_factura i ON i.usuario_id = u.id
            GROUP BY u.id, u.nombre
            ORDER BY u.id
        """)
        total_items_sistema = 0
        for user, count in cursor.fetchall():
            print(f"   - {user}: {count} items")
            total_items_sistema += count

        # Verificar si existe tabla inventario_usuario
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'inventario_usuario'
            )
        """)
        tiene_inventario = cursor.fetchone()[0]

        if tiene_inventario:
            print(f"\n📋 Inventarios por usuario:")
            cursor.execute("""
                SELECT u.nombre, COUNT(i.id)
                FROM usuarios u
                LEFT JOIN inventario_usuario i ON i.usuario_id = u.id
                GROUP BY u.id, u.nombre
                ORDER BY u.id
            """)
            total_inventarios = 0
            for user, count in cursor.fetchall():
                print(f"   - {user}: {count} productos en inventario")
                total_inventarios += count
        else:
            print(f"\n📋 Tabla inventario_usuario no existe")
            total_inventarios = 0

        # Productos maestros
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        total_productos = cursor.fetchone()[0]
        print(f"\n✅ Productos maestros (CATÁLOGO): {total_productos}")

        # Precios históricos
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'precios_productos'
            )
        """)
        tiene_precios = cursor.fetchone()[0]

        if tiene_precios:
            cursor.execute("SELECT COUNT(*) FROM precios_productos")
            total_precios = cursor.fetchone()[0]
            print(f"💰 Precios históricos: {total_precios}")
        else:
            total_precios = 0

        # Resumen
        print(f"\n" + "=" * 70)
        print(f"📊 RESUMEN:")
        print(f"   Total usuarios: {total_usuarios}")
        print(f"   Total facturas en sistema: {total_facturas_sistema}")
        print(f"   Total items en sistema: {total_items_sistema}")
        print(f"   Total productos en inventarios: {total_inventarios}")
        print(f"   Productos en catálogo maestro: {total_productos}")
        if tiene_precios:
            print(f"   Precios históricos registrados: {total_precios}")
        print("=" * 70)

        # Información adicional si hay datos
        if total_facturas_sistema > 0:
            print(f"\n💡 ESTADO: Sistema CON datos de usuarios")
            print(f"   Se puede proceder con la limpieza si lo deseas")
        else:
            print(f"\n💡 ESTADO: Sistema SIN datos de usuarios")
            print(f"   No es necesario hacer limpieza")

        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    mostrar_estadisticas()
