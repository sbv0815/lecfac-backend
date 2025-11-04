"""
LIMPIEZA TOTAL - TODOS LOS USUARIOS
Borra TODAS las facturas, items e inventarios de TODOS los usuarios
Mantiene: productos_maestros, usuarios, establecimientos
"""

import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def limpieza_total():
    print("\n" + "=" * 80)
    print("🧹 LIMPIEZA TOTAL - TODOS LOS USUARIOS")
    print("=" * 80)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conectado a PostgreSQL")

        # Ver qué hay antes de borrar
        print("\n📊 DATOS ACTUALES:")

        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM items_factura")
        total_items = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM inventario_usuario")
        total_inventario = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM precios_productos")
        total_precios = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM processing_jobs")
        total_jobs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        total_productos = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM establecimientos")
        total_establecimientos = cursor.fetchone()[0]

        print(f"\n   SE BORRARÁN:")
        print(f"   ❌ {total_facturas} facturas")
        print(f"   ❌ {total_items} items de facturas")
        print(f"   ❌ {total_inventario} productos en inventarios")
        print(f"   ❌ {total_precios} precios históricos")
        print(f"   ❌ {total_jobs} processing jobs")

        print(f"\n   SE MANTENDRÁN:")
        print(f"   ✅ {total_productos} productos_maestros (catálogo)")
        print(f"   ✅ {total_usuarios} usuarios (cuentas activas)")
        print(f"   ✅ {total_establecimientos} establecimientos")

        # Confirmar
        print("\n" + "=" * 80)
        print("⚠️  ADVERTENCIA: Esta acción NO se puede deshacer")
        print("=" * 80)
        respuesta = input("\n¿Estás COMPLETAMENTE seguro? (escribe 'BORRAR TODO'): ")

        if respuesta.strip().upper() != 'BORRAR TODO':
            print("\n❌ Cancelado - No se borró nada")
            conn.close()
            return False

        print("\n🗑️  Borrando datos...")

        # ORDEN CORRECTO: De dependencias hacia arriba

        # 1. Alertas de usuario
        print("\n1️⃣  Borrando alertas...")
        try:
            cursor.execute("DELETE FROM alertas_usuario")
            print(f"   ✅ {cursor.rowcount} alertas borradas")
        except Exception as e:
            print(f"   ⚠️  Tabla alertas_usuario no existe o error: {e}")

        # 2. Inventarios
        print("\n2️⃣  Borrando inventarios...")
        cursor.execute("DELETE FROM inventario_usuario")
        print(f"   ✅ {cursor.rowcount} productos de inventario borrados")

        # 3. Precios históricos
        print("\n3️⃣  Borrando precios históricos...")
        cursor.execute("DELETE FROM precios_productos")
        print(f"   ✅ {cursor.rowcount} precios borrados")

        # 4. Processing jobs
        print("\n4️⃣  Borrando processing jobs...")
        cursor.execute("DELETE FROM processing_jobs")
        print(f"   ✅ {cursor.rowcount} jobs borrados")

        # 5. Items de facturas
        print("\n5️⃣  Borrando items de facturas...")
        cursor.execute("DELETE FROM items_factura")
        print(f"   ✅ {cursor.rowcount} items borrados")

        # 6. Facturas
        print("\n6️⃣  Borrando facturas...")
        cursor.execute("DELETE FROM facturas")
        print(f"   ✅ {cursor.rowcount} facturas borradas")

        # COMMIT
        conn.commit()

        # Verificar que todo se borró
        print("\n" + "=" * 80)
        print("📊 VERIFICACIÓN FINAL:")
        print("=" * 80)

        cursor.execute("SELECT COUNT(*) FROM facturas")
        print(f"\n   Facturas: {cursor.fetchone()[0]} (debe ser 0)")

        cursor.execute("SELECT COUNT(*) FROM items_factura")
        print(f"   Items: {cursor.fetchone()[0]} (debe ser 0)")

        cursor.execute("SELECT COUNT(*) FROM inventario_usuario")
        print(f"   Inventarios: {cursor.fetchone()[0]} (debe ser 0)")

        cursor.execute("SELECT COUNT(*) FROM precios_productos")
        print(f"   Precios: {cursor.fetchone()[0]} (debe ser 0)")

        cursor.execute("SELECT COUNT(*) FROM processing_jobs")
        print(f"   Jobs: {cursor.fetchone()[0]} (debe ser 0)")

        print(f"\n   ✅ Productos maestros: {total_productos} (MANTENIDOS)")
        print(f"   ✅ Usuarios: {total_usuarios} (MANTENIDOS)")
        print(f"   ✅ Establecimientos: {total_establecimientos} (MANTENIDOS)")

        print("\n" + "=" * 80)
        print("✅ LIMPIEZA TOTAL COMPLETADA EXITOSAMENTE")
        print("=" * 80)

        print("\n🎯 Sistema completamente limpio y listo para usar")
        print("\n📱 PRÓXIMOS PASOS:")
        print("   1. Cierra completamente la app Flutter")
        print("   2. Borra datos de la app (o reinstálala)")
        print("   3. Abre la app y haz login")
        print("   4. Escanea UNA factura UNA SOLA VEZ")
        print("   5. Verifica el inventario")

        print("\n⚠️  IMPORTANTE:")
        print("   • NO escanees la misma factura múltiples veces")
        print("   • NO cambies de usuario para escanear la misma factura")
        print("   • Cada factura física = 1 escaneo en el sistema")

        conn.close()
        return True

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        return False


def verificar_usuarios():
    """Muestra los usuarios que seguirán existiendo"""
    print("\n" + "=" * 80)
    print("👥 USUARIOS QUE SEGUIRÁN ACTIVOS:")
    print("=" * 80)

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, email, nombre, rol
            FROM usuarios
            ORDER BY id
        """)

        usuarios = cursor.fetchall()

        if usuarios:
            print("\n   Estos usuarios podrán seguir logueándose:")
            for u in usuarios:
                print(f"\n   👤 Usuario #{u[0]}")
                print(f"      Email: {u[1]}")
                print(f"      Nombre: {u[2]}")
                print(f"      Rol: {u[3]}")
        else:
            print("\n   ⚠️  No hay usuarios registrados")

        conn.close()

    except Exception as e:
        print(f"   ❌ Error verificando usuarios: {e}")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("🧹 SISTEMA DE LIMPIEZA TOTAL - LECFAC")
    print("=" * 80)

    # Mostrar usuarios primero
    verificar_usuarios()

    # Ejecutar limpieza
    exito = limpieza_total()

    if exito:
        print("\n" + "=" * 80)
        print("🎉 ¡LISTO! Sistema limpio y preparado")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("❌ La limpieza no se completó")
        print("=" * 80)
