"""
Script de Limpieza Completa de Base de Datos
VERSIÓN CORREGIDA - Usa misma conexión para verificar cambios
"""

import psycopg2

# URL de conexión de Railway
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def get_railway_connection():
    """Conecta directamente a Railway PostgreSQL"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        print("✅ Conectado a Railway PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Railway: {e}")
        raise


def mostrar_estado_bd(conn, titulo="Estado de la Base de Datos"):
    """Muestra el estado actual usando la conexión existente"""
    cursor = conn.cursor()

    print("\n" + "=" * 70)
    print(f"📊 {titulo}")
    print("=" * 70)

    tablas = [
        ('usuarios', 'Usuarios'),
        ('facturas', 'Facturas'),
        ('items_factura', 'Items Factura'),
        ('productos_maestros', 'Productos Maestros'),
        ('inventario_usuario', 'Inventario Usuario'),
        ('processing_jobs', 'Processing Jobs')
    ]

    for tabla, nombre in tablas:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]
            print(f"   {nombre:.<30} {count:>6} registros")
        except Exception as e:
            print(f"   {nombre:.<30} ERROR: {e}")

    print("=" * 70)


def limpiar_base_datos(conn):
    """Limpia completamente facturas, items y productos"""

    cursor = conn.cursor()

    try:
        print("\n🗑️ INICIANDO LIMPIEZA...")

        # 0. Limpiar processing_jobs primero
        cursor.execute("DELETE FROM processing_jobs")
        count = cursor.rowcount
        print(f"   ✅ Processing jobs eliminados ({count} registros)")

        # 1. Inventario usuario
        cursor.execute("DELETE FROM inventario_usuario")
        count = cursor.rowcount
        print(f"   ✅ Inventario usuario eliminado ({count} registros)")

        # 2. Items de facturas
        cursor.execute("DELETE FROM items_factura")
        count = cursor.rowcount
        print(f"   ✅ Items de facturas eliminados ({count} registros)")

        # 3. Facturas
        cursor.execute("DELETE FROM facturas")
        count = cursor.rowcount
        print(f"   ✅ Facturas eliminadas ({count} registros)")

        # 4. Productos maestros
        cursor.execute("DELETE FROM productos_maestros")
        count = cursor.rowcount
        print(f"   ✅ Productos maestros eliminados ({count} registros)")

        # 5. Resetear secuencias
        secuencias = [
            'facturas_id_seq',
            'items_factura_id_seq',
            'productos_maestros_id_seq',
            'inventario_usuario_id_seq'
        ]

        for seq in secuencias:
            try:
                cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                print(f"   ✅ Secuencia {seq} reseteada")
            except Exception as e:
                print(f"   ⚠️ No se pudo resetear {seq}: {e}")

        # ✅ CRÍTICO: Confirmar cambios
        conn.commit()
        print("\n💾 CAMBIOS GUARDADOS EN LA BASE DE DATOS")
        print("✅ LIMPIEZA COMPLETADA EXITOSAMENTE")

    except Exception as e:
        print(f"\n❌ ERROR durante limpieza: {e}")
        conn.rollback()
        raise


def main():
    """Función principal con confirmación"""

    print("\n" + "=" * 70)
    print("⚠️  LIMPIEZA TOTAL DE BASE DE DATOS - RAILWAY POSTGRESQL")
    print("=" * 70)
    print("""
Este script eliminará:
  ❌ TODAS las facturas
  ❌ TODOS los items de facturas
  ❌ TODOS los productos maestros
  ❌ TODO el inventario de usuarios

Conservará:
  ✅ Usuarios
  ✅ Estructura de la base de datos
  ✅ Configuraciones

⚠️  ESTA ACCIÓN NO SE PUEDE DESHACER
""")

    # Conectar UNA SOLA VEZ
    conn = get_railway_connection()

    try:
        # Mostrar estado actual
        mostrar_estado_bd(conn, "ESTADO ACTUAL (ANTES DE LIMPIAR)")

        # Confirmación
        print("\n" + "=" * 70)
        confirmacion = input("Para continuar, escribe 'LIMPIAR TODO' (exacto): ")

        if confirmacion != "LIMPIAR TODO":
            print("\n❌ Operación CANCELADA - No se realizaron cambios")
            conn.close()
            return

        print("\n⚠️  ÚLTIMA CONFIRMACIÓN")
        confirmar2 = input("¿Estás 100% seguro? (sí/no): ")

        if confirmar2.lower() != "sí" and confirmar2.lower() != "si":
            print("\n❌ Operación CANCELADA - No se realizaron cambios")
            conn.close()
            return

        # Ejecutar limpieza
        limpiar_base_datos(conn)

        # Mostrar estado final (usando LA MISMA conexión)
        mostrar_estado_bd(conn, "ESTADO FINAL (DESPUÉS DE LIMPIAR)")

        print("\n" + "=" * 70)
        print("🎉 BASE DE DATOS LISTA PARA RE-ESCANEAR FACTURAS")
        print("=" * 70)
        print("""
Próximos pasos:
  1. ✅ Sistema corregido está en producción
  2. ✅ Base de datos limpia (verificado arriba ↑)
  3. 📸 Escanear todas las facturas nuevamente
  4. ✅ Los precios se guardarán CORRECTAMENTE

Ejemplo:
  - Factura dice: $284.220 → Se guarda: 284220 ✅
  - Factura dice: $12,456 → Se guarda: 12456 ✅
  - Factura dice: $512.352 → Se guarda: 512352 ✅

IMPORTANTE: Sin conversiones, sin divisiones, sin multiplicaciones.
Solo se limpian separadores (puntos y comas).
""")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()
        print("\n🔌 Conexión cerrada")


if __name__ == "__main__":
    main()
