"""
Script de Limpieza Completa de Base de Datos
Elimina TODAS las facturas, items y productos
CONSERVA usuarios y estructura
"""

from database import get_db_connection

def mostrar_estado_bd(titulo="Estado de la Base de Datos"):
    """Muestra el estado actual de la BD"""
    conn = get_db_connection()
    cursor = conn.cursor()

    print("\n" + "=" * 70)
    print(f"📊 {titulo}")
    print("=" * 70)

    tablas = [
        ('usuarios', 'Usuarios'),
        ('facturas', 'Facturas'),
        ('items_factura', 'Items Factura'),
        ('productos_maestros', 'Productos Maestros'),
        ('inventario_personal', 'Inventario Personal')
    ]

    for tabla, nombre in tablas:
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        count = cursor.fetchone()[0]
        print(f"   {nombre:.<30} {count:>6} registros")

    conn.close()
    print("=" * 70)


def limpiar_base_datos():
    """Limpia completamente facturas, items y productos"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("\n🗑️ INICIANDO LIMPIEZA...")

        # 1. Inventario personal
        cursor.execute("DELETE FROM inventario_personal")
        print("   ✅ Inventario personal eliminado")

        # 2. Items de facturas
        cursor.execute("DELETE FROM items_factura")
        print("   ✅ Items de facturas eliminados")

        # 3. Facturas
        cursor.execute("DELETE FROM facturas")
        print("   ✅ Facturas eliminadas")

        # 4. Productos maestros
        cursor.execute("DELETE FROM productos_maestros")
        print("   ✅ Productos maestros eliminados")

        # 5. Resetear secuencias
        secuencias = [
            'facturas_id_seq',
            'items_factura_id_seq',
            'productos_maestros_id_seq',
            'inventario_personal_id_seq'
        ]

        for seq in secuencias:
            cursor.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")

        print("   ✅ Secuencias reseteadas (IDs vuelven a 1)")

        # Confirmar cambios
        conn.commit()
        print("\n✅ LIMPIEZA COMPLETADA EXITOSAMENTE")

    except Exception as e:
        print(f"\n❌ ERROR durante limpieza: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()


def main():
    """Función principal con confirmación"""

    print("\n" + "=" * 70)
    print("⚠️  LIMPIEZA TOTAL DE BASE DE DATOS")
    print("=" * 70)
    print("""
Este script eliminará:
  ❌ TODAS las facturas
  ❌ TODOS los items de facturas
  ❌ TODOS los productos maestros
  ❌ TODO el inventario personal

Conservará:
  ✅ Usuarios
  ✅ Estructura de la base de datos
  ✅ Configuraciones

⚠️  ESTA ACCIÓN NO SE PUEDE DESHACER
""")

    # Mostrar estado actual
    mostrar_estado_bd("ESTADO ACTUAL (ANTES DE LIMPIAR)")

    # Confirmación
    print("\n" + "=" * 70)
    confirmacion = input("Para continuar, escribe 'LIMPIAR TODO' (exacto): ")

    if confirmacion != "LIMPIAR TODO":
        print("\n❌ Operación CANCELADA - No se realizaron cambios")
        return

    print("\n⚠️  ÚLTIMA CONFIRMACIÓN")
    confirmar2 = input("¿Estás 100% seguro? (sí/no): ")

    if confirmar2.lower() != "sí" and confirmar2.lower() != "si":
        print("\n❌ Operación CANCELADA - No se realizaron cambios")
        return

    # Ejecutar limpieza
    limpiar_base_datos()

    # Mostrar estado final
    mostrar_estado_bd("ESTADO FINAL (DESPUÉS DE LIMPIAR)")

    print("\n" + "=" * 70)
    print("🎉 BASE DE DATOS LISTA PARA RE-ESCANEAR FACTURAS")
    print("=" * 70)
    print("""
Próximos pasos:
  1. ✅ Sistema corregido está en producción
  2. ✅ Base de datos limpia
  3. 📸 Escanear todas las facturas nuevamente
  4. ✅ Los precios se guardarán CORRECTAMENTE (sin divisiones)

Ejemplo:
  - Factura dice: $284.220 → Se guarda: 284220 ✅
  - Factura dice: $12,456 → Se guarda: 12456 ✅
""")


if __name__ == "__main__":
    main()
