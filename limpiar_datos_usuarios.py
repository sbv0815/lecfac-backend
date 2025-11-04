"""
Script de Limpieza de Datos de Usuarios - Railway PostgreSQL
=============================================================
Borra TODA la información de facturas, items e inventarios de usuarios
Mantiene: productos_maestros, usuarios, establecimientos

IMPORTANTE: Este script borra datos de los 3 usuarios pero mantiene sus cuentas
"""

import psycopg

# URL de conexión de Railway
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"


def confirmar_limpieza():
    """Solicita confirmación antes de borrar datos"""
    print("\n" + "=" * 70)
    print("⚠️  ADVERTENCIA: LIMPIEZA DE DATOS DE USUARIOS")
    print("=" * 70)
    print("\n📋 Se borrarán:")
    print("   ❌ Todas las facturas (12 facturas)")
    print("   ❌ Todos los items de facturas (513 items)")
    print("   ❌ Todos los inventarios de usuarios (290 productos)")
    print("   ❌ Todos los precios históricos")
    print("   ❌ Todas las alertas de stock")
    print("\n✅ Se mantendrán:")
    print("   ✓ productos_maestros (338 productos en catálogo)")
    print("   ✓ usuarios (Santiago, Victoria, Margarita podrán seguir logueándose)")
    print("   ✓ establecimientos")
    print("\n" + "=" * 70)

    respuesta = input("\n¿Estás seguro de continuar? (escribe 'SI' para confirmar): ")

    return respuesta.strip().upper() == "SI"


def limpiar_datos_usuarios():
    """
    Limpia TODOS los datos de usuarios manteniendo productos maestros
    """

    if not confirmar_limpieza():
        print("\n❌ Limpieza cancelada por el usuario")
        return False

    print("\n🔄 Iniciando limpieza de datos...")

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa")

        # Contar registros antes de borrar
        print("\n📊 Contando registros actuales...")

        cursor.execute("SELECT COUNT(*) FROM facturas")
        count_facturas = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM items_factura")
        count_items = cursor.fetchone()[0]

        # Verificar si existe tabla inventario_usuario
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'inventario_usuario'
            )
        """)
        tiene_inventarios = cursor.fetchone()[0]

        count_inventarios = 0
        if tiene_inventarios:
            cursor.execute("SELECT COUNT(*) FROM inventario_usuario")
            count_inventarios = cursor.fetchone()[0]

        # Verificar si existe tabla precios_productos
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'precios_productos'
            )
        """)
        tiene_precios = cursor.fetchone()[0]

        count_precios = 0
        if tiene_precios:
            cursor.execute("SELECT COUNT(*) FROM precios_productos")
            count_precios = cursor.fetchone()[0]

        # Verificar si existe tabla alertas_usuario
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'alertas_usuario'
            )
        """)
        tiene_alertas = cursor.fetchone()[0]

        count_alertas = 0
        if tiene_alertas:
            cursor.execute("SELECT COUNT(*) FROM alertas_usuario")
            count_alertas = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        count_productos = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM usuarios")
        count_usuarios = cursor.fetchone()[0]

        print(f"\n📊 Registros encontrados:")
        print(f"   📄 Facturas: {count_facturas}")
        print(f"   📦 Items de facturas: {count_items}")
        if tiene_inventarios:
            print(f"   📋 Inventarios: {count_inventarios}")
        if tiene_precios:
            print(f"   💰 Precios históricos: {count_precios}")
        if tiene_alertas:
            print(f"   🔔 Alertas de stock: {count_alertas}")
        print(f"   ✅ Productos maestros (NO se borran): {count_productos}")
        print(f"   👥 Usuarios (NO se borran): {count_usuarios}")

        # Última confirmación
        print("\n" + "=" * 70)
        respuesta_final = input("¿Confirmas el borrado de estos datos? (escribe 'CONFIRMO'): ")

        if respuesta_final.strip().upper() != "CONFIRMO":
            print("\n❌ Limpieza cancelada")
            conn.close()
            return False

        print("\n🗑️  Borrando datos...")

        # ORDEN IMPORTANTE: Borrar en orden inverso de dependencias

        # 1. Alertas de usuario (si existe)
        if tiene_alertas:
            print("   🔔 Borrando alertas de usuario...")
            cursor.execute("DELETE FROM alertas_usuario")
            print(f"      ✅ {cursor.rowcount} alertas borradas")

        # 2. Inventarios de usuarios (si existe)
        if tiene_inventarios:
            print("   📋 Borrando inventarios de usuarios...")
            cursor.execute("DELETE FROM inventario_usuario")
            print(f"      ✅ {cursor.rowcount} inventarios borrados")

        # 3. Precios históricos (si existe)
        if tiene_precios:
            print("   💰 Borrando precios históricos...")
            cursor.execute("DELETE FROM precios_productos")
            print(f"      ✅ {cursor.rowcount} precios borrados")

        # 4. Items de facturas (tiene FK a facturas)
        print("   📦 Borrando items de facturas...")
        cursor.execute("DELETE FROM items_factura")
        print(f"      ✅ {cursor.rowcount} items borrados")

        # 5. Processing jobs (tiene FK a facturas) - NUEVO
        print("   🔧 Borrando processing jobs...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'processing_jobs'
            )
        """)
        tiene_jobs = cursor.fetchone()[0]

        if tiene_jobs:
            cursor.execute("DELETE FROM processing_jobs")
            print(f"      ✅ {cursor.rowcount} jobs borrados")
        else:
            print(f"      ⚠️ Tabla processing_jobs no existe")

        # 6. Facturas
        print("   📄 Borrando facturas...")
        cursor.execute("DELETE FROM facturas")
        print(f"      ✅ {cursor.rowcount} facturas borradas")

        # Commit de todos los cambios
        conn.commit()

        print("\n" + "=" * 70)
        print("✅ LIMPIEZA COMPLETADA EXITOSAMENTE")
        print("=" * 70)
        print("\n📊 Estado final:")

        # Verificar que todo se borró
        cursor.execute("SELECT COUNT(*) FROM facturas")
        print(f"   📄 Facturas: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM items_factura")
        print(f"   📦 Items: {cursor.fetchone()[0]}")

        if tiene_inventarios:
            cursor.execute("SELECT COUNT(*) FROM inventario_usuario")
            print(f"   📋 Inventarios: {cursor.fetchone()[0]}")

        if tiene_precios:
            cursor.execute("SELECT COUNT(*) FROM precios_productos")
            print(f"   💰 Precios: {cursor.fetchone()[0]}")

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        print(f"   ✅ Productos maestros: {cursor.fetchone()[0]} (mantenidos)")

        cursor.execute("SELECT COUNT(*) FROM usuarios")
        print(f"   👥 Usuarios: {cursor.fetchone()[0]} (mantenidos)")

        print("\n🎯 Sistema listo para re-escanear facturas con el nuevo OCR")
        print("   ✅ Sin duplicados automáticos")
        print("   ✅ Con normalización de códigos mejorada")
        print("   ✅ Con detección de duplicados en facturas")
        print("   ✅ Datos limpios desde cero")
        print("\n" + "=" * 70)

        conn.close()
        return True

    except Exception as e:
        print(f"\n❌ Error durante la limpieza: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        return False


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🧹 SISTEMA DE LIMPIEZA DE DATOS - LECFAC (RAILWAY)")
    print("=" * 70)

    exito = limpiar_datos_usuarios()

    if exito:
        print("\n✅ Puedes empezar a escanear facturas nuevamente")
        print("   Los 3 usuarios pueden loguearse con sus credenciales")
        print("   Todas las mejoras están activas:")
        print("      - Detección automática de duplicados ✅")
        print("      - Normalización de códigos ✅")
        print("      - Matching inteligente ✅")
