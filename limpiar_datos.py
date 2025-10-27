"""
Script para limpiar DATOS basura de LecFac
Mantiene la estructura de las tablas, solo elimina registros
"""
import psycopg2
from database import get_db_connection

def limpiar_datos():
    """Elimina todos los datos de las tablas manteniendo la estructura"""

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        print("🧹 Iniciando limpieza de datos...")
        print("=" * 60)

        # Orden importante: primero tablas hijas, luego padres
        tablas = [
            "items_factura",           # Hija de facturas y productos_maestro
            "inventario",              # Hija de usuarios y productos_maestro
            "alertas_inventario",      # Hija de usuarios
            "presupuesto_mensual",     # Hija de usuarios
            "facturas",                # Hija de usuarios
            "productos_maestro",       # Tabla padre
            "usuarios"                 # Tabla padre
        ]

        for tabla in tablas:
            # Contar registros antes
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count_antes = cursor.fetchone()[0]

            # Limpiar datos
            cursor.execute(f"DELETE FROM {tabla}")

            # Reiniciar secuencia de IDs (si existe)
            try:
                cursor.execute(f"SELECT pg_get_serial_sequence('{tabla}', 'id')")
                secuencia = cursor.fetchone()
                if secuencia and secuencia[0]:
                    cursor.execute(f"ALTER SEQUENCE {secuencia[0]} RESTART WITH 1")
                    print(f"   ✓ Secuencia reiniciada para {tabla}")
            except:
                pass

            conn.commit()
            print(f"✅ {tabla:25} - {count_antes:4} registros eliminados")

        print("=" * 60)
        print("✅ LIMPIEZA COMPLETADA EXITOSAMENTE")
        print()
        print("🎯 Siguiente paso:")
        print("   1. Crea un usuario NUEVO desde el app móvil")
        print("   2. Escanea UNA factura limpia")
        print("   3. Verifica que producto_maestro_id NO sea NULL")
        print()

    except Exception as e:
        print(f"❌ Error durante limpieza: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print()
    respuesta = input("⚠️  ¿Estás SEGURO de eliminar TODOS los datos? (escribe 'SI' para confirmar): ")
    print()

    if respuesta.strip().upper() == "SI":
        limpiar_datos()
    else:
        print("❌ Limpieza cancelada")
