"""
Script para limpiar TODOS los datos de Railway PostgreSQL
Versión corregida con nombres reales de tablas
"""
import psycopg2

def limpiar_datos_railway():
    """Elimina todos los datos de Railway PostgreSQL"""

    database_url = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

    print(f"🔗 Conectando a Railway PostgreSQL...")

    try:
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()

        print("✅ Conexión exitosa a Railway")
        print("🧹 Iniciando limpieza de datos...")
        print("=" * 60)

        # DESACTIVAR foreign keys temporalmente
        cursor.execute("SET session_replication_role = 'replica';")
        conn.commit()
        print("🔓 Foreign keys desactivadas temporalmente")
        print()

        # ORDEN: Tablas hijas primero, luego padres
        tablas = [
            # Tablas hijas (dependientes)
            "items_factura",
            "inventario_usuario",
            "alertas_usuario",
            "presupuesto_usuario",
            "historial_compras_usuario",
            "patrones_compra",
            "gastos_mensuales",
            "precios_historicos",
            "precios_productos",
            "correcciones_productos",
            "codigos_locales",
            "matching_logs",
            "ocr_logs",
            "password_resets",
            "audit_logs",
            "processing_jobs",

            # Tablas padres
            "facturas",
            "productos_maestros",
            "productos_maestro",
            "productos_catalogo",
            "productos",
            "establecimientos",
            "usuarios"
        ]

        total_eliminados = 0

        for tabla in tablas:
            try:
                # Contar registros antes
                cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count_antes = cursor.fetchone()[0]

                if count_antes > 0:
                    # Limpiar datos
                    cursor.execute(f"DELETE FROM {tabla}")

                    # Reiniciar secuencia de IDs si existe
                    try:
                        cursor.execute(f"""
                            SELECT setval(
                                pg_get_serial_sequence('{tabla}', 'id'),
                                1,
                                false
                            )
                        """)
                    except:
                        pass  # Algunas tablas no tienen secuencias

                    conn.commit()
                    total_eliminados += count_antes
                    print(f"✅ {tabla:30} - {count_antes:5} registros eliminados")
                else:
                    print(f"⚪ {tabla:30} - Ya estaba vacía")

            except Exception as e:
                print(f"⚠️  {tabla:30} - Error: {str(e)[:50]}")
                conn.rollback()

        # REACTIVAR foreign keys
        cursor.execute("SET session_replication_role = 'origin';")
        conn.commit()
        print()
        print("🔒 Foreign keys reactivadas")

        print("=" * 60)
        print(f"✅ LIMPIEZA COMPLETADA - {total_eliminados} registros eliminados")
        print("=" * 60)
        print()
        print("🎯 Siguiente paso:")
        print("   1. Abre el app móvil")
        print("   2. Crea un usuario NUEVO desde cero")
        print("   3. Escanea UNA factura limpia")
        print("   4. Verifica logs en Railway → Deberías ver:")
        print("      ✅ Producto creado: ID=1")
        print("      ✅ Producto creado: ID=2")
        print("      ✅ Inventario actualizado: 1 productos agregados")
        print()

    except Exception as e:
        print(f"❌ Error de conexión: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    print()
    print("⚠️  ADVERTENCIA FINAL: Esto eliminará TODOS los datos de Railway PostgreSQL")
    print()
    print("   Se eliminarán:")
    print("   - 3 usuarios")
    print("   - 34 facturas")
    print("   - 136 productos maestros")
    print("   - 127 precios")
    print("   - 64 items de inventario")
    print("   - Todo el historial")
    print()
    print("   La base de datos quedará COMPLETAMENTE LIMPIA")
    print()
    respuesta = input("¿Estás ABSOLUTAMENTE SEGURO? (escribe 'SI ELIMINAR TODO'): ")
    print()

    if respuesta.strip() == "SI ELIMINAR TODO":
        limpiar_datos_railway()
    else:
        print("❌ Limpieza cancelada")
        print("   (Debes escribir exactamente: SI ELIMINAR TODO)")
