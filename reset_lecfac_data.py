#!/usr/bin/env python3
"""
reset_lecfac_CASCADE.py
Script que borra TODOS los datos usando CASCADE para manejar foreign keys
"""

import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def reset_completo_cascade():
    """Hace el reset completo usando TRUNCATE CASCADE"""
    print("\n" + "=" * 70)
    print("🔥 RESET COMPLETO CON CASCADE")
    print("=" * 70)

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # Deshabilitar triggers temporalmente para evitar problemas
        print("\n⚠️  Deshabilitando triggers temporalmente...")
        cur.execute("SET session_replication_role = replica;")

        # Lista de tablas a borrar (en cualquier orden, CASCADE se encarga)
        tablas = [
            # Datos de facturas
            'items_factura',
            'facturas',

            # Precios e historial
            'precios_historicos_v2',
            'precios_historicos',
            'precios_productos',

            # Productos
            'productos_por_establecimiento',
            'codigos_establecimiento',  # ← NUEVA TABLA
            'codigos_locales',
            'codigos_normalizados',
            'codigos_alternativos',
            'variantes_nombres',
            'productos_variantes',
            'auditoria_productos',
            'historial_cambios_productos',
            'log_mejoras_nombres',

            # Inventario y patrones
            'inventario_usuario',
            'historial_compras_usuario',
            'patrones_compra',
            'gastos_mensuales',
            'alertas_usuario',
            'presupuesto_usuario',

            # Procesamiento
            'processing_jobs',
            'ocr_logs',
            'matching_logs',
            'correcciones_productos',

            # NO BORRAR: usuarios, productos_maestros, productos_maestros_v2, establecimientos
        ]

        print(f"\n🗑️  Borrando {len(tablas)} tablas...\n")

        total_borrados = 0

        for tabla in tablas:
            try:
                # Verificar si existe
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """, (tabla,))

                if not cur.fetchone()[0]:
                    print(f"   ⚠️  {tabla}: no existe")
                    continue

                # Contar registros antes
                cur.execute(f"SELECT COUNT(*) FROM {tabla}")
                antes = cur.fetchone()[0]

                # TRUNCATE con CASCADE (borra todo sin problemas de FK)
                cur.execute(f"TRUNCATE TABLE {tabla} CASCADE")

                total_borrados += antes
                print(f"   ✅ {tabla}: {antes} registros eliminados")

            except Exception as e:
                print(f"   ❌ {tabla}: {str(e)}")
                conn.rollback()
                continue

        # Resetear contadores de usuarios
        print("\n👥 Reseteando contadores de usuarios...")
        try:
            cur.execute("""
                UPDATE usuarios
                SET facturas_aportadas = 0,
                    productos_aportados = 0,
                    puntos_contribucion = 0
            """)
            print("   ✅ Contadores reseteados")
        except Exception as e:
            print(f"   ❌ Error: {e}")

        # Resetear secuencias
        print("\n🔄 Reseteando secuencias...")
        secuencias = [
            'facturas_id_seq',
            'items_factura_id_seq',
            'precios_productos_id_seq',
            'inventario_usuario_id_seq',
            'productos_por_establecimiento_id_seq',
            'precios_historicos_v2_id_seq',
            'codigos_establecimiento_id_seq',  # ← NUEVA
        ]

        for seq in secuencias:
            try:
                cur.execute(f"SELECT EXISTS (SELECT FROM pg_class WHERE relname = %s)", (seq,))
                if cur.fetchone()[0]:
                    cur.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
                    print(f"   ✅ {seq}")
            except Exception as e:
                print(f"   ⚠️  {seq}: {e}")

        # Habilitar triggers de nuevo
        print("\n✅ Habilitando triggers...")
        cur.execute("SET session_replication_role = DEFAULT;")

        # Commit final
        conn.commit()
        print(f"\n{'=' * 70}")
        print(f"✅ RESET COMPLETADO: {total_borrados} registros eliminados")
        print(f"{'=' * 70}")

    except Exception as e:
        print(f"\n❌ ERROR GENERAL: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()


def verificar_estado():
    """Verifica el estado final de la BD"""
    print("\n📊 ESTADO FINAL DE LA BASE DE DATOS")
    print("=" * 70)

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    tablas = [
        # Tablas que deben tener datos
        ('usuarios', True),
        ('establecimientos', True),
        ('productos_maestros_v2', True),

        # Tablas que deben estar vacías
        ('facturas', False),
        ('items_factura', False),
        ('inventario_usuario', False),
        ('precios_historicos_v2', False),
        ('productos_por_establecimiento', False),
        ('codigos_establecimiento', False),
    ]

    print(f"{'Tabla':<35} {'Registros':>10} {'Estado':>10}")
    print("-" * 70)

    for tabla, debe_tener_datos in tablas:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cur.fetchone()[0]

            if debe_tener_datos:
                estado = "✅" if count > 0 else "⚠️ VACÍA"
            else:
                estado = "✅ VACÍA" if count == 0 else "❌ CON DATOS"

            print(f"{tabla:<35} {count:>10} {estado:>15}")
        except:
            print(f"{tabla:<35} {'ERROR':>10} {'❌':>15}")

    print("-" * 70)

    cur.close()
    conn.close()


def main():
    print("\n" + "=" * 70)
    print("🔥 LECFAC - RESET TOTAL CON CASCADE")
    print("=" * 70)
    print("\nEste script borrará:")
    print("  ✅ Todas las facturas")
    print("  ✅ Todos los items de factura")
    print("  ✅ Todo el inventario")
    print("  ✅ Todos los precios históricos")
    print("  ✅ Todos los códigos PLU")
    print("\nPero mantendrá:")
    print("  ✅ Usuarios")
    print("  ✅ Establecimientos")
    print("  ✅ Productos maestros")

    respuesta = input("\n⚠️  ¿Continuar? (escribe 'SI BORRAR TODO'): ")

    if respuesta != 'SI BORRAR TODO':
        print("❌ Cancelado")
        return

    reset_completo_cascade()
    verificar_estado()

    print("\n✨ ¡Base de datos reseteada!")
    print("\n📱 Ahora puedes:")
    print("  1. Escanear una factura nueva desde la app")
    print("  2. Ver los PLUs guardarse automáticamente")
    print("  3. Ver el inventario con precios correctos")


if __name__ == "__main__":
    main()
