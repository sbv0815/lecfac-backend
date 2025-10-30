"""
VERIFICACIÓN DE DATOS Y DASHBOARD
==================================
Script para ver dónde están guardados los datos y cómo actualizar el dashboard

Autor: Santiago
Fecha: 2025-10-30
"""
import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"


def ver_estructura_completa():
    """Muestra dónde está guardada TODA la información"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n" + "="*80)
    print("📊 ESTRUCTURA COMPLETA DE DATOS")
    print("="*80 + "\n")

    # ============================================
    # TABLA 1: productos_maestros (PRINCIPAL)
    # ============================================
    print("="*80)
    print("📦 TABLA 1: productos_maestros (LA MÁS IMPORTANTE)")
    print("="*80)
    print("Esta es la tabla PRINCIPAL que debe mostrar el dashboard\n")

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN marca IS NOT NULL THEN 1 END) as con_marca,
            COUNT(CASE WHEN categoria IS NOT NULL THEN 1 END) as con_categoria,
            COUNT(CASE WHEN subcategoria IS NOT NULL THEN 1 END) as con_subcategoria
        FROM productos_maestros
    """)

    stats = cur.fetchone()
    print(f"📊 ESTADÍSTICAS:")
    print(f"   Total productos: {stats[0]}")
    print(f"   Con marca: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
    print(f"   Con categoría: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
    print(f"   Con subcategoría: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)\n")

    print("📋 COLUMNAS DISPONIBLES:")
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'productos_maestros'
        ORDER BY ordinal_position
    """)

    for col, tipo in cur.fetchall():
        print(f"   • {col}: {tipo}")

    print("\n📝 EJEMPLOS DE PRODUCTOS (primeros 5):")
    cur.execute("""
        SELECT
            id,
            codigo_ean,
            nombre_normalizado,
            marca,
            categoria,
            subcategoria,
            precio_promedio_global,
            total_reportes
        FROM productos_maestros
        ORDER BY total_reportes DESC
        LIMIT 5
    """)

    for row in cur.fetchall():
        print(f"\n   ID: {row[0]}")
        print(f"   EAN: {row[1] or 'N/A'}")
        print(f"   Nombre: {row[2]}")
        print(f"   Marca: {row[3] or 'Sin marca'}")
        print(f"   Categoría: {row[4] or 'Sin categoría'}")
        print(f"   Subcategoría: {row[5] or 'Sin subcategoría'}")
        print(f"   Precio promedio: ${row[6]:,}" if row[6] else "   Precio: N/A")
        print(f"   Reportes: {row[7]}")

    # ============================================
    # TABLA 2: productos_referencia
    # ============================================
    print("\n\n" + "="*80)
    print("📚 TABLA 2: productos_referencia (BASE DE CONOCIMIENTO)")
    print("="*80)
    print("Esta tabla tiene los nombres 'oficiales' de productos\n")

    cur.execute("SELECT COUNT(*) FROM productos_referencia WHERE activo = TRUE")
    total_ref = cur.fetchone()[0]
    print(f"📊 Total productos de referencia activos: {total_ref}\n")

    print("📝 EJEMPLOS (primeros 5):")
    cur.execute("""
        SELECT
            codigo_ean,
            nombre_completo,
            marca,
            categoria,
            subcategoria
        FROM productos_referencia
        WHERE activo = TRUE
        LIMIT 5
    """)

    for row in cur.fetchall():
        print(f"\n   EAN: {row[0]}")
        print(f"   Nombre: {row[1]}")
        print(f"   Marca: {row[2] or 'N/A'}")
        print(f"   Categoría: {row[3] or 'N/A'}")
        print(f"   Subcategoría: {row[4] or 'N/A'}")

    # ============================================
    # TABLA 3: codigos_normalizados (MEMORIA)
    # ============================================
    print("\n\n" + "="*80)
    print("🧠 TABLA 3: codigos_normalizados (MEMORIA DEL SISTEMA)")
    print("="*80)
    print("Esta tabla guarda códigos aprendidos para matching rápido\n")

    cur.execute("SELECT COUNT(*) FROM codigos_normalizados")
    total_mem = cur.fetchone()[0]
    print(f"📊 Total códigos en memoria: {total_mem}\n")

    if total_mem > 0:
        print("📝 EJEMPLOS (primeros 5):")
        cur.execute("""
            SELECT
                codigo_leido,
                nombre_leido,
                producto_maestro_id,
                tipo_codigo,
                confianza,
                veces_usado
            FROM codigos_normalizados
            ORDER BY veces_usado DESC
            LIMIT 5
        """)

        for row in cur.fetchall():
            print(f"\n   Código leído: {row[0]}")
            print(f"   Nombre leído: {row[1]}")
            print(f"   Producto maestro ID: {row[2]}")
            print(f"   Tipo: {row[3]}")
            print(f"   Confianza: {row[4]:.0%}")
            print(f"   Veces usado: {row[5]}")

    # ============================================
    # TABLA 4: items_factura
    # ============================================
    print("\n\n" + "="*80)
    print("📋 TABLA 4: items_factura (PRODUCTOS EN FACTURAS)")
    print("="*80)
    print("Esta tabla conecta facturas con productos_maestros\n")

    cur.execute("""
        SELECT
            COUNT(*) as total,
            COUNT(CASE WHEN producto_maestro_id IS NOT NULL THEN 1 END) as con_producto
        FROM items_factura
    """)

    stats_items = cur.fetchone()
    print(f"📊 ESTADÍSTICAS:")
    print(f"   Total items: {stats_items[0]}")
    print(f"   Con producto_maestro_id: {stats_items[1]} ({stats_items[1]/stats_items[0]*100:.1f}%)")
    print(f"   Sin producto_maestro_id: {stats_items[0] - stats_items[1]}\n")

    # ============================================
    # RESUMEN PARA DASHBOARD
    # ============================================
    print("\n" + "="*80)
    print("📊 RESUMEN PARA DASHBOARD")
    print("="*80 + "\n")

    print("✅ TU DASHBOARD DEBE MOSTRAR: productos_maestros")
    print(f"   Total productos: {stats[0]}")
    print(f"   Con información completa: {stats[1]} productos\n")

    print("📝 COLUMNAS QUE DEBE MOSTRAR EL DASHBOARD:")
    print("   1. ID")
    print("   2. Código EAN")
    print("   3. Nombre (nombre_normalizado)")
    print("   4. Marca")
    print("   5. Categoría")
    print("   6. Subcategoría")
    print("   7. Precio promedio")
    print("   8. Total reportes")
    print("   9. Fecha última actualización\n")

    cur.close()
    conn.close()


def generar_query_dashboard():
    """Genera el query correcto para el dashboard"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("="*80)
    print("🔧 QUERY PARA TU DASHBOARD")
    print("="*80 + "\n")

    query = """
    SELECT
        pm.id,
        pm.codigo_ean,
        pm.nombre_normalizado,
        pm.marca,
        pm.categoria,
        pm.subcategoria,
        pm.precio_promedio_global,
        pm.total_reportes,
        pm.ultima_actualizacion,
        pm.primera_vez_reportado,
        -- Contar cuántos usuarios lo han comprado
        COUNT(DISTINCT if.usuario_id) as usuarios_compraron,
        -- Contar cuántas facturas lo incluyen
        COUNT(DISTINCT if.factura_id) as facturas_incluyen
    FROM productos_maestros pm
    LEFT JOIN items_factura if ON if.producto_maestro_id = pm.id
    GROUP BY pm.id
    ORDER BY pm.total_reportes DESC, pm.id DESC
    """

    print("📋 QUERY COMPLETO PARA dashboard.html:")
    print("-"*80)
    print(query)
    print("-"*80 + "\n")

    print("🔍 Ejecutando query de prueba (primeros 10 productos)...\n")

    cur.execute(query + " LIMIT 10")

    print("📊 RESULTADOS:")
    print("="*80)

    resultados = cur.fetchall()

    if not resultados:
        print("⚠️  No hay productos en la base de datos")
    else:
        for row in resultados:
            print(f"\nID: {row[0]}")
            print(f"EAN: {row[1] or 'N/A'}")
            print(f"Nombre: {row[2]}")
            print(f"Marca: {row[3] or 'Sin asignar'}")
            print(f"Categoría: {row[4] or 'Sin asignar'}")
            print(f"Subcategoría: {row[5] or 'Sin asignar'}")
            print(f"Precio promedio: ${row[6]:,}" if row[6] else "Precio: N/A")
            print(f"Reportes: {row[7]}")
            print(f"Usuarios que lo compraron: {row[10]}")
            print(f"Facturas que lo incluyen: {row[11]}")
            print(f"Última actualización: {row[8]}")

    cur.close()
    conn.close()


def verificar_endpoint_dashboard():
    """Verifica qué query usa actualmente tu dashboard"""
    print("\n" + "="*80)
    print("🔍 VERIFICACIÓN DE ENDPOINT DEL DASHBOARD")
    print("="*80 + "\n")

    print("Tu dashboard.html debe llamar a un endpoint del backend.")
    print("Probablemente algo como:\n")
    print("   GET /api/productos")
    print("   o")
    print("   GET /admin/productos\n")

    print("📝 BUSCA EN TU CÓDIGO BACKEND:")
    print("   1. Archivo: probablemente main.py o api.py")
    print("   2. Ruta: @app.get('/productos') o similar")
    print("   3. Query actual: puede estar usando tabla antigua\n")

    print("✅ DEBE USAR: productos_maestros (no productos_catalogo ni productos_maestro)")
    print("❌ NO USAR: productos, productos_maestro (singular), productos_catalogo\n")


def main():
    """Función principal"""
    print("\n" + "="*80)
    print("🔍 DIAGNÓSTICO COMPLETO DE DATOS Y DASHBOARD")
    print("Sistema LecFac")
    print("="*80)

    try:
        ver_estructura_completa()
        generar_query_dashboard()
        verificar_endpoint_dashboard()

        print("\n" + "="*80)
        print("✅ DIAGNÓSTICO COMPLETADO")
        print("="*80)
        print("\nPróximos pasos:")
        print("1. Verifica que tu dashboard use el query correcto")
        print("2. Asegúrate que consulte productos_maestros (no productos_catalogo)")
        print("3. Actualiza el endpoint en tu backend si es necesario")
        print("4. Recarga el dashboard para ver los productos completos\n")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
