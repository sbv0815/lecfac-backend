"""
PASO 1: Crear tabla productos_por_establecimiento y migrar PLUs
LecFac - Sistema de gestión de PLUs por establecimiento
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()

print("=" * 80)
print("🔧 CREANDO SISTEMA DE PLUs POR ESTABLECIMIENTO")
print("=" * 80)

try:
    # ========== PASO 1: CREAR TABLA ==========
    print("\n📦 PASO 1: Creando tabla productos_por_establecimiento...")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS productos_por_establecimiento (
        id SERIAL PRIMARY KEY,
        producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id) ON DELETE CASCADE,
        establecimiento_id INTEGER NOT NULL REFERENCES establecimientos(id) ON DELETE CASCADE,
        codigo_plu VARCHAR(20),
        precio_actual INTEGER,
        precio_minimo INTEGER,
        precio_maximo INTEGER,
        ultima_actualizacion TIMESTAMP DEFAULT NOW(),
        total_reportes INTEGER DEFAULT 0,
        UNIQUE(producto_maestro_id, establecimiento_id)
    )
    """)

    print("   ✅ Tabla creada exitosamente")

    # Crear índices
    print("\n📑 Creando índices...")

    indices = [
        "CREATE INDEX IF NOT EXISTS idx_ppe_producto ON productos_por_establecimiento(producto_maestro_id)",
        "CREATE INDEX IF NOT EXISTS idx_ppe_establecimiento ON productos_por_establecimiento(establecimiento_id)",
        "CREATE INDEX IF NOT EXISTS idx_ppe_plu ON productos_por_establecimiento(codigo_plu)",
    ]

    for idx_sql in indices:
        cur.execute(idx_sql)
        print(f"   ✅ Índice creado")

    # ========== PASO 2: MIGRAR PLUs EXISTENTES ==========
    print("\n" + "=" * 80)
    print("🔄 PASO 2: Migrando PLUs desde productos_maestros.codigo_ean")
    print("=" * 80)

    # Obtener productos con código corto (PLUs) agrupados por establecimiento
    cur.execute("""
    SELECT
        pm.id as producto_id,
        pm.codigo_ean as plu,
        e.id as establecimiento_id,
        e.nombre_normalizado as establecimiento_nombre,
        AVG(if.precio_pagado)::INTEGER as precio_promedio,
        MIN(if.precio_pagado) as precio_minimo,
        MAX(if.precio_pagado) as precio_maximo,
        COUNT(DISTINCT if.id) as num_items
    FROM productos_maestros pm
    JOIN items_factura if ON if.producto_maestro_id = pm.id
    JOIN facturas f ON if.factura_id = f.id
    JOIN establecimientos e ON f.establecimiento_id = e.id
    WHERE pm.codigo_ean IS NOT NULL
      AND pm.codigo_ean != ''
      AND LENGTH(pm.codigo_ean) < 13
      AND pm.codigo_ean ~ '^[0-9]+$'
    GROUP BY pm.id, pm.codigo_ean, e.id, e.nombre_normalizado
    ORDER BY e.nombre_normalizado, pm.id
    """)

    productos_plu = cur.fetchall()

    print(f"\n📊 Encontrados {len(productos_plu)} registros PLU para migrar\n")

    migrados = 0
    errores = 0

    for row in productos_plu:
        producto_id, plu, est_id, est_nombre, precio_prom, precio_min, precio_max, num_items = row

        try:
            # Insertar en productos_por_establecimiento
            cur.execute("""
            INSERT INTO productos_por_establecimiento
                (producto_maestro_id, establecimiento_id, codigo_plu,
                 precio_actual, precio_minimo, precio_maximo, total_reportes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (producto_maestro_id, establecimiento_id)
            DO UPDATE SET
                codigo_plu = EXCLUDED.codigo_plu,
                precio_actual = EXCLUDED.precio_actual,
                precio_minimo = EXCLUDED.precio_minimo,
                precio_maximo = EXCLUDED.precio_maximo,
                total_reportes = EXCLUDED.total_reportes,
                ultima_actualizacion = NOW()
            """, (producto_id, est_id, plu, precio_prom, precio_min, precio_max, num_items))

            migrados += 1

            if migrados % 10 == 0:
                print(f"   ✅ {migrados} PLUs migrados...")

        except Exception as e:
            errores += 1
            print(f"   ❌ Error migrando PLU {plu} (producto {producto_id}): {e}")

    print(f"\n📊 RESULTADO MIGRACIÓN:")
    print(f"   • Total procesados: {len(productos_plu)}")
    print(f"   • Migrados exitosamente: {migrados}")
    print(f"   • Errores: {errores}")

    # ========== PASO 3: LIMPIAR CODIGO_EAN ==========
    print("\n" + "=" * 80)
    print("🧹 PASO 3: Limpiando PLUs de productos_maestros.codigo_ean")
    print("=" * 80)

    # Confirmar antes de limpiar
    print("\n⚠️  ADVERTENCIA: Se va a poner NULL en codigo_ean para productos con PLU")
    print("    Esto NO borra los productos, solo limpia el campo codigo_ean")
    print("    Los PLUs ahora están en productos_por_establecimiento")

    respuesta = input("\n¿Continuar con la limpieza? (si/no): ").strip().lower()

    if respuesta == 'si':
        # Actualizar productos que tienen PLU (código corto)
        cur.execute("""
        UPDATE productos_maestros pm
        SET codigo_ean = NULL
        WHERE EXISTS (
            SELECT 1
            FROM productos_por_establecimiento ppe
            WHERE ppe.producto_maestro_id = pm.id
            AND ppe.codigo_plu IS NOT NULL
        )
        AND LENGTH(pm.codigo_ean) < 13
        """)

        actualizados = cur.rowcount
        print(f"\n   ✅ {actualizados} productos limpiados (codigo_ean → NULL)")
        print(f"   ✅ Sus PLUs están ahora en productos_por_establecimiento")
    else:
        print("\n   ⏭️  Limpieza omitida (PLUs quedan duplicados)")

    # ========== PASO 4: ESTADÍSTICAS FINALES ==========
    print("\n" + "=" * 80)
    print("📊 ESTADÍSTICAS FINALES")
    print("=" * 80)

    queries_stats = [
        ("Productos totales", "SELECT COUNT(*) FROM productos_maestros"),
        ("Con EAN válido (13 dígitos)", "SELECT COUNT(*) FROM productos_maestros WHERE LENGTH(codigo_ean) = 13"),
        ("Con PLU en nueva tabla", "SELECT COUNT(DISTINCT producto_maestro_id) FROM productos_por_establecimiento WHERE codigo_plu IS NOT NULL"),
        ("Sin código (ni EAN ni PLU)", "SELECT COUNT(*) FROM productos_maestros WHERE (codigo_ean IS NULL OR codigo_ean = '') AND id NOT IN (SELECT producto_maestro_id FROM productos_por_establecimiento)"),
    ]

    for nombre, query in queries_stats:
        cur.execute(query)
        count = cur.fetchone()[0]
        print(f"   • {nombre:40}: {count:6}")

    # Resumen por establecimiento
    print("\n📊 PLUs por Establecimiento:")

    cur.execute("""
    SELECT
        e.nombre_normalizado,
        COUNT(*) as total_plus
    FROM productos_por_establecimiento ppe
    JOIN establecimientos e ON ppe.establecimiento_id = e.id
    WHERE ppe.codigo_plu IS NOT NULL
    GROUP BY e.nombre_normalizado
    ORDER BY total_plus DESC
    """)

    for row in cur.fetchall():
        est_nombre, total = row
        print(f"   • {est_nombre:30}: {total:6} PLUs")

    # ========== COMMIT ==========
    print("\n" + "=" * 80)
    respuesta = input("¿Guardar cambios en la base de datos? (si/no): ").strip().lower()

    if respuesta == 'si':
        conn.commit()
        print("✅ CAMBIOS GUARDADOS EXITOSAMENTE")
    else:
        conn.rollback()
        print("❌ CAMBIOS DESCARTADOS (rollback)")

    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    conn.rollback()
    print("\n❌ Cambios revertidos (rollback)")

finally:
    cur.close()
    conn.close()
    print("\n✅ Script finalizado")
