"""
MIGRACIÓN SIMPLIFICADA - Ejecutar en Railway desde local
"""
import psycopg2
import sys

# ============================================================================
# INSTRUCCIONES:
# 1. Obtén tu DATABASE_URL de Railway
# 2. Pégala aquí abajo (reemplaza la línea)
# 3. Ejecuta: python migrar_railway_simple.py
# ============================================================================

print("=" * 80)
print("🔧 MIGRACIÓN DE PLUs - RAILWAY")
print("=" * 80)

# PEGAR TU DATABASE_URL DE RAILWAY AQUÍ:
DATABASE_URL = input("\n📋 Pega tu DATABASE_URL de Railway: ").strip()

if not DATABASE_URL or DATABASE_URL == "":
    print("❌ No se proporcionó DATABASE_URL")
    sys.exit(1)

try:
    print(f"\n🔗 Conectando a Railway...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    print("✅ Conectado exitosamente\n")

    # PASO 1: Crear tabla
    print("📦 PASO 1: Creando tabla productos_por_establecimiento...")

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
    print("   ✅ Tabla creada")

    # Crear índices
    print("📑 Creando índices...")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ppe_producto ON productos_por_establecimiento(producto_maestro_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ppe_establecimiento ON productos_por_establecimiento(establecimiento_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ppe_plu ON productos_por_establecimiento(codigo_plu)")
    print("   ✅ Índices creados")

    # PASO 2: Migrar PLUs
    print("\n🔄 PASO 2: Migrando PLUs...")

    cur.execute("""
    INSERT INTO productos_por_establecimiento
        (producto_maestro_id, establecimiento_id, codigo_plu, precio_actual, precio_minimo, precio_maximo, total_reportes)
    SELECT
        pm.id,
        e.id,
        pm.codigo_ean,
        AVG(if.precio_pagado)::INTEGER,
        MIN(if.precio_pagado),
        MAX(if.precio_pagado),
        COUNT(DISTINCT if.id)
    FROM productos_maestros pm
    JOIN items_factura if ON if.producto_maestro_id = pm.id
    JOIN facturas f ON if.factura_id = f.id
    JOIN establecimientos e ON f.establecimiento_id = e.id
    WHERE pm.codigo_ean IS NOT NULL
      AND pm.codigo_ean != ''
      AND LENGTH(pm.codigo_ean) < 13
      AND pm.codigo_ean ~ '^[0-9]+$'
    GROUP BY pm.id, pm.codigo_ean, e.id
    ON CONFLICT (producto_maestro_id, establecimiento_id) DO NOTHING
    """)

    migrados = cur.rowcount
    print(f"   ✅ {migrados} PLUs migrados")

    # PASO 3: Limpiar codigo_ean
    print("\n🧹 PASO 3: Limpiando codigo_ean...")

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

    limpiados = cur.rowcount
    print(f"   ✅ {limpiados} productos limpiados")

    # PASO 4: Estadísticas
    print("\n📊 ESTADÍSTICAS:")

    cur.execute("SELECT COUNT(*) FROM productos_maestros")
    total = cur.fetchone()[0]
    print(f"   • Total productos: {total}")

    cur.execute("SELECT COUNT(*) FROM productos_maestros WHERE LENGTH(codigo_ean) = 13")
    con_ean = cur.fetchone()[0]
    print(f"   • Con EAN válido: {con_ean}")

    cur.execute("SELECT COUNT(DISTINCT producto_maestro_id) FROM productos_por_establecimiento WHERE codigo_plu IS NOT NULL")
    con_plu = cur.fetchone()[0]
    print(f"   • Con PLU en nueva tabla: {con_plu}")

    # CONFIRMAR
    print("\n" + "=" * 80)
    respuesta = input("¿Guardar cambios? (si/no): ").strip().lower()

    if respuesta == 'si':
        conn.commit()
        print("✅ CAMBIOS GUARDADOS EN RAILWAY")
    else:
        conn.rollback()
        print("❌ CAMBIOS DESCARTADOS")

    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    if conn:
        conn.rollback()
        print("❌ Cambios revertidos")

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("\n✅ Desconectado de Railway")
