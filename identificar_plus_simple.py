"""
Script para identificar PLUs y sus establecimientos en LecFac
Versión simplificada - usa solo productos_maestros y facturas
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Conexión a BD
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("=" * 80)
print("🔍 IDENTIFICANDO PLUs EN LA BASE DE DATOS")
print("=" * 80)

# Paso 1: Ver estructura de productos_maestros
print("\n📊 PASO 1: Verificando columnas de productos_maestros...\n")

cur.execute("""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'productos_maestros'
ORDER BY ordinal_position
""")

columnas = cur.fetchall()
print("Columnas disponibles:")
for col in columnas:
    print(f"   • {col[0]:30} ({col[1]})")

# Paso 2: Buscar productos con código EAN corto (probable PLU)
print("\n" + "=" * 80)
print("🔍 PASO 2: Productos con código CORTO (< 13 dígitos) - Probable PLU")
print("=" * 80)

query = """
SELECT
    pm.id,
    pm.codigo_ean,
    pm.nombre_normalizado,
    LENGTH(pm.codigo_ean) as longitud,
    COUNT(DISTINCT if.factura_id) as num_facturas,
    STRING_AGG(DISTINCT e.nombre_normalizado, ', ') as establecimientos
FROM productos_maestros pm
LEFT JOIN items_factura if ON if.producto_maestro_id = pm.id
LEFT JOIN facturas f ON if.factura_id = f.id
LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
WHERE pm.codigo_ean IS NOT NULL
  AND pm.codigo_ean != ''
  AND pm.codigo_ean ~ '^[0-9]+$'
  AND LENGTH(pm.codigo_ean) < 13
GROUP BY pm.id, pm.codigo_ean, pm.nombre_normalizado
ORDER BY num_facturas DESC, LENGTH(pm.codigo_ean)
LIMIT 50
"""

cur.execute(query)
rows = cur.fetchall()

print(f"\n{'ID':6} | {'Código':12} | {'Long':4} | {'Facturas':9} | {'Establecimientos':20} | Nombre")
print("-" * 120)

plus_por_establecimiento = {}

for row in rows:
    prod_id, codigo, nombre, longitud, num_facturas, establecimientos = row
    establecimientos = establecimientos or "Sin facturas"

    print(f"{prod_id:6} | {codigo:12} | {longitud:4} | {num_facturas:9} | {establecimientos[:20]:20} | {nombre[:50]}")

    # Agrupar por establecimiento
    if establecimientos and establecimientos != "Sin facturas":
        for est in establecimientos.split(', '):
            if est not in plus_por_establecimiento:
                plus_por_establecimiento[est] = []
            plus_por_establecimiento[est].append({
                'id': prod_id,
                'codigo': codigo,
                'nombre': nombre,
                'facturas': num_facturas
            })

print("\n" + "=" * 80)
print(f"📊 RESUMEN POR ESTABLECIMIENTO")
print("=" * 80)

for est, productos in sorted(plus_por_establecimiento.items()):
    print(f"\n🏪 {est.upper()} ({len(productos)} PLUs)")
    print("-" * 80)
    for p in productos[:10]:  # Primeros 10
        print(f"   PLU {p['codigo']:12} → {p['nombre'][:50]:50} ({p['facturas']} facturas)")

    if len(productos) > 10:
        print(f"   ... y {len(productos) - 10} más")

# Paso 3: Productos SIN código EAN (también pueden tener PLU en otro campo)
print("\n" + "=" * 80)
print("🔍 PASO 3: Productos SIN código EAN")
print("=" * 80)

query_sin_ean = """
SELECT
    pm.id,
    pm.nombre_normalizado,
    COUNT(DISTINCT if.factura_id) as num_facturas,
    STRING_AGG(DISTINCT e.nombre_normalizado, ', ') as establecimientos
FROM productos_maestros pm
LEFT JOIN items_factura if ON if.producto_maestro_id = pm.id
LEFT JOIN facturas f ON if.factura_id = f.id
LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
WHERE (pm.codigo_ean IS NULL OR pm.codigo_ean = '')
GROUP BY pm.id, pm.nombre_normalizado
HAVING COUNT(DISTINCT if.factura_id) > 0
ORDER BY num_facturas DESC
LIMIT 30
"""

cur.execute(query_sin_ean)
rows = cur.fetchall()

print(f"\n{'ID':6} | {'Facturas':9} | {'Establecimientos':20} | Nombre")
print("-" * 100)

for row in rows:
    prod_id, nombre, num_facturas, establecimientos = row
    establecimientos = establecimientos or "Desconocido"
    print(f"{prod_id:6} | {num_facturas:9} | {establecimientos[:20]:20} | {nombre[:60]}")

print(f"\n⚠️  {len(rows)} productos sin código EAN (pueden tener PLU no guardado)")

# Paso 4: Estadísticas generales
print("\n" + "=" * 80)
print("📊 ESTADÍSTICAS GENERALES")
print("=" * 80)

queries_stats = [
    ("Total productos", "SELECT COUNT(*) FROM productos_maestros"),
    ("Con EAN válido (13 dígitos)", "SELECT COUNT(*) FROM productos_maestros WHERE LENGTH(codigo_ean) = 13"),
    ("Con código corto (< 13)", "SELECT COUNT(*) FROM productos_maestros WHERE codigo_ean IS NOT NULL AND LENGTH(codigo_ean) < 13"),
    ("Sin código", "SELECT COUNT(*) FROM productos_maestros WHERE codigo_ean IS NULL OR codigo_ean = ''"),
]

for nombre, query in queries_stats:
    cur.execute(query)
    count = cur.fetchone()[0]
    print(f"   • {nombre:30}: {count:6}")

# Paso 5: Verificar si existe campo codigo_plu en alguna tabla
print("\n" + "=" * 80)
print("🔍 PASO 5: Buscando campo 'codigo_plu' en el esquema")
print("=" * 80)

cur.execute("""
SELECT table_name, column_name
FROM information_schema.columns
WHERE column_name LIKE '%plu%'
AND table_schema = 'public'
""")

campos_plu = cur.fetchall()

if campos_plu:
    print("\n✅ Campos PLU encontrados:")
    for tabla, columna in campos_plu:
        print(f"   • {tabla}.{columna}")
else:
    print("\n⚠️  No se encontró ningún campo 'codigo_plu' en el esquema")
    print("   → Los PLUs están mezclados en productos_maestros.codigo_ean")

cur.close()
conn.close()

print("\n" + "=" * 80)
print("✅ Análisis completado")
print("=" * 80)
