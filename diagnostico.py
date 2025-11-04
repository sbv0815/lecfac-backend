import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("\n📊 FACTURAS RECIENTES:")
cursor.execute("SELECT id, usuario_id, establecimiento, total_factura, fecha_cargue FROM facturas ORDER BY fecha_cargue DESC LIMIT 5")
for row in cursor.fetchall():
    print(f"  Factura #{row[0]}: Usuario={row[1]}, {row[2]}, Total=${row[3]:,}")

print("\n📦 ITEMS CON/SIN PRODUCTO_MAESTRO_ID:")
cursor.execute("""
    SELECT
        f.id,
        COUNT(if.id) as total,
        SUM(CASE WHEN if.producto_maestro_id IS NULL THEN 1 ELSE 0 END) as sin_id
    FROM facturas f
    LEFT JOIN items_factura if ON f.id = if.factura_id
    GROUP BY f.id
    ORDER BY f.fecha_cargue DESC
    LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  Factura #{row[0]}: {row[1]} items, {row[2]} sin producto_maestro_id")

print("\n🏠 INVENTARIOS:")
cursor.execute("SELECT usuario_id, COUNT(*) FROM inventario_usuario GROUP BY usuario_id")
inv = cursor.fetchall()
if inv:
    for row in inv:
        print(f"  Usuario {row[0]}: {row[1]} productos")
else:
    print("  ❌ NO HAY INVENTARIOS")

cursor.close()
conn.close()
