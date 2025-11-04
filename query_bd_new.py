import psycopg2
import os

# Tu DATABASE_URL de Railway
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

# Conectar
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

# Query 1: Ver facturas recientes
print("\n📊 FACTURAS RECIENTES:")
cursor.execute("""
    SELECT id, usuario_id, establecimiento, total, fecha_compra, created_at
    FROM facturas
    ORDER BY created_at DESC
    LIMIT 10;
""")
for row in cursor.fetchall():
    print(row)

# Query 2: Items por factura
print("\n📦 ITEMS POR FACTURA:")
cursor.execute("""
    SELECT
        f.id as factura_id,
        f.usuario_id,
        COUNT(if.id) as total_items,
        SUM(CASE WHEN if.producto_maestro_id IS NULL THEN 1 ELSE 0 END) as items_sin_producto
    FROM facturas f
    LEFT JOIN items_factura if ON f.id = if.factura_id
    GROUP BY f.id, f.usuario_id
    ORDER BY f.created_at DESC
    LIMIT 10;
""")
for row in cursor.fetchall():
    print(row)

# Query 3: Inventario por usuario
print("\n🏠 INVENTARIO POR USUARIO:")
cursor.execute("""
    SELECT
        usuario_id,
        COUNT(*) as total_productos
    FROM inventario_personal
    GROUP BY usuario_id;
""")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
