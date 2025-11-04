import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("\n🔍 ITEMS DE FACTURAS DEL USUARIO 3:")
print("="*100)

cursor.execute("""
    SELECT
        if.id,
        if.factura_id,
        if.nombre_leido,
        if.codigo_leido,
        if.precio_pagado,
        if.producto_maestro_id
    FROM items_factura if
    JOIN facturas f ON if.factura_id = f.id
    WHERE f.usuario_id = 3
    ORDER BY if.factura_id DESC, if.id
    LIMIT 30
""")

for row in cursor.fetchall():
    status = "✅" if row[5] else "❌"
    print(f"{status} Item #{row[0]} (Factura #{row[1]}): '{row[2]}' | EAN: {row[3]} | ${row[4]:,}")

print("\n🔍 ¿EXISTEN ESOS CÓDIGOS EN PRODUCTOS_MAESTROS?")
print("="*100)

# Tomar algunos códigos de ejemplo
cursor.execute("""
    SELECT DISTINCT if.codigo_leido
    FROM items_factura if
    JOIN facturas f ON if.factura_id = f.id
    WHERE f.usuario_id = 3
      AND if.codigo_leido IS NOT NULL
      AND if.codigo_leido != ''
    LIMIT 10
""")

codigos = [row[0] for row in cursor.fetchall()]

for codigo in codigos:
    cursor.execute("""
        SELECT id, codigo_ean, nombre_normalizado
        FROM productos_maestros
        WHERE codigo_ean = %s
    """, (codigo,))

    producto = cursor.fetchone()
    if producto:
        print(f"✅ EAN {codigo}: EXISTE → Producto #{producto[0]} '{producto[2]}'")
    else:
        print(f"❌ EAN {codigo}: NO EXISTE en productos_maestros")

cursor.close()
conn.close()
