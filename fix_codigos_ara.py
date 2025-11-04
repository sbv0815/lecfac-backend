import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("\n🔧 CORRIGIENDO CÓDIGOS EAN DE ARA/JERONIMO MARTINS")
print("="*80)

# 1. Obtener items del usuario 3 con códigos que empiezan con 0
cursor.execute("""
    SELECT
        if.id,
        if.codigo_leido,
        if.nombre_leido,
        f.establecimiento
    FROM items_factura if
    JOIN facturas f ON if.factura_id = f.id
    WHERE f.usuario_id = 3
      AND if.codigo_leido IS NOT NULL
      AND if.codigo_leido LIKE '0%'
      AND LENGTH(if.codigo_leido) IN (11, 12, 13)
""")

items_a_corregir = cursor.fetchall()

print(f"📦 Encontrados {len(items_a_corregir)} items con códigos que empiezan con 0")

corregidos = 0
creados = 0
errores = 0

for item in items_a_corregir:
    item_id = item[0]
    codigo_viejo = item[1]
    nombre = item[2]
    establecimiento = item[3]

    # Quitar el 0 inicial
    codigo_corregido = codigo_viejo.lstrip('0')

    # Si quedó muy corto, restaurar algunos ceros
    if len(codigo_corregido) < 8:
        continue

    try:
        # Buscar si existe el producto con el código corregido
        cursor.execute("""
            SELECT id FROM productos_maestros
            WHERE codigo_ean = %s
        """, (codigo_corregido,))

        producto = cursor.fetchone()

        if producto:
            # Actualizar el item con el producto_maestro_id correcto
            cursor.execute("""
                UPDATE items_factura
                SET producto_maestro_id = %s,
                    codigo_leido = %s
                WHERE id = %s
            """, (producto[0], codigo_corregido, item_id))

            print(f"✅ Item #{item_id}: {codigo_viejo} → {codigo_corregido} (Producto #{producto[0]})")
            corregidos += 1
        else:
            # Crear el producto si no existe
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean,
                    nombre_normalizado,
                    total_reportes,
                    primera_vez_reportado
                ) VALUES (%s, %s, 1, CURRENT_TIMESTAMP)
                RETURNING id
            """, (codigo_corregido, nombre))

            nuevo_producto_id = cursor.fetchone()[0]

            # Actualizar el item
            cursor.execute("""
                UPDATE items_factura
                SET producto_maestro_id = %s,
                    codigo_leido = %s
                WHERE id = %s
            """, (nuevo_producto_id, codigo_corregido, item_id))

            print(f"➕ Item #{item_id}: {codigo_viejo} → {codigo_corregido} (Nuevo producto #{nuevo_producto_id})")
            creados += 1

    except Exception as e:
        print(f"❌ Error en item #{item_id}: {e}")
        errores += 1
        conn.rollback()
        continue

conn.commit()

print("\n" + "="*80)
print(f"✅ RESUMEN:")
print(f"   - {corregidos} items corregidos (producto existía)")
print(f"   - {creados} productos nuevos creados")
print(f"   - {errores} errores")
print("="*80)

# Verificar resultados
print("\n🔍 VERIFICANDO CORRECCIONES...")
cursor.execute("""
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN if.producto_maestro_id IS NULL THEN 1 ELSE 0 END) as sin_id,
        SUM(CASE WHEN if.producto_maestro_id IS NOT NULL THEN 1 ELSE 0 END) as con_id
    FROM items_factura if
    JOIN facturas f ON if.factura_id = f.id
    WHERE f.usuario_id = 3
""")

stats = cursor.fetchone()
print(f"📊 ESTADO FINAL USUARIO 3:")
print(f"   Total items: {stats[0]}")
print(f"   ❌ Sin producto_maestro_id: {stats[1]}")
print(f"   ✅ Con producto_maestro_id: {stats[2]}")

cursor.close()
conn.close()

print("\n⚠️ SIGUIENTE PASO: Ejecuta actualizar_inventario_usuario3.py")
