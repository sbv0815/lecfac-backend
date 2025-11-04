# verificar_resultado_v3.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("=" * 80)
print("📊 VERIFICACIÓN RESULTADO V3.0")
print("=" * 80)

# Facturas
cur.execute("SELECT COUNT(*) FROM facturas")
total_facturas = cur.fetchone()[0]
print(f"\n📄 Total facturas: {total_facturas}")

if total_facturas > 0:
    cur.execute("""
        SELECT id, usuario_id, establecimiento, total_factura, productos_guardados
        FROM facturas
        ORDER BY id DESC
        LIMIT 1
    """)
    factura = cur.fetchone()
    print(f"\n✅ Última factura:")
    print(f"   ID: {factura[0]}")
    print(f"   Usuario: {factura[1]}")
    print(f"   Establecimiento: {factura[2]}")
    print(f"   Total: ${factura[3]:,}")
    print(f"   Productos guardados: {factura[4]}")

    factura_id = factura[0]
    usuario_id = factura[1]

    # Items de la factura
    cur.execute("""
        SELECT COUNT(*)
        FROM items_factura
        WHERE factura_id = %s
    """, (factura_id,))
    total_items = cur.fetchone()[0]
    print(f"\n📦 Items en factura: {total_items}")

    # Items con producto_maestro_id
    cur.execute("""
        SELECT COUNT(*)
        FROM items_factura
        WHERE factura_id = %s AND producto_maestro_id IS NOT NULL
    """, (factura_id,))
    items_con_maestro = cur.fetchone()[0]
    print(f"   ✅ Con producto_maestro_id: {items_con_maestro}")

    # Items con canonico_id
    cur.execute("""
        SELECT COUNT(*)
        FROM items_factura
        WHERE factura_id = %s AND producto_canonico_id IS NOT NULL
    """, (factura_id,))
    items_con_canonico = cur.fetchone()[0]
    print(f"   ✅ Con producto_canonico_id: {items_con_canonico}")

    # Productos canónicos creados
    cur.execute("SELECT COUNT(*) FROM productos_canonicos")
    total_canonicos = cur.fetchone()[0]
    print(f"\n🎯 Productos canónicos: {total_canonicos}")

    # Variantes creadas
    cur.execute("SELECT COUNT(*) FROM productos_variantes")
    total_variantes = cur.fetchone()[0]
    print(f"🔄 Variantes: {total_variantes}")

    # Productos maestros
    cur.execute("SELECT COUNT(*) FROM productos_maestros")
    total_maestros = cur.fetchone()[0]
    print(f"📚 Productos maestros: {total_maestros}")

    # Inventario del usuario
    cur.execute("""
        SELECT COUNT(*)
        FROM inventario_usuario
        WHERE usuario_id = %s
    """, (usuario_id,))
    total_inventario = cur.fetchone()[0]
    print(f"\n🏠 Inventario usuario {usuario_id}: {total_inventario} productos")

    # Verificar que NO haya mermeladas
    cur.execute("""
        SELECT COUNT(*)
        FROM inventario_usuario inv
        JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
        WHERE inv.usuario_id = %s
          AND LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
    """, (usuario_id,))
    mermeladas = cur.fetchone()[0]

    if mermeladas > 0:
        print(f"\n⚠️ ADVERTENCIA: {mermeladas} mermeladas en inventario")
        cur.execute("""
            SELECT pm.nombre_normalizado, pm.codigo_ean
            FROM inventario_usuario inv
            JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
            WHERE inv.usuario_id = %s
              AND LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
        """, (usuario_id,))
        for m in cur.fetchall():
            print(f"   🍓 {m[0]} (EAN: {m[1]})")
    else:
        print(f"\n✅ NO hay mermeladas fantasma")

    # Mostrar algunos productos del inventario
    print(f"\n📦 Primeros 10 productos en inventario:")
    cur.execute("""
        SELECT pm.nombre_normalizado, inv.cantidad_actual, pm.codigo_ean
        FROM inventario_usuario inv
        JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
        WHERE inv.usuario_id = %s
        ORDER BY pm.nombre_normalizado
        LIMIT 10
    """, (usuario_id,))

    for p in cur.fetchall():
        print(f"   • {p[0][:50]:<50} x{p[1]:<3} EAN: {p[2] or 'N/A'}")

cur.close()
conn.close()

print("\n" + "=" * 80)
print("✅ VERIFICACIÓN COMPLETADA")
print("=" * 80)
