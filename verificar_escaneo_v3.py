# verificar_escaneo_v3.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("📊 VERIFICACIÓN POST-ESCANEO V3.0\n")

# 1. Facturas
cur.execute("SELECT id, usuario_id, establecimiento, total_factura, productos_guardados FROM facturas ORDER BY id DESC LIMIT 1")
factura = cur.fetchone()

if factura:
    print(f"✅ Factura #{factura[0]}")
    print(f"   Usuario: {factura[1]}")
    print(f"   Establecimiento: {factura[2]}")
    print(f"   Total: ${factura[3]:,}")
    print(f"   Productos guardados: {factura[4]}")

    factura_id = factura[0]
    usuario_id = factura[1]

    # 2. Items con ProductResolver
    cur.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN producto_canonico_id IS NOT NULL THEN 1 ELSE 0 END),
               SUM(CASE WHEN variante_id IS NOT NULL THEN 1 ELSE 0 END)
        FROM items_factura
        WHERE factura_id = %s
    """, (factura_id,))

    stats = cur.fetchone()
    print(f"\n📦 Items en factura:")
    print(f"   Total: {stats[0]}")
    print(f"   Con canonico_id: {stats[1]} {'✅' if stats[1] == stats[0] else '⚠️'}")
    print(f"   Con variante_id: {stats[2]} {'✅' if stats[2] == stats[0] else '⚠️'}")

    # 3. Inventario del usuario
    cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = %s", (usuario_id,))
    total_inv = cur.fetchone()[0]
    print(f"\n🏠 Inventario usuario {usuario_id}: {total_inv} productos")

    # 4. Verificar que NO haya productos de otros usuarios
    cur.execute("""
        SELECT COUNT(*) FROM inventario_usuario
        WHERE usuario_id != %s
    """, (usuario_id,))
    otros = cur.fetchone()[0]

    if otros > 0:
        print(f"   ⚠️ ADVERTENCIA: Hay {otros} productos de otros usuarios")
    else:
        print(f"   ✅ Solo este usuario tiene inventario")

    # 5. Mostrar productos
    print(f"\n📦 Productos en inventario:")
    cur.execute("""
        SELECT pm.nombre_normalizado, inv.cantidad_actual, pm.codigo_ean
        FROM inventario_usuario inv
        JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
        WHERE inv.usuario_id = %s
        ORDER BY pm.nombre_normalizado
        LIMIT 10
    """, (usuario_id,))

    for p in cur.fetchall():
        print(f"   • {p[0][:50]} x{p[1]} (EAN: {p[2] or 'N/A'})")

else:
    print("⚠️ No hay facturas en el sistema")

cur.close()
conn.close()
