# verificar_ultima_factura.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 ÚLTIMA FACTURA Y SU PROCESAMIENTO\n")

# Ver la última factura
cur.execute("""
    SELECT id, usuario_id, establecimiento, total_factura,
           productos_detectados, productos_guardados, estado_validacion,
           fecha_cargue
    FROM facturas
    ORDER BY id DESC
    LIMIT 1
""")

factura = cur.fetchone()

if factura:
    factura_id = factura[0]
    print(f"📄 Factura #{factura_id}")
    print(f"   Usuario: {factura[1]}")
    print(f"   Establecimiento: {factura[2]}")
    print(f"   Total: ${factura[3]:,}")
    print(f"   Productos detectados: {factura[4]}")
    print(f"   Productos guardados: {factura[5]}")
    print(f"   Estado: {factura[6]}")
    print(f"   Fecha cargue: {factura[7]}")

    # Ver items
    print(f"\n📦 ITEMS EN FACTURA #{factura_id}:")
    cur.execute("""
        SELECT COUNT(*),
               COUNT(producto_canonico_id),
               COUNT(variante_id),
               COUNT(producto_maestro_id)
        FROM items_factura
        WHERE factura_id = %s
    """, (factura_id,))

    stats = cur.fetchone()
    print(f"   Total items: {stats[0]}")
    print(f"   Con canonico_id: {stats[1]} {'✅' if stats[1] > 0 else '❌'}")
    print(f"   Con variante_id: {stats[2]} {'✅' if stats[2] > 0 else '❌'}")
    print(f"   Con maestro_id: {stats[3]} {'✅' if stats[3] > 0 else '❌'}")

    # Ver processing job
    print(f"\n🔧 PROCESSING JOB:")
    cur.execute("""
        SELECT id, status, productos_detectados, error_message,
               created_at, completed_at
        FROM processing_jobs
        WHERE factura_id = %s
    """, (factura_id,))

    job = cur.fetchone()
    if job:
        print(f"   Job ID: {job[0][:8]}...")
        print(f"   Status: {job[1]}")
        print(f"   Productos detectados: {job[2]}")
        print(f"   Error: {job[3] or 'N/A'}")
        print(f"   Creado: {job[4]}")
        print(f"   Completado: {job[5]}")
    else:
        print(f"   ⚠️ NO HAY PROCESSING JOB")

    # Ver inventario del usuario
    usuario_id = factura[1]
    cur.execute("""
        SELECT COUNT(*)
        FROM inventario_usuario
        WHERE usuario_id = %s
    """, (usuario_id,))

    inv_count = cur.fetchone()[0]
    print(f"\n🏠 INVENTARIO USUARIO {usuario_id}: {inv_count} productos")

else:
    print("⚠️ No hay facturas en el sistema")

cur.close()
conn.close()
