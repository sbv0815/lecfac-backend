# corregir_usuario_factura22.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔧 Corrigiendo usuario_id de factura 22...")

# Cambiar usuario de 1 a 2
cur.execute("UPDATE facturas SET usuario_id = 2 WHERE id = 22")
print(f"✅ Factura actualizada: {cur.rowcount} fila")

# Borrar inventario del usuario 2 (si existe)
cur.execute("DELETE FROM inventario WHERE usuario_id = 2")
print(f"🗑️ Inventario limpio: {cur.rowcount} filas borradas")

# Forzar actualización del inventario
cur.execute("""
    INSERT INTO inventario (usuario_id, producto_maestro_id, cantidad_actual, unidad_medida, fecha_actualizacion)
    SELECT 2, i.producto_maestro_id, SUM(i.cantidad), 'unidad', NOW()
    FROM items_factura i
    WHERE i.factura_id = 22
      AND i.producto_maestro_id IS NOT NULL
    GROUP BY i.producto_maestro_id
    ON CONFLICT (usuario_id, producto_maestro_id)
    DO UPDATE SET
        cantidad_actual = inventario.cantidad_actual + EXCLUDED.cantidad_actual,
        fecha_actualizacion = NOW()
""")
print(f"✅ Inventario actualizado: {cur.rowcount} productos")

conn.commit()

# Verificar
cur.execute("SELECT COUNT(*) FROM inventario WHERE usuario_id = 2")
count = cur.fetchone()[0]
print(f"\n📊 Usuario 2 ahora tiene {count} productos en inventario")

cur.close()
conn.close()

print("\n✅ LISTO! Ahora verifica en Flutter")
