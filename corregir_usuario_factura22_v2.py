# corregir_usuario_factura22_v3.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔧 Corrigiendo usuario_id de factura 22...")

# 1. Cambiar usuario de 1 a 2
cur.execute("UPDATE facturas SET usuario_id = 2 WHERE id = 22")
print(f"✅ Factura actualizada: {cur.rowcount} fila")

# 2. Borrar inventario del usuario 2 (si existe)
cur.execute("DELETE FROM inventario_usuario WHERE usuario_id = 2")
print(f"🗑️ Inventario del usuario 2 limpio: {cur.rowcount} filas borradas")

# 3. Verificar cuántos items tienen producto_maestro_id
cur.execute("""
    SELECT COUNT(*)
    FROM items_factura
    WHERE factura_id = 22 AND producto_maestro_id IS NOT NULL
""")
items_validos = cur.fetchone()[0]
print(f"📦 Items con producto_maestro_id: {items_validos}")

# 4. Forzar actualización del inventario (CON COLUMNAS CORRECTAS)
cur.execute("""
    INSERT INTO inventario_usuario (
        usuario_id,
        producto_maestro_id,
        cantidad_actual,
        unidad_medida,
        fecha_ultima_actualizacion
    )
    SELECT
        2,
        i.producto_maestro_id,
        SUM(i.cantidad),
        'unidades',
        NOW()
    FROM items_factura i
    WHERE i.factura_id = 22
      AND i.producto_maestro_id IS NOT NULL
    GROUP BY i.producto_maestro_id
    ON CONFLICT (usuario_id, producto_maestro_id)
    DO UPDATE SET
        cantidad_actual = inventario_usuario.cantidad_actual + EXCLUDED.cantidad_actual,
        fecha_ultima_actualizacion = NOW()
""")
productos_insertados = cur.rowcount
print(f"✅ Inventario actualizado: {productos_insertados} productos")

conn.commit()

# 5. Verificar resultado
print("\n" + "="*60)
print("📊 VERIFICACIÓN FINAL")
print("="*60)

cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = 2")
count_inv = cur.fetchone()[0]
print(f"✅ Usuario 2 tiene {count_inv} productos en inventario")

cur.execute("""
    SELECT pm.nombre_normalizado, inv.cantidad_actual, inv.unidad_medida
    FROM inventario_usuario inv
    JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
    WHERE inv.usuario_id = 2
    ORDER BY pm.nombre_normalizado
    LIMIT 10
""")
productos = cur.fetchall()

print(f"\n📦 Primeros 10 productos en inventario del usuario 2:")
for p in productos:
    print(f"   • {p[0]} - {p[1]} {p[2]}")

cur.close()
conn.close()

print("\n✅ LISTO! Ahora verifica:")
print("   1. En navegador: https://lecfac-backend-production.up.railway.app/api/inventario/usuario/2")
print("   2. En Flutter: Ve a la pantalla de Inventario")
