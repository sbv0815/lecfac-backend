# limpiar_factura_23.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🧹 LIMPIANDO DATOS CORRUPTOS DE FACTURA #23\n")

# 1. Ver qué hay antes de limpiar
print("📊 ANTES DE LIMPIAR:")
cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = 1")
inv_antes = cur.fetchone()[0]
print(f"   Inventario usuario 1: {inv_antes} productos")

cur.execute("SELECT COUNT(*) FROM items_factura WHERE factura_id = 23")
items_antes = cur.fetchone()[0]
print(f"   Items factura 23: {items_antes} items")

# 2. Eliminar inventario del usuario 1 (tiene datos malos)
print("\n🗑️ Eliminando inventario corrupto...")
cur.execute("DELETE FROM inventario_usuario WHERE usuario_id = 1")
inv_eliminados = cur.rowcount
print(f"   ✅ {inv_eliminados} productos eliminados del inventario")

# 3. Eliminar items de factura 23
cur.execute("DELETE FROM items_factura WHERE factura_id = 23")
items_eliminados = cur.rowcount
print(f"   ✅ {items_eliminados} items eliminados")

# 4. Eliminar precios de factura 23
cur.execute("DELETE FROM precios_productos WHERE factura_id = 23")
precios_eliminados = cur.rowcount
print(f"   ✅ {precios_eliminados} precios eliminados")

# 5. Eliminar processing job
cur.execute("DELETE FROM processing_jobs WHERE factura_id = 23")
job_eliminado = cur.rowcount
print(f"   ✅ {job_eliminado} processing job eliminado")

# 6. Eliminar la factura
cur.execute("DELETE FROM facturas WHERE id = 23")
factura_eliminada = cur.rowcount
print(f"   ✅ {factura_eliminada} factura eliminada")

conn.commit()

# 7. Verificar después de limpiar
print("\n📊 DESPUÉS DE LIMPIAR:")
cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = 1")
inv_despues = cur.fetchone()[0]
print(f"   Inventario usuario 1: {inv_despues} productos")

cur.execute("SELECT COUNT(*) FROM facturas WHERE usuario_id = 1")
facturas_restantes = cur.fetchone()[0]
print(f"   Facturas restantes: {facturas_restantes}")

cur.close()
conn.close()

print("\n✅ LIMPIEZA COMPLETADA")
print("🎯 Ahora puedes escanear la factura de nuevo con V3.0")
