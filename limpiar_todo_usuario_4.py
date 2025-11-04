# limpiar_todo_usuario_4.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🧹 LIMPIANDO USUARIO 4\n")

# Limpiar todo del usuario 4
cur.execute("DELETE FROM inventario_usuario WHERE usuario_id = 4")
print(f"✅ {cur.rowcount} items de inventario eliminados")

cur.execute("DELETE FROM items_factura WHERE usuario_id = 4")
print(f"✅ {cur.rowcount} items de factura eliminados")

cur.execute("DELETE FROM precios_productos WHERE usuario_id = 4")
print(f"✅ {cur.rowcount} precios eliminados")

cur.execute("DELETE FROM processing_jobs WHERE usuario_id = 4")
print(f"✅ {cur.rowcount} processing jobs eliminados")

cur.execute("DELETE FROM facturas WHERE usuario_id = 4")
print(f"✅ {cur.rowcount} facturas eliminadas")

conn.commit()
cur.close()
conn.close()

print("\n✅ Usuario 4 limpio y listo para escanear")
