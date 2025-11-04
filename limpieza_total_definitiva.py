# limpieza_total_definitiva.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("=" * 80)
print("🧹 LIMPIEZA TOTAL - PREPARACIÓN PARA PRUEBA V3.0")
print("=" * 80)

# 1. Borrar items_factura
cur.execute("DELETE FROM items_factura")
items_borrados = cur.rowcount
print(f"\n✅ Items de factura borrados: {items_borrados}")

# 2. Borrar facturas
cur.execute("DELETE FROM facturas")
facturas_borradas = cur.rowcount
print(f"✅ Facturas borradas: {facturas_borradas}")

# 3. Borrar inventarios
cur.execute("DELETE FROM inventario_usuario")
inventarios_borrados = cur.rowcount
print(f"✅ Inventarios borrados: {inventarios_borrados}")

# 4. Borrar precios
cur.execute("DELETE FROM precios_productos")
precios_borrados = cur.rowcount
print(f"✅ Precios borrados: {precios_borrados}")

# 5. Borrar variantes (nuevo sistema)
cur.execute("DELETE FROM productos_variantes")
variantes_borradas = cur.rowcount
print(f"✅ Variantes borradas: {variantes_borradas}")

# 6. Borrar canónicos (nuevo sistema)
cur.execute("DELETE FROM productos_canonicos")
canonicos_borrados = cur.rowcount
print(f"✅ Productos canónicos borrados: {canonicos_borrados}")

# 7. Borrar productos maestros
cur.execute("DELETE FROM productos_maestros")
maestros_borrados = cur.rowcount
print(f"✅ Productos maestros borrados: {maestros_borrados}")

conn.commit()

# Verificación
print("\n" + "=" * 80)
print("📊 VERIFICACIÓN POST-LIMPIEZA")
print("=" * 80)

tablas_verificar = [
    'items_factura',
    'facturas',
    'inventario_usuario',
    'precios_productos',
    'productos_variantes',
    'productos_canonicos',
    'productos_maestros'
]

for tabla in tablas_verificar:
    cur.execute(f"SELECT COUNT(*) FROM {tabla}")
    count = cur.fetchone()[0]
    emoji = "✅" if count == 0 else "⚠️"
    print(f"{emoji} {tabla:<25} {count} registros")

# Verificar usuarios (NO borrar)
cur.execute("SELECT id, email FROM usuarios ORDER BY id")
usuarios = cur.fetchall()

print("\n" + "=" * 80)
print("👥 USUARIOS DISPONIBLES (NO BORRADOS)")
print("=" * 80)

for u in usuarios:
    print(f"   • Usuario {u[0]}: {u[1]}")

cur.close()
conn.close()

print("\n" + "=" * 80)
print("✅ LIMPIEZA COMPLETADA")
print("=" * 80)
print("\n🎯 SIGUIENTE PASO:")
print("   1. Abre la app en Flutter")
print("   2. Login con cualquier usuario (santiago, vicky o mama)")
print("   3. Escanea UNA factura UNA vez")
print("   4. Verifica el inventario")
print("\n⚠️ REGLA: Cada factura física = 1 escaneo por 1 usuario")
print("=" * 80)
