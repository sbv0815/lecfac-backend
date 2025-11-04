# limpieza_solo_datos_usuarios_FIXED.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("=" * 80)
print("🧹 LIMPIEZA DE DATOS DE USUARIOS")
print("   ✅ PRESERVA: TODO EL CATÁLOGO DE PRODUCTOS")
print("=" * 80)

# Mostrar qué se va a mantener
cur.execute("SELECT COUNT(*) FROM productos_maestros")
total_maestros = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM productos_canonicos")
total_canonicos = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM productos_variantes")
total_variantes = cur.fetchone()[0]

print(f"\n✅ SE MANTENDRÁN:")
print(f"   📚 {total_maestros:,} productos maestros (tu catálogo)")
print(f"   🎯 {total_canonicos:,} productos canónicos")
print(f"   🔄 {total_variantes:,} variantes")
print(f"   🏪 Todos los establecimientos")
print(f"   👥 Todos los usuarios")

# Mostrar qué se va a borrar
cur.execute("SELECT COUNT(*) FROM facturas")
total_facturas = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM items_factura")
total_items = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM inventario_usuario")
total_inventarios = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM precios_productos")
total_precios = cur.fetchone()[0]

cur.execute("SELECT COUNT(*) FROM processing_jobs")
total_jobs = cur.fetchone()[0]

print(f"\n❌ SE BORRARÁN (datos de pruebas):")
print(f"   📄 {total_facturas:,} facturas escaneadas")
print(f"   📦 {total_items:,} items de facturas")
print(f"   🏠 {total_inventarios:,} registros de inventario")
print(f"   💰 {total_precios:,} precios históricos")
print(f"   ⚙️ {total_jobs:,} jobs de procesamiento")

print("\n" + "="*80)
respuesta = input("¿Continuar con la limpieza? (escribe 'SI'): ")

if respuesta != "SI":
    print("❌ Limpieza cancelada")
    cur.close()
    conn.close()
    exit()

print("\n🧹 Ejecutando limpieza en orden correcto...")

# ============================================
# ORDEN CORRECTO (de hijos a padres)
# ============================================

# 1. Borrar items_factura (hijos de facturas)
cur.execute("DELETE FROM items_factura")
items_borrados = cur.rowcount
print(f"   ✅ Items de factura: {items_borrados:,}")

# 2. Borrar processing_jobs (referencia a facturas)
cur.execute("DELETE FROM processing_jobs")
jobs_borrados = cur.rowcount
print(f"   ✅ Processing jobs: {jobs_borrados:,}")

# 3. Ahora sí, borrar facturas
cur.execute("DELETE FROM facturas")
facturas_borradas = cur.rowcount
print(f"   ✅ Facturas: {facturas_borradas:,}")

# 4. Borrar inventarios
cur.execute("DELETE FROM inventario_usuario")
inventarios_borrados = cur.rowcount
print(f"   ✅ Inventarios: {inventarios_borrados:,}")

# 5. Borrar precios
cur.execute("DELETE FROM precios_productos")
precios_borrados = cur.rowcount
print(f"   ✅ Precios: {precios_borrados:,}")

# 6. Borrar gastos mensuales (si existen)
try:
    cur.execute("DELETE FROM gastos_mensuales")
    gastos_borrados = cur.rowcount
    print(f"   ✅ Gastos mensuales: {gastos_borrados:,}")
except:
    pass

# 7. Borrar patrones de compra (si existen)
try:
    cur.execute("DELETE FROM patrones_compra")
    patrones_borrados = cur.rowcount
    print(f"   ✅ Patrones de compra: {patrones_borrados:,}")
except:
    pass

# 8. Borrar alertas de usuario (si existen)
try:
    cur.execute("DELETE FROM alertas_usuario")
    alertas_borradas = cur.rowcount
    print(f"   ✅ Alertas de usuario: {alertas_borradas:,}")
except:
    pass

# 9. Borrar presupuestos (si existen)
try:
    cur.execute("DELETE FROM presupuesto_usuario")
    presupuestos_borrados = cur.rowcount
    print(f"   ✅ Presupuestos: {presupuestos_borrados:,}")
except:
    pass

conn.commit()

print("\n" + "="*80)
print("📊 VERIFICACIÓN FINAL")
print("="*80)

# Verificar que están vacías
tablas_vacias = [
    'items_factura',
    'processing_jobs',
    'facturas',
    'inventario_usuario',
    'precios_productos'
]

print("\n❌ TABLAS LIMPIADAS:")
for tabla in tablas_vacias:
    cur.execute(f"SELECT COUNT(*) FROM {tabla}")
    count = cur.fetchone()[0]
    emoji = "✅" if count == 0 else "⚠️"
    print(f"   {emoji} {tabla}: {count}")

# Verificar que se mantienen
print("\n✅ CATÁLOGO PRESERVADO:")
cur.execute("SELECT COUNT(*) FROM productos_maestros")
print(f"   📚 productos_maestros: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM productos_canonicos")
print(f"   🎯 productos_canonicos: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM productos_variantes")
print(f"   🔄 productos_variantes: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM establecimientos")
print(f"   🏪 establecimientos: {cur.fetchone()[0]:,}")

cur.execute("SELECT COUNT(*) FROM usuarios")
print(f"   👥 usuarios: {cur.fetchone()[0]:,}")

cur.close()
conn.close()

print("\n" + "="*80)
print("✅ LIMPIEZA COMPLETADA - CATÁLOGO INTACTO")
print("="*80)
print("\n🎯 SIGUIENTE PASO:")
print("   1. Abre Flutter y login con cualquier usuario")
print("   2. Escanea UNA factura UNA vez")
print("   3. El sistema usará tu catálogo existente")
print("   4. Solo verás productos de TU factura en el inventario")
print("\n💡 VENTAJAS:")
print("   ✅ Productos con nombres correctos que ya tienes")
print("   ✅ No se crean duplicados si el EAN ya existe")
print("   ✅ Cada usuario ve solo sus productos")
print("   ✅ El catálogo sigue creciendo")
print("="*80)
