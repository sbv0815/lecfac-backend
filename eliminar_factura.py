#!/usr/bin/env python3
"""
Eliminar factura #2 para reescanear
VERSIÓN FINAL: Incluye processing_jobs
"""

import psycopg2

DATABASE_URL = input("📎 DATABASE_URL: ").strip()
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("\n⚠️  ELIMINAR FACTURA #2")
print("=" * 70)
print("\nEsto eliminará:")
print("  • Factura #2")
print("  • Todos los items asociados")
print("  • Processing jobs relacionados")
print("  • Datos del inventario relacionados")
print("\nPodrás reescanear la factura limpiamente después.")
print()

confirmacion = input("¿Continuar? (escribe SI): ").strip()

if confirmacion != "SI":
    print("❌ Cancelado")
    exit(0)

try:
    print("\n🔧 Eliminando...")

    # 1. Eliminar processing_jobs (primero, tiene FK a factura)
    cursor.execute("DELETE FROM processing_jobs WHERE factura_id = 2")
    jobs_eliminados = cursor.rowcount
    print(f"   ✅ {jobs_eliminados} processing jobs eliminados")

    # 2. Eliminar items_factura
    cursor.execute("DELETE FROM items_factura WHERE factura_id = 2")
    items_eliminados = cursor.rowcount
    print(f"   ✅ {items_eliminados} items eliminados")

    # 3. Eliminar de inventario_usuario
    cursor.execute("""
        DELETE FROM inventario_usuario
        WHERE usuario_id = (SELECT usuario_id FROM facturas WHERE id = 2)
        AND fecha_ultima_compra >= '2025-10-28'
    """)
    inv_eliminados = cursor.rowcount
    print(f"   ✅ {inv_eliminados} items de inventario eliminados")

    # 4. Eliminar factura
    cursor.execute("DELETE FROM facturas WHERE id = 2")
    print(f"   ✅ Factura eliminada")

    conn.commit()
    print("\n" + "=" * 70)
    print("✅ ELIMINACIÓN COMPLETADA")
    print("=" * 70)
    print("\n🎯 SIGUIENTE PASO:")
    print("   1. Abre la app móvil LecFac")
    print("   2. Escanea la factura de Olímpica nuevamente")
    print("   3. Espera 1-2 minutos al procesamiento")
    print("   4. Verifica en el dashboard:")
    print("      ✅ Total = $284,220")
    print("      ✅ Productos = 32 items")
    print("      ✅ Precios correctos")

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    conn.rollback()
    print("   ✅ ROLLBACK ejecutado")
    import traceback
    traceback.print_exc()

cursor.close()
conn.close()
