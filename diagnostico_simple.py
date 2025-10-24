#!/usr/bin/env python3
"""
🔍 DIAGNÓSTICO RÁPIDO - Total Gastado x100
Ejecutar en Railway Shell o localmente con DATABASE_URL
"""

import os
import sys

# Configurar PostgreSQL
os.environ["DATABASE_TYPE"] = "postgresql"

try:
    from database import get_db_connection
except ImportError:
    print("❌ No se puede importar database.py")
    print("Asegúrate de ejecutar esto desde la carpeta del proyecto")
    sys.exit(1)

def main():
    print("\n" + "=" * 70)
    print("🔍 DIAGNÓSTICO: Total Gastado x100")
    print("=" * 70 + "\n")

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()

    # 1. Usuario
    cursor.execute("""
        SELECT id, email, facturas_aportadas
        FROM usuarios
        WHERE email = 'santiago@tscamp.co'
    """)
    usuario = cursor.fetchone()

    if not usuario:
        print("❌ Usuario no encontrado")
        return

    usuario_id = usuario[0]
    print(f"👤 Usuario: {usuario[1]}")
    print(f"   ID: {usuario[0]}")
    print(f"   Facturas: {usuario[2]}\n")

    # 2. Facturas
    cursor.execute("""
        SELECT id, establecimiento, total, cantidad_productos
        FROM facturas
        WHERE usuario_id = %s
        ORDER BY id DESC
        LIMIT 2
    """, (usuario_id,))

    facturas = cursor.fetchall()
    print(f"📄 Facturas recientes:")
    for fac in facturas:
        print(f"   Factura #{fac[0]}: {fac[1]} - ${fac[2]:,.0f} ({fac[3]} productos)")
    print()

    # 3. Items de primera factura
    if facturas:
        factura_id = facturas[0][0]
        cursor.execute("""
            SELECT nombre_producto, cantidad, precio_unitario, precio_total
            FROM items_factura
            WHERE factura_id = %s
            ORDER BY precio_total DESC
            LIMIT 5
        """, (factura_id,))

        print(f"🛒 Items de Factura #{factura_id}:")
        items = cursor.fetchall()

        for item in items:
            calculado = item[1] * item[2]
            dif = item[3] - calculado

            print(f"\n   {item[0][:40]}")
            print(f"   Cantidad: {item[1]}")
            print(f"   Precio unit: ${item[2]:,.0f}")
            print(f"   Total calc: ${calculado:,.0f}")
            print(f"   Total BD:   ${item[3]:,.0f}", end="")

            if abs(dif) > 1:
                print(f" ⚠️ DIFERENCIA: ${dif:,.0f}")
            else:
                print(" ✅")

        print()

    # 4. Inventario
    cursor.execute("""
        SELECT
            pm.nombre,
            iu.cantidad_total_comprada,
            iu.precio_promedio,
            iu.total_gastado,
            iu.numero_compras
        FROM inventario_usuario iu
        JOIN producto_maestro pm ON iu.producto_maestro_id = pm.id
        WHERE iu.usuario_id = %s
        ORDER BY iu.total_gastado DESC
        LIMIT 5
    """, (usuario_id,))

    print("📦 Inventario (Top 5 por gasto):")
    inventarios = cursor.fetchall()

    problemas = 0
    for inv in inventarios:
        esperado = inv[1] * inv[2] if inv[2] else 0
        ratio = inv[3] / esperado if esperado > 0 else 0
        es_problema = 99 < ratio < 101

        if es_problema:
            problemas += 1

        print(f"\n   {inv[0][:40]}")
        print(f"   Compras: {inv[4]}x | Cantidad total: {inv[1]}")
        print(f"   Precio prom: ${inv[2]:,.2f}")
        print(f"   Total BD:    ${inv[3]:,.0f}")
        print(f"   Esperado:    ${esperado:,.0f}")
        print(f"   Ratio:       {ratio:.1f}x", end="")

        if es_problema:
            print(" ⚠️ PROBLEMA x100")
        else:
            print()

    print()

    # 5. Total general
    cursor.execute("""
        SELECT SUM(total_gastado)
        FROM inventario_usuario
        WHERE usuario_id = %s
    """, (usuario_id,))

    total = cursor.fetchone()[0] or 0

    print("=" * 70)
    print(f"💰 TOTAL SISTEMA: ${total:,.0f}")
    print(f"⚠️  Productos con ratio ~100x: {problemas}")

    if total > 10000000:
        print("\n🚨 DIAGNÓSTICO: Total gastado está INFLADO x100")
    elif problemas > 0:
        print(f"\n⚠️  DIAGNÓSTICO: {problemas} productos tienen ratio ~100x")
    else:
        print("\n✅ DIAGNÓSTICO: Datos parecen correctos")

    print("=" * 70 + "\n")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
