#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🔧 RESTAURAR TOTAL CORRECTO - Factura ID 3
El total de $284,220 es CORRECTO (suma de todos los items)
Este script lo restaura al valor correcto
"""

import psycopg

# 🔗 CONEXIÓN DIRECTA A RAILWAY
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def format_price(price):
    """Formatea precio para mostrar en pesos colombianos"""
    if price is None:
        return "$0"
    return f"${price:,.0f}".replace(",", ".")

def restaurar_total_correcto():
    """Restaura el total correcto de la factura ID 3"""

    print("=" * 80)
    print("🔧 RESTAURAR TOTAL CORRECTO - Factura ID 3")
    print("=" * 80)
    print()

    try:
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa a Railway\n")

        # 1. Ver estado actual
        cursor.execute("""
            SELECT id, establecimiento, total_factura, fecha_factura
            FROM facturas
            WHERE id = 3
        """)

        factura = cursor.fetchone()

        if not factura:
            print("❌ Factura ID 3 no encontrada")
            conn.close()
            return

        id_fac, establecimiento, total_actual, fecha = factura

        print("📋 ESTADO ACTUAL:")
        print(f"   ID: {id_fac}")
        print(f"   Establecimiento: {establecimiento}")
        print(f"   Total en BD: {format_price(total_actual)}")
        print(f"   Fecha: {fecha}")
        print()

        # 2. Calcular total real sumando items
        cursor.execute("""
            SELECT
                nombre_leido,
                precio_pagado,
                cantidad
            FROM items_factura
            WHERE factura_id = 3
            ORDER BY id
        """)

        items = cursor.fetchall()

        print(f"📦 ITEMS EN LA FACTURA ({len(items)} productos):")
        print()

        total_real = 0

        for item in items:
            nombre, precio, cantidad = item
            subtotal = precio * cantidad
            total_real += subtotal

            print(f"   • {nombre}")
            print(f"     {format_price(precio)} × {cantidad} = {format_price(subtotal)}")

        print()
        print("─" * 80)
        print(f"💰 TOTAL REAL (suma items):    {format_price(total_real)}")
        print(f"📊 Total actual en BD:         {format_price(total_actual)}")
        print(f"📉 Diferencia:                 {format_price(abs(total_real - total_actual))}")
        print("─" * 80)
        print()

        if total_real == total_actual:
            print("✅ El total YA está correcto. No se requiere actualización.")
            conn.close()
            return

        print(f"🔄 CORRECCIÓN NECESARIA:")
        print(f"   Cambiar de {format_price(total_actual)} → {format_price(total_real)}")
        print()

        confirmacion = input("¿Actualizar el total? Escribe 'SI': ")

        if confirmacion.strip().upper() != "SI":
            print("❌ Operación cancelada")
            conn.close()
            return

        # 3. Actualizar total
        cursor.execute("""
            UPDATE facturas
            SET total_factura = %s
            WHERE id = 3
        """, (total_real,))

        conn.commit()

        print()
        print("✅ Total restaurado correctamente")

        # 4. Verificar
        cursor.execute("""
            SELECT total_factura
            FROM facturas
            WHERE id = 3
        """)

        total_verificado = cursor.fetchone()[0]

        print()
        print("📋 ESTADO FINAL:")
        print(f"   Total factura ID 3: {format_price(total_verificado)}")
        print("   ✅ Total ahora coincide con la suma real de los items")

        cursor.close()
        conn.close()

        print()
        print("=" * 80)
        print("✅ RESTAURACIÓN COMPLETADA")
        print("=" * 80)

    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    restaurar_total_correcto()
