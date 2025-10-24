#!/usr/bin/env python3
"""
🔍 DIAGNÓSTICO: Total Gastado Multiplicado x100
================================================
Este script investiga por qué los totales gastados aparecen 100 veces más grandes

Verifica:
1. Datos reales en items_factura (precios y cantidades)
2. Datos en inventario_usuario (total_gastado)
3. Cálculos manuales vs valores guardados
"""

import os
import sys
from datetime import datetime

# Configurar para usar PostgreSQL
os.environ["DATABASE_TYPE"] = "postgresql"

# Importar después de configurar
from database import get_db_connection


def diagnosticar_total_gastado():
    """Diagnosticar el problema del total gastado"""

    print("=" * 80)
    print("🔍 DIAGNÓSTICO: TOTAL GASTADO x100")
    print("=" * 80)
    print()

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()

    # 1. VERIFICAR USUARIO santiago@tscamp.co
    print("👤 PASO 1: Verificar usuario")
    print("-" * 80)

    cursor.execute("""
        SELECT id, email, nombre, facturas_aportadas, productos_aportados
        FROM usuarios
        WHERE email = 'santiago@tscamp.co'
    """)

    usuario = cursor.fetchone()

    if not usuario:
        print("❌ Usuario santiago@tscamp.co no encontrado")
        cursor.close()
        conn.close()
        return

    usuario_id = usuario[0]
    print(f"✅ Usuario encontrado:")
    print(f"   ID: {usuario[0]}")
    print(f"   Email: {usuario[1]}")
    print(f"   Nombre: {usuario[2]}")
    print(f"   Facturas: {usuario[3]}")
    print(f"   Productos: {usuario[4]}")
    print()

    # 2. VERIFICAR FACTURAS DEL USUARIO
    print("📄 PASO 2: Ver facturas del usuario")
    print("-" * 80)

    cursor.execute("""
        SELECT id, establecimiento, fecha_compra, total,
               cantidad_productos, fecha_carga
        FROM facturas
        WHERE usuario_id = %s
        ORDER BY fecha_carga DESC
        LIMIT 5
    """, (usuario_id,))

    facturas = cursor.fetchall()
    print(f"Total facturas: {len(facturas)}")
    print()

    for i, fac in enumerate(facturas[:3], 1):
        print(f"Factura {i}:")
        print(f"   ID: {fac[0]}")
        print(f"   Establecimiento: {fac[1]}")
        print(f"   Fecha compra: {fac[2]}")
        print(f"   Total factura: ${fac[3]:,.0f}")
        print(f"   Productos: {fac[4]}")
        print(f"   Fecha carga: {fac[5]}")
        print()

    # 3. VERIFICAR ITEMS DE UNA FACTURA
    print("🛒 PASO 3: Ver items de la primera factura")
    print("-" * 80)

    if facturas:
        factura_id = facturas[0][0]

        cursor.execute("""
            SELECT
                if.id,
                if.nombre_producto,
                if.cantidad,
                if.precio_unitario,
                if.precio_total,
                if.producto_maestro_id,
                pm.nombre as nombre_maestro
            FROM items_factura if
            LEFT JOIN producto_maestro pm ON if.producto_maestro_id = pm.id
            WHERE if.factura_id = %s
            ORDER BY if.precio_total DESC
            LIMIT 10
        """, (factura_id,))

        items = cursor.fetchall()

        print(f"Factura ID: {factura_id}")
        print(f"Total items: {len(items)}")
        print()

        suma_manual = 0
        for i, item in enumerate(items[:5], 1):
            precio_calculado = item[2] * item[3]  # cantidad * precio_unitario
            diferencia = item[4] - precio_calculado
            suma_manual += item[4]

            print(f"Item {i}: {item[1][:40]}")
            print(f"   Cantidad: {item[2]}")
            print(f"   Precio unitario: ${item[3]:,.0f}")
            print(f"   Precio total BD: ${item[4]:,.0f}")
            print(f"   Precio calculado: ${precio_calculado:,.0f}")

            if abs(diferencia) > 0.01:
                print(f"   ⚠️ DIFERENCIA: ${diferencia:,.0f}")
            else:
                print(f"   ✅ CORRECTO")

            print()

        print(f"Suma manual (primeros 5): ${suma_manual:,.0f}")
        print()

    # 4. VERIFICAR INVENTARIO DEL USUARIO
    print("📦 PASO 4: Ver inventario del usuario")
    print("-" * 80)

    cursor.execute("""
        SELECT
            iu.id,
            pm.nombre,
            iu.cantidad_actual,
            iu.precio_ultima_compra,
            iu.numero_compras,
            iu.cantidad_total_comprada,
            iu.total_gastado,
            iu.precio_promedio
        FROM inventario_usuario iu
        JOIN producto_maestro pm ON iu.producto_maestro_id = pm.id
        WHERE iu.usuario_id = %s
        ORDER BY iu.total_gastado DESC
        LIMIT 10
    """, (usuario_id,))

    inventarios = cursor.fetchall()

    print(f"Total productos en inventario: {len(inventarios)}")
    print()

    total_gastado_suma = 0
    problemas_encontrados = []

    for i, inv in enumerate(inventarios[:5], 1):
        nombre = inv[1][:40]
        cantidad_actual = inv[2]
        precio_ultima = inv[3]
        num_compras = inv[4]
        cant_total = inv[5]
        total_gastado = inv[6]
        precio_promedio = inv[7]

        # Calcular total esperado
        total_esperado = precio_promedio * cant_total if precio_promedio else 0
        diferencia = total_gastado - total_esperado

        total_gastado_suma += total_gastado

        print(f"Producto {i}: {nombre}")
        print(f"   Cantidad actual: {cantidad_actual}")
        print(f"   Precio última compra: ${precio_ultima:,.0f}")
        print(f"   Número compras: {num_compras}")
        print(f"   Cantidad total comprada: {cant_total}")
        print(f"   Total gastado BD: ${total_gastado:,.0f}")
        print(f"   Precio promedio: ${precio_promedio:,.2f}")
        print(f"   Total esperado: ${total_esperado:,.0f}")

        # Verificar si hay problema
        if abs(diferencia) > 1:
            ratio = total_gastado / total_esperado if total_esperado > 0 else 0
            print(f"   ⚠️ DIFERENCIA: ${diferencia:,.0f}")
            print(f"   ⚠️ RATIO: {ratio:.2f}x")

            if 99 < ratio < 101:
                problemas_encontrados.append({
                    'producto': nombre,
                    'bd': total_gastado,
                    'esperado': total_esperado,
                    'ratio': ratio
                })
        else:
            print(f"   ✅ CORRECTO")

        print()

    print(f"Suma total gastado (primeros 5): ${total_gastado_suma:,.0f}")
    print()

    # 5. VERIFICAR SI HAY PATRÓN x100
    if problemas_encontrados:
        print("🚨 PASO 5: Problemas encontrados")
        print("-" * 80)
        print(f"Se encontraron {len(problemas_encontrados)} productos con ratios ~100x:")
        print()

        for prob in problemas_encontrados:
            print(f"   • {prob['producto']}")
            print(f"     BD: ${prob['bd']:,.0f}")
            print(f"     Esperado: ${prob['esperado']:,.0f}")
            print(f"     Ratio: {prob['ratio']:.2f}x")
            print()

        print("💡 DIAGNÓSTICO: Los valores en BD están multiplicados por 100")
        print()

    # 6. VERIFICAR UN PRODUCTO ESPECÍFICO EN DETALLE
    print("🔬 PASO 6: Análisis detallado de un producto")
    print("-" * 80)

    if inventarios:
        producto_id = inventarios[0][0]

        cursor.execute("""
            SELECT pm.id, pm.nombre
            FROM inventario_usuario iu
            JOIN producto_maestro pm ON iu.producto_maestro_id = pm.id
            WHERE iu.id = %s
        """, (producto_id,))

        producto_info = cursor.fetchone()

        if producto_info:
            producto_maestro_id = producto_info[0]

            print(f"Producto: {producto_info[1]}")
            print(f"Producto Maestro ID: {producto_maestro_id}")
            print()

            # Ver todas las compras de este producto
            cursor.execute("""
                SELECT
                    f.id,
                    f.fecha_compra,
                    if.cantidad,
                    if.precio_unitario,
                    if.precio_total
                FROM items_factura if
                JOIN facturas f ON if.factura_id = f.id
                WHERE if.producto_maestro_id = %s
                  AND f.usuario_id = %s
                ORDER BY f.fecha_compra DESC
            """, (producto_maestro_id, usuario_id))

            compras = cursor.fetchall()

            print(f"Historial de compras ({len(compras)}):")
            print()

            suma_manual_compras = 0
            for j, compra in enumerate(compras[:5], 1):
                precio_calc = compra[2] * compra[3]
                suma_manual_compras += compra[4]

                print(f"   Compra {j} - Factura #{compra[0]}")
                print(f"      Fecha: {compra[1]}")
                print(f"      Cantidad: {compra[2]}")
                print(f"      Precio unit: ${compra[3]:,.0f}")
                print(f"      Total BD: ${compra[4]:,.0f}")
                print(f"      Total calc: ${precio_calc:,.0f}")
                print()

            print(f"Suma manual de compras: ${suma_manual_compras:,.0f}")
            print(f"Total gastado en inventario: ${inventarios[0][6]:,.0f}")

            if abs(inventarios[0][6] - suma_manual_compras) > 1:
                print(f"⚠️ NO COINCIDEN")
            else:
                print(f"✅ COINCIDEN")

    # 7. RESUMEN FINAL
    print()
    print("=" * 80)
    print("📊 RESUMEN DEL DIAGNÓSTICO")
    print("=" * 80)
    print()

    cursor.execute("""
        SELECT SUM(total_gastado)
        FROM inventario_usuario
        WHERE usuario_id = %s
    """, (usuario_id,))

    total_sistema = cursor.fetchone()[0] or 0

    print(f"Total gastado en sistema: ${total_sistema:,.0f}")

    if total_sistema > 100000000:  # Más de 100 millones
        print("🚨 CONFIRMADO: Total gastado está inflado ~100x")
        print()
        print("🔧 POSIBLES CAUSAS:")
        print("   1. Los precios se guardaron multiplicados por 100")
        print("   2. Hay un error en el cálculo de total_gastado")
        print("   3. El OCR devuelve precios en centavos y se guardaron sin convertir")
    elif problemas_encontrados:
        print("⚠️ Se encontraron inconsistencias en algunos productos")
    else:
        print("✅ Los datos parecen correctos")

    print()

    cursor.close()
    conn.close()


if __name__ == "__main__":
    try:
        diagnosticar_total_gastado()
    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")
        import traceback
        traceback.print_exc()
