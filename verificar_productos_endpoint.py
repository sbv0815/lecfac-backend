#!/usr/bin/env python3
"""
Script de diagnóstico para verificar que el endpoint /api/productos
está devolviendo los PLUs correctamente
"""

import os
import sys

# Agregar el directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import get_db_connection

def verificar_endpoint_productos():
    """Simula la query que hace productos_api_v2.py"""

    print("=" * 80)
    print("🔍 DIAGNÓSTICO: Verificando endpoint /api/productos")
    print("=" * 80)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Esta es la query que debería estar usando productos_api_v2.py
        query = """
        WITH producto_plus AS (
            SELECT
                ppe.producto_maestro_id,
                STRING_AGG(
                    ppe.codigo_plu || ' (' || e.nombre_normalizado || ')',
                    ', '
                ) as plus_texto
            FROM productos_por_establecimiento ppe
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.codigo_plu IS NOT NULL
            GROUP BY ppe.producto_maestro_id
        )
        SELECT
            pm.id,
            pm.codigo_ean,
            pm.nombre_normalizado,
            pm.nombre_comercial,
            pm.marca,
            pm.categoria,
            pm.subcategoria,
            pm.precio_promedio_global,
            pm.total_reportes,
            COALESCE(pp.plus_texto, '-') as codigo_plu
        FROM productos_maestros pm
        LEFT JOIN producto_plus pp ON pm.id = pp.producto_maestro_id
        WHERE pm.id IN (830, 67)  -- IDs de ejemplo del resumen
        ORDER BY pm.id
        """

        print("\n📊 Ejecutando query de prueba...")
        print("=" * 80)

        cursor.execute(query)
        rows = cursor.fetchall()

        if not rows:
            print("⚠️ No se encontraron productos con IDs 830 y 67")
            print("\n🔍 Buscando cualquier producto con PLU...")

            cursor.execute("""
                SELECT pm.id, pm.nombre_normalizado,
                       COUNT(ppe.id) as total_plus
                FROM productos_maestros pm
                JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
                WHERE ppe.codigo_plu IS NOT NULL
                GROUP BY pm.id, pm.nombre_normalizado
                LIMIT 5
            """)

            productos_con_plu = cursor.fetchall()

            if productos_con_plu:
                print(f"\n✅ Encontrados {len(productos_con_plu)} productos con PLU:")
                for row in productos_con_plu:
                    print(f"   ID: {row[0]} | Nombre: {row[1]} | PLUs: {row[2]}")

                # Intentar con el primer producto encontrado
                primer_id = productos_con_plu[0][0]
                print(f"\n🔄 Re-intentando query con ID {primer_id}...")

                cursor.execute(query.replace("WHERE pm.id IN (830, 67)", f"WHERE pm.id = {primer_id}"))
                rows = cursor.fetchall()

        print("\n📋 RESULTADOS:")
        print("=" * 80)

        if not rows:
            print("❌ ERROR: No se obtuvieron resultados")
            print("\n🔍 Verificando estructura de tabla productos_por_establecimiento...")

            cursor.execute("""
                SELECT COUNT(*) FROM productos_por_establecimiento
                WHERE codigo_plu IS NOT NULL
            """)
            total_plus = cursor.fetchone()[0]
            print(f"   Total PLUs en BD: {total_plus}")

            if total_plus == 0:
                print("\n⚠️ PROBLEMA: No hay PLUs en la tabla productos_por_establecimiento")
                print("   La migración de datos podría no haberse ejecutado correctamente")

        else:
            for i, row in enumerate(rows, 1):
                print(f"\n📦 Producto {i}:")
                print(f"   ID: {row[0]}")
                print(f"   EAN: {row[1] or '-'}")
                print(f"   Nombre: {row[2]}")
                print(f"   Marca: {row[4] or '-'}")
                print(f"   Categoría: {row[5] or '-'}")
                print(f"   Precio Promedio: ${row[7]:,.0f}" if row[7] else "   Precio: -")
                print(f"   Compras: {row[8] or 0}")
                print(f"   ✅ PLU: {row[9]}")  # <-- Este es el campo crítico

                if row[9] == '-':
                    print("      ⚠️ No tiene PLU asignado")
                else:
                    print(f"      ✅ PLU correctamente formateado: {row[9]}")

        # Verificar estructura del JSON que se enviaría
        print("\n" + "=" * 80)
        print("📤 FORMATO JSON QUE DEBERÍA ENVIARSE AL FRONTEND:")
        print("=" * 80)

        if rows:
            import json
            ejemplo = {
                "id": rows[0][0],
                "codigo_ean": rows[0][1],
                "nombre": rows[0][2],
                "marca": rows[0][4],
                "categoria": rows[0][5],
                "precio_promedio": float(rows[0][7]) if rows[0][7] else 0,
                "veces_comprado": rows[0][8] or 0,
                "codigo_plu": rows[0][9]  # <-- Campo que el JS busca
            }

            print(json.dumps(ejemplo, indent=2, ensure_ascii=False))

        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ DIAGNÓSTICO COMPLETADO")
        print("=" * 80)

        return len(rows) > 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    exito = verificar_endpoint_productos()

    if not exito:
        print("\n⚠️ ACCIONES RECOMENDADAS:")
        print("   1. Verificar que productos_api_v2.py esté usando la query correcta")
        print("   2. Verificar que main.py tenga: app.include_router(productos_v2_router)")
        print("   3. Verificar que la migración de PLUs se haya ejecutado")
        print("   4. Hacer git push y esperar redeploy en Railway")
        sys.exit(1)
    else:
        print("\n✅ Todo parece estar correcto")
        sys.exit(0)
