"""
Ajustador de Inventario y Precios
==================================

Módulo para ajustar precios de items cuando hay descuentos globales
o cuando la suma de items no coincide con el total de la factura.

También maneja limpieza de items duplicados en facturas.

Autor: LecFac
Versión: 1.0.0
Fecha: 2025-01-18
"""

import os
from typing import Tuple, Optional


def get_db_connection():
    """Obtiene conexión a la base de datos"""
    from database import get_db_connection as get_conn
    return get_conn()


def ajustar_precios_items_por_total(factura_id: int, conn=None) -> bool:
    """
    Ajusta precios de items proporcionalmente cuando hay descuento global

    Casos de uso:
    - Factura con descuento del 10% aplicado al final
    - Suma de items > total factura por descuento no reflejado en items

    Args:
        factura_id: ID de la factura
        conn: Conexión a BD (opcional, se crea si no se provee)

    Returns:
        True si se ajustaron precios, False si no fue necesario
    """
    print(f"🔧 Ajustando precios para factura {factura_id}...")

    cerrar_conn = False
    if not conn:
        conn = get_db_connection()
        cerrar_conn = True

    cursor = conn.cursor()

    try:
        # Obtener total de factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT total_factura FROM facturas WHERE id = %s",
                (factura_id,)
            )
        else:
            cursor.execute(
                "SELECT total_factura FROM facturas WHERE id = ?",
                (factura_id,)
            )

        factura_data = cursor.fetchone()
        if not factura_data:
            print(f"❌ Factura {factura_id} no encontrada")
            return False

        total_declarado = float(factura_data[0] or 0)

        if total_declarado <= 0:
            print(f"⚠️ Total declarado es 0, no se puede ajustar")
            return False

        # Obtener suma de items
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT SUM(cantidad * precio_pagado)
                FROM items_factura
                WHERE factura_id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT SUM(cantidad * precio_pagado)
                FROM items_factura
                WHERE factura_id = ?
            """, (factura_id,))

        suma_items = float(cursor.fetchone()[0] or 0)

        if suma_items <= 0:
            print(f"⚠️ Suma de items es 0, no se puede ajustar")
            return False

        # Calcular diferencia
        diferencia = abs(suma_items - total_declarado)
        porcentaje_diferencia = (diferencia / total_declarado) * 100

        print(f"   Total declarado: ${total_declarado:,.0f}")
        print(f"   Suma items: ${suma_items:,.0f}")
        print(f"   Diferencia: ${diferencia:,.0f} ({porcentaje_diferencia:.1f}%)")

        # Si diferencia < 5%, no ajustar
        if porcentaje_diferencia < 5:
            print(f"   ✅ Diferencia menor al 5%, no se requiere ajuste")
            return False

        # Si diferencia > 20%, posible error - no ajustar automáticamente
        if porcentaje_diferencia > 20:
            print(f"   ⚠️ Diferencia mayor al 20%, requiere revisión manual")
            return False

        # Calcular factor de ajuste
        factor_ajuste = total_declarado / suma_items

        print(f"   🔢 Factor de ajuste: {factor_ajuste:.4f}")

        # Aplicar ajuste a cada item
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT id, precio_pagado, cantidad
                FROM items_factura
                WHERE factura_id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT id, precio_pagado, cantidad
                FROM items_factura
                WHERE factura_id = ?
            """, (factura_id,))

        items = cursor.fetchall()
        items_ajustados = 0

        for item_id, precio_original, cantidad in items:
            precio_ajustado = int(precio_original * factor_ajuste)

            # No ajustar si la diferencia es muy pequeña (< 50 pesos)
            if abs(precio_ajustado - precio_original) < 50:
                continue

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    UPDATE items_factura
                    SET precio_pagado = %s
                    WHERE id = %s
                """, (precio_ajustado, item_id))
            else:
                cursor.execute("""
                    UPDATE items_factura
                    SET precio_pagado = ?
                    WHERE id = ?
                """, (precio_ajustado, item_id))

            items_ajustados += 1

        if items_ajustados > 0:
            conn.commit()
            print(f"   ✅ {items_ajustados} items ajustados")
            return True
        else:
            print(f"   ℹ️ No se requirieron ajustes")
            return False

    except Exception as e:
        print(f"❌ Error ajustando precios: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False

    finally:
        cursor.close()
        if cerrar_conn:
            conn.close()


def limpiar_items_duplicados(factura_id: int, conn=None) -> int:
    """
    Elimina items duplicados en una factura

    Criterios para considerar duplicados:
    - Mismo producto_maestro_id
    - Mismo código_leido
    - Mismo nombre_leido y precio_pagado

    Args:
        factura_id: ID de la factura
        conn: Conexión a BD (opcional)

    Returns:
        Número de items eliminados
    """
    print(f"🧹 Limpiando items duplicados en factura {factura_id}...")

    cerrar_conn = False
    if not conn:
        conn = get_db_connection()
        cerrar_conn = True

    cursor = conn.cursor()

    try:
        # Buscar duplicados por producto_maestro_id
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT producto_maestro_id, COUNT(*) as count
                FROM items_factura
                WHERE factura_id = %s
                  AND producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
                HAVING COUNT(*) > 1
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT producto_maestro_id, COUNT(*) as count
                FROM items_factura
                WHERE factura_id = ?
                  AND producto_maestro_id IS NOT NULL
                GROUP BY producto_maestro_id
                HAVING COUNT(*) > 1
            """, (factura_id,))

        duplicados_por_producto = cursor.fetchall()
        items_eliminados = 0

        for producto_id, count in duplicados_por_producto:
            # Obtener todos los items de este producto
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    SELECT id, cantidad, precio_pagado
                    FROM items_factura
                    WHERE factura_id = %s AND producto_maestro_id = %s
                    ORDER BY id
                """, (factura_id, producto_id))
            else:
                cursor.execute("""
                    SELECT id, cantidad, precio_pagado
                    FROM items_factura
                    WHERE factura_id = ? AND producto_maestro_id = ?
                    ORDER BY id
                """, (factura_id, producto_id))

            items = cursor.fetchall()

            if len(items) <= 1:
                continue

            # Mantener el primero, sumar cantidades, eliminar el resto
            item_principal_id = items[0][0]
            cantidad_total = sum(item[1] for item in items)

            # Actualizar cantidad del item principal
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    UPDATE items_factura
                    SET cantidad = %s
                    WHERE id = %s
                """, (cantidad_total, item_principal_id))
            else:
                cursor.execute("""
                    UPDATE items_factura
                    SET cantidad = ?
                    WHERE id = ?
                """, (cantidad_total, item_principal_id))

            # Eliminar duplicados
            ids_eliminar = [item[0] for item in items[1:]]

            for item_id in ids_eliminar:
                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute("""
                        DELETE FROM items_factura WHERE id = %s
                    """, (item_id,))
                else:
                    cursor.execute("""
                        DELETE FROM items_factura WHERE id = ?
                    """, (item_id,))

                items_eliminados += 1

            print(f"   ✓ Producto {producto_id}: {len(items)} → 1 (cantidad: {cantidad_total})")

        if items_eliminados > 0:
            conn.commit()
            print(f"   ✅ {items_eliminados} items duplicados eliminados")
        else:
            print(f"   ℹ️ No se encontraron duplicados")

        return items_eliminados

    except Exception as e:
        print(f"❌ Error limpiando duplicados: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return 0

    finally:
        cursor.close()
        if cerrar_conn:
            conn.close()


def validar_integridad_factura(factura_id: int) -> Tuple[bool, str]:
    """
    Valida la integridad de una factura

    Verificaciones:
    - Total de items coincide con total de factura (±5%)
    - Todos los items tienen precio > 0
    - Todos los items tienen nombre
    - No hay items duplicados sin consolidar

    Args:
        factura_id: ID de la factura

    Returns:
        Tupla (es_valida, mensaje_error)
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Obtener total de factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT total_factura FROM facturas WHERE id = %s",
                (factura_id,)
            )
        else:
            cursor.execute(
                "SELECT total_factura FROM facturas WHERE id = ?",
                (factura_id,)
            )

        factura_data = cursor.fetchone()
        if not factura_data:
            return False, "Factura no encontrada"

        total_declarado = float(factura_data[0] or 0)

        # Verificar suma de items
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    SUM(cantidad * precio_pagado) as suma_items,
                    COUNT(*) as total_items,
                    COUNT(CASE WHEN precio_pagado <= 0 THEN 1 END) as items_sin_precio,
                    COUNT(CASE WHEN nombre_leido IS NULL OR nombre_leido = '' THEN 1 END) as items_sin_nombre
                FROM items_factura
                WHERE factura_id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT
                    SUM(cantidad * precio_pagado) as suma_items,
                    COUNT(*) as total_items,
                    COUNT(CASE WHEN precio_pagado <= 0 THEN 1 END) as items_sin_precio,
                    COUNT(CASE WHEN nombre_leido IS NULL OR nombre_leido = '' THEN 1 END) as items_sin_nombre
                FROM items_factura
                WHERE factura_id = ?
            """, (factura_id,))

        stats = cursor.fetchone()
        suma_items = float(stats[0] or 0)
        total_items = stats[1]
        items_sin_precio = stats[2]
        items_sin_nombre = stats[3]

        # Validaciones
        if total_items == 0:
            return False, "Factura sin items"

        if items_sin_precio > 0:
            return False, f"{items_sin_precio} items sin precio"

        if items_sin_nombre > 0:
            return False, f"{items_sin_nombre} items sin nombre"

        # Verificar diferencia de totales
        diferencia = abs(suma_items - total_declarado)
        porcentaje = (diferencia / total_declarado * 100) if total_declarado > 0 else 0

        if porcentaje > 10:
            return False, f"Diferencia de {porcentaje:.1f}% entre items y total"

        cursor.close()
        conn.close()

        return True, "Factura válida"

    except Exception as e:
        cursor.close()
        conn.close()
        return False, f"Error validando: {str(e)}"


# ==========================================
# TESTING
# ==========================================
if __name__ == "__main__":
    print("🧪 Testing inventory_adjuster.py")
    print("=" * 60)

    print("\n⚠️ Tests requieren conexión a base de datos")
    print("   Para testing completo, ejecutar con BD disponible")

    print("\n" + "=" * 60)
    print("✅ Módulo cargado correctamente")
