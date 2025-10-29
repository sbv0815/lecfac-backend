"""
🔧 AJUSTADOR DE INVENTARIO
Corrige diferencias entre total declarado y suma de items
debido a descuentos, duplicados OCR, etc.
"""

def ajustar_precios_items_por_total(factura_id: int, conn):
    """
    Ajusta proporcionalmente los precios de items_factura
    para que la suma coincida con el total declarado.

    Esto soluciona:
    - Descuentos no detectados por OCR
    - Productos duplicados
    - Diferencias de redondeo
    """
    cursor = conn.cursor()

    # Obtener total declarado
    cursor.execute("""
        SELECT total_factura
        FROM facturas
        WHERE id = %s
    """, (factura_id,))

    result = cursor.fetchone()
    if not result:
        return False

    total_declarado = float(result[0] or 0)
    if total_declarado <= 0:
        return False

    # Calcular suma actual
    cursor.execute("""
        SELECT SUM(cantidad * precio_pagado) as suma_actual
        FROM items_factura
        WHERE factura_id = %s
    """, (factura_id,))

    suma_actual = float(cursor.fetchone()[0] or 0)
    if suma_actual <= 0:
        return False

    # Calcular factor de corrección
    factor = total_declarado / suma_actual
    diferencia_porcentaje = abs(1 - factor) * 100

    print(f"\n🔧 AJUSTADOR DE INVENTARIO - Factura #{factura_id}")
    print(f"   Total declarado: ${total_declarado:,.0f}")
    print(f"   Suma actual: ${suma_actual:,.0f}")
    print(f"   Factor corrección: {factor:.4f}")
    print(f"   Diferencia: {diferencia_porcentaje:.1f}%")

    # Solo ajustar si la diferencia es significativa (>5%) pero razonable (<100%)
    if diferencia_porcentaje < 5:
        print(f"   ✅ Diferencia menor al 5%, no se requiere ajuste")
        return True

    if diferencia_porcentaje > 100:
        print(f"   ⚠️ Diferencia muy grande (>{diferencia_porcentaje:.0f}%), revisar manualmente")
        return False

    # Aplicar factor de corrección a todos los items
    cursor.execute("""
        UPDATE items_factura
        SET precio_pagado = ROUND(precio_pagado * %s)
        WHERE factura_id = %s
    """, (factor, factura_id))

    filas_actualizadas = cursor.rowcount
    conn.commit()

    # Verificar resultado
    cursor.execute("""
        SELECT SUM(cantidad * precio_pagado) as suma_nueva
        FROM items_factura
        WHERE factura_id = %s
    """, (factura_id,))

    suma_nueva = float(cursor.fetchone()[0] or 0)
    diferencia_final = abs(suma_nueva - total_declarado)

    print(f"   ✅ Items ajustados: {filas_actualizadas}")
    print(f"   💰 Suma nueva: ${suma_nueva:,.0f}")
    print(f"   📊 Diferencia final: ${diferencia_final:,.0f}")

    return True


def limpiar_items_duplicados(factura_id: int, conn):
    """
    Detecta y elimina items duplicados que el OCR pudo haber leído dos veces
    """
    cursor = conn.cursor()

    print(f"\n🧹 LIMPIEZA DE DUPLICADOS - Factura #{factura_id}")

    # Detectar duplicados exactos (mismo nombre, precio y cantidad)
    cursor.execute("""
        SELECT
            nombre_leido,
            codigo_leido,
            precio_pagado,
            cantidad,
            COUNT(*) as veces
        FROM items_factura
        WHERE factura_id = %s
        GROUP BY nombre_leido, codigo_leido, precio_pagado, cantidad
        HAVING COUNT(*) > 1
    """, (factura_id,))

    duplicados = cursor.fetchall()

    if not duplicados:
        print(f"   ✅ No se encontraron duplicados exactos")
        return 0

    print(f"   ⚠️ Se encontraron {len(duplicados)} grupos de duplicados")

    items_eliminados = 0

    for dup in duplicados:
        nombre = dup[0]
        codigo = dup[1]
        precio = dup[2]
        cantidad = dup[3]
        veces = dup[4]

        print(f"   📦 '{nombre}' aparece {veces} veces")

        # Obtener IDs de los duplicados
        cursor.execute("""
            SELECT id
            FROM items_factura
            WHERE factura_id = %s
              AND nombre_leido = %s
              AND COALESCE(codigo_leido, '') = COALESCE(%s, '')
              AND precio_pagado = %s
              AND cantidad = %s
            ORDER BY id
        """, (factura_id, nombre, codigo, precio, cantidad))

        ids = [row[0] for row in cursor.fetchall()]

        # Mantener solo el primero, eliminar los demás
        if len(ids) > 1:
            ids_a_eliminar = ids[1:]  # Todos excepto el primero

            cursor.execute(f"""
                DELETE FROM items_factura
                WHERE id IN ({','.join(['%s'] * len(ids_a_eliminar))})
            """, ids_a_eliminar)

            items_eliminados += len(ids_a_eliminar)
            print(f"      ✅ Eliminados {len(ids_a_eliminar)} duplicados")

    conn.commit()

    print(f"   ✅ Total items duplicados eliminados: {items_eliminados}")
    return items_eliminados
