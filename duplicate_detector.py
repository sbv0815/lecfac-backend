"""
duplicate_detector.py - VERSIÓN CORREGIDA PARA VIDEO
NO suma cantidades de múltiples frames, toma cantidad = 1
"""

import os
from typing import List, Dict, Tuple


def normalizar_nombre_simple(nombre: str) -> str:
    """Normalización simple para nombres sin código"""
    if not nombre:
        return ""
    return nombre.strip().lower()


def detectar_duplicados_automaticamente(
    productos: List[Dict], total_factura: float
) -> Dict:
    """
    VERSIÓN CORREGIDA - Para videos de facturas
    NO suma cantidades de múltiples frames, toma cantidad = 1 por defecto

    Cuando grabas un video de la factura, cada frame detecta los mismos productos.
    Esto NO significa que compraste 10 unidades, sino que el mismo producto
    apareció en 10 frames diferentes.
    """
    print(f"\n{'='*80}")
    print(f"🔍 DETECTOR DE DUPLICADOS - VERSIÓN CORREGIDA PARA VIDEO")
    print(f"{'='*80}")
    print(f"📦 Productos recibidos: {len(productos)}")
    print(f"💰 Total factura: ${total_factura:,.0f}")

    if not productos:
        return {
            "productos_limpios": [],
            "duplicados_detectados": False,
            "productos_eliminados": [],
            "metricas": {
                "productos_originales": 0,
                "productos_despues_limpieza": 0,
                "duplicados_consolidados": 0,
                "productos_con_codigo": 0,
                "productos_sin_codigo": 0,
            },
        }

    # PASO 1: Agrupar productos por CÓDIGO o NOMBRE
    grupos_productos = {}
    productos_con_codigo = 0
    productos_sin_codigo = 0

    for idx, prod in enumerate(productos):
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        valor = float(prod.get("valor", 0))
        cantidad = int(prod.get("cantidad", 1))

        if not nombre and not codigo:
            print(f"   ⚠️ Producto {idx+1} sin nombre ni código, omitiendo")
            continue

        # Determinar clave única
        if codigo and codigo.isdigit() and len(codigo) >= 3:
            clave = f"CODE:{codigo}"
            productos_con_codigo += 1
        else:
            clave = f"NAME:{normalizar_nombre_simple(nombre)}"
            productos_sin_codigo += 1

        if clave not in grupos_productos:
            grupos_productos[clave] = []

        grupos_productos[clave].append(
            {"codigo": codigo, "nombre": nombre, "valor": valor, "cantidad": cantidad}
        )

    print(f"\n📊 Análisis de agrupación:")
    print(f"   Grupos únicos identificados: {len(grupos_productos)}")
    print(f"   Productos con código: {productos_con_codigo}")
    print(f"   Productos sin código: {productos_sin_codigo}")

    # PASO 2: ✅ CORRECCIÓN PRINCIPAL - NO sumar cantidades
    productos_consolidados = []
    duplicados_eliminados = 0

    for clave, items in grupos_productos.items():
        if len(items) == 1:
            # Solo una ocurrencia, mantener tal cual pero forzar cantidad=1
            item = items[0]
            productos_consolidados.append(
                {
                    "codigo": item["codigo"],
                    "nombre": item["nombre"],
                    "valor": item["valor"],
                    "cantidad": 1,  # ✅ FORZAR cantidad = 1
                }
            )
        else:
            # Múltiples ocurrencias del MISMO producto (de diferentes frames)
            print(f"\n🔍 Consolidando: {clave}")
            print(f"   Ocurrencias en frames: {len(items)}")

            # Tomar el primero como referencia
            primer_item = items[0]

            # Verificar si todos tienen el mismo precio
            precios_unicos = set(item["valor"] for item in items)

            if len(precios_unicos) == 1:
                # Mismo precio en todos los frames = mismo producto
                # ✅ CAMBIO CLAVE: Cantidad = 1, NO suma de frames
                productos_consolidados.append(
                    {
                        "codigo": primer_item["codigo"],
                        "nombre": primer_item["nombre"],
                        "valor": primer_item["valor"],
                        "cantidad": 1,  # ✅ FORZAR cantidad = 1
                    }
                )

                duplicados_eliminados += len(items) - 1
                print(f"   ✅ Consolidado: {primer_item['nombre']}")
                print(f"      Precio: ${primer_item['valor']:,.0f}")
                print(f"      Frames eliminados: {len(items) - 1}")
                print(f"      Cantidad final: 1")
            else:
                # Precios diferentes = puede ser descuento o error de OCR
                print(f"   ⚠️ Precios diferentes detectados: {precios_unicos}")
                print(f"      Acción: Tomar el precio más frecuente")

                # Contar frecuencia de precios
                precio_frecuencia = {}
                for item in items:
                    p = item["valor"]
                    precio_frecuencia[p] = precio_frecuencia.get(p, 0) + 1

                # Tomar el precio más frecuente
                precio_mas_comun = max(
                    precio_frecuencia.keys(), key=lambda x: precio_frecuencia[x]
                )

                productos_consolidados.append(
                    {
                        "codigo": primer_item["codigo"],
                        "nombre": primer_item["nombre"],
                        "valor": precio_mas_comun,
                        "cantidad": 1,  # ✅ FORZAR cantidad = 1
                    }
                )

                duplicados_eliminados += len(items) - 1
                print(f"      Precio seleccionado: ${precio_mas_comun:,.0f}")
                print(f"      Cantidad final: 1")

    # PASO 3: Validar totales
    suma_productos = sum(p["valor"] * p["cantidad"] for p in productos_consolidados)
    diferencia = abs(suma_productos - total_factura)
    diferencia_porcentaje = (
        (diferencia / total_factura * 100) if total_factura > 0 else 0
    )

    print(f"\n💰 Validación de totales:")
    print(f"   Suma productos: ${suma_productos:,.0f}")
    print(f"   Total factura: ${total_factura:,.0f}")
    print(f"   Diferencia: ${diferencia:,.0f} ({diferencia_porcentaje:.2f}%)")

    if diferencia_porcentaje > 10:
        print(f"   ⚠️ ADVERTENCIA: Diferencia significativa (>{10}%)")
    elif diferencia_porcentaje > 5:
        print(f"   ⚠️ Diferencia moderada")
    else:
        print(f"   ✅ Totales validados correctamente")

    # PASO 4: Resultado final
    resultado = {
        "productos_limpios": productos_consolidados,
        "duplicados_detectados": duplicados_eliminados > 0,
        "productos_eliminados": [],
        "metricas": {
            "productos_originales": len(productos),
            "productos_despues_limpieza": len(productos_consolidados),
            "duplicados_consolidados": duplicados_eliminados,
            "productos_con_codigo": productos_con_codigo,
            "productos_sin_codigo": productos_sin_codigo,
            "diferencia_total": diferencia,
            "diferencia_porcentaje": diferencia_porcentaje,
            "suma_productos": suma_productos,
            "total_factura": total_factura,
        },
    }

    print(f"\n{'='*80}")
    print(f"✅ CONSOLIDACIÓN COMPLETADA")
    print(f"{'='*80}")
    print(f"📊 Resumen:")
    print(f"   Productos originales (de todos los frames): {len(productos)}")
    print(f"   Productos únicos finales: {len(productos_consolidados)}")
    print(f"   Duplicados de frames eliminados: {duplicados_eliminados}")
    print(f"{'='*80}\n")

    return resultado


def limpiar_items_duplicados_db(factura_id: int, conn) -> int:
    """
    Limpia y consolida items duplicados en la BD
    VERSIÓN CORREGIDA: NO suma cantidades, toma 1
    """
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 CONSOLIDANDO ITEMS EN BD - FACTURA {factura_id}")
        print(f"{'='*80}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    ARRAY_AGG(id ORDER BY id) as ids,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado, producto_maestro_id
                HAVING COUNT(*) > 1
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    GROUP_CONCAT(id) as ids,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = ?
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado, producto_maestro_id
                HAVING COUNT(*) > 1
            """,
                (factura_id,),
            )

        grupos_duplicados = cursor.fetchall()

        if not grupos_duplicados:
            print(f"✅ No se encontraron items duplicados")
            print(f"{'='*80}\n")
            return 0

        items_consolidados = 0

        print(f"\n📦 Grupos a consolidar: {len(grupos_duplicados)}")

        for grupo in grupos_duplicados:
            codigo, nombre, precio, ocurrencias, ids, producto_id = grupo

            if isinstance(ids, str):
                ids_list = [int(x) for x in ids.split(",")]
            else:
                ids_list = list(ids)

            print(f"\n   🔄 Consolidando:")
            print(f"      Código: {codigo if codigo != 'SIN_CODIGO' else 'Sin código'}")
            print(f"      Producto: {nombre}")
            print(f"      Precio: ${precio:,.0f}")
            print(f"      {ocurrencias} líneas → 1 línea con cantidad = 1")

            primer_id = ids_list[0]
            otros_ids = ids_list[1:]

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                # ✅ CORRECCIÓN: Cantidad = 1, NO suma
                cursor.execute(
                    """
                    UPDATE items_factura
                    SET cantidad = 1
                    WHERE id = %s
                """,
                    (primer_id,),
                )

                if otros_ids:
                    cursor.execute(
                        """
                        DELETE FROM items_factura
                        WHERE id = ANY(%s)
                    """,
                        (otros_ids,),
                    )
            else:
                cursor.execute(
                    """
                    UPDATE items_factura
                    SET cantidad = 1
                    WHERE id = ?
                """,
                    (primer_id,),
                )

                if otros_ids:
                    placeholders = ",".join("?" * len(otros_ids))
                    cursor.execute(
                        f"""
                        DELETE FROM items_factura
                        WHERE id IN ({placeholders})
                    """,
                        otros_ids,
                    )

            items_consolidados += len(otros_ids)
            print(f"      ✅ Consolidado exitosamente")

        conn.commit()

        print(f"\n{'='*80}")
        print(f"✅ CONSOLIDACIÓN COMPLETADA")
        print(f"   Items eliminados: {items_consolidados}")
        print(f"{'='*80}\n")

        return items_consolidados

    except Exception as e:
        print(f"❌ Error consolidando items: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
        return 0
    finally:
        cursor.close()


def detectar_y_consolidar_en_bd(factura_id: int, conn) -> Dict:
    """Wrapper para consolidación en BD"""
    items_consolidados = limpiar_items_duplicados_db(factura_id, conn)
    return {
        "success": True,
        "items_consolidados": items_consolidados,
        "factura_id": factura_id,
    }


def diagnosticar_factura(factura_id: int, conn) -> Dict:
    """Diagnostica una factura para debugging"""
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 DIAGNÓSTICO DE FACTURA {factura_id}")
        print(f"{'='*80}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad, producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad, producto_maestro_id
                FROM items_factura
                WHERE factura_id = ?
                ORDER BY id
            """,
                (factura_id,),
            )

        items = cursor.fetchall()

        print(f"\n📦 Items: {len(items)}")
        for item in items:
            item_id, codigo, nombre, precio, cantidad, prod_id = item
            print(
                f"   {item_id}: {codigo or 'N/A'} | {nombre[:30]} | ${precio:,.0f} x{cantidad}"
            )

        return {"success": True, "total_items": len(items)}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
