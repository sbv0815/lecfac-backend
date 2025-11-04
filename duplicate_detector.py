"""
duplicate_detector.py - VERSIÓN FINAL CON LÓGICA DE CÓDIGO
Detecta duplicados REALES vs compras múltiples legítimas
PRIORIDAD: Código EAN/PLU > Nombre
"""

import os
from typing import List, Dict, Tuple


def normalizar_nombre_simple(nombre: str) -> str:
    """Normalización simple para nombres sin código"""
    if not nombre:
        return ""
    return nombre.strip().lower()


def detectar_duplicados_automaticamente(productos: List[Dict], total_factura: float) -> Dict:
    """
    Detecta duplicados REALES en una factura usando CÓDIGO como clave primaria

    LÓGICA ACTUALIZADA:
    1. Si tiene código EAN/PLU → usar código como identificador único
    2. Si NO tiene código → usar nombre normalizado
    3. Si mismo identificador aparece múltiples veces con mismo precio → CONSOLIDAR cantidades
    4. Si mismo identificador pero precios diferentes → MANTENER separados (puede ser descuento)

    Args:
        productos: Lista de productos con estructura {codigo, nombre, valor, cantidad}
        total_factura: Total de la factura para validación

    Returns:
        Dict con productos_limpios, duplicados_detectados, metricas
    """
    print(f"\n{'='*80}")
    print(f"🔍 DETECTOR DE DUPLICADOS - LÓGICA BASADA EN CÓDIGO")
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
                "productos_sin_codigo": 0
            }
        }

    # PASO 1: Agrupar productos por CÓDIGO (prioritario) o NOMBRE
    grupos_productos = {}
    productos_con_codigo = 0
    productos_sin_codigo = 0

    for idx, prod in enumerate(productos):
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        valor = float(prod.get("valor", 0))
        cantidad = int(prod.get("cantidad", 1))

        # Validar si el producto tiene datos mínimos
        if not nombre and not codigo:
            print(f"   ⚠️ Producto {idx+1} sin nombre ni código, omitiendo")
            continue

        # LÓGICA DE IDENTIFICACIÓN ÚNICA:
        # 1. Si tiene código EAN (8+ dígitos) → usar código
        # 2. Si tiene PLU (3-7 dígitos) → usar código con prefijo
        # 3. Si NO tiene código → usar nombre normalizado

        if codigo and codigo.isdigit():
            if len(codigo) >= 8:
                # Código EAN estándar
                clave = f"EAN:{codigo}"
                tipo_clave = "EAN"
                productos_con_codigo += 1
            elif len(codigo) >= 3:
                # Código PLU (productos frescos)
                clave = f"PLU:{codigo}"
                tipo_clave = "PLU"
                productos_con_codigo += 1
            else:
                # Código muy corto, usar nombre
                clave = f"NOMBRE:{normalizar_nombre_simple(nombre)}"
                tipo_clave = "NOMBRE"
                productos_sin_codigo += 1
        else:
            # Sin código válido, usar nombre
            clave = f"NOMBRE:{normalizar_nombre_simple(nombre)}"
            tipo_clave = "NOMBRE"
            productos_sin_codigo += 1

        if clave not in grupos_productos:
            grupos_productos[clave] = {
                "tipo_identificacion": tipo_clave,
                "items": []
            }

        grupos_productos[clave]["items"].append({
            "idx_original": idx,
            "codigo": codigo,
            "nombre": nombre,
            "valor": valor,
            "cantidad": cantidad
        })

    print(f"\n📊 Análisis de agrupación:")
    print(f"   Grupos únicos identificados: {len(grupos_productos)}")
    print(f"   Productos con código EAN/PLU: {productos_con_codigo}")
    print(f"   Productos sin código (por nombre): {productos_sin_codigo}")

    # PASO 2: Procesar cada grupo y consolidar
    productos_consolidados = []
    duplicados_consolidados = 0

    for clave, grupo in grupos_productos.items():
        items = grupo["items"]
        tipo_id = grupo["tipo_identificacion"]

        if len(items) == 1:
            # Solo una ocurrencia, mantener tal cual
            item = items[0]
            productos_consolidados.append({
                "codigo": item["codigo"],
                "nombre": item["nombre"],
                "valor": item["valor"],
                "cantidad": item["cantidad"]
            })
        else:
            # Múltiples ocurrencias del mismo producto
            print(f"\n🔍 Analizando: {clave} ({tipo_id})")
            print(f"   Ocurrencias: {len(items)}")

            # Obtener info del primer item
            primer_item = items[0]
            codigo_producto = primer_item["codigo"]
            nombre_producto = primer_item["nombre"]

            # Agrupar por precio (para detectar descuentos)
            grupos_por_precio = {}
            for item in items:
                precio = item["valor"]
                if precio not in grupos_por_precio:
                    grupos_por_precio[precio] = []
                grupos_por_precio[precio].append(item)

            if len(grupos_por_precio) == 1:
                # CASO 1: Todas las ocurrencias tienen el mismo precio
                # → Es compra múltiple del mismo producto
                precio = list(grupos_por_precio.keys())[0]
                cantidad_total = sum(item["cantidad"] for item in items)

                print(f"   ✅ COMPRA MÚLTIPLE CONSOLIDADA")
                print(f"      Producto: {nombre_producto}")
                print(f"      Código: {codigo_producto}")
                print(f"      Precio unitario: ${precio:,.0f}")
                print(f"      Ocurrencias consolidadas: {len(items)}")
                print(f"      Cantidad total: {cantidad_total}")
                print(f"      Total línea: ${precio * cantidad_total:,.0f}")

                productos_consolidados.append({
                    "codigo": codigo_producto,
                    "nombre": nombre_producto,
                    "valor": precio,
                    "cantidad": cantidad_total
                })

                duplicados_consolidados += len(items) - 1
            else:
                # CASO 2: Mismo producto pero diferentes precios
                # Puede ser: descuento, promoción, o error de lectura
                print(f"   ⚠️ MISMO PRODUCTO, PRECIOS DIFERENTES")
                print(f"      Precios encontrados: {list(grupos_por_precio.keys())}")
                print(f"      Acción: Consolidar por precio")

                # Consolidar cada grupo de precio por separado
                for precio, items_precio in grupos_por_precio.items():
                    cantidad_grupo = sum(item["cantidad"] for item in items_precio)

                    productos_consolidados.append({
                        "codigo": codigo_producto,
                        "nombre": nombre_producto,
                        "valor": precio,
                        "cantidad": cantidad_grupo
                    })

                    if len(items_precio) > 1:
                        duplicados_consolidados += len(items_precio) - 1

                    print(f"         Precio ${precio:,.0f} x{cantidad_grupo}")

    # PASO 3: Validar contra el total de la factura
    suma_productos = sum(p["valor"] * p["cantidad"] for p in productos_consolidados)
    diferencia = abs(suma_productos - total_factura)
    diferencia_porcentaje = (diferencia / total_factura * 100) if total_factura > 0 else 0

    print(f"\n💰 Validación de totales:")
    print(f"   Suma productos: ${suma_productos:,.0f}")
    print(f"   Total factura: ${total_factura:,.0f}")
    print(f"   Diferencia: ${diferencia:,.0f} ({diferencia_porcentaje:.2f}%)")

    if diferencia_porcentaje > 10:
        print(f"   ⚠️ ADVERTENCIA: Diferencia significativa (>{10}%)")
    elif diferencia_porcentaje > 5:
        print(f"   ⚠️ Diferencia moderada ({diferencia_porcentaje:.2f}%)")
    else:
        print(f"   ✅ Totales validados correctamente")

    # PASO 4: Preparar resultado final
    hay_duplicados = duplicados_consolidados > 0

    resultado = {
        "productos_limpios": productos_consolidados,
        "duplicados_detectados": hay_duplicados,
        "productos_eliminados": [],
        "metricas": {
            "productos_originales": len(productos),
            "productos_despues_limpieza": len(productos_consolidados),
            "duplicados_consolidados": duplicados_consolidados,
            "productos_con_codigo": productos_con_codigo,
            "productos_sin_codigo": productos_sin_codigo,
            "diferencia_total": diferencia,
            "diferencia_porcentaje": diferencia_porcentaje,
            "suma_productos": suma_productos,
            "total_factura": total_factura
        }
    }

    print(f"\n{'='*80}")
    print(f"✅ DETECCIÓN Y CONSOLIDACIÓN COMPLETADA")
    print(f"{'='*80}")
    print(f"📊 Resumen:")
    print(f"   Productos en factura original: {len(productos)}")
    print(f"   Productos después de consolidar: {len(productos_consolidados)}")
    print(f"   Líneas consolidadas: {duplicados_consolidados}")
    print(f"   Productos identificados por código: {productos_con_codigo}")
    print(f"   Productos identificados por nombre: {productos_sin_codigo}")
    print(f"{'='*80}\n")

    return resultado


def limpiar_items_duplicados_db(factura_id: int, conn) -> int:
    """
    Limpia y consolida items duplicados en la BD usando CÓDIGO como identificador

    Esta función opera DESPUÉS de que los items fueron guardados en la BD.
    Usa el código EAN/PLU como identificador primario.

    Args:
        factura_id: ID de la factura
        conn: Conexión a la base de datos

    Returns:
        int: Número de items consolidados
    """
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 CONSOLIDANDO ITEMS EN BD - FACTURA {factura_id}")
        print(f"{'='*80}")

        # ESTRATEGIA:
        # 1. Buscar items con mismo código_leido y precio_pagado
        # 2. Si hay múltiples, consolidar sumando cantidades

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Buscar grupos duplicados por CÓDIGO
            cursor.execute("""
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    SUM(cantidad) as cantidad_total,
                    ARRAY_AGG(id ORDER BY id) as ids,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado, producto_maestro_id
                HAVING COUNT(*) > 1
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    SUM(cantidad) as cantidad_total,
                    GROUP_CONCAT(id) as ids,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = ?
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado, producto_maestro_id
                HAVING COUNT(*) > 1
            """, (factura_id,))

        grupos_duplicados = cursor.fetchall()

        if not grupos_duplicados:
            print(f"✅ No se encontraron items para consolidar")
            print(f"{'='*80}\n")
            return 0

        items_consolidados = 0

        print(f"\n📦 Grupos a consolidar: {len(grupos_duplicados)}")

        for grupo in grupos_duplicados:
            codigo, nombre, precio, ocurrencias, cantidad_total, ids, producto_id = grupo

            # Convertir ids a lista
            if isinstance(ids, str):
                ids_list = [int(x) for x in ids.split(',')]
            else:
                ids_list = list(ids)

            print(f"\n   🔄 Consolidando:")
            print(f"      Código: {codigo if codigo != 'SIN_CODIGO' else 'Sin código'}")
            print(f"      Producto: {nombre}")
            print(f"      Precio: ${precio:,.0f}")
            print(f"      {ocurrencias} líneas → 1 línea con cantidad {cantidad_total}")

            # Mantener el primer item, actualizar su cantidad
            primer_id = ids_list[0]
            otros_ids = ids_list[1:]

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                # Actualizar cantidad del primer item
                cursor.execute("""
                    UPDATE items_factura
                    SET cantidad = %s
                    WHERE id = %s
                """, (cantidad_total, primer_id))

                # Eliminar los demás items
                if otros_ids:
                    cursor.execute("""
                        DELETE FROM items_factura
                        WHERE id = ANY(%s)
                    """, (otros_ids,))
            else:
                cursor.execute("""
                    UPDATE items_factura
                    SET cantidad = ?
                    WHERE id = ?
                """, (cantidad_total, primer_id))

                if otros_ids:
                    placeholders = ','.join('?' * len(otros_ids))
                    cursor.execute(f"""
                        DELETE FROM items_factura
                        WHERE id IN ({placeholders})
                    """, otros_ids)

            items_consolidados += len(otros_ids)
            print(f"      ✅ Consolidado exitosamente")

        conn.commit()

        print(f"\n{'='*80}")
        print(f"✅ CONSOLIDACIÓN COMPLETADA")
        print(f"   Items consolidados: {items_consolidados}")
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
    """
    Wrapper para consolidación en BD con estadísticas

    Returns:
        Dict con estadísticas de la consolidación
    """
    items_consolidados = limpiar_items_duplicados_db(factura_id, conn)

    return {
        "success": True,
        "items_consolidados": items_consolidados,
        "factura_id": factura_id
    }


# ==========================================
# FUNCIÓN DE DIAGNÓSTICO
# ==========================================

def diagnosticar_factura(factura_id: int, conn) -> Dict:
    """
    Diagnostica una factura para ver cómo están agrupados los items

    Útil para debugging y entender qué está pasando con los duplicados
    """
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 DIAGNÓSTICO DE FACTURA {factura_id}")
        print(f"{'='*80}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    id,
                    codigo_leido,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT
                    id,
                    codigo_leido,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = ?
                ORDER BY id
            """, (factura_id,))

        items = cursor.fetchall()

        print(f"\n📦 Items en la factura: {len(items)}")
        print(f"\n{'ID':<6} {'Código':<15} {'Nombre':<30} {'Precio':<12} {'Cant':<6} {'Prod_ID':<8}")
        print(f"{'-'*6} {'-'*15} {'-'*30} {'-'*12} {'-'*6} {'-'*8}")

        for item in items:
            item_id, codigo, nombre, precio, cantidad, prod_id = item
            codigo_str = codigo if codigo else "N/A"
            nombre_str = (nombre[:27] + "...") if len(nombre) > 30 else nombre
            print(f"{item_id:<6} {codigo_str:<15} {nombre_str:<30} ${precio:<11,.0f} {cantidad:<6} {prod_id or 'N/A':<8}")

        # Agrupar por código
        print(f"\n📊 Agrupación por código:")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    codigo_leido,
                    COUNT(*) as ocurrencias,
                    SUM(cantidad) as cantidad_total,
                    STRING_AGG(nombre_leido, ' | ') as nombres
                FROM items_factura
                WHERE factura_id = %s
                GROUP BY codigo_leido
                HAVING COUNT(*) > 1
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT
                    codigo_leido,
                    COUNT(*) as ocurrencias,
                    SUM(cantidad) as cantidad_total,
                    GROUP_CONCAT(nombre_leido, ' | ') as nombres
                FROM items_factura
                WHERE factura_id = ?
                GROUP BY codigo_leido
                HAVING COUNT(*) > 1
            """, (factura_id,))

        grupos = cursor.fetchall()

        if grupos:
            for codigo, ocurrencias, cantidad_total, nombres in grupos:
                print(f"\n   Código: {codigo}")
                print(f"   Ocurrencias: {ocurrencias}")
                print(f"   Cantidad total: {cantidad_total}")
                print(f"   Nombres: {nombres}")
        else:
            print(f"   ✅ No hay productos con múltiples líneas")

        print(f"\n{'='*80}\n")

        return {
            "success": True,
            "total_items": len(items),
            "grupos_duplicados": len(grupos)
        }

    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
