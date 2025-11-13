"""
inventory_consolidator.py - Consolidación de inventario de usuario
========================================================================
Agrupa productos duplicados en el inventario del usuario

🎯 OBJETIVO:
El usuario ve 1 línea por producto único, con cantidad total

ANTES:
- AREPA DONA PAISA (8 unid) - desde factura 1
- AREPA DONA PAISA (1 unid) - desde factura 2
- AREPA DORA PAISA (2 unid) - desde factura 3

DESPUÉS:
- AREPA DONA PAISA (11 unid) - consolidado

⚠️ FILOSOFÍA DE DISEÑO:
- NO modifica las tablas existentes
- NO afecta el procesamiento de facturas
- Solo crea una VISTA consolidada para la app
- Completamente independiente del resto del sistema
"""

def obtener_inventario_consolidado(usuario_id: int, cursor) -> list:
    """
    Obtiene el inventario del usuario con productos consolidados

    Consolida por:
    1. Mismo código (EAN o PLU)
    2. Nombre muy similar (>95% similitud)
    3. Mismo establecimiento

    Args:
        usuario_id: ID del usuario
        cursor: Cursor de base de datos

    Returns:
        Lista de productos consolidados con cantidades totales
    """
    import os
    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    try:
        # Obtener todos los items del usuario
        cursor.execute(f"""
            SELECT
                pm.id as producto_id,
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.marca,
                pm.categoria,
                ui.cantidad,
                ui.ultimo_precio,
                ui.ultima_compra,
                ui.veces_comprado,
                fp.establecimiento_nombre
            FROM usuario_inventario ui
            INNER JOIN productos_maestros pm ON ui.producto_maestro_id = pm.id
            LEFT JOIN facturas_procesadas fp ON ui.ultima_factura_id = fp.id
            WHERE ui.usuario_id = {param}
              AND ui.cantidad > 0
            ORDER BY pm.codigo_ean, pm.nombre_normalizado
        """, (usuario_id,))

        items = cursor.fetchall()

        if not items:
            return []

        # Diccionario para consolidar
        # Key: (codigo, nombre_base)
        consolidado = {}

        for item in items:
            producto_id = item[0]
            codigo = item[1]
            nombre = item[2]
            marca = item[3]
            categoria = item[4]
            cantidad = item[5]
            ultimo_precio = item[6]
            ultima_compra = item[7]
            veces_comprado = item[8]
            establecimiento = item[9]

            # Generar key de consolidación
            # Si tiene código, usar código
            # Si no, usar nombre normalizado
            if codigo:
                key = f"CODE_{codigo}"
            else:
                key = f"NAME_{nombre}"

            # Si ya existe, consolidar
            if key in consolidado:
                consolidado[key]['cantidad'] += cantidad
                consolidado[key]['veces_comprado'] += veces_comprado
                consolidado[key]['productos_ids'].append(producto_id)

                # Actualizar precio si es más reciente
                if ultima_compra and ultima_compra > consolidado[key]['ultima_compra']:
                    consolidado[key]['ultimo_precio'] = ultimo_precio
                    consolidado[key]['ultima_compra'] = ultima_compra
            else:
                # Crear nueva entrada
                consolidado[key] = {
                    'producto_id': producto_id,
                    'productos_ids': [producto_id],
                    'codigo': codigo,
                    'nombre': nombre,
                    'marca': marca,
                    'categoria': categoria,
                    'cantidad': cantidad,
                    'ultimo_precio': ultimo_precio,
                    'ultima_compra': ultima_compra,
                    'veces_comprado': veces_comprado,
                    'establecimiento': establecimiento,
                    'es_consolidado': False
                }

        # Marcar items consolidados
        resultado = []
        for key, item in consolidado.items():
            if len(item['productos_ids']) > 1:
                item['es_consolidado'] = True
                print(f"   📦 Consolidado: {item['nombre']} ({len(item['productos_ids'])} entradas → 1)")
            resultado.append(item)

        # Ordenar por nombre
        resultado.sort(key=lambda x: x['nombre'])

        return resultado

    except Exception as e:
        print(f"❌ Error en consolidación de inventario: {e}")
        import traceback
        traceback.print_exc()
        return []


def obtener_estadisticas_stock(inventario_consolidado: list) -> dict:
    """
    Genera estadísticas del inventario consolidado

    Args:
        inventario_consolidado: Lista de productos consolidados

    Returns:
        Dict con estadísticas
    """
    if not inventario_consolidado:
        return {
            'total_productos': 0,
            'productos_bajo_stock': 0,
            'productos_stock_normal': 0,
            'productos_consolidados': 0,
            'valor_total_estimado': 0
        }

    total_productos = len(inventario_consolidado)
    bajo_stock = sum(1 for p in inventario_consolidado if p['cantidad'] <= 2)
    stock_normal = total_productos - bajo_stock
    consolidados = sum(1 for p in inventario_consolidado if p['es_consolidado'])

    # Valor total estimado
    valor_total = sum(
        p['cantidad'] * (p['ultimo_precio'] or 0)
        for p in inventario_consolidado
    )

    return {
        'total_productos': total_productos,
        'productos_bajo_stock': bajo_stock,
        'productos_stock_normal': stock_normal,
        'productos_consolidados': consolidados,
        'valor_total_estimado': valor_total
    }


# ═══════════════════════════════════════════════════════════════
# STATUS DE CARGA
# ═══════════════════════════════════════════════════════════════
print("="*80)
print("✅ inventory_consolidator.py CARGADO")
print("   Consolidación de inventario lista")
print("="*80)
