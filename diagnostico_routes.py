"""
diagnostico_routes.py
Router de diagnóstico temporal - VERSIÓN SIMPLIFICADA
"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from database import get_db_connection
from datetime import datetime

router = APIRouter(prefix="/diagnostico", tags=["Diagnóstico"])

@router.get("/precios-inventario", response_class=HTMLResponse)
async def diagnostico_precios_inventario():
    """
    Endpoint temporal para diagnosticar problemas de precios e inventario
    """

    conn = get_db_connection()
    if not conn:
        return "<h1>❌ Error de conexión a BD</h1>"

    cursor = conn.cursor()
    html_output = []

    # Estilo CSS
    html_output.append("""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <title>Diagnóstico LecFac</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }
            h2 { color: #34495e; margin-top: 30px; background: #ecf0f1; padding: 10px; border-left: 4px solid #3498db; }
            table { width: 100%; border-collapse: collapse; margin: 10px 0; font-size: 14px; }
            th { background: #34495e; color: white; padding: 12px; text-align: left; }
            td { padding: 10px; border-bottom: 1px solid #ddd; }
            tr:hover { background: #f8f9fa; }
            .error { color: #e74c3c; font-weight: bold; }
            .critical { background: #ffe6e6; padding: 20px; border: 3px solid #e74c3c; margin: 20px 0; border-radius: 8px; }
            .badge { display: inline-block; padding: 5px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; }
            .badge-error { background: #e74c3c; color: white; }
            .badge-warning { background: #f39c12; color: white; }
            .badge-success { background: #27ae60; color: white; }
            .stat-box { background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 4px; display: inline-block; min-width: 200px; margin-right: 10px; }
            .stat-label { font-size: 12px; color: #7f8c8d; text-transform: uppercase; }
            .stat-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
            .solution { background: #e6f3ff; padding: 20px; border-left: 4px solid #3498db; margin: 20px 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔍 Diagnóstico LecFac - Precios e Inventario</h1>
    """)

    try:
        # 1. ÚLTIMA FACTURA
        cursor.execute("""
            SELECT f.id, f.usuario_id, f.total_factura, f.establecimiento,
                   f.fecha_factura, f.productos_detectados, f.productos_guardados,
                   f.estado_validacion
            FROM facturas f WHERE f.usuario_id = 3
            ORDER BY f.id DESC LIMIT 1
        """)

        factura = cursor.fetchone()

        if factura:
            factura_id = factura[0]

            html_output.append("<h2>1️⃣ Última Factura del Usuario 3</h2>")
            html_output.append(f"""
            <div class="stat-box"><div class="stat-label">Factura ID</div><div class="stat-value">{factura[0]}</div></div>
            <div class="stat-box"><div class="stat-label">Total</div><div class="stat-value">${factura[2]:,.0f}</div></div>
            <div class="stat-box"><div class="stat-label">Detectados</div><div class="stat-value">{factura[5]}</div></div>
            <div class="stat-box"><div class="stat-label">Guardados</div><div class="stat-value">{factura[6]}</div></div>
            <p><strong>Establecimiento:</strong> {factura[3]}</p>
            <p><strong>Fecha:</strong> {factura[4]}</p>
            """)

            # 2. ITEMS - ANÁLISIS CRÍTICO
            cursor.execute("""
                SELECT if.id, if.producto_maestro_id, if.nombre_leido,
                       if.codigo_leido, if.precio_pagado, if.cantidad
                FROM items_factura if WHERE if.factura_id = %s
                ORDER BY if.id LIMIT 10
            """, (factura_id,))

            items = cursor.fetchall()

            html_output.append("<h2>2️⃣ Items de la Factura (primeros 10)</h2>")

            if items:
                precios_validos = sum(1 for i in items if i[4] and i[4] > 0)
                prod_null = sum(1 for i in items if i[1] is None)

                # CONTAR TOTAL
                cursor.execute("""
                    SELECT COUNT(*) as total,
                           COUNT(CASE WHEN producto_maestro_id IS NULL THEN 1 END) as sin_prod_id
                    FROM items_factura WHERE factura_id = %s
                """, (factura_id,))
                stats = cursor.fetchone()
                total_items = stats[0]
                total_sin_prod_id = stats[1]

                html_output.append(f"""
                <p>📊 Total items: <strong>{total_items}</strong></p>
                <p>
                    <span class="badge badge-success">✅ Precios válidos: {precios_validos}/10</span>
                    <span class="badge badge-error">🚨 Sin prod_maestro_id: {total_sin_prod_id}/{total_items}</span>
                </p>
                """)

                # MOSTRAR PROBLEMA CRÍTICO
                if total_sin_prod_id > 0:
                    html_output.append(f'''
                    <div class="critical">
                        <h3 style="color:#e74c3c;margin-top:0;">🚨 PROBLEMA CRÍTICO ENCONTRADO</h3>
                        <p style="font-size:18px;"><strong>{total_sin_prod_id} de {total_items} items tienen producto_maestro_id = NULL</strong></p>
                        <p>Esto significa que la función <code>buscar_o_crear_producto_inteligente()</code> NO está creando/retornando los productos correctamente.</p>
                        <p><strong>Sin producto_maestro_id válido, NO se puede actualizar el inventario.</strong></p>
                    </div>
                    ''')

                html_output.append("""
                <table>
                    <tr><th>ID</th><th>Prod Maestro ID</th><th>Nombre</th><th>Código</th><th>Precio</th><th>Cant</th></tr>
                """)

                for item in items:
                    precio_class = 'class="error"' if (item[4] is None or item[4] == 0) else ''
                    prod_id_class = 'class="error"' if item[1] is None else ''
                    html_output.append(f"""
                    <tr>
                        <td>{item[0]}</td>
                        <td {prod_id_class}><strong>{item[1] or 'NULL'}</strong></td>
                        <td>{item[2]}</td>
                        <td>{item[3] or '-'}</td>
                        <td {precio_class}>${item[4] or 0:,.0f}</td>
                        <td>{item[5]}</td>
                    </tr>
                    """)

                html_output.append("</table>")
            else:
                html_output.append('<p class="error">❌ No hay items_factura</p>')

            # 3. INVENTARIO
            cursor.execute("""
                SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = 3
            """)

            count_inv = cursor.fetchone()[0]

            html_output.append("<h2>3️⃣ Inventario del Usuario</h2>")
            if count_inv > 0:
                html_output.append(f"<p>📦 Total productos en inventario: <strong>{count_inv}</strong></p>")
            else:
                html_output.append('<p class="error">❌ INVENTARIO VACÍO</p>')

            # 4. DIAGNÓSTICO Y SOLUCIÓN
            html_output.append("<h2>4️⃣ Causa Raíz del Problema</h2>")

            html_output.append('''
            <div class="solution">
                <h3>🎯 Problema Identificado:</h3>
                <p>La función <code>buscar_o_crear_producto_inteligente()</code> está siendo llamada pero NO está retornando IDs válidos.</p>

                <h3>🔍 Posibles Causas:</h3>
                <ol>
                    <li><strong>Falta el parámetro <code>conn=conn</code></strong> en la llamada a la función</li>
                    <li><strong>Los productos se crean pero NO se hace <code>conn.commit()</code></strong></li>
                    <li><strong>La función retorna <code>None</code></strong> en lugar del ID</li>
                    <li><strong>Hay errores silenciosos</strong> que no se están logueando</li>
                </ol>

                <h3>✅ Solución:</h3>
                <p>1. Verificar en <code>ocr_processor.py</code> línea ~333 que la llamada incluya:</p>
                <pre style="background:#2c3e50;color:#ecf0f1;padding:10px;border-radius:4px;">producto_maestro_id = buscar_o_crear_producto_inteligente(
    codigo=codigo,
    nombre=nombre,
    precio=precio,
    establecimiento=establecimiento,
    cursor=cursor,
    conn=conn  # ← CRÍTICO: Este parámetro debe estar presente
)</pre>

                <p>2. Verificar en <code>product_matching.py</code> que cada INSERT tenga un <code>conn.commit()</code> inmediatamente después.</p>

                <p>3. Verificar que la función SIEMPRE retorne un ID, nunca None.</p>
            </div>
            ''')

        else:
            html_output.append('<h2>⚠️ No se encontró factura</h2>')

    except Exception as e:
        html_output.append(f'<h2 class="error">❌ Error: {e}</h2>')
        import traceback
        html_output.append(f'<pre style="background:#f8f9fa;padding:15px;border-radius:4px;overflow:auto;">{traceback.format_exc()}</pre>')

    finally:
        cursor.close()
        conn.close()

    html_output.append(f"""
            <div style="margin-top:40px;padding-top:20px;border-top:2px solid #ecf0f1;text-align:center;color:#7f8c8d;">
                <p>🔍 Diagnóstico LecFac - Generado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            </div>
        </div>
    </body>
    </html>
    """)

    return "".join(html_output)
