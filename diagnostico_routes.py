"""
diagnostico_routes.py
Router de diagnóstico temporal
"""

from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from database import get_db_connection

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
            .badge { display: inline-block; padding: 5px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; }
            .badge-error { background: #e74c3c; color: white; }
            .badge-warning { background: #f39c12; color: white; }
            .badge-success { background: #27ae60; color: white; }
            .stat-box { background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 4px; display: inline-block; min-width: 200px; margin-right: 10px; }
            .stat-label { font-size: 12px; color: #7f8c8d; text-transform: uppercase; }
            .stat-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
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

            if factura[6] == 0:
                html_output.append('<p class="error">❌ PROBLEMA: productos_guardados = 0</p>')

            # 2. ITEMS
            cursor.execute("""
                SELECT if.id, if.producto_maestro_id, if.nombre_leido,
                       if.codigo_leido, if.precio_pagado, if.cantidad
                FROM items_factura if WHERE if.factura_id = %s ORDER BY if.id
            """, (factura_id,))

            items = cursor.fetchall()

            html_output.append("<h2>2️⃣ Items de la Factura</h2>")

            if items:
                precios_validos = sum(1 for i in items if i[4] and i[4] > 0)
                precios_null = sum(1 for i in items if i[4] is None)
                precios_cero = sum(1 for i in items if i[4] == 0)
                prod_null = sum(1 for i in items if i[1] is None)

                html_output.append(f"""
                <p>📊 Total items: <strong>{len(items)}</strong></p>
                <p>
                    <span class="badge badge-success">✅ Precios válidos: {precios_validos}</span>
                    <span class="badge badge-warning">⚠️  NULL: {precios_null}</span>
                    <span class="badge badge-error">❌ $0: {precios_cero}</span>
                    <span class="badge badge-error">🚨 Sin prod_maestro_id: {prod_null}</span>
                </p>
                <table>
                    <tr><th>ID</th><th>Prod Maestro ID</th><th>Nombre</th><th>Código</th><th>Precio</th><th>Cant</th></tr>
                """)

                for item in items:
                    precio_class = 'class="error"' if (item[4] is None or item[4] == 0) else ''
                    prod_id_class = 'class="error"' if item[1] is None else ''
                    html_output.append(f"""
                    <tr>
                        <td>{item[0]}</td>
                        <td {prod_id_class}>{item[1] or 'NULL'}</td>
                        <td>{item[2]}</td>
                        <td>{item[3] or '-'}</td>
                        <td {precio_class}>${item[4] or 0:,.0f}</td>
                        <td>{item[5]}</td>
                    </tr>
                    """)

                html_output.append("</table>")

                if prod_null > 0:
                    html_output.append(f'<p class="error">❌ CRÍTICO: {prod_null} items sin producto_maestro_id</p>')
            else:
                html_output.append('<p class="error">❌ No hay items_factura</p>')

            # 3. PRODUCTOS MAESTROS
            cursor.execute("""
                SELECT pm.id, pm.nombre, pm.codigo_ean, pm.codigo_interno, pm.precio_referencia
                FROM productos_maestros pm
                WHERE pm.id IN (SELECT DISTINCT producto_maestro_id FROM items_factura
                                WHERE factura_id = %s AND producto_maestro_id IS NOT NULL)
            """, (factura_id,))

            productos = cursor.fetchall()

            html_output.append("<h2>3️⃣ Productos Maestros</h2>")
            if productos:
                html_output.append(f"<p>📦 Total: <strong>{len(productos)}</strong></p>")
                html_output.append("""
                <table>
                    <tr><th>ID</th><th>Nombre</th><th>EAN</th><th>Cód Interno</th><th>Precio Ref</th></tr>
                """)
                for prod in productos:
                    html_output.append(f"""
                    <tr>
                        <td>{prod[0]}</td>
                        <td>{prod[1]}</td>
                        <td>{prod[2] or '-'}</td>
                        <td>{prod[3] or '-'}</td>
                        <td>${prod[4] or 0:,.0f}</td>
                    </tr>
                    """)
                html_output.append("</table>")
            else:
                html_output.append('<p class="error">❌ No hay productos_maestros</p>')

            # 4. INVENTARIO
            cursor.execute("""
                SELECT iu.id, pm.nombre, iu.cantidad_actual, iu.precio_ultima_compra,
                       iu.precio_promedio, iu.numero_compras
                FROM inventario_usuario iu
                JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = 3
                ORDER BY iu.id DESC
            """)

            inventario = cursor.fetchall()

            html_output.append("<h2>4️⃣ Inventario del Usuario</h2>")
            if inventario:
                html_output.append(f"<p>📦 Total productos: <strong>{len(inventario)}</strong></p>")

                precios_cero = sum(1 for i in inventario if i[3] == 0 or i[4] == 0)
                if precios_cero > 0:
                    html_output.append(f'<p class="error">❌ {precios_cero} productos con precio $0</p>')

                html_output.append("""
                <table>
                    <tr><th>ID</th><th>Nombre</th><th>Cantidad</th><th>Precio Ult</th><th>Precio Prom</th><th>Compras</th></tr>
                """)

                for inv in inventario:
                    precio_class = 'class="error"' if (inv[3] == 0 or inv[4] == 0) else ''
                    html_output.append(f"""
                    <tr>
                        <td>{inv[0]}</td>
                        <td>{inv[1]}</td>
                        <td>{inv[2]}</td>
                        <td {precio_class}>${inv[3]:,.0f}</td>
                        <td {precio_class}>${inv[4]:,.0f}</td>
                        <td>{inv[5]}</td>
                    </tr>
                    """)
                html_output.append("</table>")
            else:
                html_output.append('<p class="error">❌ INVENTARIO VACÍO</p>')

            # 5. DIAGNÓSTICO
            html_output.append("<h2>5️⃣ Diagnóstico Final</h2>")
            problemas = []

            if factura[6] == 0:
                problemas.append("❌ productos_guardados = 0 en tabla facturas")
            if not items:
                problemas.append("❌ No hay registros en items_factura")
            if items and prod_null > 0:
                problemas.append(f"❌ {prod_null} items sin producto_maestro_id (NO se pueden agregar al inventario)")
            if items and (precios_null > 0 or precios_cero > 0):
                problemas.append(f"❌ Precios incorrectos: {precios_null} NULL, {precios_cero} en $0")
            if not inventario:
                problemas.append("❌ inventario_usuario está VACÍO")

            if problemas:
                html_output.append('<div style="background:#ffe6e6;padding:15px;border-left:4px solid #e74c3c;margin:20px 0;">')
                html_output.append('<h3>🚨 Problemas Encontrados:</h3><ul>')
                for p in problemas:
                    html_output.append(f'<li class="error">{p}</li>')
                html_output.append('</ul></div>')

                html_output.append('<div style="background:#e6f3ff;padding:15px;border-left:4px solid #3498db;margin:20px 0;">')
                html_output.append('<h3>💡 Recomendaciones:</h3><ol>')
                html_output.append('<li>Verificar que <code>buscar_o_crear_producto_inteligente()</code> recibe el parámetro <code>conn=conn</code></li>')
                html_output.append('<li>Verificar que los INSERT en <code>product_matching.py</code> hacen <code>conn.commit()</code></li>')
                html_output.append('<li>Asegurar que <code>actualizar_inventario_desde_factura()</code> se ejecuta después del commit de items</li>')
                html_output.append('<li>Revisar logs de Railway para errores específicos durante el procesamiento OCR</li>')
                html_output.append('</ol></div>')
            else:
                html_output.append('<p class="success" style="color:#27ae60;font-weight:bold;font-size:18px;">✅ No se detectaron problemas obvios. El sistema parece estar funcionando correctamente.</p>')

        else:
            html_output.append('<h2>⚠️ No se encontró ninguna factura para el usuario 3</h2>')
            html_output.append('<p>Escanea una factura primero para poder hacer el diagnóstico.</p>')

    except Exception as e:
        html_output.append(f'<h2 class="error">❌ Error: {e}</h2>')
        import traceback
        html_output.append(f'<pre style="background:#f8f9fa;padding:15px;border-radius:4px;overflow:auto;">{traceback.format_exc()}</pre>')

    finally:
        cursor.close()
        conn.close()

    html_output.append("""
            <div style="margin-top:40px;padding-top:20px;border-top:2px solid #ecf0f1;text-align:center;color:#7f8c8d;">
                <p>🔍 Diagnóstico LecFac v1.0 - Generado el """ + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
            </div>
        </div>
    </body>
    </html>
    """)

    return "".join(html_output)
