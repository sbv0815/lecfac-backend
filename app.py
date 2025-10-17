# ========================================
# AGREGAR ESTAS RUTAS A TU app.py
# ========================================

from inventario import (
    obtener_inventario_usuario,
    actualizar_cantidad_manual,
    obtener_alertas_usuario,
)


# ========================================
# RUTA: Ver inventario personal
# ========================================
@app.route("/mi-inventario")
def mi_inventario():
    """Dashboard del inventario personal del usuario"""
    if "user_id" not in session:
        return redirect(url_for("login"))

    productos = obtener_inventario_usuario(session["user_id"])
    alertas = obtener_alertas_usuario(session["user_id"], solo_activas=True)

    # Calcular estadísticas
    total_productos = len(productos)
    productos_bajo_stock = len([p for p in productos if p["estado"] == "bajo"])
    productos_medio_stock = len([p for p in productos if p["estado"] == "medio"])

    return render_template(
        "inventario.html",
        productos=productos,
        alertas=alertas,
        stats={
            "total": total_productos,
            "bajos": productos_bajo_stock,
            "medios": productos_medio_stock,
        },
    )


# ========================================
# RUTA: Actualizar cantidad de producto
# ========================================
@app.route("/api/inventario/actualizar", methods=["POST"])
def actualizar_inventario():
    """API para actualizar manualmente la cantidad de un producto"""
    if "user_id" not in session:
        return jsonify({"error": "No autenticado"}), 401

    data = request.json
    producto_id = data.get("producto_id")
    nueva_cantidad = data.get("cantidad")

    if not producto_id or nueva_cantidad is None:
        return jsonify({"error": "Datos incompletos"}), 400

    resultado = actualizar_cantidad_manual(
        session["user_id"], producto_id, float(nueva_cantidad)
    )

    return jsonify(resultado)


# ========================================
# RUTA: Ver alertas
# ========================================
@app.route("/api/alertas")
def obtener_alertas():
    """API para obtener alertas del usuario"""
    if "user_id" not in session:
        return jsonify({"error": "No autenticado"}), 401

    alertas = obtener_alertas_usuario(session["user_id"])
    return jsonify({"alertas": alertas})


# ========================================
# MODIFICAR: Ruta de subir factura
# Agregar esto al final de tu función upload_invoice()
# ========================================

# Después de procesar la factura exitosamente, agregar:
from inventario import actualizar_inventario_desde_factura

# ... tu código existente de procesamiento OCR ...

# Al final, después de guardar la factura:
if factura_id:
    # Actualizar inventario automáticamente
    resultado_inventario = actualizar_inventario_desde_factura(
        factura_id, session["user_id"]
    )

    if resultado_inventario["success"]:
        flash(
            f"✅ Factura procesada y {resultado_inventario['productos_actualizados']} "
            f"productos agregados a tu inventario",
            "success",
        )
