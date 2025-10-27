"""
ENDPOINT DE PRUEBA PARA INSERTAR PRODUCTOS
Agrega esto a main.py para probar inserción directa
"""

from fastapi import HTTPException
from database import get_db_connection
import os

# ============================================================
# AGREGAR ESTE ENDPOINT A main.py
# ============================================================

@app.get("/api/test/insert-producto")
async def test_insert_producto():
    """
    Endpoint de prueba para insertar un producto directamente
    Accede desde el navegador:
    https://lecfac-backend-production.up.railway.app/api/test/insert-producto
    """
    try:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
        conn = get_db_connection()
        cursor = conn.cursor()

        # Producto de prueba
        producto_test = {
            "codigo_ean": "7702001030644",
            "nombre_comercial": "ISODINE SOLUCION 120ML TEST",
            "nombre_normalizado": "isodine solucion 120ml test",
            "categoria": "Farmacia",
            "subcategoria": "Antisépticos",
            "marca": "Isodine",
            "presentacion": "120ml",
            "precio_promedio_global": 21800,
            "es_producto_fresco": False
        }

        # Insertar
        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean, nombre_comercial, nombre_normalizado,
                    categoria, subcategoria, marca, presentacion,
                    precio_promedio_global, es_producto_fresco
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                producto_test["codigo_ean"],
                producto_test["nombre_comercial"],
                producto_test["nombre_normalizado"],
                producto_test["categoria"],
                producto_test["subcategoria"],
                producto_test["marca"],
                producto_test["presentacion"],
                producto_test["precio_promedio_global"],
                producto_test["es_producto_fresco"]
            ))
            producto_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean, nombre_comercial, nombre_normalizado,
                    categoria, subcategoria, marca, presentacion,
                    precio_promedio_global, es_producto_fresco
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                producto_test["codigo_ean"],
                producto_test["nombre_comercial"],
                producto_test["nombre_normalizado"],
                producto_test["categoria"],
                producto_test["subcategoria"],
                producto_test["marca"],
                producto_test["presentacion"],
                producto_test["precio_promedio_global"],
                producto_test["es_producto_fresco"]
            ))
            producto_id = cursor.lastrowid

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": "✅ Producto insertado correctamente",
            "producto_id": producto_id,
            "producto": producto_test,
            "database_type": database_type
        }

    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "database_type": database_type
        }


@app.get("/api/test/count-productos")
async def test_count_productos():
    """
    Contar productos en productos_maestros
    https://lecfac-backend-production.up.railway.app/api/test/count-productos
    """
    try:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT * FROM productos_maestros ORDER BY id DESC LIMIT 5")
        productos = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total_productos": count,
            "ultimos_5": [dict(zip([d[0] for d in cursor.description], row)) for row in productos] if productos else [],
            "database_type": database_type
        }

    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
