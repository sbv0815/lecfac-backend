from fastapi import APIRouter, HTTPException, Depends

print("=" * 80)
print("📦 CARGANDO AUDITORIA_PRODUCTOS_API.PY - VERSION 2025-10-30")
print("=" * 80)
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from database import get_db_connection
from auth import get_current_user

router = APIRouter()

print("🔧 Iniciando carga de api_auditoria_productos.py")


class ProductoBase(BaseModel):
    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None


class ProductoCreate(ProductoBase):
    pass


class ProductoUpdate(ProductoBase):
    pass


print("✅ Modelos Pydantic definidos")


@router.get("/verificar/{codigo_ean}")
async def verificar_producto(
    codigo_ean: str, current_user: dict = Depends(get_current_user)
):
    print(f"🔍 Verificando producto: {codigo_ean}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales FROM productos_maestros WHERE codigo_ean = %s LIMIT 1",
            (codigo_ean,),
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row:
            print(f"✅ Producto encontrado: {row[2]}")
            return {
                "existe": True,
                "producto": {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "categoria": row[4],
                    "auditado_manualmente": row[5],
                    "validaciones_manuales": row[6],
                },
            }
        else:
            print(f"⚠️ Producto NO encontrado: {codigo_ean}")
            return {
                "existe": False,
                "producto": None,
                "mensaje": "Producto no encontrado",
            }
    except Exception as e:
        print(f"❌ Error en verificar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/producto")
async def crear_producto(
    producto: ProductoCreate, current_user: dict = Depends(get_current_user)
):
    print(f"➕ Creando producto: {producto.codigo_ean} - {producto.nombre}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
            (producto.codigo_ean,),
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            print(f"⚠️ Producto ya existe: {producto.codigo_ean}")
            raise HTTPException(status_code=400, detail="Producto ya existe")
        cursor.execute(
            "INSERT INTO productos_maestros (codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales) VALUES (%s, %s, %s, %s, TRUE, 1) RETURNING id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales",
            (producto.codigo_ean, producto.nombre, producto.marca, producto.categoria),
        )
        row = cursor.fetchone()
        cursor.execute(
            "INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'crear', %s)",
            (current_user["id"], row[0], datetime.now()),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Producto creado exitosamente: ID {row[0]}")
        return {
            "mensaje": "Producto creado",
            "producto": {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "categoria": row[4],
                "auditado_manualmente": row[5],
                "validaciones_manuales": row[6],
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en crear_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/producto/{producto_id}")
async def actualizar_producto(
    producto_id: int,
    producto: ProductoUpdate,
    current_user: dict = Depends(get_current_user),
):
    print(f"✏️ Actualizando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE productos_maestros SET nombre = %s, marca = %s, categoria = %s WHERE id = %s RETURNING id, codigo_ean, nombre, marca, categoria, auditado_manualmente, validaciones_manuales",
            (producto.nombre, producto.marca, producto.categoria, producto_id),
        )
        row = cursor.fetchone()
        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")
        cursor.execute(
            "INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'actualizar', %s)",
            (current_user["id"], producto_id, datetime.now()),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Producto actualizado: {row[2]}")
        return {
            "mensaje": "Actualizado",
            "producto": {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "categoria": row[4],
                "auditado_manualmente": row[5],
                "validaciones_manuales": row[6],
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en actualizar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/validar/{producto_id}")
async def validar_producto(
    producto_id: int, current_user: dict = Depends(get_current_user)
):
    print(f"✅ Validando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE productos_maestros SET auditado_manualmente = TRUE, validaciones_manuales = validaciones_manuales + 1, ultima_validacion = %s WHERE id = %s",
            (datetime.now(), producto_id),
        )
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")
        cursor.execute(
            "INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha) VALUES (%s, %s, 'validar', %s)",
            (current_user["id"], producto_id, datetime.now()),
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Producto validado exitosamente")
        return {"mensaje": "Validado"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en validar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/estadisticas")
async def obtener_estadisticas(current_user: dict = Depends(get_current_user)):
    print(f"📊 Obteniendo estadísticas para usuario {current_user['id']}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT accion, COUNT(*) FROM auditoria_productos WHERE usuario_id = %s GROUP BY accion",
            (current_user["id"],),
        )
        stats = {
            "productos_creados": 0,
            "productos_actualizados": 0,
            "productos_validados": 0,
            "total_acciones": 0,
        }
        for row in cursor.fetchall():
            if row[0] == "crear":
                stats["productos_creados"] = row[1]
            elif row[0] == "actualizar":
                stats["productos_actualizados"] = row[1]
            elif row[0] == "validar":
                stats["productos_validados"] = row[1]
            stats["total_acciones"] += row[1]
        cursor.execute(
            "SELECT accion, producto_maestro_id, fecha FROM auditoria_productos WHERE usuario_id = %s ORDER BY fecha DESC LIMIT 10",
            (current_user["id"],),
        )
        stats["ultimas_acciones"] = [
            {
                "accion": r[0],
                "producto_maestro_id": r[1],
                "fecha": r[2].isoformat() if r[2] else None,
            }
            for r in cursor.fetchall()
        ]
        cursor.close()
        conn.close()
        print(f"✅ Estadísticas obtenidas")
        return stats
    except Exception as e:
        print(f"❌ Error en obtener_estadisticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# ENDPOINT: Analizar imagen con Claude Vision
# ============================================================
import anthropic
import base64
import os
import re
import json


class ImagenProductoRequest(BaseModel):
    imagen_base64: str
    mime_type: str = "image/jpeg"


@router.post("/analizar-imagen")
async def analizar_imagen_producto(request: ImagenProductoRequest):
    """
    Analiza una imagen de producto usando Claude Vision.
    """
    try:
        print("📸 Recibida imagen para análisis")

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        imagen_b64 = request.imagen_base64
        if "," in imagen_b64:
            imagen_b64 = imagen_b64.split(",")[1]

        prompt = """Analiza esta imagen de un producto de supermercado y extrae la información.

RESPONDE SOLO en formato JSON:
{
    "nombre": "NOMBRE DEL PRODUCTO EN MAYÚSCULAS",
    "marca": "Nombre de la marca",
    "presentacion": "cantidad con unidad (ej: 500g, 1L)",
    "categoria": "categoría sugerida",
    "confianza": 0.9
}

Si no puedes identificar el producto:
{"nombre": "", "marca": "", "presentacion": "", "categoria": "", "confianza": 0.0}"""

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": request.mime_type,
                                "data": imagen_b64,
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )

        respuesta_texto = message.content[0].text
        print(f"📝 Respuesta Claude: {respuesta_texto[:100]}...")

        json_match = re.search(r"\{[\s\S]*\}", respuesta_texto)

        if json_match:
            datos = json.loads(json_match.group())
            print(f"✅ Datos extraídos: {datos.get('nombre', 'N/A')}")

            return {
                "success": True,
                "nombre": datos.get("nombre", ""),
                "marca": datos.get("marca", ""),
                "presentacion": datos.get("presentacion", ""),
                "categoria": datos.get("categoria", ""),
                "confianza": datos.get("confianza", 0.8),
            }
        else:
            return {"success": False, "error": "No se pudo extraer información"}

    except Exception as e:
        print(f"❌ Error analizando imagen: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("=" * 60)
print("✅ API AUDITORIA PRODUCTOS CARGADA COMPLETAMENTE")
print("   📌 Endpoints registrados en el router:")
print("      GET  /verificar/{codigo_ean}")
print("      POST /producto")
print("      PUT  /producto/{producto_id}")
print("      POST /validar/{producto_id}")
print("      GET  /estadisticas")
print("=" * 60)
