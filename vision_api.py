# ============================================================
# ENDPOINT: Analizar imagen de producto con Claude Vision
# Agregar a auditoria_api.py o crear archivo separado
# ============================================================

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import anthropic
import base64
import os
import re
import json

router = APIRouter()

# Cliente de Anthropic
client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


class ImagenProductoRequest(BaseModel):
    imagen_base64: str
    mime_type: str = "image/jpeg"


@router.post("/api/auditoria/analizar-imagen")
async def analizar_imagen_producto(request: ImagenProductoRequest):
    """
    Analiza una imagen de producto usando Claude Vision.
    Extrae: nombre, marca, presentación, categoría.

    Body:
    {
        "imagen_base64": "base64...",
        "mime_type": "image/jpeg"
    }
    """
    try:
        print("📸 Recibida imagen para análisis")
        print(f"   Tamaño base64: {len(request.imagen_base64)} caracteres")

        # Validar que hay imagen
        if not request.imagen_base64:
            raise HTTPException(status_code=400, detail="No se recibió imagen")

        # Limpiar base64 (quitar prefijo data:image si existe)
        imagen_b64 = request.imagen_base64
        if "," in imagen_b64:
            imagen_b64 = imagen_b64.split(",")[1]

        # Prompt optimizado para extracción de datos de producto
        prompt = """Analiza esta imagen de un producto de supermercado y extrae la siguiente información.

INSTRUCCIONES:
1. Lee la etiqueta/empaque del producto
2. Extrae los datos que puedas identificar claramente
3. Si no puedes leer algo, déjalo vacío
4. El nombre debe ser descriptivo pero conciso
5. La presentación incluye peso, volumen o cantidad

RESPONDE SOLO en formato JSON así:
{
    "nombre": "NOMBRE DEL PRODUCTO EN MAYÚSCULAS",
    "marca": "Nombre de la marca",
    "presentacion": "cantidad con unidad (ej: 500g, 1L, 12 unidades)",
    "categoria": "categoría sugerida",
    "confianza": 0.9,
    "observaciones": "cualquier nota relevante"
}

CATEGORÍAS COMUNES:
- Lácteos
- Carnes y Embutidos
- Granos y Cereales
- Bebidas
- Snacks y Galletas
- Frutas y Verduras
- Aseo Personal
- Aseo Hogar
- Panadería
- Congelados
- Enlatados
- Salsas y Condimentos

Si la imagen no es clara o no es un producto, responde:
{
    "nombre": "",
    "marca": "",
    "presentacion": "",
    "categoria": "",
    "confianza": 0.0,
    "observaciones": "No se pudo identificar el producto"
}"""

        # Llamar a Claude Vision
        print("🤖 Enviando a Claude Vision...")

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

        # Extraer respuesta
        respuesta_texto = message.content[0].text
        print(f"📝 Respuesta Claude: {respuesta_texto[:200]}...")

        # Parsear JSON de la respuesta
        # Buscar el JSON en la respuesta (puede venir con texto adicional)
        json_match = re.search(r"\{[\s\S]*\}", respuesta_texto)

        if json_match:
            datos = json.loads(json_match.group())

            print(f"✅ Datos extraídos:")
            print(f"   Nombre: {datos.get('nombre', 'N/A')}")
            print(f"   Marca: {datos.get('marca', 'N/A')}")
            print(f"   Presentación: {datos.get('presentacion', 'N/A')}")

            return {
                "success": True,
                "nombre": datos.get("nombre", ""),
                "marca": datos.get("marca", ""),
                "presentacion": datos.get("presentacion", ""),
                "categoria": datos.get("categoria", ""),
                "confianza": datos.get("confianza", 0.8),
                "observaciones": datos.get("observaciones", ""),
            }
        else:
            print("⚠️ No se pudo parsear JSON de la respuesta")
            return {
                "success": False,
                "error": "No se pudo extraer información de la imagen",
                "respuesta_raw": respuesta_texto[:500],
            }

    except anthropic.BadRequestError as e:
        print(f"❌ Error de Anthropic (BadRequest): {e}")
        raise HTTPException(
            status_code=400, detail=f"Error al procesar imagen: {str(e)}"
        )
    except json.JSONDecodeError as e:
        print(f"❌ Error parseando JSON: {e}")
        return {
            "success": False,
            "error": "Error al interpretar la respuesta",
        }
    except Exception as e:
        print(f"❌ Error analizando imagen: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# Para agregar a main.py:
# ============================================================
# from vision_api import router as vision_router
# app.include_router(vision_router)
# ============================================================
