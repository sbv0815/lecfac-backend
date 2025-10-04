import anthropic
import base64
import os
import json
from typing import Dict

def parse_invoice_with_claude(image_path: str) -> Dict:
    """Procesa factura con Claude Vision API"""
    try:
        print("======================================================================")
        print("PROCESANDO FACTURA")
        print("======================================================================")
        
        # Leer imagen
        with open(image_path, 'rb') as f:
            image_data = base64.b64encode(f.read()).decode('utf-8')
        
        # Tipo MIME
        media_type = "image/png" if image_path.lower().endswith('.png') else "image/jpeg"
        
        # Cliente Anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        # Prompt mejorado y más específico
        prompt = """Analiza esta imagen de factura o recibo. Extrae TODA la información visible.

IMPORTANTE:
1. Si el texto está borroso, intenta deducir basándote en el contexto
2. Los códigos de barras largos son códigos EAN (13 dígitos)
3. Los códigos cortos (3-5 dígitos) son códigos PLU de productos frescos
4. Si no puedes leer un código, usa "SIN_CODIGO"
5. Incluye TODOS los productos, incluso si están repetidos

Devuelve un JSON con esta estructura EXACTA:
{
  "establecimiento": "nombre del comercio o tienda",
  "fecha": "YYYY-MM-DD o null si no es visible",
  "total": numero_sin_puntos,
  "productos": [
    {
      "codigo": "codigo_de_barras o PLU o SIN_CODIGO",
      "nombre": "descripción completa del producto",
      "cantidad": 1,
      "precio": precio_unitario_sin_puntos
    }
  ]
}

REGLAS ESTRICTAS:
- NO incluyas productos con precio negativo (descuentos)
- NO incluyas líneas de subtotales
- SI hay múltiples unidades del mismo producto, créalos como items separados
- Los precios deben ser números enteros sin puntos ni comas
- Si no puedes leer algo claramente, usa valores por defecto pero NUNCA omitas productos

RESPONDE SOLO CON JSON, sin explicaciones adicionales."""
        
        # Usar modelo más potente para mejor OCR
        message = client.messages.create(
            model="claude-3-sonnet-20240229",  # Modelo más potente para mejor OCR
            max_tokens=4096,
            temperature=0,  # Más determinístico
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": image_data,
                        },
                    },
                    {"type": "text", "text": prompt}
                ],
            }],
        )
        
        # Parsear respuesta
        response_text = message.content[0].text
        print(f"Respuesta de Claude: {response_text[:200]}...")  # Debug
        
        # Extraer JSON con más opciones
        json_str = response_text
        
        # Intentar diferentes formas de extraer JSON
        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0]
        elif "{" in response_text:
            # Buscar el JSON entre llaves
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start != -1 and end > start:
                json_str = response_text[start:end]
        
        json_str = json_str.strip()
        
        # Parsear JSON
        data = json.loads(json_str)
        
        # Validar y normalizar datos
        if "productos" not in data:
            data["productos"] = []
        
        # Normalizar productos
        for prod in data.get("productos", []):
            # Asegurar que precio existe y es numérico
            if "precio" in prod:
                try:
                    prod["precio"] = float(str(prod["precio"]).replace(",", "").replace(".", ""))
                except:
                    prod["precio"] = 0
            else:
                prod["precio"] = 0
            
            # Agregar valor para compatibilidad
            prod["valor"] = prod["precio"]
            
            # Asegurar cantidad
            if "cantidad" not in prod:
                prod["cantidad"] = 1
            
            # Limpiar código
            if "codigo" in prod and prod["codigo"]:
                prod["codigo"] = str(prod["codigo"]).strip()
            else:
                prod["codigo"] = "SIN_CODIGO"
        
        # Asegurar total
        if "total" not in data or not data["total"]:
            data["total"] = sum(p.get("precio", 0) for p in data.get("productos", []))
        
        print(f"Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"Total: ${data.get('total', 0):,.0f}")
        print(f"Productos detectados: {len(data.get('productos', []))}")
        print("======================================================================")
        
        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision",
                    "modelo": "claude-3-sonnet"
                }
            }
        }
        
    except json.JSONDecodeError as e:
        print(f"❌ Error parseando JSON: {e}")
        print(f"JSON recibido: {json_str[:500] if 'json_str' in locals() else 'No disponible'}")
        return {
            "success": False, 
            "error": f"No se pudo procesar la factura. Intenta con una imagen más clara.",
            "debug": str(e)
        }
    except Exception as e:
        print(f"❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False, 
            "error": "Error procesando la imagen. Verifica que sea una factura legible."
        }
