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
        
        # Prompt
        prompt = """Analiza esta factura de supermercado colombiano y extrae en formato JSON:
{
  "establecimiento": "Nombre del supermercado",
  "total": 123456,
  "fecha": "2024-01-15",
  "productos": [
    {"codigo": "7702993047842", "nombre": "Chocolate BI", "precio": 2190}
  ]
}
REGLAS:
- codigo: EAN de 13 dígitos. Si es producto fresco (BANANO, TOMATE), usa código corto PLU (ej: "116", "1045")
- nombre: Completo, expande abreviaturas
- precio: Número entero. IGNORA descuentos (negativos) y subtotales
- Incluye TODOS los productos, incluso duplicados
- SOLO JSON, sin explicaciones"""
        
        # Llamar API
        message = client.messages.create(
            model="claude-3-haiku-20240307",  # Modelo más económico que funciona
            max_tokens=4096,
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
        
        # Extraer JSON
        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0].strip()
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0].strip()
        else:
            json_str = response_text.strip()
        
        # Parsear JSON
        data = json.loads(json_str)
        
        # Normalizar
        for prod in data.get("productos", []):
            prod["valor"] = float(prod.get("precio", 0))
            if "precio" not in prod:
                prod["precio"] = prod["valor"]
        
        print(f"Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"Total: ${data.get('total', 0):,.0f}")
        print(f"Productos únicos: {len(data.get('productos', []))}")
        print("======================================================================")
        
        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision",
                    "modelo": "claude-haiku"
                }
            }
        }
        
    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        return {"success": False, "error": f"JSON inválido: {str(e)}"}
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
