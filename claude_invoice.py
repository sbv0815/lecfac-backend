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
        
        # Prompt mejorado con lista de establecimientos
        prompt = """Analiza esta imagen de factura o recibo de supermercado colombiano.

IMPORTANTE SOBRE EL ESTABLECIMIENTO:
Identifica el establecimiento comparando con estos nombres comunes en Colombia:
- JUMBO (variantes: Jumbo Bulevar, Jumbo Express, Jumbo Calle 80, etc.)
- ÉXITO (variantes: Almacenes Éxito, Éxito Express, Supertiendas Éxito)
- CARULLA (variantes: Carulla Fresh, Carulla Express)
- OLÍMPICA (variantes: Supertiendas Olímpica, Olímpica S.A.)
- ARA (variantes: Tiendas Ara)
- D1 (variantes: Tiendas D1)
- JUSTO & BUENO (variantes: Justo y Bueno)
- CAMACHO
- SURTIFRUVER
- ALKOSTO
- MAKRO
- PRICESMART
- CAFAM
- COLSUBSIDIO
- EURO (variantes: Supermercados Euro)
- METRO (variantes: Almacenes Metro)
- CRUZ VERDE (farmacia)
- FARMATODO (farmacia)
- LA REBAJA (farmacia - variantes: Drogas La Rebaja)
- FALABELLA
- HOME CENTER

**Si el nombre en la factura es similar a uno de la lista (ej: "JUMBO BULEVAR NIZA"), usa SOLO el nombre principal (ej: "JUMBO").**

REGLAS ESTRICTAS PARA PRODUCTOS:
1. Si el texto está borroso, intenta deducir basándote en el contexto
2. Los códigos de barras largos (12-13 dígitos numéricos) son códigos EAN - CÓPIALOS COMPLETOS
3. Los códigos cortos (3-5 dígitos) son códigos PLU de productos frescos - usa "SIN_CODIGO"
4. Si no puedes leer un código claramente, usa "SIN_CODIGO"
5. Incluye TODOS los productos visibles, incluso si están repetidos
6. NO incluyas líneas con precio negativo (descuentos)
7. NO incluyas líneas que empiecen con "%" o "DESCUENTO" o "DTO"
8. NO incluyas líneas de  "IVA"
9.Incluye SUBTOTAL/TOTAL como valor de lo comprado
10.Mira MUY CUIDADOSAMENTE cada producto
11.- REVISA DOS VECES cada producto para no perder ningún código
12.Todo numero largo o corto cerca del nombre del  producto al lado izquierdo ES el código siempre y cuando no tenga caracteres especiales ejemplo %,kg,gr,etc

Devuelve un JSON con esta estructura EXACTA:
{
  "establecimiento": "NOMBRE_CADENA_PRINCIPAL",
  "fecha": "YYYY-MM-DD",
  "total": numero_entero_sin_puntos,
  "productos": [
    {
      "codigo": "codigo_ean_13_digitos o SIN_CODIGO",
      "nombre": "descripción completa del producto",
      "cantidad": 1,
      "precio": precio_unitario_entero_sin_puntos
    }
  ]
}

EJEMPLO:
Si ves "JUMBO BULEVAR NIZA" → usa "JUMBO"
Si ves código "7702993047842" → cópialo exacto
Si ves " 116 BANANO URABA $5,425" tiene código corto

RESPONDE SOLO CON JSON, sin explicaciones adicionales."""
        
        # Llamar API
        message = client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=4096,
            temperature=0,
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
        print(f"Respuesta de Claude: {response_text[:200]}...")
        
        # Extraer JSON
        json_str = response_text
        
        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0]
        elif "{" in response_text:
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start != -1 and end > start:
                json_str = response_text[start:end]
        
        json_str = json_str.strip()
        
        # Parsear JSON
        data = json.loads(json_str)
        
        # Validar y normalizar
        if "productos" not in data:
            data["productos"] = []
        
        # Normalizar productos
        for prod in data.get("productos", []):
            # Normalizar precio
            if "precio" in prod:
                try:
                    prod["precio"] = int(float(str(prod["precio"]).replace(",", "").replace(".", "")))
                except:
                    prod["precio"] = 0
            else:
                prod["precio"] = 0
            
            prod["valor"] = prod["precio"]
            
            # Cantidad
            if "cantidad" not in prod:
                prod["cantidad"] = 1
            
            # Código
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()
                # Validar que sea un código EAN válido (solo números, 8-13 dígitos)
                if codigo_limpio.isdigit() and len(codigo_limpio) >= 8:
                    prod["codigo"] = codigo_limpio
                else:
                    prod["codigo"] = "SIN_CODIGO"
            else:
                prod["codigo"] = "SIN_CODIGO"
        
        # Normalizar establecimiento
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        establecimiento_normalizado = normalizar_establecimiento(establecimiento_raw)
        data["establecimiento"] = establecimiento_normalizado
        
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
                    "modelo": "claude-haiku"
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
    except anthropic.NotFoundError as e:
        print(f"❌ Error de modelo: {e}")
        return {
            "success": False,
            "error": "Error con el modelo de IA. Contacta al administrador."
        }
    except Exception as e:
        print(f"❌ Error general: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False, 
            "error": "Error procesando la imagen. Verifica que sea una factura legible."
        }


def normalizar_establecimiento(nombre_raw: str) -> str:
    """
    Normaliza el nombre del establecimiento basándose en palabras clave
    """
    nombre_lower = nombre_raw.lower()
    
    # Mapeo de palabras clave a nombres normalizados
    establecimientos = {
        'jumbo': 'JUMBO',
        'exito': 'ÉXITO',
        'éxito': 'ÉXITO',
        'carulla': 'CARULLA',
        'olimpica': 'OLÍMPICA',
        'olímpica': 'OLÍMPICA',
        'ara': 'ARA',
        'd1': 'D1',
        'justo': 'JUSTO & BUENO',
        'camacho': 'CAMACHO',
        'surtifruver': 'SURTIFRUVER',
        'alkosto': 'ALKOSTO',
        'makro': 'MAKRO',
        'pricesmart': 'PRICESMART',
        'cafam': 'CAFAM',
        'colsubsidio': 'COLSUBSIDIO',
        'euro': 'EURO',
        'metro': 'METRO',
        'cruz verde': 'CRUZ VERDE',
        'farmatodo': 'FARMATODO',
        'la rebaja': 'LA REBAJA',
        'falabella': 'FALABELLA',
        'home center': 'HOME CENTER',
        'homecenter': 'HOME CENTER'
    }
    
    # Buscar coincidencias
    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado
    
    # Si no encuentra coincidencia, retornar el original pero limpio
    return nombre_raw.strip().upper()
