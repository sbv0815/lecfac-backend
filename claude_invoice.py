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
        
        # ========== PROMPT OPTIMIZADO ==========
        prompt = """Eres un experto en análisis de facturas de supermercados colombianos.

# OBJETIVO
Extraer: establecimiento, fecha, total y CADA producto con su código.

# ESTABLECIMIENTOS COMUNES
Si el nombre contiene alguna de estas palabras, usa SOLO el nombre principal:
JUMBO | ÉXITO | CARULLA | OLÍMPICA | ARA | D1 | ALKOSTO | MAKRO | PRICESMART | CAFAM | COLSUBSIDIO | EURO | METRO | CRUZ VERDE | FARMATODO | LA REBAJA | FALABELLA | HOME CENTER

Ejemplo: "JUMBO BULEVAR NIZA" → usa "JUMBO"

# CÓDIGOS DE PRODUCTOS - CRÍTICO
Los códigos están SIEMPRE a la IZQUIERDA del nombre del producto.

EJEMPLOS REALES:
✓ "116 BANANO URABA" → código: "116"
✓ "1045 ZANAHORIA" → código: "1045"  
✓ "7702993047842 LECHE ALPINA" → código: "7702993047842"
✓ "23456 ARROZ DIANA X 500G" → código: "23456"

CÓDIGOS INVÁLIDOS (tienen letras o símbolos especiales):
✗ "343718DF.VD PRODUCTO" → NO copiar (tiene letras DF)
✗ "344476DF.20% PRODUCTO" → NO copiar (tiene letras y %)
✗ "REF123 PRODUCTO" → NO copiar (tiene letras REF)
✗ "$5.425" → NO es código (es un precio con $)
✗ "1kg" → NO es código (tiene letras kg)

REGLAS DE ORO:
1. Busca el PRIMER número a la IZQUIERDA del nombre del producto
2. Si ese número tiene SOLO DÍGITOS (sin letras, sin %, sin $, sin kg) → ES EL CÓDIGO
3. Puede ser corto (3 dígitos) o largo (13 dígitos) → AMBOS SON VÁLIDOS
4. Copia el código EXACTO, sin espacios ni caracteres extra
5. Si NO hay número a la izquierda O tiene letras/símbolos → pon ""

# PRODUCTOS
- Incluye TODOS los productos visibles
- NO incluyas líneas de descuento, IVA, subtotales
- NO incluyas líneas que empiecen con %, DESCUENTO, DTO, IVA

# FORMATO DE RESPUESTA
Responde SOLO con este JSON (sin explicaciones):

{
  "establecimiento": "NOMBRE_PRINCIPAL",
  "fecha": "YYYY-MM-DD",
  "total": 123456,
  "productos": [
    {
      "codigo": "7702993047842",
      "nombre": "LECHE ALPINA ENTERA X 1100ML",
      "cantidad": 1,
      "precio": 5600
    },
    {
      "codigo": "116",
      "nombre": "BANANO URABA",
      "cantidad": 1,
      "precio": 5425
    },
    {
      "codigo": "1045",
      "nombre": "ZANAHORIA",
      "cantidad": 1,
      "precio": 1166
    }
  ]
}

NOTAS:
- Precios sin puntos ni comas (5600, no 5.600)
- Códigos sin espacios ("116", no " 116 ")
- Si no hay código válido a la izquierda: ""

ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON:"""
        
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
        print(f"📄 Respuesta de Claude: {response_text[:200]}...")
        
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
        
        # LOG DEBUG: Ver JSON crudo
        print("=" * 80)
        print("🔍 JSON CRUDO PARSEADO:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("=" * 80)
        
        # Validar y normalizar
        if "productos" not in data:
            data["productos"] = []
        
        # ========== NORMALIZACIÓN DE PRODUCTOS ==========
        productos_procesados = 0
        codigos_validos = 0
        
        for prod in data.get("productos", []):
            productos_procesados += 1
            
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
            
            # ========== VALIDACIÓN DE CÓDIGO CORREGIDA ==========
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()
                
                # Validar que sea un código válido:
                # 1. Solo dígitos (sin letras)
                # 2. Sin caracteres especiales (%, $, kg, etc)
                # 3. Puede tener cualquier longitud (desde 3 hasta 13 dígitos)
                
                if codigo_limpio.isdigit() and len(codigo_limpio) >= 3:
                    prod["codigo"] = codigo_limpio
                    codigos_validos += 1
                    print(f"   ✓ Código válido: {codigo_limpio} → {prod['nombre'][:30]}")
                else:
                    prod["codigo"] = ""
                    print(f"   ✗ Código inválido descartado: '{codigo_limpio}' → {prod['nombre'][:30]}")
            else:
                prod["codigo"] = ""
                print(f"   ⚠️ Producto sin código → {prod['nombre'][:30]}")
        
        # Normalizar establecimiento
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        establecimiento_normalizado = normalizar_establecimiento(establecimiento_raw)
        data["establecimiento"] = establecimiento_normalizado
        
        # Asegurar total
        if "total" not in data or not data["total"]:
            data["total"] = sum(p.get("precio", 0) for p in data.get("productos", []))
        
        print("=" * 80)
        print(f"📊 RESUMEN:")
        print(f"   Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   Total: ${data.get('total', 0):,.0f}")
        print(f"   Productos detectados: {productos_procesados}")
        print(f"   Códigos válidos: {codigos_validos} ({int(codigos_validos/productos_procesados*100) if productos_procesados > 0 else 0}%)")
        print("=" * 80)
        
        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision",
                    "modelo": "claude-haiku",
                    "productos_detectados": productos_procesados,
                    "codigos_validos": codigos_validos
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
