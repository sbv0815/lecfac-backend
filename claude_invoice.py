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
        
        # Cliente Anthropic con fix de API key
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")
        
        client = anthropic.Anthropic(api_key=api_key)
        
        # ========== PROMPT OPTIMIZADO CON DETECCIÓN DE DESCUENTOS ==========
        prompt = """Eres un experto en análisis de facturas de supermercados colombianos.

# OBJETIVO
Extraer: establecimiento, fecha, total y CADA producto REAL con su código.

# IMPORTANTE: DETECTAR DESCUENTOS
⚠️ NO incluyas líneas de descuentos como productos. Los descuentos se identifican porque:
- Contienen palabras: AHORRO, DESCUENTO, DESC, DTO, REBAJA, PROMOCION, PROMO, OFERTA, %, 2X1, 3X2
- Aparecen DESPUÉS del producto real
- Tienen precio NEGATIVO o menor al producto original

EJEMPLOS DE DESCUENTOS (NO incluir):
✗ "14476 AHORRO 20% M-PLAZA" → ES DESCUENTO (tiene "AHORRO")
✗ "625 DESC 20% PRODUCTO" → ES DESCUENTO (tiene "DESC")
✗ "750 PROMOCION 2X1" → ES DESCUENTO (tiene "PROMOCION")
✗ "1174 REBAJA ESPECIAL" → ES DESCUENTO (tiene "REBAJA")

EJEMPLOS DE PRODUCTOS REALES (SÍ incluir):
✓ "14476 LIMON TAHITI" → PRODUCTO REAL
✓ "625 ZANAHORIA GRL" → PRODUCTO REAL
✓ "750 BANANO GRL" → PRODUCTO REAL
✓ "1174 MANZANA GIRALDO 1KG" → PRODUCTO REAL

# ESTABLECIMIENTOS COMUNES
Si el nombre contiene alguna de estas palabras, usa SOLO el nombre principal:
JUMBO | ÉXITO | CARULLA | OLÍMPICA | ARA | D1 | ALKOSTO | MAKRO | PRICESMART | CAFAM | COLSUBSIDIO | EURO | METRO | CRUZ VERDE | FARMATODO | LA REBAJA | FALABELLA | HOME CENTER

Ejemplo: "JUMBO BULEVAR NIZA" → usa "JUMBO"

# CÓDIGOS DE PRODUCTOS - CRÍTICO
Los códigos están SIEMPRE a la IZQUIERDA del nombre del producto.

TIPOS DE CÓDIGOS VÁLIDOS:
1. **Códigos EAN (8-13 dígitos):** 7702993047842
2. **Códigos PLU (3-7 dígitos):** 116, 1045, 14476
3. **Códigos Internos (3-7 dígitos):** 625, 750, 2107

EJEMPLOS REALES:
✓ "116 BANANO URABA" → codigo: "116"
✓ "1045 ZANAHORIA" → codigo: "1045"  
✓ "7702993047842 LECHE ALPINA" → codigo: "7702993047842"
✓ "23456 ARROZ DIANA X 500G" → codigo: "23456"
✓ "09 LIMON TAHITI" → codigo: "09"
✓ "7 TOMATE CHONTO" → codigo: "7"

CÓDIGOS INVÁLIDOS (tienen letras, símbolos o palabras clave de descuento):
✗ "343718DF.VD PRODUCTO" → "" (tiene letras DF)
✗ "344476DF.20% PRODUCTO" → "" (tiene letras y %)
✗ "REF123 PRODUCTO" → "" (tiene letras REF)
✗ "14476 AHORRO 20%" → OMITIR COMPLETAMENTE (es descuento)
✗ "625 DESC ESPECIAL" → OMITIR COMPLETAMENTE (es descuento)

REGLAS:
1. Busca el PRIMER número a la IZQUIERDA del nombre
2. Si tiene SOLO DÍGITOS → ES EL CÓDIGO
3. Si tiene letras o símbolos → pon ""
4. Si el nombre contiene palabras de descuento → OMITIR COMPLETAMENTE
5. Puede ser de 1 a 13 dígitos (acepta códigos cortos como "7" o "09")

# FORMATO CRÍTICO DE NÚMEROS
⚠️ IMPORTANTE: Los precios deben estar SIN separadores de miles:
- CORRECTO: 234890 (sin comas)
- CORRECTO: 5425 (sin puntos)
- INCORRECTO: 234,890
- INCORRECTO: 5.425
- Para cantidad con decimales SÍ usa punto: 0.878

# FORMATO DE RESPUESTA
Responde SOLO con este JSON (sin explicaciones, sin texto adicional):

{
  "establecimiento": "JUMBO",
  "fecha": "2024-12-27",
  "total": 234890,
  "productos": [
    {
      "codigo": "7702993047842",
      "nombre": "CHOCOLATE BT",
      "cantidad": 1,
      "precio": 2190
    },
    {
      "codigo": "116",
      "nombre": "BANANO URABA",
      "cantidad": 0.878,
      "precio": 5425
    },
    {
      "codigo": "09",
      "nombre": "LIMON TAHITI",
      "cantidad": 1,
      "precio": 3500
    }
  ]
}

VALIDACIONES FINALES:
- JSON válido sin errores de sintaxis
- Precios como números enteros SIN separadores: 2190 (no 2,190)
- Códigos como strings con solo dígitos: "116", "09" o ""
- Fecha formato YYYY-MM-DD
- NO incluyas descuentos, IVA, subtotales, ni líneas con palabras: AHORRO, DESCUENTO, PROMO, REBAJA, %
- Acepta códigos de 1 a 13 dígitos (frutas/verduras pueden tener códigos de 1-2 dígitos)

ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON:"""
        
        # Llamada a Claude API
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=16384,  # ✅ Máximo permitido por Haiku 3.5 (16K)
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
        
        # ========== FILTRADO INTELIGENTE DE DESCUENTOS ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])
            
            palabras_descuento = [
                'ahorro', 'descuento', 'desc', 'dto', 'rebaja', 'promocion', 'promo', 
                'oferta', '2x1', '3x2', 'precio final', 'valor final', 'dto',
                'dcto', 'descu', 'dcto.', 'ahorro%', 'desc%'
            ]
            
            productos_filtrados = []
            descuentos_eliminados = 0
            
            for prod in data["productos"]:
                nombre = str(prod.get('nombre', '')).lower().strip()
                es_descuento = any(palabra in nombre for palabra in palabras_descuento)
                
                if es_descuento:
                    descuentos_eliminados += 1
                    print(f"   🗑️ Descuento eliminado: {prod.get('codigo', 'N/A')} - {prod.get('nombre', 'N/A')[:40]}")
                else:
                    productos_filtrados.append(prod)
            
            data["productos"] = productos_filtrados
            
            if descuentos_eliminados > 0:
                print(f"✅ Filtrado inteligente: {descuentos_eliminados} descuentos eliminados de {productos_originales} entradas")
        
        # LOG DEBUG
        print("=" * 80)
        print("🔍 JSON CRUDO PARSEADO:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("=" * 80)
        
        # Validar y normalizar
        if "productos" not in data:
            data["productos"] = []
        
        # NORMALIZACIÓN DE PRODUCTOS
        productos_procesados = 0
        codigos_validos = 0
        
        for prod in data.get("productos", []):
            productos_procesados += 1
            
            # Normalización de precio
            if "precio" in prod:
                try:
                    precio_str = str(prod["precio"])
                    
                    if precio_str.isdigit():
                        prod["precio"] = int(precio_str)
                    else:
                        precio_str = precio_str.replace(" ", "")
                        
                        if "," in precio_str and "." in precio_str:
                            precio_str = precio_str.replace(".", "").replace(",", ".")
                            prod["precio"] = int(float(precio_str))
                        elif "," in precio_str and "." not in precio_str:
                            precio_str = precio_str.replace(",", ".")
                            prod["precio"] = int(float(precio_str))
                        elif "." in precio_str:
                            parts = precio_str.split(".")
                            if len(parts[-1]) >= 3:
                                precio_str = precio_str.replace(".", "")
                                prod["precio"] = int(precio_str)
                            else:
                                prod["precio"] = int(float(precio_str))
                        else:
                            prod["precio"] = int(float(precio_str))
                except Exception as e:
                    print(f"⚠️ Error procesando precio '{prod.get('precio', 'N/A')}': {e}")
                    prod["precio"] = 0
            else:
                prod["precio"] = 0
            
            prod["valor"] = prod["precio"]
            
            # Cantidad
            if "cantidad" not in prod:
                prod["cantidad"] = 1
            
            # Validación de código
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()
                
                if codigo_limpio.isdigit() and 1 <= len(codigo_limpio) <= 13:
                    prod["codigo"] = codigo_limpio
                    codigos_validos += 1
                    
                    if len(codigo_limpio) >= 8:
                        tipo = "EAN"
                    elif len(codigo_limpio) >= 3:
                        tipo = "PLU"
                    else:
                        tipo = "PLU corto"
                    
                    print(f"   ✓ {tipo}: {codigo_limpio} - {prod['nombre'][:40]}")
                else:
                    prod["codigo"] = ""
                    print(f"   ✗ Código inválido descartado: '{codigo_limpio}' → {prod['nombre'][:40]}")
            else:
                prod["codigo"] = ""
                print(f"   ⚠️ Producto sin código → {prod['nombre'][:40]}")
        
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
                    "modelo": "claude-3-5-haiku-20241022",
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
    """Normaliza el nombre del establecimiento basándose en palabras clave"""
    nombre_lower = nombre_raw.lower()
    
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
    
    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado
    
    return nombre_raw.strip().upper()
