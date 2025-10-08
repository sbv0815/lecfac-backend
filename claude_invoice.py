import anthropic
import base64
import os
import re
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

def parse_invoice_with_claude(image_path: str) -> dict:
    """
    Usa Claude Vision para extraer datos de una factura.
    Optimizado para facturas de 100+ productos con JSON compacto.
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError("❌ ANTHROPIC_API_KEY no configurada")
    
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    
    # Leer y codificar imagen
    image_data = base64.standard_b64encode(Path(image_path).read_bytes()).decode("utf-8")
    
    # Detectar tipo de imagen
    ext = Path(image_path).suffix.lower()
    media_types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg', 
        '.png': 'image/png',
        '.webp': 'image/webp',
        '.gif': 'image/gif'
    }
    media_type = media_types.get(ext, 'image/jpeg')
    
    logger.info(f"🤖 Procesando con Claude Vision API...")
    
    # 🔥 NUEVO: Prompt ultra-compacto para facturas grandes
    prompt = """Extrae todos los productos de esta factura en formato JSON COMPACTO.

⚡ FORMATO ULTRA-COMPACTO (sin espacios, sin saltos de línea innecesarios):
{"e":"Nombre Establecimiento","f":"2024-12-27","t":512352,"p":[{"c":"7702993047842","n":"Chocolate BT","q":1,"pr":2190},{"c":"","n":"BANANO","q":0.678,"pr":5425}]}

REGLAS CRÍTICAS:
1. Usa claves cortas: e=establecimiento, f=fecha, t=total, p=productos, c=codigo, n=nombre, q=cantidad, pr=precio
2. Código (c): SOLO códigos de 8+ dígitos, si no hay usa ""
3. Precios (pr): SIN separadores de miles (5425 NO 5,425)
4. Cantidad (q): número decimal si es fraccional (0.678)
5. Nombre (n): máximo 30 caracteres, abrevia si es necesario
6. NO uses saltos de línea ni espacios extra
7. Responde SOLO con JSON en una línea

Ejemplo respuesta correcta:
{"e":"EXITO","f":"2024-12-27","t":45890,"p":[{"c":"7702993047842","n":"Chocolate","q":1,"pr":2190},{"c":"","n":"Banano","q":0.5,"pr":3400}]}"""

    try:
        # 🔥 Max tokens en 8192 (máximo para Sonnet)
        # Opciones de modelo:
        # - "claude-3-5-haiku-20241022" = Más barato (74% ahorro), bueno para facturas simples
        # - "claude-3-5-sonnet-20241022" = Balance (actual), mejor precisión
        model_to_use = os.getenv("CLAUDE_MODEL", "claude-3-5-sonnet-20241022")
        
        message = client.messages.create(
            model=model_to_use,
            max_tokens=8192,
            messages=[
                {
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
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ],
                }
            ],
        )
        
        response_text = message.content[0].text.strip()
        
        logger.info("=" * 70)
        logger.info("PROCESANDO FACTURA")
        logger.info("=" * 70)
        logger.info(f"📄 Longitud respuesta: {len(response_text)} caracteres")
        logger.info(f"📄 Primeros 300 chars: {response_text[:300]}...")
        
        # Limpiar respuesta
        response_text = response_text.strip()
        if response_text.startswith('```json'):
            response_text = response_text[7:]
        if response_text.startswith('```'):
            response_text = response_text[3:]
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        response_text = response_text.strip()
        
        # 🔥 Verificar que el JSON esté completo
        if not response_text.endswith('}') and not response_text.endswith(']'):
            logger.warning("⚠️ JSON parece incompleto")
            logger.error(f"Últimos 300 caracteres: ...{response_text[-300:]}")
            
            # Intentar cerrar el JSON inteligentemente
            if '"p":[' in response_text or '"productos":[' in response_text:
                # Cerrar último producto y array
                response_text = re.sub(r',?\s*\{[^}]*$', '', response_text)  # Eliminar último producto incompleto
                if not response_text.endswith(']}'):
                    response_text = response_text.rstrip(',') + ']}'
                logger.info("🔧 JSON reparado automáticamente")
        
        try:
            data = json.loads(response_text)
        except json.JSONDecodeError as e:
            logger.error(f"❌ Error parseando JSON: {e}")
            logger.error(f"JSON recibido (primeros 1000 chars): {response_text[:1000]}")
            
            # 🔥 Intentar recuperación inteligente
            logger.info("🔧 Intentando recuperación de productos...")
            
            # Buscar el array de productos
            match = re.search(r'"p"\s*:\s*\[(.*)', response_text, re.DOTALL)
            if not match:
                match = re.search(r'"productos"\s*:\s*\[(.*)', response_text, re.DOTALL)
            
            if match:
                try:
                    productos_str = match.group(1)
                    # Eliminar último producto incompleto
                    productos_str = re.sub(r',?\s*\{[^}]*$', '', productos_str)
                    # Cerrar array
                    productos_str = productos_str.rstrip(',') + ']'
                    
                    # Construir JSON mínimo
                    partial_json = f'{{"e":"Desconocido","f":"","t":0,"p":{productos_str}}}'
                    data = json.loads(partial_json)
                    
                    logger.info(f"✅ Recuperados {len(data.get('p', []))} productos")
                except Exception as recovery_error:
                    logger.error(f"❌ Fallo en recuperación: {recovery_error}")
                    raise ValueError(f"JSON inválido y no se pudo recuperar: {str(e)}")
            else:
                raise ValueError(f"No se pudo parsear JSON: {str(e)}")
        
        # Expandir formato compacto a formato normal
        return expand_compact_format(data)
        
    except Exception as e:
        logger.error(f"❌ Error en Claude Vision: {e}")
        raise


def expand_compact_format(data: dict) -> dict:
    """
    Convierte formato compacto a formato normal y normaliza datos.
    Maneja tanto formato compacto (c,n,q,pr) como formato normal.
    """
    
    # Detectar si es formato compacto o normal
    productos_raw = data.get("p") or data.get("productos", [])
    
    productos_normalizados = []
    for p in productos_raw:
        # Extraer datos (soporta ambos formatos)
        codigo = str(p.get("c") or p.get("codigo", "")).strip()
        nombre = str(p.get("n") or p.get("nombre", "")).strip()
        cantidad = p.get("q") or p.get("cantidad", 1)
        precio = p.get("pr") or p.get("precio", 0)
        
        # Normalizar código (solo dígitos)
        codigo = re.sub(r'\D', '', codigo)
        if len(codigo) < 8:
            codigo = ""
        
        # Normalizar precio (quitar separadores)
        precio_str = str(precio).replace(",", "").replace(".", "") if isinstance(precio, str) else str(int(precio))
        try:
            precio = int(precio_str) if precio_str else 0
        except ValueError:
            logger.warning(f"⚠️ Precio inválido '{precio}' para {nombre}, usando 0")
            precio = 0
        
        # Normalizar cantidad
        try:
            cantidad = float(cantidad)
        except (ValueError, TypeError):
            cantidad = 1.0
        
        productos_normalizados.append({
            "codigo": codigo,
            "nombre": nombre[:100],  # Limitar longitud
            "cantidad": cantidad,
            "precio": precio
        })
    
    # Normalizar total
    total = data.get("t") or data.get("total", 0)
    if isinstance(total, str):
        total = total.replace(",", "").replace(".", "")
    try:
        total = int(total) if total else 0
    except (ValueError, TypeError):
        total = sum(p["precio"] * p["cantidad"] for p in productos_normalizados)
    
    # Normalizar fecha
    fecha = data.get("f") or data.get("fecha", "")
    
    # Normalizar establecimiento
    establecimiento = data.get("e") or data.get("establecimiento", "")
    
    logger.info(f"✅ Procesados {len(productos_normalizados)} productos")
    logger.info(f"📊 Establecimiento: {establecimiento}")
    logger.info(f"📅 Fecha: {fecha}")
    logger.info(f"💰 Total: ${total:,}")
    
    return {
        "establecimiento": establecimiento.strip(),
        "fecha": fecha.strip(),
        "total": total,
        "productos": productos_normalizados
    }
