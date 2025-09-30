from __future__ import annotations
import base64
import json
import os
import re
from datetime import datetime
from typing import Any, Dict

from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_INVOICE_MODEL", "gpt-4o")

def _b64_data_url(image_path: str) -> str:
    """Convierte imagen a data URL base64"""
    mime = "image/jpeg"
    low = image_path.lower()
    if low.endswith(".png"):
        mime = "image/png"
    
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

def _canonicalize_item(p: Dict[str, Any]) -> Dict[str, Any]:
    """Normaliza valores para la API"""
    codigo = p.get("codigo")
    nombre = (p.get("nombre") or "").strip()
    valor = p.get("valor")
    
    # Limpiar código
    if codigo:
        codigo = str(codigo).strip()
        if codigo == "None" or len(codigo) < 3:
            codigo = None
    
    # Limpia precios
    if isinstance(valor, str):
        num = re.sub(r"[^\d]", "", valor)
        valor = int(num) if num.isdigit() else 0
    if isinstance(valor, float):
        valor = int(round(valor))
    if not isinstance(valor, int):
        valor = 0
    
    return {
        "codigo": codigo,
        "nombre": nombre[:80] if nombre else "Sin nombre",
        "valor": valor,
        "fuente": "openai"
    }

_PROMPT = """Analiza esta factura de supermercado y extrae la información en JSON.

IMPORTANTE: Extrae cada producto UNA SOLA VEZ. NO repitas productos.

ESTRUCTURA típica:
- CÓDIGO (13 dígitos EAN o 3-6 dígitos PLU)
- NOMBRE del producto  
- PRECIO

REGLAS:
1. Lee de arriba hacia abajo, extrae cada producto UNA VEZ
2. NO repitas productos que ya extrajiste
3. Ignora descuentos (negativos), subtotales, "IVA"
4. Si un código aparece dos veces en la factura, repórtalo dos veces (son compras diferentes)

JSON (sin repetir productos):
{
  "establecimiento": "nombre del supermercado",
  "total": numero_total,
  "items": [
    {"codigo": "código", "nombre": "nombre", "valor": precio}
  ]
}"""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """Procesa factura con OpenAI Vision"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    img_url = _b64_data_url(image_path)
    
    print("Enviando imagen a OpenAI...")
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": _PROMPT},
                        {"type": "image_url", "image_url": {"url": img_url}}
                    ]
                }
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=16000  # Aumentar límite
        )
        
        content = response.choices[0].message.content
        print(f"Longitud respuesta: {len(content)} caracteres")
        
        # Intentar reparar JSON truncado
        if not content.rstrip().endswith('}'):
            print("JSON truncado detectado, intentando reparar...")
            # Buscar el último item completo
            last_complete = content.rfind('}, ')
            if last_complete > 0:
                content = content[:last_complete + 1] + ']}}'
        
        data = json.loads(content)
        
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}")
        # Intentar extraer lo que se pueda
        try:
            # Buscar hasta donde llegó bien
            items_start = content.find('"items": [')
            if items_start > 0:
                items_end = content.rfind('},')
                if items_end > items_start:
                    partial = content[:items_end + 1] + ']}}'
                    data = json.loads(partial)
                else:
                    data = {"items": []}
            else:
                data = {"items": []}
        except:
            data = {"items": []}
    
    except Exception as e:
        print(f"Error OpenAI: {e}")
        data = {"items": []}
    
    # Procesar y deduplicar
    items_raw = (data or {}).get("items") or []
    print(f"Items crudos: {len(items_raw)}")
    
    seen = {}  # Para deduplicar por código
    items = []
    
    for p in items_raw:
        if not isinstance(p, dict):
            continue
            
        try:
            item = _canonicalize_item(p)
            codigo = item.get("codigo")
            nombre = item.get("nombre", "")
            valor = item.get("valor", 0)
            
            # Validar que tenga datos mínimos
            if not nombre or valor <= 0:
                continue
            
            # Deduplicar por código+nombre+valor
            key = f"{codigo}|{nombre}|{valor}"
            
            if key in seen:
                seen[key] += 1  # Contar repeticiones
            else:
                seen[key] = 1
                items.append(item)
                
        except Exception as e:
            print(f"Error procesando item: {e}")
            continue
    
    print(f"Items únicos procesados: {len(items)}")
    print(f"Items repetidos eliminados: {len(items_raw) - len(items)}")
    
    establecimiento = (data or {}).get("establecimiento") or "Establecimiento no identificado"
    
    total = (data or {}).get("total")
    if isinstance(total, str):
        num = re.sub(r"[^\d]", "", total)
        total = int(num) if num.isdigit() else None
    
    return {
        "establecimiento": establecimiento,
        "total": total,
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": items,
        "metadatos": {
            "metodo": "openai-vision",
            "model": OPENAI_MODEL,
            "items_detectados": len(items),
        },
    }
