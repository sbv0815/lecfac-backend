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
    mime = "image/jpeg"
    if image_path.lower().endswith(".png"):
        mime = "image/png"
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

def _canonicalize_item(p: Dict[str, Any]) -> Dict[str, Any]:
    codigo = p.get("codigo")
    nombre = (p.get("nombre") or "").strip()
    valor = p.get("valor")
    
    if codigo:
        codigo = str(codigo).strip()
        if codigo == "None" or len(codigo) < 3:
            codigo = None
    
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

_PROMPT = """Eres un experto extractor de facturas de supermercados colombianos.

FORMATO VISUAL de productos en facturas colombianas:
┌─────────────────┐
│ 7702993047842   │ ← CÓDIGO (línea 1)
│ Chocolate BISO  │ ← NOMBRE (línea 2)
│ $2.190          │ ← PRECIO (línea 3)
├─────────────────┤
│ 505             │ ← CÓDIGO (línea 1)
│ Limón Tahití    │ ← NOMBRE (línea 2)
│ $8.801          │ ← PRECIO (línea 3)
└─────────────────┘

REGLAS CRÍTICAS:
1. Lee SECUENCIALMENTE de arriba hacia abajo
2. CADA producto ocupa exactamente 3 líneas: código, nombre, precio
3. Entre productos puede haber líneas con peso/cantidad (IGNÓRALAS)
4. NO repitas productos
5. Ignora líneas con: descuentos (-$), subtotales, "IVA", referencias "%REF"

EXTRAE TODOS LOS PRODUCTOS (aproximadamente 50 en esta factura).

Responde en JSON:
{
  "establecimiento": "NOMBRE DEL SUPERMERCADO",
  "total": numero_entero,
  "items": [
    {"codigo": "7702993047842", "nombre": "Chocolate BISO", "valor": 2190},
    {"codigo": "505", "nombre": "Limón Tahití", "valor": 8801}
  ]
}

IMPORTANTE: Si la factura tiene 50 productos, el array "items" debe tener ~50 objetos."""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    img_url = _b64_data_url(image_path)
    
    print("Enviando imagen a OpenAI...")
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": _PROMPT},
                    {"type": "image_url", "image_url": {"url": img_url}}
                ]
            }],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=16000
        )
        
        content = response.choices[0].message.content
        print(f"Longitud respuesta: {len(content)} caracteres")
        
        data = json.loads(content)
        
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}")
        data = {"items": []}
    except Exception as e:
        print(f"Error OpenAI: {e}")
        data = {"items": []}
    
    items_raw = (data or {}).get("items") or []
    print(f"Items crudos: {len(items_raw)}")
    
    seen = {}
    items = []
    
    for p in items_raw:
        if not isinstance(p, dict):
            continue
        
        try:
            item = _canonicalize_item(p)
            nombre = item.get("nombre", "")
            valor = item.get("valor", 0)
            
            if not nombre or valor <= 0:
                continue
            
            key = f"{item.get('codigo')}|{nombre}|{valor}"
            
            if key not in seen:
                seen[key] = True
                items.append(item)
        except Exception as e:
            print(f"Error procesando item: {e}")
    
    print(f"Items únicos procesados: {len(items)}")
    
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
