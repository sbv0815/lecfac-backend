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

_PROMPT = """Extrae productos de esta factura de supermercado colombiano.

Lee HORIZONTALMENTE: CÓDIGO | NOMBRE | PRECIO

Reglas:
1. Extraer código, nombre y precio de cada producto
2. IGNORAR: descuentos (-), subtotales, "IVA", "%REF"
3. Si código incompleto: usar nombre + precio
4. Prioridad: CÓDIGO > PRECIO > NOMBRE

JSON (sin texto extra):
{
  "establecimiento": "nombre",
  "total": numero_entero,
  "items": [
    {"codigo": "123", "nombre": "Producto", "valor": 2500}
  ]
}"""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    img_url = _b64_data_url(image_path)
    
    print("Enviando a OpenAI...")
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": _PROMPT},
                    {"type": "image_url", "image_url": {"url": img_url, "detail": "high"}}
                ]
            }],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=8000,  # Reducido para evitar timeouts
            timeout=45  # Timeout explícito de 45 segundos
        )
        
        content = response.choices[0].message.content
        print(f"Respuesta: {len(content)} chars")
        
        data = json.loads(content)
        
    except TimeoutError:
        print("Timeout de OpenAI")
        return {
            "establecimiento": "Error: Timeout",
            "total": None,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": [],
            "metadatos": {"metodo": "error-timeout", "items_detectados": 0}
        }
    except json.JSONDecodeError as e:
        print(f"Error JSON: {e}")
        data = {"items": []}
    except Exception as e:
        print(f"Error: {e}")
        data = {"items": []}
    
    items_raw = (data or {}).get("items") or []
    print(f"Items: {len(items_raw)}")
    
    seen = {}
    items = []
    
    for p in items_raw:
        if not isinstance(p, dict):
            continue
        
        try:
            item = _canonicalize_item(p)
            if not item.get("nombre") or item.get("valor", 0) <= 0:
                continue
            
            key = f"{item.get('codigo')}|{item['nombre']}|{item['valor']}"
            if key not in seen:
                seen[key] = True
                items.append(item)
        except:
            continue
    
    print(f"Únicos: {len(items)}")
    
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
