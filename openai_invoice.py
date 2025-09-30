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

_PROMPT = """Eres un experto en facturas de supermercados y droguerias colombianos.

FORMATO DE LECTURA - MUY IMPORTANTE:
Cada producto está en UNA FILA HORIZONTAL con 3 columnas:

CÓDIGO          | DESCRIPCIÓN        | PRECIO
7702993047842   | Chocolate BISO     | 2.190
116             | Banano Uraba       | 5.425
505             | Limón Tahití       | 8.801

REGLAS CRÍTICAS:
1. Lee HORIZONTALMENTE (izquierda a derecha)
2. En la MISMA fila encuentras: código + nombre + precio
3. NO tomes código de una fila y nombre de otra fila
4. IGNORA líneas como "2 X 4200" o "0.510kg X 34980" (son detalles de peso/cantidad)
5. IGNORA descuentos (-), subtotales, "%REF", "DF."

EJEMPLO CORRECTO:
Fila: "7702993047842  Chocolate BISO  2.190"
JSON: {"codigo": "7702993047842", "nombre": "Chocolate BISO", "valor": 2190}

EJEMPLO INCORRECTO:
❌ NO tomar código de línea 1 y nombre de línea 2

Extrae TODOS los productos (~50).

Responde JSON:
{
  "establecimiento": "JUMBO BULEVAR",
  "total": 512352,
  "items": [
    {"codigo": "7702993047842", "nombre": "Chocolate BISO", "valor": 2190}
  ]
}"""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    img_url = _b64_data_url(image_path)
    
    print("Enviando a OpenAI Vision...")
    
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
            max_tokens=8000
        )
        
        content = response.choices[0].message.content
        print(f"Respuesta: {len(content)} chars")
        
        data = json.loads(content)
        
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
