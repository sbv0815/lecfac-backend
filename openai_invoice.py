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

_PROMPT = """Extrae productos de factura (CÓDIGO | NOMBRE | PRECIO horizontal).

Nombres cortos (max 20 chars). Ignora descuentos (-), IVA, %REF.

JSON:
{
  "establecimiento": "nombre",
  "total": 512352,
  "items": [
    {"codigo": "123", "nombre": "Choco", "valor": 2190}
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
        
    except Exception as e:
        print(f"Error: {e}")
        data = {}
    
    # Manejar "items" o "productos"
    items_raw = data.get("items") or data.get("productos") or []
    print(f"Items raw: {len(items_raw)}")
    
    items = []
    
    for item in items_raw:
        try:
            codigo = None
            nombre = None
            valor = 0
            
            # Formato string: "codigo | nombre | precio"
            if isinstance(item, str):
                parts = [p.strip() for p in item.split('|')]
                if len(parts) >= 3:
                    codigo = parts[0] if parts[0] and len(parts[0]) >= 3 else None
                    nombre = parts[1]
                    valor_str = re.sub(r'[^\d]', '', parts[2])
                    valor = int(valor_str) if valor_str else 0
            
            # Formato objeto
            elif isinstance(item, dict):
                codigo = item.get("codigo")
                nombre = item.get("nombre")
                valor = item.get("valor")
                
                # Limpiar valor si es string
                if isinstance(valor, str):
                    valor_str = re.sub(r'[^\d]', '', valor)
                    valor = int(valor_str) if valor_str else 0
                elif isinstance(valor, float):
                    valor = int(valor)
            
            # Validar y agregar
            if nombre and len(nombre) >= 2 and valor > 0:
                items.append({
                    "codigo": codigo,
                    "nombre": nombre[:80],
                    "valor": valor,
                    "fuente": "openai"
                })
            else:
                print(f"Descartado: nombre='{nombre}', valor={valor}")
                
        except Exception as e:
            print(f"Error procesando: {e}")
            continue
    
    print(f"Items válidos: {len(items)}")
    
    # Deduplicar
    seen = set()
    items_unicos = []
    for item in items:
        key = f"{item.get('codigo')}|{item['nombre']}|{item['valor']}"
        if key not in seen:
            seen.add(key)
            items_unicos.append(item)
    
    print(f"Únicos: {len(items_unicos)}")
    
    establecimiento = data.get("establecimiento") or "Establecimiento no identificado"
    total = data.get("total")
    
    if isinstance(total, str):
        num = re.sub(r"[^\d]", "", total)
        total = int(num) if num.isdigit() else None
    
    return {
        "establecimiento": establecimiento,
        "total": total,
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": items_unicos,
        "metadatos": {
            "metodo": "openai-vision",
            "model": OPENAI_MODEL,
            "items_detectados": len(items_unicos),
        },
    }
