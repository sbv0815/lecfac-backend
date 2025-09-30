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

_PROMPT = """Analiza esta factura de supermercado colombiano y extrae la información en formato JSON.

ESTRUCTURA de productos en facturas colombianas:
- Línea 1: CÓDIGO (13 dígitos EAN, o 3-6 dígitos PLU)
- Línea 2: NOMBRE del producto
- Línea 3: PRECIO
- Líneas adicionales pueden tener peso/cantidad (ignorar)

REGLAS:
1. Lee secuencialmente: cuando veas un CÓDIGO, el NOMBRE está en la línea siguiente
2. El PRECIO está después del nombre
3. Ignora descuentos (negativos), subtotales, "IVA", totales parciales
4. Extrae TODOS los productos

Responde SOLO con este JSON (sin texto adicional):
{
  "establecimiento": "nombre del supermercado",
  "total": numero_entero_o_null,
  "items": [
    {"codigo": "código", "nombre": "nombre producto", "valor": precio_entero}
  ]
}

Ejemplo:
Si ves:
505
Limón Tahití  
$8.801

Devuelve:
{"codigo": "505", "nombre": "Limón Tahití", "valor": 8801}"""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """
    Procesa factura con OpenAI Vision.
    Retorna: {establecimiento, total, productos, metadatos}
    """
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    img_url = _b64_data_url(image_path)
    
    print("Enviando imagen a OpenAI...")
    
    try:
        # SIN schema estricto - más flexible
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
            response_format={"type": "json_object"},  # Solo pedir JSON, sin schema estricto
            temperature=0,
            max_tokens=4000
        )
        
        content = response.choices[0].message.content
        print(f"Respuesta recibida: {content[:200]}...")
        
        data = json.loads(content)
        
    except json.JSONDecodeError as e:
        print(f"Error parseando JSON: {e}")
        print(f"Contenido recibido: {content}")
        data = {"items": []}
    
    except Exception as e:
        print(f"Error OpenAI: {e}")
        data = {"items": []}
    
    # Procesar resultados
    items_raw = (data or {}).get("items") or []
    print(f"Items crudos recibidos: {len(items_raw)}")
    
    items = []
    for p in items_raw:
        if isinstance(p, dict):
            try:
                item = _canonicalize_item(p)
                if item.get("nombre") and item.get("valor", 0) > 0:
                    items.append(item)
            except Exception as e:
                print(f"Error procesando item: {e}")
                continue
    
    print(f"Items procesados: {len(items)}")
    
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
