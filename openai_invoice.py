from __future__ import annotations
import base64
import json
import os
import re
from datetime import datetime
from typing import Any, Dict

from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_INVOICE_MODEL", "gpt-4o-mini")

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
    nombre = (p.get("nombre") or "").strip()
    codigo = p.get("codigo") or None
    valor = p.get("valor")
    
    if isinstance(valor, str):
        num = re.sub(r"[^\d]", "", valor)
        valor = int(num) if num.isdigit() else 0
    if isinstance(valor, float):
        valor = int(round(valor))
    if not isinstance(valor, int):
        valor = 0
    
    if codigo:
        codigo = str(codigo).strip()
    
    return {
        "codigo": codigo,
        "nombre": nombre[:80],
        "valor": valor,
        "fuente": "openai"
    }

def _schema() -> Dict[str, Any]:
    """JSON Schema para Structured Outputs"""
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "invoice_schema",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "establecimiento": {"type": "string"},
                    "total": {
                        "anyOf": [
                            {"type": "integer"},
                            {"type": "null"}
                        ]
                    },
                    "moneda": {"type": "string"},
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "codigo": {
                                    "anyOf": [
                                        {"type": "string"},
                                        {"type": "null"}
                                    ]
                                },
                                "nombre": {"type": "string"},
                                "valor": {"type": "integer"},
                                "cantidad": {
                                    "anyOf": [
                                        {"type": "number"},
                                        {"type": "null"}
                                    ]
                                }
                            },
                            "required": ["codigo", "nombre", "valor", "cantidad"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["establecimiento", "total", "moneda", "items"],
                "additionalProperties": False
            }
        }
    }

_PROMPT = """Eres un extractor especializado en facturas de supermercados colombianos.

IMPORTANTE: Cada producto tiene su información en UNA SOLA línea o en líneas consecutivas.
Formato típico en facturas:
- Línea 1: CÓDIGO (13 dígitos EAN o 3-6 dígitos PLU)
- Línea 2: NOMBRE DEL PRODUCTO
- Línea 3: PRECIO

O también:
CÓDIGO  NOMBRE DEL PRODUCTO  PRECIO

REGLAS CRÍTICAS:
1. El CÓDIGO siempre pertenece al NOMBRE que está en la MISMA línea o en la línea INMEDIATAMENTE SIGUIENTE
2. NUNCA asocies un código con el producto de 2 líneas más abajo
3. Si ves un número de 13 dígitos (EAN) seguido de un nombre, ESE código va con ESE nombre
4. Si ves un número corto (3-6 dígitos) seguido de un nombre, ESE código va con ESE nombre

Extrae en JSON:

{
  "establecimiento": "nombre del supermercado",
  "total": monto_total_entero,
  "moneda": "COP",
  "items": [
    {
      "codigo": "código EAN o PLU que está JUNTO al producto",
      "nombre": "nombre del producto",
      "valor": precio_entero,
      "cantidad": cantidad_si_visible
    }
  ]
}

Ejemplo correcto:
Si ves:
  505
  Limón Tahití
  $8.801

El resultado debe ser:
  {"codigo": "505", "nombre": "Limón Tahití", "valor": 8801}

NO:
  {"codigo": "505", "nombre": "Bimbolajdres", "valor": ...}  ← INCORRECTO

Ignora: descuentos, subtotales, "RESUMEN DE IVA", totales parciales."""

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """
    Procesa factura con OpenAI Vision.
    Retorna: {establecimiento, total, productos, metadatos}
    """
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    img_url = _b64_data_url(image_path)
    
    try:
        # API CORRECTA: chat.completions.create
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
            response_format=_schema(),
            temperature=0,
            max_tokens=4000
        )
        
        content = response.choices[0].message.content
        data = json.loads(content)
        
    except json.JSONDecodeError:
        # Reintento sin schema
        try:
            response = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": _PROMPT + "\n\nJSON puro."},
                            {"type": "image_url", "image_url": {"url": img_url}}
                        ]
                    }
                ],
                temperature=0,
                max_tokens=4000
            )
            content = response.choices[0].message.content
            data = json.loads(content)
        except Exception:
            data = {"items": []}
    
    except Exception as e:
        print(f"Error OpenAI: {e}")
        data = {"items": []}
    
    # Procesar resultados
    items_raw = (data or {}).get("items") or []
    items = [_canonicalize_item(p) for p in items_raw if isinstance(p, dict)]
    
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
