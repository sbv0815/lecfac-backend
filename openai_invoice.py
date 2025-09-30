# openai_invoice.py
from __future__ import annotations
import base64, json, os, re
from datetime import datetime
from typing import Any, Dict, List, Optional

from openai import OpenAI

# Modelos recomendados con visión + Structured Outputs
# - "gpt-4o-mini" (rápido/barato) o "gpt-4o" (más potente)
OPENAI_MODEL = os.getenv("OPENAI_INVOICE_MODEL", "gpt-4o-mini")

def _b64_data_url(image_path: str) -> str:
    mime = "image/jpeg"
    low = image_path.lower()
    if low.endswith(".png"): mime = "image/png"
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

def _canonicalize_item(p: Dict[str, Any]) -> Dict[str, Any]:
    # Normaliza valores para nuestra API (valor en enteros COP)
    nombre = (p.get("nombre") or "").strip()
    codigo = (p.get("codigo") or None)
    valor = p.get("valor")
    # Limpia precios tipo "16.390" o "16,390"
    if isinstance(valor, str):
        num = re.sub(r"[^\d]", "", valor)
        valor = int(num) if num.isdigit() else 0
    if isinstance(valor, float):
        valor = int(round(valor))
    if not isinstance(valor, int):
        valor = 0
    if codigo:
        codigo = str(codigo).strip()
    return {"codigo": codigo, "nombre": nombre[:80], "valor": valor, "fuente": "openai"}

def _schema() -> Dict[str, Any]:
    # Structured Outputs (JSON Schema estricto).
    # Docs: Responses API + Structured Outputs. :contentReference[oaicite:1]{index=1}
    return {
        "name": "invoice_schema",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "establecimiento": {"type": "string"},
                "total": {"type": ["integer", "null"]},
                "moneda": {"type": "string", "default": "COP"},
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "codigo": {"type": ["string", "null"]},
                            "nombre": {"type": "string"},
                            "valor":  {"type": ["integer", "string"]},  # aceptamos str para limpieza
                            "cantidad": {"type": ["number", "string", "null"]},
                        },
                        "required": ["nombre", "valor"]
                    }
                }
            },
            "required": ["items"]
        }
    }

_PROMPT = (
    "Eres un extractor para facturas/tiquetes en español de supermercados.\n"
    "Devuelve SOLO el JSON exigido por el schema. Reglas:\n"
    "- 'items': cada producto con 'nombre', 'valor' (entero en COP), y si existe 'codigo' (EAN/PLU) y 'cantidad'.\n"
    "- Une descripciones partidas en dos líneas (p.ej. línea con nombre y la siguiente sólo con precio/peso).\n"
    "- Ignora descuentos globales, subtotales, totales con tarjeta, “RESUMEN DE IVA”, etc.\n"
    "- Si un código parece de peso variable (empieza por 20/28/29 y 12–13 dígitos), conviene mantenerlo como 'codigo'.\n"
    "- 'total' corresponde al total de la compra si es legible; si no, null.\n"
)

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """
    Envía la imagen al modelo vision (OpenAI) y obtiene productos estructurados.
    Devuelve un dict compatible con nuestros endpoints: {establecimiento,total,productos,metadatos}
    """
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    img_url = _b64_data_url(image_path)

    try:
        # Responses API con imagen + Structured Outputs
        # (mensajes multimodales input_text + input_image). :contentReference[oaicite:2]{index=2}
        resp = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "user", "content": [
                    {"type": "input_text", "text": _PROMPT},
                    {"type": "input_image", "image_url": {"url": img_url}}
                ]}
            ],
            response_format={"type": "json_schema", "json_schema": _schema()},
            temperature=0
        )
        # Toma el JSON del primer output
        data = None
        if resp.output and len(resp.output) > 0 and resp.output[0].type == "output_text":
            # Algunas versiones devuelven string JSON
            data = json.loads(resp.output[0].content[0].text)
        elif resp.output and resp.output[0].type == "response_object":
            data = resp.output[0].response_object  # ya dict
        else:
            # Fallback: intenta parsear texto plano a JSON
            txt = (resp.output_text or "").strip()
            data = json.loads(txt) if txt.startswith("{") else {"items": []}
    except Exception as e:
        # Reintento sin schema (menos estricto)
        try:
            resp2 = client.responses.create(
                model=OPENAI_MODEL,
                input=[{"role": "user", "content": [
                    {"type": "input_text", "text": _PROMPT + "\nSALIDA: JSON puro."},
                    {"type": "input_image", "image_url": {"url": img_url}}
                ]}],
                temperature=0
            )
            data = json.loads(resp2.output_text or "{}")
        except Exception:
            data = {"items": []}

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
