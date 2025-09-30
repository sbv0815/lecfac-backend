from __future__ import annotations
import base64
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_INVOICE_MODEL", "gpt-4o")

def _b64_data_url(image_path: str) -> str:
    mime = "image/jpeg"
    if image_path.lower().endswith(".png"):
        mime = "image/png"
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    return f"data:{mime};base64,{b64}"

def extract_text_from_invoice(image_path: str) -> str:
    """Extrae texto plano de la factura usando OpenAI Vision"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    img_url = _b64_data_url(image_path)
    
    prompt = """Extrae TODO el texto de esta factura exactamente como aparece, línea por línea.
Mantén el orden original de arriba hacia abajo.
NO interpretes ni estructures, solo transcribe el texto tal cual lo ves."""
    
    response = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {"url": img_url}}
            ]
        }],
        temperature=0,
        max_tokens=16000
    )
    
    return response.choices[0].message.content

def parse_products_from_text(text: str) -> List[Dict]:
    """Parsea productos del texto usando patrones"""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    productos = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Saltar líneas de encabezado, totales, etc
        if any(x in line.upper() for x in ['SUBTOTAL', 'TOTAL', 'IVA', 'TARJETA', 'EFECTIVO', 'CAMBIO', 'ITEMS COMPRADOS']):
            i += 1
            continue
        
        # Buscar código (número de 3-13 dígitos)
        codigo_match = re.search(r'\b(\d{3,13})\b', line)
        if not codigo_match:
            i += 1
            continue
        
        codigo = codigo_match.group(1)
        
        # El nombre debería estar en la siguiente línea
        if i + 1 >= len(lines):
            break
        
        nombre_line = lines[i + 1]
        
        # Saltar si la siguiente línea parece ser otro código o precio
        if re.match(r'^\d+$', nombre_line.strip()) or re.match(r'^\$?\d{1,3}[.,]\d{3}', nombre_line):
            i += 1
            continue
        
        nombre = nombre_line.strip()
        
        # Buscar precio en las siguientes 2-3 líneas
        precio = None
        for j in range(i + 2, min(i + 5, len(lines))):
            precio_match = re.search(r'\$?\s*(\d{1,3}[.,]\d{3}(?:[.,]\d{3})?)', lines[j])
            if precio_match:
                precio_str = precio_match.group(1).replace('.', '').replace(',', '')
                try:
                    precio = int(precio_str)
                    break
                except:
                    pass
        
        if nombre and precio and precio > 100:
            productos.append({
                "codigo": codigo,
                "nombre": nombre[:80],
                "valor": precio,
                "fuente": "openai"
            })
        
        i += 1
    
    return productos

def extract_metadata(text: str) -> Dict:
    """Extrae establecimiento y total"""
    lines = text.split('\n')
    
    # Buscar establecimiento en las primeras líneas
    establecimiento = "Establecimiento no identificado"
    for line in lines[:15]:
        if any(x in line.upper() for x in ['JUMBO', 'EXITO', 'CARULLA', 'OLIMPICA', 'D1', 'ALKOSTO']):
            establecimiento = line.strip()
            break
    
    # Buscar total
    total = None
    for line in reversed(lines[-30:]):
        if 'TOTAL' in line.upper() and not 'SUBTOTAL' in line.upper():
            match = re.search(r'\$?\s*(\d{1,3}[.,]\d{3}(?:[.,]\d{3})?)', line)
            if match:
                total_str = match.group(1).replace('.', '').replace(',', '')
                try:
                    total = int(total_str)
                    break
                except:
                    pass
    
    return {"establecimiento": establecimiento, "total": total}

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """Procesa factura: extrae texto con OpenAI, parsea con regex"""
    
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY no configurada")
    
    print("Extrayendo texto de la factura...")
    text = extract_text_from_invoice(image_path)
    print(f"Texto extraído: {len(text)} caracteres")
    
    print("Parseando productos...")
    productos = parse_products_from_text(text)
    print(f"Productos detectados: {len(productos)}")
    
    metadata = extract_metadata(text)
    
    # Deduplicar
    seen = {}
    productos_unicos = []
    for p in productos:
        key = f"{p['codigo']}|{p['nombre']}|{p['valor']}"
        if key not in seen:
            seen[key] = True
            productos_unicos.append(p)
    
    print(f"Productos únicos: {len(productos_unicos)}")
    
    return {
        "establecimiento": metadata["establecimiento"],
        "total": metadata["total"],
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": productos_unicos,
        "metadatos": {
            "metodo": "openai-vision-text+regex",
            "model": OPENAI_MODEL,
            "items_detectados": len(productos_unicos),
        },
    }
