from __future__ import annotations
import base64
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List
import tempfile

# Intentar Document AI primero
try:
    from google.cloud import documentai
    DOCUMENT_AI_AVAILABLE = True
except:
    DOCUMENT_AI_AVAILABLE = False

from openai import OpenAI

OPENAI_MODEL = os.getenv("OPENAI_INVOICE_MODEL", "gpt-4o")

def setup_document_ai():
    """Configura credenciales de Google Cloud"""
    required_vars = ["GCP_PROJECT_ID", "DOC_AI_LOCATION", "DOC_AI_PROCESSOR_ID"]
    
    for var in required_vars:
        if not os.environ.get(var):
            return False
    
    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds_json:
        try:
            json.loads(creds_json)
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                f.write(creds_json)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
            return True
        except:
            return False
    return False

def extract_text_with_docai(image_path: str) -> str:
    """Extrae texto usando Google Document AI"""
    if not DOCUMENT_AI_AVAILABLE:
        return None
    
    try:
        if not setup_document_ai():
            return None
        
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        
        with open(image_path, "rb") as f:
            content = f.read()
        
        mime = "image/jpeg" if image_path.lower().endswith(('.jpg', '.jpeg')) else "image/png"
        
        response = client.process_document(
            request=documentai.ProcessRequest(
                name=name,
                raw_document=documentai.RawDocument(content=content, mime_type=mime)
            )
        )
        
        return response.document.text
    except Exception as e:
        print(f"Error Document AI: {e}")
        return None

def extract_text_with_openai(image_path: str) -> str:
    """Fallback: extrae texto con OpenAI Vision"""
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    
    with open(image_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode("ascii")
    
    mime = "image/jpeg" if image_path.lower().endswith(('.jpg', '.jpeg')) else "image/png"
    img_url = f"data:{mime};base64,{b64}"
    
    prompt = """Extrae TODO el texto de esta factura línea por línea, exactamente como aparece.
Mantén el orden de arriba hacia abajo. Solo texto, sin interpretación."""
    
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
    """Parsea productos del texto OCR - versión mejorada"""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    productos = []
    
    # Primero, identificar bloques de productos
    # Un producto típico tiene: CÓDIGO, NOMBRE, posiblemente info adicional, PRECIO
    
    i = 0
    while i < len(lines):
        line = lines[i]
        upper = line.upper()
        
        # Saltar encabezados y totales
        if any(x in upper for x in ['CODIGO', 'DESCRIPCION', 'VALOR', 'SUBTOTAL', 'TOTAL A PAGAR', 'ITEMS COMPRADOS', 'TARJETA', 'EFECTIVO', 'CAMBIO', 'NIT', 'RESOLUCION', 'DIAN', 'IVA']):
            i += 1
            continue
        
        # Buscar código (solo números, 3-13 dígitos)
        if not re.match(r'^\d{3,13}$', line):
            i += 1
            continue
        
        codigo = line
        
        # El nombre debe estar en la siguiente línea
        if i + 1 >= len(lines):
            break
        
        i += 1
        nombre_line = lines[i]
        
        # Validar que el nombre no sea un número puro ni un precio
        if re.match(r'^\d+$', nombre_line) or re.match(r'^\$?\s*\d{1,3}[.,]\d{3}', nombre_line):
            continue
        
        # Limpiar el nombre
        nombre = nombre_line
        # Remover códigos extras que pueden estar en el nombre
        nombre = re.sub(r'\b\d{10,}\b', '', nombre)
        # Remover referencias tipo "C.15%" o "DF.20%"
        nombre = re.sub(r'[A-Z]\.\s*\d+%', '', nombre)
        # Remover sufijos tipo ".VD." ".FD."
        nombre = re.sub(r'\.[A-Z]{2}\.', '', nombre)
        nombre = nombre.strip()
        
        if len(nombre) < 3:
            continue
        
        # Buscar el precio en las siguientes 3-5 líneas
        precio = None
        j = i + 1
        while j < min(i + 6, len(lines)):
            test_line = lines[j]
            
            # Saltar líneas que claramente son info adicional
            if re.match(r'^\d+[.,]?\d*\s*(KG|GR|ML|UN|X|N|H|A|E)$', test_line.upper()):
                j += 1
                continue
            
            # Saltar líneas con descuentos o referencias
            if test_line.startswith('-') or '%REF' in test_line.upper() or 'DF.' in test_line or 'C.' in test_line:
                j += 1
                continue
            
            # Buscar precio (formato: $12.345 o 12.345 o 12,345)
            precio_match = re.search(r'\$?\s*(\d{1,3}(?:[.,]\d{3})+)', test_line)
            if precio_match:
                precio_str = precio_match.group(1).replace('.', '').replace(',', '')
                try:
                    precio_candidato = int(precio_str)
                    # Validar que sea un precio razonable (entre 100 y 1.000.000)
                    if 100 <= precio_candidato <= 1000000:
                        precio = precio_candidato
                        break
                except:
                    pass
            
            j += 1
        
        # Solo agregar si tenemos código, nombre y precio válidos
        if codigo and nombre and precio:
            productos.append({
                "codigo": codigo,
                "nombre": nombre[:80],
                "valor": precio,
                "fuente": "docai"
            })
            # Saltar hasta después del precio encontrado
            i = j
        else:
            i += 1
    
    return productos

def extract_metadata(text: str) -> Dict:
    """Extrae establecimiento y total"""
    lines = text.split('\n')
    
    establecimiento = "Establecimiento no identificado"
    for line in lines[:20]:
        upper = line.upper()
        if any(x in upper for x in ['JUMBO', 'EXITO', 'ÉXITO', 'CARULLA', 'OLIMPICA', 'OLÍMPICA', 'D1', 'ALKOSTO', 'CAFAM']):
            establecimiento = line.strip()
            break
    
    total = None
    for line in reversed(lines[-40:]):
        if re.search(r'TOTAL\s*(A\s*PAGAR)?', line.upper()) and 'SUBTOTAL' not in line.upper():
            match = re.search(r'\$?\s*(\d{1,3}[.,]\d{3}(?:[.,]\d{3})?)', line)
            if match:
                total_str = match.group(1).replace('.', '').replace(',', '')
                try:
                    total = int(total_str)
                    if total > 10000:
                        break
                except:
                    pass
    
    return {"establecimiento": establecimiento, "total": total}

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """Procesa factura: Document AI primero, OpenAI de fallback"""
    
    print("\n" + "="*70)
    print("PROCESAMIENTO DE FACTURA")
    print("="*70)
    
    # Intentar Document AI primero
    text = None
    metodo = "openai-vision"
    
    if DOCUMENT_AI_AVAILABLE and os.environ.get("GCP_PROJECT_ID"):
        print("Intentando Google Document AI...")
        text = extract_text_with_docai(image_path)
        if text:
            metodo = "document-ai"
            print(f"✓ Document AI exitoso: {len(text)} caracteres")
    
    # Fallback a OpenAI
    if not text:
        print("Usando OpenAI Vision como fallback...")
        if not os.environ.get("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY no configurada")
        text = extract_text_with_openai(image_path)
        print(f"✓ OpenAI exitoso: {len(text)} caracteres")
    
    # Parsear productos
    print("Parseando productos...")
    productos = parse_products_from_text(text)
    print(f"Productos detectados: {len(productos)}")
    
    # Extraer metadata
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
    print("="*70 + "\n")
    
    return {
        "establecimiento": metadata["establecimiento"],
        "total": metadata["total"],
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": productos_unicos,
        "metadatos": {
            "metodo": metodo,
            "model": OPENAI_MODEL if metodo == "openai-vision" else "document-ai",
            "items_detectados": len(productos_unicos),
        },
    }
