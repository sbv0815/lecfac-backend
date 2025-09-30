from __future__ import annotations
import base64
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List
import tempfile

try:
    from google.cloud import documentai
    DOCUMENT_AI_AVAILABLE = True
except:
    DOCUMENT_AI_AVAILABLE = False

def setup_document_ai():
    required = ["GCP_PROJECT_ID", "DOC_AI_LOCATION", "DOC_AI_PROCESSOR_ID"]
    if not all(os.environ.get(v) for v in required):
        return False
    
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if creds:
        try:
            with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                f.write(creds)
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f.name
            return True
        except:
            return False
    return False

def extract_text_with_docai(image_path: str) -> str:
    if not DOCUMENT_AI_AVAILABLE or not setup_document_ai():
        return None
    
    try:
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

def parse_products_from_text(text: str) -> List[Dict]:
    """Parser robusto para texto de Document AI"""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    productos = []
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Saltar encabezados/totales
        upper = line.upper()
        if any(x in upper for x in ['CODIGO', 'SUBTOTAL', 'TOTAL', 'IVA', 'TARJETA', 'NIT', 'DIAN']):
            i += 1
            continue
        
        # Buscar línea con código (número de 3-13 dígitos solo)
        if not re.match(r'^\d{3,13}$', line):
            i += 1
            continue
        
        codigo = line
        
        # Nombre en siguiente línea
        if i + 1 >= len(lines):
            break
        
        nombre = lines[i + 1].strip()
        
        # Validar que no sea otro código ni precio
        if re.match(r'^\d+$', nombre) or re.match(r'^\$?\d{1,3}[.,]\d{3}', nombre):
            i += 1
            continue
        
        # Limpiar nombre
        nombre = re.sub(r'\b\d{10,}\b', '', nombre)  # Quitar códigos largos
        nombre = re.sub(r'[A-Z]\.\s*\d+%', '', nombre)  # Quitar refs
        nombre = re.sub(r'\.[A-Z]{2}\.', '', nombre)  # Quitar sufijos
        nombre = nombre.strip()
        
        if len(nombre) < 3:
            i += 1
            continue
        
        # Buscar precio en siguientes 3-5 líneas
        precio = None
        for j in range(i + 2, min(i + 6, len(lines))):
            test = lines[j]
            
            # Saltar peso/cantidad/descuentos
            if re.match(r'^\d+[.,]?\d*\s*(KG|GR|ML|X|N|H)$', test.upper()):
                continue
            if test.startswith('-') or '%REF' in test.upper():
                continue
            
            # Buscar precio
            match = re.search(r'\$?\s*(\d{1,3}(?:[.,]\d{3})+)', test)
            if match:
                precio_str = match.group(1).replace('.', '').replace(',', '')
                try:
                    p = int(precio_str)
                    if 100 <= p <= 1000000:
                        precio = p
                        break
                except:
                    pass
        
        if precio:
            productos.append({
                "codigo": codigo,
                "nombre": nombre[:80],
                "valor": precio,
                "fuente": "document-ai"
            })
        
        i += 1
    
    return productos

def extract_metadata(text: str) -> Dict:
    lines = text.split('\n')
    
    # Establecimiento
    establecimiento = "Establecimiento no identificado"
    for line in lines[:20]:
        if any(x in line.upper() for x in ['JUMBO', 'EXITO', 'CARULLA', 'OLIMPICA', 'D1', 'ALKOSTO']):
            establecimiento = line.strip()
            break
    
    # Total
    total = None
    for line in reversed(lines[-40:]):
        if 'TOTAL' in line.upper() and 'SUBTOTAL' not in line.upper():
            match = re.search(r'\$?\s*(\d{1,3}(?:[.,]\d{3})+)', line)
            if match:
                total_str = match.group(1).replace('.', '').replace(',', '')
                try:
                    t = int(total_str)
                    if t > 10000:
                        total = t
                        break
                except:
                    pass
    
    return {"establecimiento": establecimiento, "total": total}

def parse_invoice_with_openai(image_path: str) -> Dict[str, Any]:
    """Procesa con Document AI (OpenAI solo fallback)"""
    print("\n" + "="*70)
    print("PROCESANDO FACTURA")
    print("="*70)
    
    # Intentar Document AI
    text = extract_text_with_docai(image_path)
    metodo = "document-ai"
    
    if text:
        print(f"✓ Document AI: {len(text)} caracteres")
    else:
        print("✗ Document AI no disponible")
        return {
            "establecimiento": "Error: Document AI no configurado",
            "total": None,
            "productos": [],
            "metadatos": {"metodo": "error"}
        }
    
    # Parsear
    productos = parse_products_from_text(text)
    metadata = extract_metadata(text)
    
    # Deduplicar
    seen = set()
    unicos = []
    for p in productos:
        key = f"{p['codigo']}|{p['nombre']}|{p['valor']}"
        if key not in seen:
            seen.add(key)
            unicos.append(p)
    
    print(f"Productos detectados: {len(unicos)}")
    print("="*70)
    
    return {
        "establecimiento": metadata["establecimiento"],
        "total": metadata["total"],
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": unicos,
        "metadatos": {
            "metodo": metodo,
            "items_detectados": len(unicos),
        },
    }
