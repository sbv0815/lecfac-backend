from __future__ import annotations
import os
import re
from datetime import datetime
from typing import Any, Dict, List
import tempfile
from collections import defaultdict

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

def extract_document_with_docai(image_path: str):
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
        
        return response.document
    except Exception as e:
        print(f"Error Document AI: {e}")
        return None

def parse_products_from_docai_layout(document) -> List[Dict]:
    """Parser basado en coordenadas Y (filas horizontales)"""
    lines = defaultdict(list)
    
    for page in document.pages:
        for token in page.tokens:
            y_coord = token.layout.bounding_poly.normalized_vertices[0].y
            line_key = round(y_coord * 100)
            
            text = ""
            if hasattr(token.layout, 'text_anchor') and token.layout.text_anchor.text_segments:
                for segment in token.layout.text_anchor.text_segments:
                    start = segment.start_index
                    end = segment.end_index
                    text += document.text[start:end]
            
            x_coord = token.layout.bounding_poly.normalized_vertices[0].x
            
            if text:
                lines[line_key].append((x_coord, text))
    
    for line_key in lines:
        lines[line_key].sort(key=lambda x: x[0])
    
    sorted_lines = sorted(lines.items(), key=lambda x: x[0])
    
    productos = []
    
    for line_key, tokens in sorted_lines:
        line_text = " ".join([t[1] for t in tokens])
        upper = line_text.upper()
        
        # CRÍTICO: Saltar líneas que son SOLO información de peso
        # Formato: "0.135 KG X 4780" o "195 KG X 5980"
        if re.search(r'\d+[.,]?\d*\s*KG\s*X\s*\d+', upper):
            continue
        
        # Saltar si la línea entera es peso/cantidad
        if re.match(r'^\d+[.,]?\d*\s*(KG|GR|ML|X)\b', upper):
            continue
        
        # Ignorar administrativas
        if any(x in upper for x in ['CODIGO', 'SUBTOTAL', 'TOTAL', 'TARJETA', 'NIT', 'DIAN', 'ITEMS', 'RESOLUCION']):
            continue
        
        # Ignorar descuentos
        if 'DF.' in line_text or 'C.' in line_text or '%REF' in upper or line_text.startswith('-'):
            continue
        
        codigo_match = re.search(r'\b(\d{3,13})\b', line_text)
        precio_match = re.search(r'(\d{1,3}[.,]\d{3})', line_text)
        
        if not (codigo_match and precio_match):
            continue
        
        codigo = codigo_match.group(1)
        precio_str = precio_match.group(1).replace('.', '').replace(',', '')
        
        # Extraer nombre
        codigo_pos = codigo_match.start()
        precio_pos = precio_match.start()
        
        if precio_pos > codigo_pos + len(codigo):
            nombre = line_text[codigo_pos + len(codigo):precio_pos].strip()
        else:
            nombre = line_text[codigo_match.end():].strip()
            nombre = re.sub(r'\d{1,3}[.,]\d{3}', '', nombre).strip()
        
        # Limpiar nombre agresivamente
        nombre = re.sub(r'\b\d{10,}\b', '', nombre)  # Códigos largos
        nombre = re.sub(r'\d+[.,]?\d*\s*(KG|GR|ML|X|N|H|A|E)\b', '', nombre, flags=re.IGNORECASE)  # Peso
        nombre = nombre.strip()
        
        # Validar nombre
        if not nombre or len(nombre) < 3:
            continue
        
        # Rechazar si nombre es basura
        if re.match(r'^(KG|X|N|H|A|E|LLAN|URABA|PVC)\s*(X|KG)?$', nombre.upper()):
            continue
        
        # Validar que tenga al menos 2 letras
        if len(re.findall(r'[A-Za-zÀ-ÿ]', nombre)) < 2:
            continue
        
        try:
            precio = int(precio_str)
            if 100 <= precio <= 200000:
                productos.append({
                    "codigo": codigo,
                    "nombre": nombre[:80],
                    "valor": precio,
                    "fuente": "document-ai-layout"
                })
        except:
            pass
    
    return productos
def extract_metadata(text: str) -> Dict:
    lines = text.split('\n')
    
    establecimiento = "Establecimiento no identificado"
    for line in lines[:20]:
        if any(x in line.upper() for x in ['JUMBO', 'EXITO', 'CARULLA', 'OLIMPICA', 'D1', 'ALKOSTO']):
            establecimiento = line.strip()
            break
    
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
    print("\n" + "="*70)
    print("PROCESANDO FACTURA")
    print("="*70)
    
    try:
        document = extract_document_with_docai(image_path)
        
        if not document:
            print("ERROR: Document AI no devolvió documento")
            return {
                "establecimiento": "Error: Document AI no configurado",
                "total": None,
                "productos": [],
                "metadatos": {"metodo": "error", "items_detectados": 0}
            }
        
        print("Parseando productos con layout...")
        productos = parse_products_from_docai_layout(document)
        print(f"Productos parseados: {len(productos)}")
        
        print("Extrayendo metadata...")
        metadata = extract_metadata(document.text)
        
        seen = set()
        unicos = []
        for p in productos:
            key = f"{p['codigo']}|{p['nombre']}|{p['valor']}"
            if key not in seen:
                seen.add(key)
                unicos.append(p)
        
        print(f"Productos únicos: {len(unicos)}")
        print("="*70)
        
        return {
            "establecimiento": metadata["establecimiento"],
            "total": metadata["total"],
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": unicos,
            "metadatos": {
                "metodo": "document-ai-layout",
                "items_detectados": len(unicos)
            },
        }
    
    except Exception as e:
        print(f"ERROR CRÍTICO: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        
        return {
            "establecimiento": f"Error: {str(e)[:50]}",
            "total": None,
            "productos": [],
            "metadatos": {"metodo": "error", "error": str(e)}
        }
