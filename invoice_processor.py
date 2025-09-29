import os
import re
import json
import tempfile
import time
from datetime import datetime
from google.cloud import documentai
import traceback
from PIL import Image

# Intentar importar Tesseract (opcional)
try:
    import pytesseract
    TESSERACT_AVAILABLE = True
    print("✅ Tesseract OCR disponible")
except ImportError:
    TESSERACT_AVAILABLE = False
    print("⚠️ Tesseract no disponible - solo Document AI")

def setup_environment():
    required_vars = ['GCP_PROJECT_ID', 'DOC_AI_LOCATION', 'DOC_AI_PROCESSOR_ID', 'GOOGLE_APPLICATION_CREDENTIALS_JSON']
    for var in required_vars:
        if not os.environ.get(var):
            raise Exception(f"Variable requerida: {var}")
    credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    try:
        json.loads(credentials_json)
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(credentials_json)
            temp_file_path = temp_file.name
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_path
        return True
    except:
        raise Exception("JSON inválido")

def clean_amount(amount_str):
    if not amount_str:
        return None
    amount_str = str(amount_str).strip()
    cleaned = re.sub(r'[$\s]', '', amount_str).replace('.', '').replace(',', '')
    cleaned = re.sub(r'[^\d]', '', cleaned)
    if not cleaned:
        return None
    try:
        amount = int(cleaned)
        if 10 < amount < 10000000:
            return amount
    except:
        pass
    return None

def extract_vendor_name(raw_text):
    patterns = [
        r'(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)', r'((?:EXITO|ÉXITO)\s+\w+)', 
        r'(CARULLA[^\n]*)', r'(TIENDAS\s+D1[^\n]*)', r'(JUMBO[^\n]*)', 
        r'(OLIMPICA[^\n]*)', r'(ALKOSTO[^\n]*)', r'(CRUZ\s+VERDE[^\n]*)', 
        r'(LA\s+REBAJA[^\n]*)', r'(CAFAM[^\n]*)'
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            return re.sub(r'\s+', ' ', match.group(1).strip())[:50]
    lines = raw_text.split('\n')[:15]
    for line in lines:
        line = line.strip()
        if 5 <= len(line) <= 50 and line.isupper() and not re.match(r'^[\d\s\-\.]+$', line):
            if not re.search(r'(CRA|CALLE|NIT|RUT|\d{3,})', line):
                return line
    return "Establecimiento no identificado"

def extract_product_code(text):
    if not text:
        return None
    patterns = [
        r'^(\d{13})\s', r'^(\d{12})\s', r'^(\d{10})\s', r'^(\d{7})\s', 
        r'\b(\d{13})\b', r'\b(\d{10})\b', r'\b(\d{7})\b'
    ]
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            code = match.group(1)
            if 6 <= len(code) <= 13:
                return code
    return None

def clean_product_name(text, product_code=None):
    if not text:
        return None
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    text = re.sub(r'^\d+DF\.[A-Z]{2}\.', '', text)
    text = re.sub(r'^DF\.[A-Z]{2}\.', '', text)
    text = re.sub(r'\d+[,\.]\d{3}', '', text)
    text = re.sub(r'\$\d+', '', text)
    text = re.sub(r'[XNH]\s*$', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = [w for w in text.split() if re.search(r'[A-Za-zÀ-ÿ]{2,}', w) and len(w) >= 2 and not w.isdigit()]
    return ' '.join(words[:5])[:50] if words else None

def extract_products_document_ai(document):
    """Método 1: Document AI (rápido, ~30 productos)"""
    line_items = [e for e in document.entities if e.type_ == "line_item" and e.confidence > 0.35]
    productos = []
    i = 0
    
    while i < len(line_items):
        entity = line_items[i]
        product_code = extract_product_code(entity.mention_text)
        product_name = clean_product_name(entity.mention_text, product_code)
        unit_price = None
        
        for prop in entity.properties:
            if prop.type_ == "line_item/amount" and prop.confidence > 0.3:
                unit_price = clean_amount(prop.mention_text)
                if unit_price:
                    break
        
        if not unit_price and i + 1 < len(line_items):
            for prop in line_items[i + 1].properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        i += 1
                        break
        
        if product_code or (product_name and len(product_name) > 2):
            productos.append({
                "codigo": product_code,
                "nombre": product_name or "Sin descripción",
                "valor": unit_price or 0,
                "fuente": "document_ai"
            })
        i += 1
    
    return productos

def extract_products_tesseract(image_path):
    """Método 2: Tesseract OCR (lento, texto completo)"""
    if not TESSERACT_AVAILABLE:
        return []
    
    try:
        img = Image.open(image_path)
        texto_completo = pytesseract.image_to_string(img, lang='spa', config='--psm 6')
        
        productos = []
        patterns = [
            r'(\d{6,13})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s/\-\.]{3,50}?)\s+.*?(\d{1,3}[,\.]\d{3})',
            r'(\d{3,5})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s/\-\.]{3,50}?)\s+.*?(\d{1,3}[,\.]\d{3})',
        ]
        
        for pattern in patterns:
            matches = re.finditer(pattern, texto_completo, re.MULTILINE)
            for match in matches:
                codigo = match.group(1).strip()
                nombre = match.group(2).strip()
                precio = clean_amount(match.group(3))
                
                nombre = re.sub(r'\d+', '', nombre)
                nombre = re.sub(r'\s+', ' ', nombre).strip()
                
                if len(codigo) >= 3 and len(nombre) > 3 and precio:
                    productos.append({
                        "codigo": codigo,
                        "nombre": nombre[:50],
                        "valor": precio,
                        "fuente": "tesseract"
                    })
        
        return productos
    
    except Exception as e:
        print(f"Error Tesseract: {e}")
        return []

def combinar_y_deduplicar(productos_ai, productos_tesseract):
    """Método 3: Combinar resultados priorizando calidad"""
    productos_finales = {}
    
    # Primero agregar Document AI (alta confianza)
    for prod in productos_ai:
        if prod.get('codigo'):
            productos_finales[prod['codigo']] = prod
    
    # Luego agregar Tesseract (productos no detectados por AI)
    for prod in productos_tesseract:
        codigo = prod.get('codigo')
        if codigo and codigo not in productos_finales:
            # Validar que tenga sentido
            if prod.get('valor', 0) > 0 and prod.get('nombre'):
                productos_finales[codigo] = prod
    
    # Convertir a lista y ordenar por código
    resultado = list(productos_finales.values())
    resultado.sort(key=lambda x: x.get('codigo', ''))
    
    return resultado

def process_invoice_complete(file_path):
    """Procesamiento COMPLETO con triple método"""
    inicio = time.time()
    
    try:
        print(f"\n{'='*60}")
        print("PROCESAMIENTO COMPLETO ASÍNCRONO")
        print(f"{'='*60}")
        
        setup_environment()
        
        # MÉTODO 1: Document AI
        print("\n[1/3] Procesando con Document AI...")
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        
        with open(file_path, "rb") as image:
            content = image.read()
        
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        result = client.process_document(
            request=documentai.ProcessRequest(
                name=name,
                raw_document=documentai.RawDocument(content=content, mime_type=mime_type)
            )
        )
        
        establecimiento = extract_vendor_name(result.document.text)
        productos_ai = extract_products_document_ai(result.document)
        print(f"✓ Document AI: {len(productos_ai)} productos")
        
        # MÉTODO 2: Tesseract OCR
        print("\n[2/3] Procesando con Tesseract OCR...")
        productos_tesseract = extract_products_tesseract(file_path)
        print(f"✓ Tesseract: {len(productos_tesseract)} productos")
        
        # MÉTODO 3: Combinar y deduplicar
        print("\n[3/3] Combinando resultados...")
        productos_finales = combinar_y_deduplicar(productos_ai, productos_tesseract)
        
        tiempo_total = int(time.time() - inicio)
        
        print(f"\n{'='*60}")
        print(f"RESULTADOS FINALES")
        print(f"{'='*60}")
        print(f"Establecimiento: {establecimiento}")
        print(f"Productos totales: {len(productos_finales)}")
        print(f"  - Document AI: {len(productos_ai)}")
        print(f"  - Tesseract: {len(productos_tesseract)}")
        print(f"  - Únicos finales: {len(productos_finales)}")
        print(f"Tiempo: {tiempo_total} segundos")
        print(f"{'='*60}\n")
        
        return {
            "establecimiento": establecimiento,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": "triple_extraction",
                "productos_ai": len(productos_ai),
                "productos_tesseract": len(productos_tesseract),
                "productos_finales": len(productos_finales),
                "tiempo_segundos": tiempo_total
            }
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None

# Mantener compatibilidad con versión anterior
def process_invoice_products(file_path):
    """Versión legacy - usa el nuevo procesamiento completo"""
    return process_invoice_complete(file_path)
