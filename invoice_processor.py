import os
import re
import json
import tempfile
from datetime import datetime
from google.cloud import documentai
import traceback

def setup_environment():
    """Configura las variables de entorno para Google Cloud"""
    required_vars = [
        'GCP_PROJECT_ID',
        'DOC_AI_LOCATION', 
        'DOC_AI_PROCESSOR_ID',
        'GOOGLE_APPLICATION_CREDENTIALS_JSON'
    ]
    
    for var in required_vars:
        if not os.environ.get(var):
            raise Exception(f"Variable de entorno requerida no encontrada: {var}")
    
    credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    
    try:
        json.loads(credentials_json)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(credentials_json)
            temp_file_path = temp_file.name
        
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_path
        return True
        
    except json.JSONDecodeError:
        raise Exception("GOOGLE_APPLICATION_CREDENTIALS_JSON no es un JSON válido")
    except Exception as e:
        raise Exception(f"Error configurando credenciales: {str(e)}")

def clean_amount(amount_str):
    """Limpia y convierte montos a números enteros (pesos colombianos)"""
    if not amount_str:
        return None
    
    amount_str = str(amount_str).strip()
    cleaned = re.sub(r'[$\s]', '', amount_str)
    cleaned = cleaned.replace('.', '').replace(',', '')
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
    """Extrae el nombre del establecimiento"""
    patterns = [
        r'(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)',
        r'((?:EXITO|ÉXITO)\s+\w+)',
        r'(CARULLA[^\n]*)',
        r'(TIENDAS\s+D1[^\n]*)',
        r'(D1\s+COLOMBIA)',
        r'(JUMBO[^\n]*)',
        r'(OLIMPICA[^\n]*)',
        r'(ALKOSTO[^\n]*)',
        r'(METRO[^\n]*)',
        r'(MAKRO[^\n]*)',
        r'(SURTIMAX[^\n]*)',
        r'(ARA[^\n]*)',
        r'(FALABELLA[^\n]*)',
        r'(LA\s+14[^\n]*)',
        r'(CRUZ\s+VERDE[^\n]*)',
        r'(DROGAS\s+LA\s+REBAJA[^\n]*)',
        r'(LA\s+REBAJA[^\n]*)',
        r'(CAFAM[^\n]*)',
        r'(COLSUBSIDIO[^\n]*)',
        r'(LOCATEL[^\n]*)',
        r'(HOMECENTER[^\n]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            name = re.sub(r'\s+', ' ', name)
            return name[:50]
    
    lines = raw_text.split('\n')[:15]
    for line in lines:
        line = line.strip()
        if 5 <= len(line) <= 50 and line.isupper() and not re.match(r'^[\d\s\-\.]+$', line):
            if not re.search(r'(CRA|CALLE|KR|CL|NIT|RUT|\d{3,})', line):
                return line
    
    return "Establecimiento no identificado"

def extract_product_code(text):
    """Extrae código de producto"""
    if not text:
        return None
    
    text = text.strip()
    
    patterns = [
        r'^(\d{13})\s',
        r'^(\d{12})\s',
        r'^(\d{10})\s',
        r'^(\d{8})\s',
        r'^(\d{7})\s',
        r'^(\d{6})\s',
        r'\b(\d{13})\b',
        r'\b(\d{12})\b',
        r'\b(\d{10})\b',
        r'\b(\d{7})\b',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            code = match.group(1)
            if 6 <= len(code) <= 13:
                if not (len(code) == 4 and code.endswith(('00', '90', '50'))):
                    return code
    
    return None

def clean_product_name(text, product_code=None):
    """Extrae el nombre limpio del producto"""
    if not text:
        return None
    
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    else:
        text = re.sub(r'^\d{6,}\s*', '', text)
    
    text = re.sub(r'\d+[,\.]\d{3}', '', text)
    text = re.sub(r'\$\d+', '', text)
    text = re.sub(r'\d+\s*[xX]\s*\d+', '', text)
    text = re.sub(r'[XNH]\s*$', '', text)
    text = re.sub(r'\s+DF\.[A-Z\.%\s]*', '', text)
    text = re.sub(r'-\d+,\d+', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n+', ' ', text)
    
    words = []
    for word in text.split():
        if re.search(r'[A-Za-zÀ-ÿ]{2,}', word):
            word = re.sub(r'[^\w\sÀ-ÿ]+$', '', word)
            if len(word) >= 2:
                words.append(word)
    
    if words:
        name = ' '.join(words[:5])
        return name[:50] if len(name) > 0 else None
    
    return None

def extract_unit_price_from_text(text):
    """Extrae precio unitario del texto"""
    if not text:
        return None
    
    price_patterns = [
        r'\$\s*(\d{1,3}[,\.]\d{3})',
        r'(\d{1,3}[,\.]\d{3})\s*$',
        r'\$\s*(\d{1,6})',
        r'(\d{4,6})\s*$',
    ]
    
    prices = []
    for pattern in price_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            price = clean_amount(match)
            if price and 50 <= price <= 500000:
                prices.append(price)
    
    if prices:
        prices.sort()
        return prices[len(prices)//2]
    
    return None

def extract_products_with_prices(document):
    """Extrae productos - Document AI + Fallback regex"""
    
    raw_text = document.text
    
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": []
    }
    
    print(f"\n=== ANÁLISIS DE ENTIDADES ===")
    print(f"Total entidades: {len(document.entities)}")
    
    line_items = [e for e in document.entities if e.type_ == "line_item" and e.confidence > 0.35]
    print(f"Line items válidos: {len(line_items)}")
    
    productos_finales = []
    i = 0
    
    while i < len(line_items):
        entity = line_items[i]
        
        raw_item_text = entity.mention_text
        product_code = extract_product_code(raw_item_text)
        product_name = clean_product_name(raw_item_text, product_code)
        
        unit_price = None
        
        for prop in entity.properties:
            if prop.type_ == "line_item/amount" and prop.confidence > 0.3:
                price_candidate = clean_amount(prop.mention_text)
                if price_candidate:
                    unit_price = price_candidate
        
        if not unit_price:
            unit_price = extract_unit_price_from_text(raw_item_text)
        
        if (product_code or (product_name and len(product_name) > 2)) and not unit_price:
            if i + 1 < len(line_items):
                next_entity = line_items[i + 1]
                for prop in next_entity.properties:
                    if prop.type_ == "line_item/amount" and prop.confidence > 0.3:
                        price_candidate = clean_amount(prop.mention_text)
                        if price_candidate:
                            unit_price = price_candidate
                            i += 1
                            break
        
        if product_code or (product_name and len(product_name) > 2):
            producto = {
                "codigo": product_code,
                "nombre": product_name or "Producto sin descripción",
                "valor": unit_price or 0
            }
            productos_finales.append(producto)
        
        i += 1
    
    print(f"Document AI: {len(productos_finales)} productos")
    
    # FALLBACK REGEX
    print(f"Activando fallback regex...")
    productos_regex = extract_products_from_text_aggressive(raw_text)
    print(f"Regex: {len(productos_regex)} productos")
    
    codigos_existentes = {p['codigo'] for p in productos_finales if p['codigo']}
    
    for prod_regex in productos_regex:
        if prod_regex['codigo'] not in codigos_existentes:
            productos_finales.append(prod_regex)
            codigos_existentes.add(prod_regex['codigo'])
    
    invoice_data["productos"] = productos_finales
    
    print(f"TOTAL: {len(productos_finales)} productos")
    
    return invoice_data

def extract_products_from_text_aggressive(text):
    """Fallback: Extrae productos con regex"""
    productos = []
    
    patterns = [
        r'(\d{6,13})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s/\-]{3,50}?)(?:\n|\s+).*?(\d{1,3}[,\.]\d{3})',
        r'(\d{6,13})\s+([A-ZÀ-Ÿ\s/\-]+?)\s+.*?(\d{1,3}[,\.]\d{3})',
        r'(\d{2,5})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s]{4,30}?)\s+.*?(\d{1,3}[,\.]\d{3})'
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, text, re.MULTILINE)
        
        for match in matches:
            codigo = match.group(1).strip()
            nombre_raw = match.group(2).strip()
            precio_raw = match.group(3).strip()
            
            nombre = re.sub(r'\d+', '', nombre_raw)
            nombre = re.sub(r'\s+', ' ', nombre)
            nombre = nombre.strip()
            
            precio = clean_amount(precio_raw)
            
            if codigo and len(nombre) > 3 and precio and precio > 0:
                producto = {
                    "codigo": codigo,
                    "nombre": nombre[:50],
                    "valor": precio
                }
                
                if not any(p['codigo'] == codigo for p in productos):
                    productos.append(producto)
    
    return productos

def process_invoice_products(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO ===")
        
        setup_environment()
        
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("Enviando a Document AI...")
        result = client.process_document(request=request)
        document = result.document
        
        print(f"Texto: {len(document.text)} caracteres")
        print(f"Entidades: {len(document.entities)}")
        
        invoice_data = extract_products_with_prices(document)
        
        print(f"Establecimiento: {invoice_data['establecimiento']}")
        print(f"Productos: {len(invoice_data['productos'])}")
        
        return invoice_data
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None
