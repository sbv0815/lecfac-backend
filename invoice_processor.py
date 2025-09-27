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
    
    # Crear archivo temporal con las credenciales JSON
    credentials_json = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    
    try:
        # Validar que el JSON sea válido
        json.loads(credentials_json)
        
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            temp_file.write(credentials_json)
            temp_file_path = temp_file.name
        
        # Configurar la variable para Google Cloud
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_file_path
        
        return True
        
    except json.JSONDecodeError:
        raise Exception("GOOGLE_APPLICATION_CREDENTIALS_JSON no es un JSON válido")
    except Exception as e:
        raise Exception(f"Error configurando credenciales: {str(e)}")

def clean_amount(amount_str):
    """Limpia y convierte montos a números"""
    if not amount_str:
        return None
    
    cleaned = re.sub(r'[^\d,.-]', '', str(amount_str))
    
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace('.', '').replace(',', '.')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '')
    
    try:
        amount = float(cleaned)
        if amount > 0 and amount < 500000:
            return amount
    except:
        pass
    
    return None

def extract_vendor_name(raw_text):
    """Extrae el nombre del establecimiento"""
    patterns = [
        r'(JUMBO\s+\w+)',
        r'(EXITO\s+\w+)',
        r'(CARULLA\s+\w+)',
        r'(OLIMPICA\s+\w+)',
        r'(D1\s+\w*)',
        r'(SURTIMAX\s+\w*)',
         r'(ARA\s+\w*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    lines = raw_text.split('\n')[:10]
    for line in lines:
        line = line.strip()
        if len(line) > 3 and line.isupper() and not re.match(r'^[\d\s\-\.]+$', line):
            return line
    
    return "Establecimiento no identificado"

def extract_product_code(text):
    """Extrae código de producto (8-13 dígitos)"""
    if not text:
        return None
    
    patterns = [
        r'^(\d{8,13})\s',
        r'(\d{10,13})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            code = match.group(1)
            if len(code) >= 8:
                return code
    
    return None

def clean_product_name(text, product_code=None):
    """Extrae el nombre limpio del producto"""
    if not text:
        return None
    
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    else:
        text = re.sub(r'^\d{8,}\s*', '', text)
    
    text = re.sub(r'\d+DF\.[A-Z\.%\s]*', '', text)
    text = re.sub(r'-\d+,\d+', '', text)
    text = re.sub(r'[XNH]\s*$', '', text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n+', ' ', text)
    
    match = re.match(r'^([A-Za-zÀ-ÿ\s]+)', text)
    if match:
        name = match.group(1).strip()
        if len(name) > 2:
            return name[:30]
    
    words = text.split()
    valid_words = []
    for word in words:
        if re.match(r'^[A-Za-zÀ-ÿ]+', word) and len(word) > 1:
            valid_words.append(word)
            if len(valid_words) >= 3:
                break
    
    if valid_words:
        return ' '.join(valid_words)[:30]
    
    return None

def extract_unit_price_from_text(text):
    """Extrae precio unitario del texto del producto"""
    if not text:
        return None
    
    price_patterns = [
        r'(\d{1,5},\d{3})',
        r'(\d{1,5}\.\d{3})',
        r'(\d{1,5})',
    ]
    
    prices = []
    for pattern in price_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            price = clean_amount(match)
            if price and 100 <= price <= 100000:
                prices.append(price)
    
    if prices:
        prices.sort()
        return prices[len(prices)//2] if len(prices) > 1 else prices[0]
    
    return None

def extract_products_with_prices(document):
    """Extrae productos con sus precios unitarios"""
    
    raw_text = document.text
    
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": []
    }
    
    for entity in document.entities:
        if entity.type_ == "line_item" and entity.confidence > 0.4:
            
            raw_item_text = entity.mention_text
            product_code = extract_product_code(raw_item_text)
            product_name = clean_product_name(raw_item_text, product_code)
            
            unit_price = None
            
            for prop in entity.properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        break
            
            if not unit_price:
                unit_price = extract_unit_price_from_text(raw_item_text)
            
            if product_code or (product_name and len(product_name) > 2):
                producto = {
                    "codigo": product_code,
                    "nombre": product_name,
                    "valor": unit_price
                }
                invoice_data["productos"].append(producto)
    
    return invoice_data

def process_invoice_products(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO ===")
        print(f"Archivo: {file_path}")
        print(f"Archivo existe: {os.path.exists(file_path)}")
        
        setup_environment()
        print("Variables de entorno configuradas")
        
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"Project ID: {project_id}")
        print(f"Location: {location}")
        print(f"Processor ID: {processor_id}")
        
        print("Creando cliente Document AI...")
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        print("Leyendo archivo...")
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        print(f"Tamaño del archivo: {len(image_content)} bytes")
        
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        print(f"MIME type: {mime_type}")
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("Enviando solicitud a Document AI...")
        result = client.process_document(request=request)
        document = result.document
        
        print("Documento procesado por Document AI")
        print(f"Texto extraído: {len(document.text)} caracteres")
        print(f"Entidades encontradas: {len(document.entities)}")
        
        print("Extrayendo productos...")
        invoice_data = extract_products_with_prices(document)
        
        print(f"Productos extraídos: {len(invoice_data['productos'])}")
        
        return invoice_data
        
    except Exception as e:
        print(f"ERROR EN PROCESO: {str(e)}")
        print(f"Tipo de error: {type(e).__name__}")
        print(f"Traceback: {traceback.format_exc()}")
        return None
