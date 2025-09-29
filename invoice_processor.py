import os
import re
import json
import tempfile
from datetime import datetime
from google.cloud import documentai
import traceback

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
    patterns = [r'(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)', r'((?:EXITO|ÉXITO)\s+\w+)', r'(CARULLA[^\n]*)', r'(TIENDAS\s+D1[^\n]*)', r'(JUMBO[^\n]*)', r'(OLIMPICA[^\n]*)', r'(ALKOSTO[^\n]*)', r'(CRUZ\s+VERDE[^\n]*)', r'(LA\s+REBAJA[^\n]*)']
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
    patterns = [r'^(\d{13})\s', r'^(\d{12})\s', r'^(\d{10})\s', r'^(\d{7})\s', r'\b(\d{13})\b', r'\b(\d{10})\b']
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            code = match.group(1)
            if 6 <= len(code) <= 13 and not (len(code) == 4 and code.endswith(('00', '90'))):
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

def extract_unit_price_from_text(text):
    if not text:
        return None
    patterns = [r'\$\s*(\d{1,3}[,\.]\d{3})', r'(\d{1,3}[,\.]\d{3})\s*$', r'\$\s*(\d{1,6})']
    prices = []
    for pattern in patterns:
        for match in re.findall(pattern, text):
            price = clean_amount(match)
            if price and 50 <= price <= 500000:
                prices.append(price)
    return sorted(prices)[len(prices)//2] if prices else None

def extract_products_with_prices(document):
    raw_text = document.text
    invoice_data = {"establecimiento": extract_vendor_name(raw_text), "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "productos": []}
    line_items = [e for e in document.entities if e.type_ == "line_item" and e.confidence > 0.35]
    productos_finales = []
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
        if not unit_price:
            unit_price = extract_unit_price_from_text(entity.mention_text)
        if (product_code or (product_name and len(product_name) > 2)) and not unit_price and i + 1 < len(line_items):
            for prop in line_items[i + 1].properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        i += 1
                        break
        if product_code or (product_name and len(product_name) > 2):
            productos_finales.append({"codigo": product_code, "nombre": product_name or "Sin descripción", "valor": unit_price or 0})
        i += 1
    invoice_data["productos"] = productos_finales
    print(f"TOTAL: {len(productos_finales)} productos")
    return invoice_data

def process_invoice_products(file_path):
    try:
        setup_environment()
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        with open(file_path, "rb") as image:
            content = image.read()
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        result = client.process_document(request=documentai.ProcessRequest(name=name, raw_document=documentai.RawDocument(content=content, mime_type=mime_type)))
        return extract_products_with_prices(result.document)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None
