import os
import sys
import re
from datetime import datetime
from google.cloud import documentai
import json

def setup_environment():
    """Configura las variables de entorno automáticamente"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    credentials_path = os.path.join(current_dir, "lecfac-d344d8033994.json")
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    os.environ['GCP_PROJECT_ID'] = 'lecfac'
    os.environ['DOC_AI_LOCATION'] = 'us'
    os.environ['DOC_AI_PROCESSOR_ID'] = 'cf7db72ec2eb6c57'
    
    return True

def clean_amount(amount_str):
    """Limpia y convierte montos a números"""
    if not amount_str:
        return None
    
    cleaned = re.sub(r'[^\d,.-]', '', str(amount_str))
    
    # Manejar formato colombiano
    if ',' in cleaned and '.' in cleaned:
        cleaned = cleaned.replace('.', '').replace(',', '.')
    elif ',' in cleaned:
        cleaned = cleaned.replace(',', '')
    
    try:
        amount = float(cleaned)
        # Filtrar valores que no parecen precios unitarios (muy altos o negativos)
        if amount > 0 and amount < 500000:  # Precios razonables
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
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
    # Buscar en las primeras líneas
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
        r'^(\d{8,13})\s',  # Código al inicio seguido de espacio
        r'(\d{10,13})',    # Códigos largos EAN/UPC
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
    
    # Remover código de producto del inicio
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    else:
        # Remover códigos largos del inicio
        text = re.sub(r'^\d{8,}\s*', '', text)
    
    # Remover códigos promocionales y descuentos
    text = re.sub(r'\d+DF\.[A-Z\.%\s]*', '', text)
    text = re.sub(r'-\d+,\d+', '', text)
    text = re.sub(r'[XNH]\s*$', '', text)  # Remover letras al final
    
    # Limpiar espacios múltiples y saltos de línea
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n+', ' ', text)
    
    # Extraer solo la parte del nombre (antes de números)
    match = re.match(r'^([A-Za-zÀ-ÿ\s]+)', text)
    if match:
        name = match.group(1).strip()
        if len(name) > 2:
            return name[:30]  # Máximo 30 caracteres
    
    # Si no encuentra nombre, intentar extraer palabras válidas
    words = text.split()
    valid_words = []
    for word in words:
        if re.match(r'^[A-Za-zÀ-ÿ]+', word) and len(word) > 1:
            valid_words.append(word)
            if len(valid_words) >= 3:  # Máximo 3 palabras
                break
    
    if valid_words:
        return ' '.join(valid_words)[:30]
    
    return None

def extract_unit_price_from_text(text):
    """Extrae precio unitario del texto del producto"""
    if not text:
        return None
    
    # Buscar patrones de precios en el texto
    price_patterns = [
        r'(\d{1,5},\d{3})',     # Formato: 12,345
        r'(\d{1,5}\.\d{3})',    # Formato: 12.345  
        r'(\d{1,5})',           # Número simple
    ]
    
    prices = []
    for pattern in price_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            price = clean_amount(match)
            if price and 100 <= price <= 100000:  # Rango razonable para productos
                prices.append(price)
    
    # Retornar el precio más probable (ni muy alto ni muy bajo)
    if prices:
        prices.sort()
        # Tomar el precio del medio o el más común
        return prices[len(prices)//2] if len(prices) > 1 else prices[0]
    
    return None

def process_invoice_products(image_path):
    """Procesa factura y extrae productos con precios unitarios"""
    
    if not setup_environment():
        return None
    
    if not os.path.exists(image_path):
        print(f"❌ ERROR: El archivo {image_path} no existe")
        return None
    
    try:
        # Configuración
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"🔗 Conectando con Document AI...")
        print(f"📄 Procesando: {image_path}")
        
        # Cliente Document AI
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        # Leer archivo
        with open(image_path, "rb") as image:
            image_content = image.read()
        
        # Detectar tipo MIME
        mime_type = "image/jpeg"
        if image_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif image_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("⏳ Procesando...")
        result = client.process_document(request=request)
        document = result.document
        
        print("✅ ¡Procesado exitosamente!")
        
        # Extraer productos con precios
        invoice_data = extract_products_with_prices(document)
        
        # Mostrar resultados
        print(f"\n📊 PRODUCTOS EXTRAÍDOS:")
        print("=" * 70)
        print(f"🏪 Establecimiento: {invoice_data['establecimiento']}")
        print(f"📅 Fecha cargue: {invoice_data['fecha_cargue']}")
        print(f"📦 Productos encontrados: {len(invoice_data['productos'])}")
        
        print(f"\n📝 LISTA DE PRODUCTOS:")
        print("-" * 70)
        print(f"{'#':<3} {'Código':<13} {'Producto':<25} {'Precio':<10}")
        print("-" * 70)
        
        for i, producto in enumerate(invoice_data['productos'][:20], 1):  # Mostrar 20 productos
            codigo = producto['codigo'] if producto['codigo'] else "Sin código"
            nombre = producto['nombre'] if producto['nombre'] else "Sin nombre"
            precio = f"${producto['valor']:.0f}" if producto['valor'] else "Sin precio"
            
            print(f"{i:<3} {codigo:<13} {nombre:<25} {precio:<10}")
        
        if len(invoice_data['productos']) > 20:
            print(f"... y {len(invoice_data['productos']) - 20} productos más")
        
        # Estadísticas
        productos_con_precio = [p for p in invoice_data['productos'] if p['valor']]
        productos_con_codigo = [p for p in invoice_data['productos'] if p['codigo']]
        
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"✅ Con código: {len(productos_con_codigo)}")
        print(f"💰 Con precio: {len(productos_con_precio)}")
        print(f"📋 Completos (código + nombre + precio): {len([p for p in invoice_data['productos'] if p['codigo'] and p['nombre'] and p['valor']])}")
        
        print(f"\n📄 JSON PARA BASE DE DATOS:")
        print(json.dumps(invoice_data, indent=2, ensure_ascii=False))
        
        return invoice_data
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return None

def extract_products_with_prices(document):
    """Extrae productos con sus precios unitarios"""
    
    raw_text = document.text
    
    # Datos principales
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": []
    }
    
    # Procesar cada line_item
    for entity in document.entities:
        if entity.type_ == "line_item" and entity.confidence > 0.7:
            
            raw_item_text = entity.mention_text
            product_code = extract_product_code(raw_item_text)
            product_name = clean_product_name(raw_item_text, product_code)
            
            # Buscar precio unitario
            unit_price = None
            
            # Primero buscar en las propiedades del item
            for prop in entity.properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        break
            
            # Si no encontramos precio en propiedades, buscar en el texto
            if not unit_price:
                unit_price = extract_unit_price_from_text(raw_item_text)
            
            # Solo agregar productos que tengan al menos código O nombre
            if product_code or (product_name and len(product_name) > 2):
                producto = {
                    "codigo": product_code,
                    "nombre": product_name,
                    "valor": unit_price
                }
                invoice_data["productos"].append(producto)
    
    return invoice_data

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ Uso: python simple_invoice_parser.py <ruta_imagen>")
        print("📄 Ejemplo: python simple_invoice_parser.py factura_test.jpg")
        sys.exit(1)
    
    image_path = sys.argv[1]
    result = process_invoice_products(image_path)
    
    if result:
        productos_completos = [p for p in result['productos'] if p['codigo'] and p['nombre'] and p['valor']]
        print(f"\n🎉 ¡Extracción completada!")
        print(f"📊 {len(productos_completos)} productos completos listos para BD")
    else:
        print("\n❌ Error en el procesamiento")