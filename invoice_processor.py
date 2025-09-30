import os
import re
import json
import tempfile
import time
from datetime import datetime
import traceback

# Document AI
try:
    from google.cloud import documentai
    DOCUMENT_AI_AVAILABLE = True
    print("✅ Document AI disponible")
except:
    DOCUMENT_AI_AVAILABLE = False
    print("❌ Document AI no disponible")

# ========================================
# CONFIGURACIÓN
# ========================================

def setup_document_ai():
    """Configura credenciales de Document AI"""
    required_vars = [
        "GCP_PROJECT_ID",
        "DOC_AI_LOCATION", 
        "DOC_AI_PROCESSOR_ID",
        "GOOGLE_APPLICATION_CREDENTIALS_JSON"
    ]
    
    for var in required_vars:
        if not os.environ.get(var):
            raise Exception(f"Variable requerida: {var}")
    
    credentials_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    try:
        json.loads(credentials_json)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(credentials_json)
            temp_path = f.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_path
        return True
    except:
        raise Exception("Credenciales JSON inválidas")

# ========================================
# UTILIDADES
# ========================================

def clean_price(text):
    """Convierte texto de precio a entero"""
    if not text:
        return None
    
    match = re.search(r'(\d{1,3}(?:[,\.]\d{3})+|\d{3,7})', str(text))
    if not match:
        return None
    
    cleaned = match.group(1).replace(",", "").replace(".", "")
    
    try:
        amount = int(cleaned)
        if 100 < amount < 10000000:
            return amount
    except:
        pass
    
    return None

def is_valid_ean(code):
    """Valida código EAN"""
    if not code:
        return False
    code = str(code).strip()
    return code.isdigit() and 8 <= len(code) <= 13

# ========================================
# EXTRACCIÓN MEJORADA DE DOCUMENT AI
# ========================================

def extract_products_from_entities(document):
    """Extrae productos directamente de las entidades de Document AI"""
    products = []
    seen_codes = set()
    
    # Obtener line_items
    line_items = [e for e in document.entities 
                  if e.type_ == "line_item" and e.confidence > 0.25]
    
    print(f"Entidades line_item encontradas: {len(line_items)}")
    
    for entity in line_items:
        text = entity.mention_text.strip()
        
        # Buscar código EAN en el texto
        ean_match = re.search(r'\b(\d{8,13})\b', text)
        if not ean_match:
            continue
        
        code = ean_match.group(1)
        
        if not is_valid_ean(code) or code in seen_codes:
            continue
        
        # Extraer nombre (después del código)
        name = re.sub(r'^\d{8,13}\s*', '', text)
        name = re.sub(r'\d{1,3}[,\.]\d{3}.*$', '', name)  # Remover precio
        name = re.sub(r'\s+', ' ', name).strip()[:60]
        
        if not name or len(name) < 3:
            name = "Producto sin descripción"
        
        # Extraer precio de propiedades
        price = 0
        for prop in entity.properties:
            if prop.type_ == "line_item/amount" and prop.confidence > 0.2:
                price = clean_price(prop.mention_text) or 0
                break
        
        products.append({
            "codigo": code,
            "nombre": name,
            "valor": price
        })
        
        seen_codes.add(code)
    
    return products

def parse_text_line_by_line(text):
    """Parser línea por línea del texto crudo"""
    lines = text.split('\n')
    products = []
    seen_codes = set()
    
    print(f"Analizando {len(lines)} líneas de texto...")
    
    for line in lines:
        line = line.strip()
        
        if len(line) < 15:
            continue
        
        # Saltar líneas de encabezado/total
        upper = line.upper()
        if any(x in upper for x in ['RESOLUCION', 'TOTAL', 'IVA', 'SUBTOTAL', '****', 'ITEMS']):
            continue
        
        # Buscar patrón: CÓDIGO (inicio) + texto + precio (final)
        # Ej: "7702007084542 Chocolate CORONA 16,390 N"
        match = re.match(r'^(\d{8,13})\s+(.+?)\s+(\d{1,3}[,\.]\d{3})\s*[A-Z]?\s*$', line)
        
        if not match:
            continue
        
        code, desc, price_str = match.groups()
        
        if not is_valid_ean(code) or code in seen_codes:
            continue
        
        # Limpiar descripción
        desc = re.sub(r'\s+', ' ', desc).strip()[:60]
        
        if len(desc) < 3:
            continue
        
        price = clean_price(price_str) or 0
        
        products.append({
            "codigo": code,
            "nombre": desc,
            "valor": price
        })
        
        seen_codes.add(code)
    
    return products

# ========================================
# EXTRACCIÓN DE METADATA
# ========================================

def extract_vendor(text):
    """Extrae establecimiento"""
    patterns = [
        r'(JUMBO\s+[A-Z\s]+)',
        r'(EXITO\s+[A-Z\s]+)',
        r'(CARULLA[^\n]*)',
        r'(OLIMPICA[^\n]*)',
        r'(D1[^\n]*)',
        r'(CRUZ\s+VERDE[^\n]*)',
        r'(ALKOSTO[^\n]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).split('\n')[0].strip()
            return re.sub(r'\s+', ' ', name)[:50]
    
    return "Establecimiento no identificado"

def extract_total(text):
    """Extrae total"""
    lines = text.split('\n')
    
    for line in reversed(lines[-60:]):
        upper = line.upper()
        if 'SUBTOTAL/TOTAL' in upper or (upper.strip().startswith('TOTAL') and ':' not in upper):
            numbers = re.findall(r'\d{1,3}(?:[,\.]\d{3})+', line)
            if numbers:
                total = clean_price(numbers[-1])
                if total and total > 5000:
                    return total
    
    return None

# ========================================
# PROCESAMIENTO PRINCIPAL
# ========================================

def process_invoice_products(file_path):
    """Procesamiento principal optimizado con Document AI"""
    inicio = time.time()
    
    try:
        print("\n" + "="*70)
        print("PROCESAMIENTO DE FACTURA - DOCUMENT AI OPTIMIZADO")
        print("="*70)
        
        if not DOCUMENT_AI_AVAILABLE:
            raise Exception("Document AI no disponible")
        
        setup_document_ai()
        
        # Llamar a Document AI
        print("\n[1/3] Procesando con Document AI...")
        from google.cloud import documentai
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        
        with open(file_path, "rb") as f:
            content = f.read()
        
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        
        result = client.process_document(
            request=documentai.ProcessRequest(
                name=name,
                raw_document=documentai.RawDocument(
                    content=content,
                    mime_type=mime_type
                )
            )
        )
        
        text = result.document.text
        
        # Extraer metadata
        print("\n[2/3] Extrayendo metadata...")
        establecimiento = extract_vendor(text)
        total = extract_total(text)
        
        # Extraer productos con DOBLE estrategia
        print("\n[3/3] Extrayendo productos (estrategia dual)...")
        
        # Estrategia 1: Entidades de Document AI
        productos_entities = extract_products_from_entities(result.document)
        print(f"  • Desde entidades: {len(productos_entities)}")
        
        # Estrategia 2: Texto línea por línea
        productos_text = parse_text_line_by_line(text)
        print(f"  • Desde texto: {len(productos_text)}")
        
        # Combinar sin duplicados
        all_codes = set()
        productos = []
        
        for p in productos_entities + productos_text:
            code = p['codigo']
            if code not in all_codes:
                productos.append(p)
                all_codes.add(code)
        
        tiempo = int(time.time() - inicio)
        
        print("\n" + "="*70)
        print("RESULTADO FINAL")
        print("="*70)
        print(f"Establecimiento: {establecimiento}")
        print(f"Total: ${total:,}" if total else "Total: No detectado")
        print(f"Productos: {len(productos)}")
        print(f"Tiempo: {tiempo}s")
        print("="*70 + "\n")
        
        if productos:
            print("Primeros 5 productos:")
            for p in productos[:5]:
                precio = f"${p['valor']:,}" if p['valor'] else "Sin precio"
                print(f"  {p['codigo']}: {p['nombre']} - {precio}")
        
        return {
            "establecimiento": establecimiento,
            "total": total,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos,
            "metadatos": {
                "metodo": "document_ai_dual_strategy",
                "productos_entities": len(productos_entities),
                "productos_text": len(productos_text),
                "productos_totales": len(productos),
                "tiempo_segundos": tiempo
            }
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None

# Alias
process_invoice_complete = process_invoice_products
