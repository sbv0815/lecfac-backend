import os
import re
import json
import tempfile
import time
import hashlib
from datetime import datetime
from google.cloud import documentai
import traceback

# Tesseract opcional (generalmente NO disponible en Render)
try:
    import pytesseract
    import shutil
    if shutil.which("tesseract"):
        TESSERACT_AVAILABLE = True
        print("✅ Tesseract OCR disponible")
    else:
        TESSERACT_AVAILABLE = False
        print("⚠️ Tesseract no instalado - solo Document AI")
except ImportError:
    TESSERACT_AVAILABLE = False
    print("⚠️ pytesseract no disponible - solo Document AI")

# ========================================
# CONFIGURACIÓN
# ========================================

def setup_environment():
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
        raise Exception("JSON inválido")

# ========================================
# UTILIDADES DE LIMPIEZA
# ========================================

def clean_price(text):
    """Convierte string de precio a entero. Ej: '16,390' -> 16390"""
    if not text:
        return None
    
    # Remover símbolos y espacios
    cleaned = str(text).replace("$", "").replace(" ", "")
    cleaned = cleaned.replace(".", "").replace(",", "")
    
    # Solo dígitos
    cleaned = re.sub(r"[^\d-]", "", cleaned)
    
    if not cleaned or cleaned == "-":
        return None
    
    try:
        amount = int(cleaned)
        # Validar rango razonable
        if -50000 < amount < 10000000:
            return abs(amount)  # Tomar valor absoluto
    except:
        pass
    
    return None

def is_valid_ean(code):
    """Valida que sea un código EAN de 8-13 dígitos"""
    if not code:
        return False
    code = str(code).strip()
    return code.isdigit() and 8 <= len(code) <= 13

def is_noise_line(text):
    """Detecta líneas que NO son productos"""
    if not text:
        return True
    
    text_upper = text.upper()
    
    # Encabezados y separadores
    noise_patterns = [
        "RESOLUCION DIAN",
        "RESPONSABLE DE IVA",
        "AGENTE RETENEDOR",
        "****",
        "====",
        "----",
        "SUBTOTAL",
        "TOTAL A PAGAR",
        "NRO. CUENTA",
        "TARJ CRE/DEB",
        "VISA",
        "ITEMS COMPRADOS",
        "RESUMEN DE IVA",
        "IVA-TARIFA",
        "CUOTAS",
        "CAMBIO"
    ]
    
    for pattern in noise_patterns:
        if pattern in text_upper:
            return True
    
    # Líneas de descuento (empiezan con -)
    if text.strip().startswith("-"):
        return True
    
    # Líneas muy cortas
    if len(text.strip()) < 8:
        return True
    
    return False

def clean_product_name(text):
    """Limpia el nombre del producto"""
    if not text:
        return None
    
    # Remover prefijos comunes
    text = re.sub(r'^\d+DF\.[A-Z]{2}\.', '', text)
    text = re.sub(r'^DF\.[A-Z]{2}\.', '', text)
    
    # Remover números al inicio (códigos mal parseados)
    text = re.sub(r'^\d{3,}\s*', '', text)
    
    # Remover precios al final
    text = re.sub(r'\d{1,3}[,\.]\d{3}.*$', '', text)
    
    # Normalizar espacios
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Validar que tenga al menos 2 palabras reales
    words = re.findall(r'[A-Za-zÀ-ÿ]{2,}', text)
    if len(words) < 2:
        return None
    
    return text[:80] if text else None

# ========================================
# EXTRACCIÓN DE INFORMACIÓN
# ========================================

def extract_vendor_name(text):
    """Extrae el nombre del establecimiento"""
    patterns = [
        r"(JUMBO[^\n]*)",
        r"(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)",
        r"(CARULLA[^\n]*)",
        r"(OLIMPICA[^\n]*)",
        r"(D1[^\n]*)",
        r"(ALKOSTO[^\n]*)",
        r"(CRUZ\s+VERDE[^\n]*)",
        r"(LA\s+REBAJA[^\n]*)",
        r"(CAFAM[^\n]*)",
        r"(MAKRO[^\n]*)",
        r"(ARA[^\n]*)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return re.sub(r'\s+', ' ', match.group(1).strip())[:50]
    
    return "Establecimiento no identificado"

def extract_total(text):
    """Extrae el total de la factura"""
    lines = text.split('\n')
    
    # Buscar en las últimas 50 líneas
    for line in reversed(lines[-50:]):
        upper = line.upper()
        if "SUBTOTAL/TOTAL" in upper or (upper.startswith("TOTAL") and len(upper) < 40):
            # Extraer número
            numbers = re.findall(r'\d{1,3}(?:[,\.]\d{3})+', line)
            if numbers:
                total = clean_price(numbers[-1])  # Último número de la línea
                if total and total > 1000:
                    return total
    
    return None

# ========================================
# PARSER PRINCIPAL
# ========================================

def parse_products_from_text(text):
    """
    Parser conservador: solo extrae líneas con estructura clara.
    Formato esperado: [CODIGO] NOMBRE PRECIO [LETRA]
    """
    if not text:
        return []
    
    lines = text.split('\n')
    products = []
    seen_codes = set()
    
    print(f"\n📄 Analizando {len(lines)} líneas...")
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Filtrar ruido
        if is_noise_line(line):
            continue
        
        # Buscar patrón: código (6-13 dígitos) + texto + precio
        # Ejemplos:
        # 7702007084542 Chocolate CO 16,390 N
        # 2905669005107 Molida de re 18,539 E
        pattern = r'(\d{6,13})\s+(.+?)\s+(\d{1,3}[,\.]\d{3})\s*[A-Z]?\s*$'
        match = re.search(pattern, line)
        
        if match:
            code = match.group(1)
            name_raw = match.group(2)
            price_raw = match.group(3)
            
            # Validar código EAN
            if not is_valid_ean(code):
                continue
            
            # Evitar duplicados
            if code in seen_codes:
                continue
            
            # Limpiar nombre
            name = clean_product_name(name_raw)
            if not name:
                continue
            
            # Limpiar precio
            price = clean_price(price_raw)
            if not price:
                price = 0  # Permitir precio 0 si todo lo demás es válido
            
            products.append({
                "codigo": code,
                "nombre": name,
                "valor": price,
                "fuente": "text_parser",
                "linea": i + 1
            })
            
            seen_codes.add(code)
    
    print(f"✓ Productos extraídos: {len(products)}")
    return products

# ========================================
# DOCUMENT AI
# ========================================

def extract_products_document_ai(document):
    """Extrae productos de Document AI con validaciones estrictas"""
    products = []
    
    # Primero parsear el texto crudo (más confiable)
    text_products = parse_products_from_text(document.text or "")
    
    # Agregar entidades de Document AI como complemento
    line_items = [e for e in document.entities 
                  if e.type_ == "line_item" and e.confidence > 0.3]
    
    seen_codes = {p["codigo"] for p in text_products}
    
    for entity in line_items:
        text = entity.mention_text.strip()
        
        if is_noise_line(text):
            continue
        
        # Buscar código EAN en el texto
        ean_match = re.search(r'\b(\d{8,13})\b', text)
        code = ean_match.group(1) if ean_match else None
        
        if not code or not is_valid_ean(code):
            continue
        
        if code in seen_codes:
            continue
        
        # Extraer precio
        price = 0
        for prop in entity.properties:
            if prop.type_ == "line_item/amount":
                price = clean_price(prop.mention_text) or 0
                break
        
        # Limpiar nombre
        name = clean_product_name(text)
        if not name:
            continue
        
        products.append({
            "codigo": code,
            "nombre": name,
            "valor": price,
            "fuente": "document_ai"
        })
        
        seen_codes.add(code)
    
    # Combinar ambas fuentes
    all_products = text_products + products
    
    return all_products

# ========================================
# PROCESAMIENTO PRINCIPAL
# ========================================

def process_invoice_complete(file_path):
    """Procesamiento completo de factura"""
    inicio = time.time()
    
    try:
        print("\n" + "="*70)
        print("🔍 PROCESAMIENTO DE FACTURA - MODO PRECISO")
        print("="*70)
        
        setup_environment()
        
        # Document AI
        print("\n[1/2] 📄 Procesando con Document AI...")
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        
        with open(file_path, "rb") as f:
            content = f.read()
        
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        result = client.process_document(
            request=documentai.ProcessRequest(
                name=name,
                raw_document=documentai.RawDocument(
                    content=content,
                    mime_type=mime_type
                )
            )
        )
        
        raw_text = result.document.text
        
        # Extraer información básica
        establecimiento = extract_vendor_name(raw_text)
        total = extract_total(raw_text)
        
        # Extraer productos
        print("\n[2/2] 📦 Extrayendo productos...")
        productos = extract_products_document_ai(result.document)
        
        tiempo_total = int(time.time() - inicio)
        
        print("\n" + "="*70)
        print("✅ RESULTADO FINAL")
        print("="*70)
        print(f"📍 Establecimiento: {establecimiento}")
        print(f"💰 Total: ${total:,}" if total else "💰 Total: No detectado")
        print(f"📦 Productos detectados: {len(productos)}")
        print(f"⏱️  Tiempo: {tiempo_total}s")
        print("="*70 + "\n")
        
        # Mostrar primeros 5 productos
        if productos:
            print("Primeros 5 productos:")
            for p in productos[:5]:
                print(f"  • {p['codigo']}: {p['nombre']} - ${p['valor']:,}")
        
        return {
            "establecimiento": establecimiento,
            "total": total,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos,
            "metadatos": {
                "metodo": "document_ai_precise",
                "productos_detectados": len(productos),
                "tiempo_segundos": tiempo_total
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()
        return None

# Mantener compatibilidad
def process_invoice_products(file_path):
    """Alias para compatibilidad"""
    return process_invoice_complete(file_path)
def process_invoice_products(file_path):
    """Versión legacy - redirige al nuevo procesador"""
    return process_invoice_complete(file_path)
