import os
import re
import json
import tempfile
import time
import shutil
from datetime import datetime
from PIL import Image, ImageEnhance, ImageFilter
import traceback

# Verificar disponibilidad de Tesseract
TESSERACT_AVAILABLE = False
try:
    import pytesseract
    if shutil.which("tesseract"):
        TESSERACT_AVAILABLE = True
        print("✅ Tesseract OCR disponible")
    else:
        print("⚠️ Tesseract no instalado en sistema")
except ImportError:
    print("⚠️ pytesseract no instalado")

# Document AI como fallback
try:
    from google.cloud import documentai
    DOCUMENT_AI_AVAILABLE = True
    print("✅ Document AI disponible como fallback")
except:
    DOCUMENT_AI_AVAILABLE = False
    print("⚠️ Document AI no disponible")

# ========================================
# PREPROCESAMIENTO DE IMAGEN
# ========================================

def preprocess_for_ocr(image_path):
    """Preprocesamiento agresivo para maximizar precisión de OCR"""
    try:
        img = Image.open(image_path)
        
        # Convertir a escala de grises
        if img.mode != 'L':
            img = img.convert('L')
        
        # Redimensionar si es muy pequeña (mínimo 2000px de ancho)
        width, height = img.size
        if width < 2000:
            scale = 2000 / width
            new_size = (int(width * scale), int(height * scale))
            img = img.resize(new_size, Image.Resampling.LANCZOS)
        
        # Aumentar contraste agresivamente
        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(3.0)
        
        # Aumentar nitidez
        enhancer = ImageEnhance.Sharpness(img)
        img = enhancer.enhance(2.0)
        
        # Reducir ruido
        img = img.filter(ImageFilter.MedianFilter(size=3))
        
        # Binarización (umbral adaptativo)
        img = img.point(lambda x: 255 if x > 140 else 0, mode='1')
        
        # Guardar procesada
        processed_path = image_path.replace('.jpg', '_proc.png').replace('.png', '_proc.png')
        img.save(processed_path, 'PNG')
        
        print(f"✓ Imagen preprocesada: {width}x{height} -> {img.size[0]}x{img.size[1]}")
        return processed_path
        
    except Exception as e:
        print(f"⚠️ Error en preprocesamiento: {e}")
        return image_path

# ========================================
# UTILIDADES
# ========================================

def clean_price(text):
    """Extrae precio de texto. Ej: '16,390 N' -> 16390"""
    if not text:
        return None
    
    match = re.search(r'(\d{1,3}(?:[,\.]\d{3})+|\d{3,7})', str(text))
    if not match:
        return None
    
    cleaned = match.group(1).replace(",", "").replace(".", "")
    
    try:
        amount = int(cleaned)
        if 50 < amount < 10000000:
            return amount
    except:
        pass
    
    return None

def is_valid_ean(code):
    """Valida código EAN 8-13 dígitos"""
    if not code:
        return False
    code = str(code).strip()
    return code.isdigit() and 8 <= len(code) <= 13

def clean_description(text):
    """Limpia nombre de producto"""
    if not text:
        return None
    
    # Remover códigos al inicio
    text = re.sub(r'^\d{6,13}\s*', '', text)
    
    # Remover números sueltos al inicio
    text = re.sub(r'^\d{1,3}\s+', '', text)
    
    # Remover precios
    text = re.sub(r'\d{1,3}[,\.]\d{3}', '', text)
    
    # Limpiar símbolos
    text = re.sub(r'[;:]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Validar que tenga contenido real
    words = re.findall(r'[A-Za-zÀ-ÿ]{3,}', text)
    if len(words) < 2:
        return None
    
    return text[:70]

# ========================================
# EXTRACCIÓN CON TESSERACT
# ========================================

def extract_text_tesseract(image_path):
    """Extrae texto usando Tesseract con múltiples configuraciones"""
    if not TESSERACT_AVAILABLE:
        return ""
    
    processed_path = preprocess_for_ocr(image_path)
    
    try:
        img = Image.open(processed_path)
        
        # Configuración 1: PSM 6 (bloque uniforme de texto)
        config1 = '--psm 6 --oem 3 -l spa'
        text1 = pytesseract.image_to_string(img, config=config1)
        
        # Configuración 2: PSM 4 (columna de texto)
        config2 = '--psm 4 --oem 3 -l spa'
        text2 = pytesseract.image_to_string(img, config=config2)
        
        # Combinar resultados
        combined = text1 + "\n" + text2
        
        # Limpiar archivo temporal
        if processed_path != image_path:
            try:
                os.remove(processed_path)
            except:
                pass
        
        print(f"✓ Texto extraído: {len(combined)} caracteres")
        return combined
        
    except Exception as e:
        print(f"⚠️ Error en Tesseract: {e}")
        return ""

# ========================================
# PARSER POR COLUMNAS
# ========================================

def detect_price_column(lines):
    """Detecta posición de columna de precios"""
    for line in lines[:40]:
        upper = line.upper()
        if 'CODIGO' in upper and 'TOTAL' in upper:
            idx = upper.rfind('TOTAL')
            if idx > 30:
                return idx
    return 55

def is_noise_line(text):
    """Detecta líneas que no son productos"""
    if not text or len(text.strip()) < 10:
        return True
    
    upper = text.upper()
    noise = [
        'RESOLUCION', 'DIAN', 'RESPONSABLE', 'IVA', 'AGENTE',
        'RETENEDOR', '****', '====', '----', 'SUBTOTAL',
        'ITEMS COMPRADOS', 'NRO', 'CUENTA', 'TARJ', 'VISA',
        'CUOTAS', 'CAMBIO', 'RESUMEN', 'TOTAL A PAGAR'
    ]
    
    return any(kw in upper for kw in noise)

def parse_products_from_text(text):
    """Parser optimizado por columnas"""
    if not text:
        return []
    
    lines = text.split('\n')
    price_col = detect_price_column(lines)
    
    print(f"Analizando {len(lines)} líneas (columna precio: {price_col})...")
    
    products = []
    seen_codes = set()
    
    for i, line in enumerate(lines):
        if len(line) < 20:
            continue
        
        if is_noise_line(line):
            continue
        
        # Extraer por columnas
        code_zone = line[:18].strip()
        desc_zone = line[18:price_col].strip()
        price_zone = line[price_col:].strip()
        
        # Buscar código EAN
        code_match = re.search(r'\b(\d{8,13})\b', code_zone)
        if not code_match:
            continue
        
        code = code_match.group(1)
        
        if code in seen_codes:
            continue
        
        # Limpiar descripción
        desc = clean_description(desc_zone)
        if not desc:
            continue
        
        # Extraer precio
        price_match = re.search(r'(\d{1,3}[,\.]\d{3})', price_zone)
        price = clean_price(price_match.group(1)) if price_match else 0
        
        products.append({
            "codigo": code,
            "nombre": desc,
            "valor": price
        })
        
        seen_codes.add(code)
    
    print(f"✓ Productos extraídos: {len(products)}")
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
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).split('\n')[0].strip()
            return re.sub(r'\s+', ' ', name)[:50]
    
    return "Establecimiento no identificado"

def extract_total(text):
    """Extrae total de factura"""
    lines = text.split('\n')
    
    for line in reversed(lines[-50:]):
        upper = line.upper()
        if 'SUBTOTAL/TOTAL' in upper or (upper.strip().startswith('TOTAL') and len(upper) < 50):
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
    """Procesamiento principal con Tesseract"""
    inicio = time.time()
    
    try:
        print("\n" + "="*70)
        print("PROCESAMIENTO CON TESSERACT OCR")
        print("="*70)
        
        # Extraer texto con Tesseract
        print("\n[1/3] Extrayendo texto con Tesseract...")
        text = extract_text_tesseract(file_path)
        
        if not text or len(text) < 100:
            print("⚠️ Poco texto extraído, intentando con Document AI...")
            # Fallback a Document AI si está disponible
            if DOCUMENT_AI_AVAILABLE:
                return process_with_document_ai(file_path)
            else:
                raise Exception("No se pudo extraer texto de la imagen")
        
        # Extraer metadata
        print("\n[2/3] Extrayendo información básica...")
        establecimiento = extract_vendor(text)
        total = extract_total(text)
        
        # Parsear productos
        print("\n[3/3] Parseando productos...")
        productos = parse_products_from_text(text)
        
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
                "metodo": "tesseract_optimized",
                "productos_detectados": len(productos),
                "tiempo_segundos": tiempo
            }
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None

def process_with_document_ai(file_path):
    """Fallback: usar Document AI si Tesseract falla"""
    print("Usando Document AI como fallback...")
    # Aquí iría tu código de Document AI actual
    return None

# Alias
process_invoice_complete = process_invoice_products
