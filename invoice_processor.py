import os
import re
import json
import tempfile
import time
from datetime import datetime
from google.cloud import documentai
import traceback
from PIL import Image, ImageEnhance, ImageFilter
import cv2
import numpy as np

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

def preprocess_image(image_path):
    """Preprocesamiento agresivo para mejorar OCR"""
    try:
        # Leer imagen con OpenCV
        img = cv2.imread(image_path)
        
        # Convertir a escala de grises
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        
        # Aumentar contraste con CLAHE
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        enhanced = clahe.apply(gray)
        
        # Reducir ruido
        denoised = cv2.fastNlMeansDenoising(enhanced, None, 10, 7, 21)
        
        # Binarización adaptativa
        binary = cv2.adaptiveThreshold(
            denoised, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
            cv2.THRESH_BINARY, 11, 2
        )
        
        # Guardar imagen procesada
        processed_path = image_path.replace('.jpg', '_processed.jpg').replace('.png', '_processed.png')
        cv2.imwrite(processed_path, binary)
        
        return processed_path
    except Exception as e:
        print(f"⚠️ Error en preprocesamiento: {e}")
        return image_path

def clean_amount(amount_str):
    """Limpia y convierte montos a enteros"""
    if not amount_str:
        return None
    amount_str = str(amount_str).strip()
    
    # Remover símbolos y espacios
    cleaned = re.sub(r'[$\s]', '', amount_str)
    cleaned = cleaned.replace('.', '').replace(',', '')
    cleaned = re.sub(r'[^\d]', '', cleaned)
    
    if not cleaned:
        return None
    
    try:
        amount = int(cleaned)
        # Validar rango razonable (entre $10 y $10M)
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
        r'(JUMBO[^\n]*)', 
        r'(OLIMPICA[^\n]*)', 
        r'(ALKOSTO[^\n]*)', 
        r'(CRUZ\s+VERDE[^\n]*)', 
        r'(LA\s+REBAJA[^\n]*)', 
        r'(CAFAM[^\n]*)',
        r'(MAKRO[^\n]*)',
        r'(METRO[^\n]*)',
        r'(ARA[^\n]*)',
        r'(D1[^\n]*)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            return re.sub(r'\s+', ' ', match.group(1).strip())[:50]
    
    # Fallback: primera línea con mayúsculas
    lines = raw_text.split('\n')[:15]
    for line in lines:
        line = line.strip()
        if 5 <= len(line) <= 50 and line.isupper():
            if not re.search(r'(CRA|CALLE|NIT|RUT|\d{3,})', line):
                return line
    
    return "Establecimiento no identificado"

def extract_total_invoice(raw_text):
    """Extrae el total de la factura"""
    patterns = [
        r'TOTAL\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)',
        r'TOTAL\s+A\s+PAGAR\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)',
        r'VALOR\s+TOTAL\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            total = clean_amount(match.group(1))
            if total and total > 1000:  # Validar que sea razonable
                return total
    
    return None

def extract_product_code(text):
    """Extrae código de producto (más flexible)"""
    if not text:
        return None
    
    # Patrones de códigos EAN/PLU (de 6 a 13 dígitos)
    patterns = [
        r'^\s*(\d{13})\s',  # EAN-13 al inicio
        r'^\s*(\d{12})\s',  # EAN-12
        r'^\s*(\d{10})\s',  # EAN-10
        r'^\s*(\d{8})\s',   # EAN-8
        r'^\s*(\d{7})\s',   # PLU 7 dígitos
        r'^\s*(\d{6})\s',   # PLU 6 dígitos
        r'\b(\d{13})\b',    # EAN-13 en cualquier parte
        r'\b(\d{10})\b',    # EAN-10 en cualquier parte
        r'\b(\d{8})\b',     # EAN-8 en cualquier parte
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            code = match.group(1)
            if 6 <= len(code) <= 13:
                return code
    
    return None

def clean_product_name(text, product_code=None):
    """Limpia el nombre del producto"""
    if not text:
        return None
    
    # Remover código si existe
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    
    # Remover prefijos comunes
    text = re.sub(r'^\d+DF\.[A-Z]{2}\.', '', text)
    text = re.sub(r'^DF\.[A-Z]{2}\.', '', text)
    
    # Remover precios y números al final
    text = re.sub(r'\d+[,\.]\d{3}.*$', '', text)
    text = re.sub(r'\$\d+.*$', '', text)
    
    # Limpiar espacios
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Mantener solo palabras con letras
    words = [w for w in text.split() 
             if re.search(r'[A-Za-zÀ-ÿ]{2,}', w) and len(w) >= 2]
    
    return ' '.join(words[:8])[:80] if words else None

def extract_products_document_ai(document):
    """Extrae productos usando Document AI"""
    line_items = [e for e in document.entities 
                  if e.type_ == "line_item" and e.confidence > 0.3]  # Bajado de 0.35
    
    productos = []
    i = 0
    
    while i < len(line_items):
        entity = line_items[i]
        product_code = extract_product_code(entity.mention_text)
        product_name = clean_product_name(entity.mention_text, product_code)
        unit_price = None
        
        # Buscar precio en propiedades
        for prop in entity.properties:
            if prop.type_ == "line_item/amount" and prop.confidence > 0.25:  # Bajado de 0.3
                unit_price = clean_amount(prop.mention_text)
                if unit_price:
                    break
        
        # Buscar en siguiente línea si no encontró precio
        if not unit_price and i + 1 < len(line_items):
            for prop in line_items[i + 1].properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        i += 1
                        break
        
        # Aceptar productos con código O nombre válido
        if product_code or (product_name and len(product_name) > 2):
            productos.append({
                "codigo": product_code,
                "nombre": product_name or "Sin descripción",
                "valor": unit_price or 0,
                "fuente": "document_ai"
            })
        
        i += 1
    
    return productos

def extract_products_tesseract_aggressive(image_path):
    """Extracción agresiva con Tesseract - múltiples pasadas"""
    if not TESSERACT_AVAILABLE:
        return []
    
    productos = []
    
    try:
        # Preprocesar imagen
        processed_path = preprocess_image(image_path)
        img = Image.open(processed_path)
        
        # PASADA 1: PSM 6 (bloque uniforme)
        texto_psm6 = pytesseract.image_to_string(
            img, lang='spa', 
            config='--psm 6 --oem 3'
        )
        
        # PASADA 2: PSM 4 (columna de texto)
        texto_psm4 = pytesseract.image_to_string(
            img, lang='spa', 
            config='--psm 4 --oem 3'
        )
        
        # Combinar textos
        texto_completo = texto_psm6 + "\n" + texto_psm4
        
        # Patrones más flexibles
        patterns = [
            # Patrón principal: Código + Nombre + Precio
            r'(\d{6,13})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s/\-\.\(\)]{2,80}?)\s+.*?(\d{1,3}[,\.]\d{3})',
            
            # Patrón alternativo: Solo código visible al inicio de línea
            r'^(\d{6,13})\s+(.+?)\s+(\d{1,3}[,\.]\d{3})',
            
            # Patrón PLU corto
            r'(\d{3,6})\s+([A-ZÀ-Ÿ][A-ZÀ-Ÿ\s/\-\.]{3,50}?)\s+.*?(\d{1,3}[,\.]\d{3})',
        ]
        
        codigos_vistos = set()
        
        for pattern in patterns:
            matches = re.finditer(pattern, texto_completo, re.MULTILINE)
            for match in matches:
                codigo = match.group(1).strip()
                nombre = match.group(2).strip()
                precio = clean_amount(match.group(3))
                
                # Validaciones
                if codigo in codigos_vistos:
                    continue
                
                # Limpiar nombre
                nombre = re.sub(r'\d{4,}', '', nombre)  # Remover números largos
                nombre = re.sub(r'\s+', ' ', nombre).strip()
                
                # Validar calidad
                if (len(codigo) >= 3 and 
                    len(nombre) > 3 and 
                    precio and 
                    precio > 100):  # Precio mínimo razonable
                    
                    productos.append({
                        "codigo": codigo,
                        "nombre": nombre[:80],
                        "valor": precio,
                        "fuente": "tesseract"
                    })
                    codigos_vistos.add(codigo)
        
        # Limpiar archivo temporal
        if processed_path != image_path:
            try:
                os.remove(processed_path)
            except:
                pass
        
        return productos
    
    except Exception as e:
        print(f"⚠️ Error Tesseract: {e}")
        traceback.print_exc()
        return []

def combinar_y_deduplicar(productos_ai, productos_tesseract):
    """Combina resultados priorizando calidad y cobertura"""
    productos_finales = {}
    
    # Prioridad 1: Document AI con código válido
    for prod in productos_ai:
        codigo = prod.get('codigo')
        if codigo and len(codigo) >= 8:  # Solo códigos EAN largos de AI
            productos_finales[codigo] = prod
    
    # Prioridad 2: Tesseract con códigos no vistos
    for prod in productos_tesseract:
        codigo = prod.get('codigo')
        if codigo:
            if codigo not in productos_finales:
                productos_finales[codigo] = prod
            elif productos_finales[codigo].get('valor', 0) == 0 and prod.get('valor', 0) > 0:
                # Actualizar precio si AI no lo detectó
                productos_finales[codigo]['valor'] = prod['valor']
    
    # Prioridad 3: Productos de AI sin código pero con nombre
    for prod in productos_ai:
        if not prod.get('codigo') and prod.get('nombre'):
            # Usar hash del nombre como código temporal
            import hashlib
            nombre_hash = hashlib.md5(prod['nombre'].encode()).hexdigest()[:8]
            temp_codigo = f"TEMP_{nombre_hash}"
            
            if temp_codigo not in productos_finales:
                prod['codigo'] = temp_codigo
                productos_finales[temp_codigo] = prod
    
    # Convertir a lista
    resultado = list(productos_finales.values())
    
    # Ordenar: primero con código numérico, luego temporales
    resultado.sort(key=lambda x: (
        x.get('codigo', '').startswith('TEMP'),
        x.get('codigo', '')
    ))
    
    return resultado

def process_invoice_complete(file_path):
    """Procesamiento completo mejorado"""
    inicio = time.time()
    
    try:
        print(f"\n{'='*70}")
        print("🔍 PROCESAMIENTO MEJORADO - OBJETIVO: 70%+ PRODUCTOS")
        print(f"{'='*70}")
        
        setup_environment()
        
        # MÉTODO 1: Document AI
        print("\n[1/3] 📄 Document AI procesando...")
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
        total_factura = extract_total_invoice(result.document.text)
        productos_ai = extract_products_document_ai(result.document)
        print(f"   ✓ {len(productos_ai)} productos detectados")
        
        # MÉTODO 2: Tesseract agresivo
        print("\n[2/3] 🔬 Tesseract OCR (modo agresivo)...")
        productos_tesseract = extract_products_tesseract_aggressive(file_path)
        print(f"   ✓ {len(productos_tesseract)} productos detectados")
        
        # MÉTODO 3: Combinar inteligentemente
        print("\n[3/3] 🔀 Combinando y deduplicando...")
        productos_finales = combinar_y_deduplicar(productos_ai, productos_tesseract)
        
        tiempo_total = int(time.time() - inicio)
        
        print(f"\n{'='*70}")
        print(f"✅ RESULTADO FINAL")
        print(f"{'='*70}")
        print(f"📍 Establecimiento: {establecimiento}")
        print(f"💰 Total factura: ${total_factura:,}" if total_factura else "💰 Total: No detectado")
        print(f"📦 Productos únicos: {len(productos_finales)}")
        print(f"   ├─ Document AI: {len(productos_ai)}")
        print(f"   ├─ Tesseract: {len(productos_tesseract)}")
        print(f"   └─ Finales (sin duplicados): {len(productos_finales)}")
        print(f"⏱️  Tiempo: {tiempo_total}s")
        print(f"{'='*70}\n")
        
        return {
            "establecimiento": establecimiento,
            "total": total_factura,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": "enhanced_triple_extraction",
                "productos_ai": len(productos_ai),
                "productos_tesseract": len(productos_tesseract),
                "productos_finales": len(productos_finales),
                "tiempo_segundos": tiempo_total
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        traceback.print_exc()
        return None

# Mantener compatibilidad
def process_invoice_products(file_path):
    """Versión legacy - redirige al nuevo procesador"""
    return process_invoice_complete(file_path)
