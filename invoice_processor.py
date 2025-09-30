import os
import re
import json
import tempfile
import time
from datetime import datetime
from google.cloud import documentai
import traceback

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
# UTILIDADES
# ========================================

def clean_price(text):
    """Convierte texto de precio a entero. Ej: '16,390 N' -> 16390"""
    if not text:
        return None
    
    # Tomar solo la parte numérica
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
    """Valida código EAN de 8-13 dígitos"""
    if not code:
        return False
    code = str(code).strip()
    return code.isdigit() and 8 <= len(code) <= 13

def is_noise_keyword(text):
    """Detecta palabras clave de líneas no-producto"""
    noise = [
        "RESOLUCION", "DIAN", "RESPONSABLE", "IVA", "AGENTE",
        "RETENEDOR", "****", "====", "----", "SUBTOTAL", 
        "ITEMS COMPRADOS", "NRO", "CUENTA", "TARJ", "VISA",
        "CUOTAS", "CAMBIO", "RESUMEN"
    ]
    upper = text.upper()
    return any(kw in upper for kw in noise)

def clean_description(text):
    """Limpia la descripción del producto"""
    if not text:
        return None
    
    # Remover números iniciales sueltos
    text = re.sub(r'^\d{1,3}\s+', '', text)
    
    # Remover precios mezclados
    text = re.sub(r'\d{1,3}[,\.]\d{3}', '', text)
    
    # Remover símbolos y limpiar espacios
    text = re.sub(r'[;:]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Validar que tenga contenido real
    words = re.findall(r'[A-Za-zÀ-ÿ]{3,}', text)
    if len(words) < 2:
        return None
    
    return text[:60]

# ========================================
# EXTRACCIÓN DE INFORMACIÓN
# ========================================

def extract_vendor_name(text):
    """Extrae nombre del establecimiento"""
    patterns = [
        r"(JUMBO\s+[A-Z\s]+)",
        r"(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)",
        r"(CARULLA[^\n]*)",
        r"(OLIMPICA[^\n]*)",
        r"(D1[^\n]*)",
        r"(ALKOSTO[^\n]*)",
        r"(CRUZ\s+VERDE[^\n]*)",
        r"(LA\s+REBAJA[^\n]*)",
        r"(CAFAM[^\n]*)",
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            # Tomar solo primera línea si hay saltos
            name = name.split('\n')[0]
            return re.sub(r'\s+', ' ', name)[:50]
    
    return "Establecimiento no identificado"

def extract_total(text):
    """Extrae el total de la factura"""
    lines = text.split('\n')
    
    # Buscar "****SUBTOTAL/TOTAL" en últimas líneas
    for line in reversed(lines[-50:]):
        upper = line.upper()
        if "SUBTOTAL/TOTAL" in upper or (upper.strip().startswith("TOTAL") and ":" not in upper):
            numbers = re.findall(r'\d{1,3}(?:[,\.]\d{3})+', line)
            if numbers:
                # Tomar el último número (más a la derecha)
                total = clean_price(numbers[-1])
                if total and total > 5000:
                    return total
    
    return None

# ========================================
# PARSER POR COLUMNAS
# ========================================

def detect_price_column(lines):
    """Detecta la posición de la columna de precios"""
    for line in lines[:40]:
        upper = line.upper()
        if 'CODIGO' in upper and 'TOTAL' in upper:
            # Buscar posición de TOTAL
            idx = upper.rfind('TOTAL')
            if idx > 40:
                return idx
    
    # Fallback: asumir columna estándar
    return 58

def parse_products_by_columns(text):
    """
    Parser principal: usa posiciones de columnas fijas.
    Formato típico de factura colombiana:
    [0-15] CÓDIGO | [15-58] DESCRIPCIÓN | [58+] PRECIO
    """
    if not text:
        return []
    
    lines = text.split('\n')
    price_col = detect_price_column(lines)
    
    print(f"Columna de precio detectada: posición {price_col}")
    print(f"Analizando {len(lines)} líneas...")
    
    products = []
    seen_codes = set()
    valid_count = 0
    
    for i, line in enumerate(lines):
        # Saltar líneas cortas
        if len(line) < 20:
            continue
        
        # Saltar ruido conocido
        if is_noise_keyword(line):
            continue
        
        # Si la línea tiene "****", "TOTAL", etc al inicio, saltar
        if line.strip().startswith(('*', '=', '-', 'TOTAL')):
            continue
        
        # Extraer zonas por columna
        code_zone = line[:15].strip()
        desc_zone = line[15:price_col].strip()
        price_zone = line[price_col:].strip()
        
        # 1. CÓDIGO: debe empezar con EAN de 8-13 dígitos
        code_match = re.match(r'^(\d{8,13})\b', code_zone)
        if not code_match:
            continue
        
        code = code_match.group(1)
        
        # Evitar duplicados exactos
        if code in seen_codes:
            continue
        
        # 2. DESCRIPCIÓN: limpiar
        desc = clean_description(desc_zone)
        if not desc or len(desc) < 4:
            continue
        
        # 3. PRECIO: buscar primer número válido
        price = 0
        price_match = re.search(r'(\d{1,3}(?:[,\.]\d{3})+)', price_zone)
        if price_match:
            price = clean_price(price_match.group(1)) or 0
        
        # Agregar producto
        products.append({
            "codigo": code,
            "nombre": desc,
            "valor": price,
            "fuente": "columns",
            "linea": i + 1
        })
        
        seen_codes.add(code)
        valid_count += 1
    
    print(f"Productos extraídos: {valid_count}")
    return products

# ========================================
# PARSER COMPLEMENTARIO (REGEX)
# ========================================

def parse_products_by_regex(text, existing_codes):
    """
    Parser secundario: usa regex para capturar productos
    que el parser de columnas pudo haber omitido.
    """
    if not text:
        return []
    
    lines = text.split('\n')
    products = []
    
    # Patrón: CÓDIGO (espacio) TEXTO (espacio) PRECIO
    pattern = r'^(\d{8,13})\s+(.+?)\s+(\d{1,3}[,\.]\d{3})\s*[A-Z]?\s*$'
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        if len(line) < 20:
            continue
        
        if is_noise_keyword(line):
            continue
        
        match = re.match(pattern, line)
        if not match:
            continue
        
        code, desc_raw, price_raw = match.groups()
        
        # Saltar si ya está capturado
        if code in existing_codes:
            continue
        
        # Validar y limpiar
        if not is_valid_ean(code):
            continue
        
        desc = clean_description(desc_raw)
        if not desc:
            continue
        
        price = clean_price(price_raw) or 0
        
        products.append({
            "codigo": code,
            "nombre": desc,
            "valor": price,
            "fuente": "regex",
            "linea": i + 1
        })
        
        existing_codes.add(code)
    
    print(f"Productos adicionales (regex): {len(products)}")
    return products

# ========================================
# PROCESAMIENTO PRINCIPAL
# ========================================

def process_invoice_complete(file_path):
    """Procesamiento completo usando Document AI + parsers duales"""
    inicio = time.time()
    
    try:
        print("\n" + "="*70)
        print("PROCESAMIENTO DE FACTURA - PARSER DE COLUMNAS")
        print("="*70)
        
        setup_environment()
        
        # Document AI
        print("\n[1/3] Procesando con Document AI...")
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
        
        # Extraer metadata
        establecimiento = extract_vendor_name(raw_text)
        total = extract_total(raw_text)
        
        # [2/3] Parser por columnas (principal)
        print("\n[2/3] Extrayendo productos (parser de columnas)...")
        productos_cols = parse_products_by_columns(raw_text)
        
        # [3/3] Parser regex (complementario)
        print("\n[3/3] Buscando productos adicionales (regex)...")
        existing_codes = {p["codigo"] for p in productos_cols}
        productos_regex = parse_products_by_regex(raw_text, existing_codes)
        
        # Combinar
        productos = productos_cols + productos_regex
        
        tiempo_total = int(time.time() - inicio)
        
        print("\n" + "="*70)
        print("RESULTADO FINAL")
        print("="*70)
        print(f"Establecimiento: {establecimiento}")
        print(f"Total: ${total:,}" if total else "Total: No detectado")
        print(f"Productos detectados: {len(productos)}")
        print(f"  - Por columnas: {len(productos_cols)}")
        print(f"  - Por regex: {len(productos_regex)}")
        print(f"Tiempo: {tiempo_total}s")
        print("="*70 + "\n")
        
        # Mostrar primeros 5
        if productos:
            print("Primeros 5 productos:")
            for p in productos[:5]:
                precio_str = f"${p['valor']:,}" if p['valor'] else "Sin precio"
                print(f"  {p['codigo']}: {p['nombre']} - {precio_str}")
        
        return {
            "establecimiento": establecimiento,
            "total": total,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos,
            "metadatos": {
                "metodo": "columns_dual_parser",
                "productos_columnas": len(productos_cols),
                "productos_regex": len(productos_regex),
                "productos_totales": len(productos),
                "tiempo_segundos": tiempo_total
            }
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        traceback.print_exc()
        return None

# Alias para compatibilidad
def process_invoice_products(file_path):
    return process_invoice_complete(file_path)
def process_invoice_products(file_path):
    """Versión legacy - redirige al nuevo procesador"""
    return process_invoice_complete(file_path)
