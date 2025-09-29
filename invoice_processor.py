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
    
    # Remover símbolos de moneda y espacios
    cleaned = re.sub(r'[$\s]', '', amount_str)
    
    # En Colombia: punto o coma son separadores de miles, NO decimales
    # Ejemplos válidos: 12.500, 12,500, 5.290
    
    # Remover separadores de miles (puntos y comas)
    cleaned = cleaned.replace('.', '').replace(',', '')
    
    # Remover cualquier carácter no numérico
    cleaned = re.sub(r'[^\d]', '', cleaned)
    
    if not cleaned:
        return None
    
    try:
        amount = int(cleaned)
        # Validación de rangos razonables para productos de supermercado
        # Ampliado para incluir productos baratos y caros
        if 10 < amount < 10000000:  # Entre $10 y $10.000.000
            return amount
    except:
        pass
    
    return None

def extract_vendor_name(raw_text):
    """Extrae el nombre del establecimiento - Mejorado"""
    # Patrones de supermercados colombianos
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
        r'(MERCADEFAM[^\n]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            # Limpiar caracteres extraños
            name = re.sub(r'\s+', ' ', name)
            return name[:50]
    
    # Buscar en las primeras líneas
    lines = raw_text.split('\n')[:15]
    for line in lines:
        line = line.strip()
        # Línea en mayúsculas, sin solo números, longitud razonable
        if 5 <= len(line) <= 50 and line.isupper() and not re.match(r'^[\d\s\-\.]+$', line):
            # Evitar líneas que son solo direcciones o NITs
            if not re.search(r'(CRA|CALLE|KR|CL|NIT|RUT|\d{3,})', line):
                return line
    
    return "Establecimiento no identificado"

def extract_product_code(text):
    """Extrae código de producto - Mejorado para más formatos"""
    if not text:
        return None
    
    text = text.strip()
    
    patterns = [
        r'^(\d{13})\s',          # EAN-13 al inicio
        r'^(\d{12})\s',          # UPC
        r'^(\d{10})\s',          # Códigos de 10 dígitos
        r'^(\d{8})\s',           # Códigos de 8 dígitos
        r'^(\d{7})\s',           # Códigos de 7 dígitos (Olímpica)
        r'^(\d{6})\s',           # Códigos de 6 dígitos
        r'\b(\d{13})\b',         # EAN-13 en cualquier parte
        r'\b(\d{12})\b',         # UPC en cualquier parte
        r'\b(\d{10})\b',         # Códigos de 10 dígitos
        r'\b(\d{7})\b',          # Códigos de 7 dígitos (común en Olímpica)
        r'PLU[:\s]*(\d{6,10})',  # PLU codes
        r'SKU[:\s]*(\w{6,12})',  # SKU codes
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            code = match.group(1)
            # Códigos válidos: entre 6 y 13 dígitos
            if 6 <= len(code) <= 13:
                # Verificar que no sea un precio (no debe terminar en 0,90,00,etc típicos de precios)
                if not (len(code) == 4 and code.endswith(('00', '90', '50'))):
                    return code
    
    return None

def clean_product_name(text, product_code=None):
    """Extrae el nombre limpio del producto - Mejorado"""
    if not text:
        return None
    
    # Remover código si existe
    if product_code:
        text = re.sub(rf'^{re.escape(product_code)}\s*', '', text)
    else:
        text = re.sub(r'^\d{6,}\s*', '', text)
    
    # Remover precios y cantidades
    text = re.sub(r'\d+[,\.]\d{3}', '', text)
    text = re.sub(r'\$\d+', '', text)
    text = re.sub(r'\d+\s*[xX]\s*\d+', '', text)
    
    # Remover sufijos comunes
    text = re.sub(r'[XNH]\s*$', '', text)
    text = re.sub(r'\s+DF\.[A-Z\.%\s]*', '', text)
    text = re.sub(r'-\d+,\d+', '', text)
    
    # Normalizar espacios
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n+', ' ', text)
    
    # Extraer solo palabras válidas
    words = []
    for word in text.split():
        # Palabra debe tener al menos 2 letras
        if re.search(r'[A-Za-zÀ-ÿ]{2,}', word):
            # Limpiar caracteres especiales del final
            word = re.sub(r'[^\w\sÀ-ÿ]+$', '', word)
            if len(word) >= 2:
                words.append(word)
    
    if words:
        name = ' '.join(words[:5])  # Máximo 5 palabras
        return name[:50] if len(name) > 0 else None
    
    return None

def extract_unit_price_from_text(text):
    """Extrae precio unitario del texto - Pesos colombianos (enteros)"""
    if not text:
        return None
    
    # Buscar patrones de precio
    price_patterns = [
        r'\$\s*(\d{1,3}[,\.]\d{3})',      # $12,500 o $12.500
        r'(\d{1,3}[,\.]\d{3})\s*

def extract_all_entities_info(document):
    """Extrae TODA la información de las entidades para análisis"""
    entities_info = []
    
    for i, entity in enumerate(document.entities):
        entity_data = {
            "index": i,
            "type": entity.type_,
            "confidence": round(entity.confidence, 3),
            "text": entity.mention_text[:100],  # Primeros 100 caracteres
            "properties": []
        }
        
        for prop in entity.properties:
            entity_data["properties"].append({
                "type": prop.type_,
                "confidence": round(prop.confidence, 3),
                "text": prop.mention_text
            })
        
        entities_info.append(entity_data)
    
    return entities_info

def extract_products_with_prices(document):
    """Extrae productos con sus precios - OPTIMIZADO"""
    
    raw_text = document.text
    
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": [],
        "metadatos": {
            "total_entidades": len(document.entities),
            "entidades_procesadas": 0,
            "umbral_confianza": 0.35  # Bajado para capturar más
        }
    }
    
    print(f"\n=== ANÁLISIS DE ENTIDADES ===")
    print(f"Total de entidades encontradas: {len(document.entities)}")
    
    productos_unicos = {}  # Para evitar duplicados
    
    for i, entity in enumerate(document.entities):
        entity_type = entity.type_
        confidence = entity.confidence
        
        print(f"\n--- Entidad {i+1}/{len(document.entities)} ---")
        print(f"Tipo: {entity_type}")
        print(f"Confianza: {confidence:.3f}")
        print(f"Texto completo: {entity.mention_text[:150]}")
        
        # Procesar line_items con umbral bajo
        if entity_type == "line_item" and confidence > 0.35:
            invoice_data["metadatos"]["entidades_procesadas"] += 1
            
            raw_item_text = entity.mention_text
            
            # Extraer campos CON LOGGING
            product_code = extract_product_code(raw_item_text)
            print(f"  → Código extraído: {product_code}")
            
            product_name = clean_product_name(raw_item_text, product_code)
            print(f"  → Nombre extraído: {product_name}")
            
            unit_price = None
            cantidad = 1
            
            # Buscar precio en propiedades CON LOGGING
            for prop in entity.properties:
                prop_type = prop.type_
                prop_text = prop.mention_text
                prop_conf = prop.confidence
                
                print(f"    Propiedad: {prop_type} = '{prop_text}' (conf: {prop_conf:.3f})")
                
                if prop_type == "line_item/amount" and prop_conf > 0.3:
                    price_candidate = clean_amount(prop_text)
                    print(f"      → Conversión precio: '{prop_text}' → {price_candidate}")
                    
                    if price_candidate:
                        unit_price = price_candidate
                        print(f"      ✓ Precio ACEPTADO: {unit_price}")
                
                if prop_type == "line_item/quantity":
                    try:
                        cantidad = int(prop_text)
                        print(f"      ✓ Cantidad: {cantidad}")
                    except:
                        pass
            
            # Si no hay precio en propiedades, buscar en texto
            if not unit_price:
                unit_price = extract_unit_price_from_text(raw_item_text)
                if unit_price:
                    print(f"    ✓ Precio extraído del texto: {unit_price}")
            
            # Validar que tengamos al menos código O nombre
            if product_code or (product_name and len(product_name) > 2):
                
                # Usar código como key para evitar duplicados
                producto_key = product_code or product_name
                
                # Solo agregar si es nuevo o tiene mejor información
                if producto_key not in productos_unicos or unit_price:
                    producto = {
                        "codigo": product_code or f"AUTO_{i+1}",
                        "nombre": product_name or "Producto sin descripción",
                        "valor": unit_price or 0,  # Valor entero, sin decimales
                        "datos_adicionales": {
                            "confianza": round(entity.confidence, 3),
                            "cantidad": cantidad,
                            "texto_original": raw_item_text[:100],
                            "entidad_index": i
                        }
                    }
                    
                    productos_unicos[producto_key] = producto
                    print(f"  ✓ PRODUCTO AGREGADO: {producto['codigo']} - {producto['nombre']} - ${producto['valor']}")
            else:
                print(f"  ✗ Producto omitido: sin código ni nombre válido")
    
    # Convertir diccionario a lista
    invoice_data["productos"] = list(productos_unicos.values())
    
    print(f"\n=== RESUMEN ===")
    print(f"Productos únicos extraídos: {len(invoice_data['productos'])}")
    print(f"Entidades procesadas: {invoice_data['metadatos']['entidades_procesadas']}")
    
    return invoice_data

def process_invoice_products(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO OPTIMIZADO ===")
        print(f"Archivo: {file_path}")
        print(f"Archivo existe: {os.path.exists(file_path)}")
        
        setup_environment()
        print("✓ Variables de entorno configuradas")
        
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"✓ Project ID: {project_id}")
        print(f"✓ Location: {location}")
        print(f"✓ Processor ID: {processor_id}")
        
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        print(f"✓ Archivo leído: {len(image_content)} bytes")
        
        # Detectar MIME type
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        print(f"✓ MIME type: {mime_type}")
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("→ Enviando a Document AI...")
        result = client.process_document(request=request)
        document = result.document
        
        print(f"✓ Documento procesado")
        print(f"✓ Texto extraído: {len(document.text)} caracteres")
        print(f"✓ Entidades encontradas: {len(document.entities)}")
        
        # Extraer productos optimizado
        invoice_data = extract_products_with_prices(document)
        
        print(f"\n✓ PROCESO COMPLETADO")
        print(f"✓ Establecimiento: {invoice_data['establecimiento']}")
        print(f"✓ Productos extraídos: {len(invoice_data['productos'])}")
        
        # Mostrar primeros 3 productos como muestra
        for i, prod in enumerate(invoice_data['productos'][:3]):
            print(f"  {i+1}. {prod['codigo']} - {prod['nombre']} - ${prod['valor']}")
        
        return invoice_data
        
    except Exception as e:
        print(f"\n✗ ERROR EN PROCESO: {str(e)}")
        print(f"✗ Tipo de error: {type(e).__name__}")
        print(f"✗ Traceback:")
        traceback.print_exc()
        return None
,       # 12,500 al final
        r'\$\s*(\d{1,6})',                # $12500
        r'(\d{4,6})\s*

def extract_all_entities_info(document):
    """Extrae TODA la información de las entidades para análisis"""
    entities_info = []
    
    for i, entity in enumerate(document.entities):
        entity_data = {
            "index": i,
            "type": entity.type_,
            "confidence": round(entity.confidence, 3),
            "text": entity.mention_text[:100],  # Primeros 100 caracteres
            "properties": []
        }
        
        for prop in entity.properties:
            entity_data["properties"].append({
                "type": prop.type_,
                "confidence": round(prop.confidence, 3),
                "text": prop.mention_text
            })
        
        entities_info.append(entity_data)
    
    return entities_info

def extract_products_with_prices(document):
    """Extrae productos con sus precios - OPTIMIZADO"""
    
    raw_text = document.text
    
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": [],
        "metadatos": {
            "total_entidades": len(document.entities),
            "entidades_procesadas": 0,
            "umbral_confianza": 0.35  # Bajado para capturar más
        }
    }
    
    print(f"\n=== ANÁLISIS DE ENTIDADES ===")
    print(f"Total de entidades encontradas: {len(document.entities)}")
    
    productos_unicos = {}  # Para evitar duplicados
    
    for i, entity in enumerate(document.entities):
        print(f"\nEntidad {i+1}: {entity.type_} (confianza: {entity.confidence:.3f})")
        
        # Procesar line_items con umbral bajo
        if entity.type_ == "line_item" and entity.confidence > 0.35:
            invoice_data["metadatos"]["entidades_procesadas"] += 1
            
            raw_item_text = entity.mention_text
            print(f"  Texto: {raw_item_text[:80]}")
            
            # Extraer campos
            product_code = extract_product_code(raw_item_text)
            product_name = clean_product_name(raw_item_text, product_code)
            
            unit_price = None
            cantidad = 1
            
            # Buscar precio en propiedades
            for prop in entity.properties:
                print(f"    Propiedad: {prop.type_} = {prop.mention_text} (conf: {prop.confidence:.3f})")
                
                if prop.type_ == "line_item/amount" and prop.confidence > 0.3:
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        print(f"    ✓ Precio encontrado en propiedad: {unit_price}")
                
                if prop.type_ == "line_item/quantity":
                    try:
                        cantidad = int(prop.mention_text)
                    except:
                        pass
            
            # Si no hay precio en propiedades, buscar en texto
            if not unit_price:
                unit_price = extract_unit_price_from_text(raw_item_text)
                if unit_price:
                    print(f"    ✓ Precio extraído del texto: {unit_price}")
            
            # Validar que tengamos al menos código O nombre
            if product_code or (product_name and len(product_name) > 2):
                
                # Usar código como key para evitar duplicados
                producto_key = product_code or product_name
                
                # Solo agregar si es nuevo o tiene mejor información
                if producto_key not in productos_unicos or unit_price:
                    producto = {
                        "codigo": product_code or f"AUTO_{i+1}",
                        "nombre": product_name or "Producto sin descripción",
                        "valor": unit_price or 0.0,
                        "datos_adicionales": {
                            "confianza": round(entity.confidence, 3),
                            "cantidad": cantidad,
                            "texto_original": raw_item_text[:100],
                            "entidad_index": i
                        }
                    }
                    
                    productos_unicos[producto_key] = producto
                    print(f"  ✓ PRODUCTO AGREGADO: {producto['codigo']} - {producto['nombre']} - ${producto['valor']}")
            else:
                print(f"  ✗ Producto omitido: sin código ni nombre válido")
    
    # Convertir diccionario a lista
    invoice_data["productos"] = list(productos_unicos.values())
    
    print(f"\n=== RESUMEN ===")
    print(f"Productos únicos extraídos: {len(invoice_data['productos'])}")
    print(f"Entidades procesadas: {invoice_data['metadatos']['entidades_procesadas']}")
    
    return invoice_data

def process_invoice_products(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO OPTIMIZADO ===")
        print(f"Archivo: {file_path}")
        print(f"Archivo existe: {os.path.exists(file_path)}")
        
        setup_environment()
        print("✓ Variables de entorno configuradas")
        
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"✓ Project ID: {project_id}")
        print(f"✓ Location: {location}")
        print(f"✓ Processor ID: {processor_id}")
        
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        print(f"✓ Archivo leído: {len(image_content)} bytes")
        
        # Detectar MIME type
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        print(f"✓ MIME type: {mime_type}")
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("→ Enviando a Document AI...")
        result = client.process_document(request=request)
        document = result.document
        
        print(f"✓ Documento procesado")
        print(f"✓ Texto extraído: {len(document.text)} caracteres")
        print(f"✓ Entidades encontradas: {len(document.entities)}")
        
        # Extraer productos optimizado
        invoice_data = extract_products_with_prices(document)
        
        print(f"\n✓ PROCESO COMPLETADO")
        print(f"✓ Establecimiento: {invoice_data['establecimiento']}")
        print(f"✓ Productos extraídos: {len(invoice_data['productos'])}")
        
        # Mostrar primeros 3 productos como muestra
        for i, prod in enumerate(invoice_data['productos'][:3]):
            print(f"  {i+1}. {prod['codigo']} - {prod['nombre']} - ${prod['valor']}")
        
        return invoice_data
        
    except Exception as e:
        print(f"\n✗ ERROR EN PROCESO: {str(e)}")
        print(f"✗ Tipo de error: {type(e).__name__}")
        print(f"✗ Traceback:")
        traceback.print_exc()
        return None
,                 # 12500 al final
    ]
    
    prices = []
    for pattern in price_patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            price = clean_amount(match)
            if price and 50 <= price <= 500000:  # Rango razonable en pesos
                prices.append(price)
    
    if prices:
        # Retornar el precio más consistente
        prices.sort()
        # Evitar outliers - retornar la mediana
        return prices[len(prices)//2]
    
    return None

def extract_all_entities_info(document):
    """Extrae TODA la información de las entidades para análisis"""
    entities_info = []
    
    for i, entity in enumerate(document.entities):
        entity_data = {
            "index": i,
            "type": entity.type_,
            "confidence": round(entity.confidence, 3),
            "text": entity.mention_text[:100],  # Primeros 100 caracteres
            "properties": []
        }
        
        for prop in entity.properties:
            entity_data["properties"].append({
                "type": prop.type_,
                "confidence": round(prop.confidence, 3),
                "text": prop.mention_text
            })
        
        entities_info.append(entity_data)
    
    return entities_info

def extract_products_with_prices(document):
    """Extrae productos con sus precios - OPTIMIZADO"""
    
    raw_text = document.text
    
    invoice_data = {
        "establecimiento": extract_vendor_name(raw_text),
        "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "productos": [],
        "metadatos": {
            "total_entidades": len(document.entities),
            "entidades_procesadas": 0,
            "umbral_confianza": 0.35  # Bajado para capturar más
        }
    }
    
    print(f"\n=== ANÁLISIS DE ENTIDADES ===")
    print(f"Total de entidades encontradas: {len(document.entities)}")
    
    productos_unicos = {}  # Para evitar duplicados
    
    for i, entity in enumerate(document.entities):
        print(f"\nEntidad {i+1}: {entity.type_} (confianza: {entity.confidence:.3f})")
        
        # Procesar line_items con umbral bajo
        if entity.type_ == "line_item" and entity.confidence > 0.35:
            invoice_data["metadatos"]["entidades_procesadas"] += 1
            
            raw_item_text = entity.mention_text
            print(f"  Texto: {raw_item_text[:80]}")
            
            # Extraer campos
            product_code = extract_product_code(raw_item_text)
            product_name = clean_product_name(raw_item_text, product_code)
            
            unit_price = None
            cantidad = 1
            
            # Buscar precio en propiedades
            for prop in entity.properties:
                print(f"    Propiedad: {prop.type_} = {prop.mention_text} (conf: {prop.confidence:.3f})")
                
                if prop.type_ == "line_item/amount" and prop.confidence > 0.3:
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        print(f"    ✓ Precio encontrado en propiedad: {unit_price}")
                
                if prop.type_ == "line_item/quantity":
                    try:
                        cantidad = int(prop.mention_text)
                    except:
                        pass
            
            # Si no hay precio en propiedades, buscar en texto
            if not unit_price:
                unit_price = extract_unit_price_from_text(raw_item_text)
                if unit_price:
                    print(f"    ✓ Precio extraído del texto: {unit_price}")
            
            # Validar que tengamos al menos código O nombre
            if product_code or (product_name and len(product_name) > 2):
                
                # Usar código como key para evitar duplicados
                producto_key = product_code or product_name
                
                # Solo agregar si es nuevo o tiene mejor información
                if producto_key not in productos_unicos or unit_price:
                    producto = {
                        "codigo": product_code or f"AUTO_{i+1}",
                        "nombre": product_name or "Producto sin descripción",
                        "valor": unit_price or 0.0,
                        "datos_adicionales": {
                            "confianza": round(entity.confidence, 3),
                            "cantidad": cantidad,
                            "texto_original": raw_item_text[:100],
                            "entidad_index": i
                        }
                    }
                    
                    productos_unicos[producto_key] = producto
                    print(f"  ✓ PRODUCTO AGREGADO: {producto['codigo']} - {producto['nombre']} - ${producto['valor']}")
            else:
                print(f"  ✗ Producto omitido: sin código ni nombre válido")
    
    # Convertir diccionario a lista
    invoice_data["productos"] = list(productos_unicos.values())
    
    print(f"\n=== RESUMEN ===")
    print(f"Productos únicos extraídos: {len(invoice_data['productos'])}")
    print(f"Entidades procesadas: {invoice_data['metadatos']['entidades_procesadas']}")
    
    return invoice_data

def process_invoice_products(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO OPTIMIZADO ===")
        print(f"Archivo: {file_path}")
        print(f"Archivo existe: {os.path.exists(file_path)}")
        
        setup_environment()
        print("✓ Variables de entorno configuradas")
        
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"✓ Project ID: {project_id}")
        print(f"✓ Location: {location}")
        print(f"✓ Processor ID: {processor_id}")
        
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        print(f"✓ Archivo leído: {len(image_content)} bytes")
        
        # Detectar MIME type
        mime_type = "image/jpeg"
        if file_path.lower().endswith('.png'):
            mime_type = "image/png"
        elif file_path.lower().endswith('.pdf'):
            mime_type = "application/pdf"
        
        print(f"✓ MIME type: {mime_type}")
        
        raw_document = documentai.RawDocument(
            content=image_content,
            mime_type=mime_type
        )
        
        request = documentai.ProcessRequest(
            name=name,
            raw_document=raw_document
        )
        
        print("→ Enviando a Document AI...")
        result = client.process_document(request=request)
        document = result.document
        
        print(f"✓ Documento procesado")
        print(f"✓ Texto extraído: {len(document.text)} caracteres")
        print(f"✓ Entidades encontradas: {len(document.entities)}")
        
        # Extraer productos optimizado
        invoice_data = extract_products_with_prices(document)
        
        print(f"\n✓ PROCESO COMPLETADO")
        print(f"✓ Establecimiento: {invoice_data['establecimiento']}")
        print(f"✓ Productos extraídos: {len(invoice_data['productos'])}")
        
        # Mostrar primeros 3 productos como muestra
        for i, prod in enumerate(invoice_data['productos'][:3]):
            print(f"  {i+1}. {prod['codigo']} - {prod['nombre']} - ${prod['valor']}")
        
        return invoice_data
        
    except Exception as e:
        print(f"\n✗ ERROR EN PROCESO: {str(e)}")
        print(f"✗ Tipo de error: {type(e).__name__}")
        print(f"✗ Traceback:")
        traceback.print_exc()
        return None
