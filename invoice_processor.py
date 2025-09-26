def process_invoice_document(file_path):
    """Función principal que procesa una factura"""
    
    try:
        print("=== INICIANDO PROCESAMIENTO ===")
        print(f"Archivo: {file_path}")
        print(f"Archivo existe: {os.path.exists(file_path)}")
        
        setup_environment()
        print("✅ Variables de entorno configuradas")
        
        # Configuración desde variables de entorno
        project_id = os.environ['GCP_PROJECT_ID']
        location = os.environ['DOC_AI_LOCATION']
        processor_id = os.environ['DOC_AI_PROCESSOR_ID']
        
        print(f"Project ID: {project_id}")
        print(f"Location: {location}")
        print(f"Processor ID: {processor_id}")
        
        # Cliente Document AI
        print("Creando cliente Document AI...")
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
        print(f"Processor name: {name}")
        
        # Leer archivo
        print("Leyendo archivo...")
        with open(file_path, "rb") as image:
            image_content = image.read()
        
        print(f"Tamaño del archivo: {len(image_content)} bytes")
        
        # Detectar tipo MIME
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
        # Procesar documento
        result = client.process_document(request=request)
        document = result.document
        
        print("✅ Documento procesado por Document AI")
        print(f"Texto extraído: {len(document.text)} caracteres")
        print(f"Entidades encontradas: {len(document.entities)}")
        
        # Extraer productos con precios
        print("Extrayendo productos...")
        invoice_data = extract_products_with_prices(document)
        
        print(f"✅ Productos extraídos: {len(invoice_data['productos'])}")
        
        return invoice_data
        
    except Exception as e:
        print(f"❌ ERROR EN PROCESO: {str(e)}")
        print(f"Tipo de error: {type(e).__name__}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None
