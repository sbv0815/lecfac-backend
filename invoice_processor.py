def setup_environment():
    """Configura las variables de entorno para Google Cloud"""
    import json
    import tempfile
    
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
