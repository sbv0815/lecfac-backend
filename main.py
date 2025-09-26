from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
import os
import tempfile
from datetime import datetime

# Importar el procesador (necesitamos adaptarlo)
from invoice_processor import process_invoice_products

app = FastAPI(title="LecFac API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "LecFac API funcionando", "version": "1.0.0"}

@app.get("/test", response_class=HTMLResponse)
async def test_page():
    """Página de prueba para subir facturas"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>LecFac - Test de Upload</title>
        <style>
            body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
            .container { border: 2px solid #ddd; padding: 30px; border-radius: 10px; }
            .upload-area { border: 2px dashed #ccc; padding: 40px; text-align: center; margin: 20px 0; }
            .result { margin-top: 20px; padding: 15px; background: #f5f5f5; border-radius: 5px; }
            .error { background: #ffe6e6; color: #d00; }
            .success { background: #e6ffe6; color: #080; }
            .loading { background: #fff3cd; color: #856404; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🧾 LecFac - Prueba de OCR</h1>
            <p>Sube una imagen de factura para probar el procesamiento</p>
            
            <form id="uploadForm" enctype="multipart/form-data">
                <div class="upload-area">
                    <input type="file" id="fileInput" name="file" accept=".jpg,.jpeg,.png,.pdf" required>
                    <p>Selecciona JPG, PNG o PDF</p>
                </div>
                <button type="submit">📤 Procesar Factura</button>
            </form>
            
            <div id="result" class="result" style="display: none;"></div>
        </div>

        <script>
            document.getElementById('uploadForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                
                const fileInput = document.getElementById('fileInput');
                const resultDiv = document.getElementById('result');
                
                if (!fileInput.files[0]) {
                    alert('Por favor selecciona un archivo');
                    return;
                }
                
                // Mostrar loading
                resultDiv.style.display = 'block';
                resultDiv.className = 'result loading';
                resultDiv.innerHTML = '⏳ Procesando factura... Esto puede tomar unos segundos.';
                
                const formData = new FormData();
                formData.append('file', fileInput.files[0]);
                
                try {
                    const response = await fetch('/invoices/parse', {
                        method: 'POST',
                        body: formData
                    });
                    
                    const data = await response.json();
                    
                    if (response.ok) {
                        resultDiv.className = 'result success';
                        resultDiv.innerHTML = `
                            <h3>✅ Factura procesada exitosamente</h3>
                            <p><strong>Establecimiento:</strong> ${data.data.establecimiento}</p>
                            <p><strong>Fecha:</strong> ${data.data.fecha_cargue}</p>
                            <p><strong>Productos encontrados:</strong> ${data.data.productos.length}</p>
                            <h4>Primeros 5 productos:</h4>
                            <ul>
                                ${data.data.productos.slice(0, 5).map(p => 
                                    `<li><strong>${p.codigo || 'Sin código'}:</strong> ${p.nombre || 'Sin nombre'} - $${p.valor || 'Sin precio'}</li>`
                                ).join('')}
                            </ul>
                            <details>
                                <summary>Ver JSON completo</summary>
                                <pre>${JSON.stringify(data, null, 2)}</pre>
                            </details>
                        `;
                    } else {
                        throw new Error(data.detail || 'Error desconocido');
                    }
                } catch (error) {
                    resultDiv.className = 'result error';
                    resultDiv.innerHTML = `
                        <h3>❌ Error procesando factura</h3>
                        <p><strong>Error:</strong> ${error.message}</p>
                        <p><strong>Posibles causas:</strong></p>
                        <ul>
                            <li>Variables de entorno mal configuradas</li>
                            <li>Problemas con credenciales de Google Cloud</li>
                            <li>Archivo no válido o muy grande</li>
                        </ul>
                    `;
                    console.error('Error:', error);
                }
            });
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.get("/debug")
async def debug_info():
    """Endpoint de debug para verificar configuración"""
    return {
        "environment_variables": {
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "NO CONFIGURADO"),
            "DOC_AI_LOCATION": os.environ.get("DOC_AI_LOCATION", "NO CONFIGURADO"), 
            "DOC_AI_PROCESSOR_ID": os.environ.get("DOC_AI_PROCESSOR_ID", "NO CONFIGURADO"),
            "GOOGLE_APPLICATION_CREDENTIALS_JSON": "CONFIGURADO" if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") else "NO CONFIGURADO"
        },
        "python_version": os.sys.version,
        "working_directory": os.getcwd()
    }

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    allowed_types = ["image/jpeg", "image/png", "application/pdf"]
    if file.content_type not in allowed_types:
        raise HTTPException(400, f"Tipo de archivo no soportado: {file.content_type}")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        result = process_invoice_products(temp_file_path)
        os.unlink(temp_file_path)
        
        if not result:
            raise HTTPException(500, "Error procesando factura")
        
        return {"success": True, "data": result}
        
    except Exception as e:
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
            except:
                pass
        raise HTTPException(500, f"Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

