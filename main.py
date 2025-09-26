from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
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

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    allowed_types = ["image/jpeg", "image/png", "application/pdf"]
    if file.content_type not in allowed_types:
        raise HTTPException(400, "Tipo de archivo no soportado")
    
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
