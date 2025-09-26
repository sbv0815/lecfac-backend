from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import os
import tempfile
from datetime import datetime

# Importar el procesador y database
from invoice_processor import process_invoice_products
from database import create_tables, get_db_connection, hash_password, verify_password

app = FastAPI(title="LecFac API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelos para requests
class UserRegister(BaseModel):
    email: str
    password: str
    nombre: str = None

class UserLogin(BaseModel):
    email: str
    password: str

class SaveInvoice(BaseModel):
    usuario_id: int
    establecimiento: str
    productos: list

@app.on_event("startup")
async def startup_event():
    create_tables()

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
            .section { margin: 30px 0; padding: 20px; border: 1px solid #eee; border-radius: 5px; }
            .button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 5px; cursor: pointer; margin: 5px; }
            .button:hover { background: #0056b3; }
            input[type="email"], input[type="password"], input[type="text"] { width: 100%; padding: 10px; margin: 5px 0; border: 1px solid #ddd; border-radius: 3px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🧾 LecFac - Sistema Completo</h1>
            
            <!-- Sección de Registro -->
            <div class="section">
                <h2>📝 Registro de Usuario</h2>
                <form id="registerForm">
                    <input type="text" id="regNombre" placeholder="Nombre completo" required>
                    <input type="email" id="regEmail" placeholder="Email" required>
                    <input type="password" id="regPassword" placeholder="Contraseña" required>
                    <button type="submit" class="button">Registrarse</button>
                </form>
                <div id="registerResult" class="result" style="display: none;"></div>
            </div>
            
            <!-- Sección de Login -->
            <div class="section">
                <h2>🔐 Iniciar Sesión</h2>
                <form id="loginForm">
                    <input type="email" id="loginEmail" placeholder="Email" required>
                    <input type="password" id="loginPassword" placeholder="Contraseña" required>
                    <button type="submit" class="button">Ingresar</button>
                </form>
                <div id="loginResult" class="result" style="display: none;"></div>
            </div>
            
            <!-- Sección de Upload (solo visible después del login) -->
            <div class="section" id="uploadSection" style="display: none;">
                <h2>📄 Procesar Factura</h2>
                <p>Usuario: <span id="currentUser"></span></p>
                <form id="uploadForm" enctype="multipart/form-data">
                    <div class="upload-area">
                        <input type="file" id="fileInput" name="file" accept=".jpg,.jpeg,.png,.pdf" required>
                        <p>Selecciona JPG, PNG o PDF</p>
                    </div>
                    <button type="submit" class="button">📤 Procesar Factura</button>
                </form>
                <div id="uploadResult" class="result" style="display: none;"></div>
            </div>
        </div>

        <script>
            let currentUserId = null;
            
            // Registro de usuario
            document.getElementById('registerForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                
                const resultDiv = document.getElementById('registerResult');
                const data = {
                    nombre: document.getElementById('regNombre').value,
                    email: document.getElementById('regEmail').value,
                    password: document.getElementById('regPassword').value
                };
                
                resultDiv.style.display = 'block';
                resultDiv.className = 'result loading';
                resultDiv.innerHTML = '⏳ Registrando usuario...';
                
                try {
                    const response = await fetch('/users/register', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(data)
                    });
                    
                    const result = await response.json();
                    
                    if (response.ok) {
                        resultDiv.className = 'result success';
                        resultDiv.innerHTML = `✅ Usuario registrado exitosamente. ID: ${result.user_id}`;
                        document.getElementById('registerForm').reset();
                    } else {
                        throw new Error(result.detail || 'Error en registro');
                    }
                } catch (error) {
                    resultDiv.className = 'result error';
                    resultDiv.innerHTML = `❌ Error: ${error.message}`;
                }
            });
            
            // Login de usuario
            document.getElementById('loginForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                
                const resultDiv = document.getElementById('loginResult');
                const data = {
                    email: document.getElementById('loginEmail').value,
                    password: document.getElementById('loginPassword').value
                };
                
                resultDiv.style.display = 'block';
                resultDiv.className = 'result loading';
                resultDiv.innerHTML = '⏳ Iniciando sesión...';
                
                try {
                    const response = await fetch('/users/login', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(data)
                    });
                    
                    const result = await response.json();
                    
                    if (response.ok) {
                        resultDiv.className = 'result success';
                        resultDiv.innerHTML = `✅ Bienvenido ${result.nombre}`;
                        
                        // Guardar info del usuario y mostrar sección de upload
                        currentUserId = result.user_id;
                        document.getElementById('currentUser').textContent = result.nombre;
                        document.getElementById('uploadSection').style.display = 'block';
                        document.getElementById('loginForm').reset();
                    } else {
                        throw new Error(result.detail || 'Error en login');
                    }
                } catch (error) {
                    resultDiv.className = 'result error';
                    resultDiv.innerHTML = `❌ Error: ${error.message}`;
                }
            });
            
            // Upload de factura
            document.getElementById('uploadForm').addEventListener('submit', async function(e) {
                e.preventDefault();
                
                if (!currentUserId) {
                    alert('Debe iniciar sesión primero');
                    return;
                }
                
                const fileInput = document.getElementById('fileInput');
                const resultDiv = document.getElementById('uploadResult');
                
                if (!fileInput.files[0]) {
                    alert('Por favor selecciona un archivo');
                    return;
                }
                
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
                        // Guardar factura en base de datos
                        const saveData = {
                            usuario_id: currentUserId,
                            establecimiento: data.data.establecimiento,
                            productos: data.data.productos
                        };
                        
                        const saveResponse = await fetch('/invoices/save', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify(saveData)
                        });
                        
                        const saveResult = await saveResponse.json();
                        
                        resultDiv.className = 'result success';
                        resultDiv.innerHTML = `
                            <h3>✅ Factura procesada y guardada</h3>
                            <p><strong>Establecimiento:</strong> ${data.data.establecimiento}</p>
                            <p><strong>Productos encontrados:</strong> ${data.data.productos.length}</p>
                            <p><strong>ID Factura:</strong> ${saveResult.factura_id}</p>
                            <h4>Primeros 5 productos:</h4>
                            <ul>
                                ${data.data.productos.slice(0, 5).map(p => 
                                    `<li><strong>${p.codigo || 'Sin código'}:</strong> ${p.nombre || 'Sin nombre'} - $${p.valor || 'Sin precio'}</li>`
                                ).join('')}
                            </ul>
                        `;
                    } else {
                        throw new Error(data.detail || 'Error desconocido');
                    }
                } catch (error) {
                    resultDiv.className = 'result error';
                    resultDiv.innerHTML = `❌ Error: ${error.message}`;
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

@app.post("/users/register")
async def register_user(user: UserRegister):
    """Registra un nuevo usuario"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de base de datos")
    
    try:
        cursor = conn.cursor()
        
        # Verificar si el email ya existe
        cursor.execute("SELECT id FROM usuarios WHERE email = ?", (user.email,))
        if cursor.fetchone():
            raise HTTPException(400, "El email ya está registrado")
        
        # Crear usuario
        password_hash = hash_password(user.password)
        cursor.execute(
            "INSERT INTO usuarios (email, password_hash, nombre) VALUES (?, ?, ?)",
            (user.email, password_hash, user.nombre)
        )
        
        user_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return {"success": True, "user_id": user_id, "message": "Usuario creado"}
        
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/users/login")
async def login_user(user: UserLogin):
    """Inicia sesión de usuario"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de base de datos")
    
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, password_hash, nombre FROM usuarios WHERE email = ?", 
            (user.email,)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(401, "Email o contraseña incorrectos")
        
        return {
            "success": True, 
            "user_id": result[0],
            "nombre": result[2],
            "message": "Login exitoso"
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """Procesa una factura y extrae productos"""
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

@app.post("/invoices/save")
async def save_invoice(invoice: SaveInvoice):
    """Guarda una factura procesada en la base de datos"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de base de datos")
    
    try:
        cursor = conn.cursor()
        
        # Insertar factura
        cursor.execute(
            "INSERT INTO facturas (usuario_id, establecimiento, fecha_cargue) VALUES (?, ?, ?)",
            (invoice.usuario_id, invoice.establecimiento, datetime.now())
        )
        
        factura_id = cursor.lastrowid
        
        # Insertar productos
        for producto in invoice.productos:
            cursor.execute(
                "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (?, ?, ?, ?)",
                (factura_id, producto.get('codigo'), producto.get('nombre'), producto.get('valor'))
            )
        
        conn.commit()
        conn.close()
        
        return {
            "success": True, 
            "factura_id": factura_id,
            "productos_guardados": len(invoice.productos),
            "message": "Factura guardada exitosamente"
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Error guardando factura: {str(e)}")

@app.get("/users/{user_id}/invoices")
async def get_user_invoices(user_id: int):
    """Obtiene todas las facturas de un usuario"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de base de datos")
    
    try:
        cursor = conn.cursor()
        
        # Obtener facturas del usuario
        cursor.execute(
            """SELECT f.id, f.establecimiento, f.fecha_cargue, 
               COUNT(p.id) as total_productos
               FROM facturas f 
               LEFT JOIN productos p ON f.id = p.factura_id
               WHERE f.usuario_id = ? 
               GROUP BY f.id, f.establecimiento, f.fecha_cargue
               ORDER BY f.fecha_cargue DESC""", 
            (user_id,)
        )
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1], 
                "fecha_cargue": row[2],
                "total_productos": row[3]
            })
        
        conn.close()
        
        return {
            "success": True,
            "facturas": facturas,
            "total": len(facturas)
        }
        
    except Exception as e:
        conn.close()
        raise HTTPException(500, f"Error obteniendo facturas: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)






