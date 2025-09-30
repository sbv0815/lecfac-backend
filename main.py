from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import os
import tempfile
from datetime import datetime
# Importar el procesador y database
from invoice_processor import process_invoice_products
from database import create_tables, get_db_connection, hash_password, verify_password, test_database_connection
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager


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

from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización de la aplicación"""
    # TODO LO QUE VA ANTES DEL yield ES EL STARTUP
    print("🚀 Iniciando LecFac API...")
    print("✅ Tesseract OCR disponible")
    print("✅ psycopg3 disponible")
    
    # Probar conexión a base de datos
    if test_database_connection():
        print("📊 Conexión a base de datos exitosa")
    else:
        print("❌ Error de conexión a base de datos")
    
    # Crear tablas
    try:
        create_tables()
        print("🗃️ Tablas verificadas/creadas")
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")
    
    yield  # <-- Aquí la app está corriendo
    
    # TODO LO QUE VA DESPUÉS DEL yield ES EL SHUTDOWN (limpieza)
    print("👋 Cerrando LecFac API...")

# DESPUÉS defines la app usando la función lifespan
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    """Endpoint raíz con información del sistema"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    return {
        "message": "LecFac API funcionando", 
        "version": "1.0.0",
        "database": database_type,
        "status": "active"
    }

@app.get("/health")
async def health_check():
    """Endpoint de salud para verificar el sistema"""
    try:
        # Verificar conexión a BD
        conn = get_db_connection()
        if conn:
            conn.close()
            db_status = "connected"
        else:
            db_status = "disconnected"
        
        return {
            "status": "healthy" if db_status == "connected" else "unhealthy",
            "database": db_status,
            "database_type": os.environ.get("DATABASE_TYPE", "sqlite"),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

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
            .status { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🧾 LecFac - Sistema Completo</h1>
            
            <!-- Estado del sistema -->
            <div class="status" id="systemStatus">
                <h3>📊 Estado del Sistema</h3>
                <p id="statusText">Verificando...</p>
            </div>
            
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
            
            // Verificar estado del sistema al cargar
            async function checkSystemStatus() {
                try {
                    const response = await fetch('/health');
                    const status = await response.json();
                    
                    const statusText = document.getElementById('statusText');
                    if (status.status === 'healthy') {
                        statusText.innerHTML = `✅ Sistema funcionando - Base de datos: ${status.database_type} (${status.database})`;
                        statusText.style.color = '#2e7d32';
                    } else {
                        statusText.innerHTML = `❌ Sistema con problemas - ${status.error || 'Error desconocido'}`;
                        statusText.style.color = '#d32f2f';
                    }
                } catch (error) {
                    document.getElementById('statusText').innerHTML = `❌ Error conectando con API: ${error.message}`;
                    document.getElementById('statusText').style.color = '#d32f2f';
                }
            }
            
            // Verificar estado al cargar la página
            checkSystemStatus();
            
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
                            <p><strong>Productos guardados:</strong> ${saveResult.productos_guardados}/${saveResult.total_productos}</p>
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
            "DATABASE_TYPE": os.environ.get("DATABASE_TYPE", "sqlite"),
            "DATABASE_URL": "CONFIGURADO" if os.environ.get("DATABASE_URL") else "NO CONFIGURADO",
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "NO CONFIGURADO"),
            "DOC_AI_LOCATION": os.environ.get("DOC_AI_LOCATION", "NO CONFIGURADO"), 
            "DOC_AI_PROCESSOR_ID": os.environ.get("DOC_AI_PROCESSOR_ID", "NO CONFIGURADO"),
            "GOOGLE_APPLICATION_CREDENTIALS_JSON": "CONFIGURADO" if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON") else "NO CONFIGURADO"
        },
        "python_version": os.sys.version,
        "working_directory": os.getcwd()
    }

# Función auxiliar para manejar queries de base de datos
def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """
    Función auxiliar para ejecutar queries con manejo de diferencias entre PostgreSQL y SQLite
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de conexión a base de datos")
    
    try:
        cursor = conn.cursor()
        
        # Convertir placeholders si es necesario (SQLite usa ?, PostgreSQL usa %s)
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        if database_type == "postgresql" and query.count("?") > 0:
            # Convertir ? a %s para PostgreSQL
            query = query.replace("?", "%s")
        
        cursor.execute(query, params or ())
        
        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        else:
            result = cursor.rowcount
        
        conn.commit()
        conn.close()
        return result
        
    except Exception as e:
        conn.close()
        raise e

@app.post("/users/register")
async def register_user(user: UserRegister):
    """Registra un nuevo usuario"""
    try:
        # Verificar si el email ya existe
        existing = execute_query(
            "SELECT id FROM usuarios WHERE email = ?", 
            (user.email,), 
            fetch_one=True
        )
        
        if existing:
            raise HTTPException(400, "El email ya está registrado")
        
        # Crear usuario
        password_hash = hash_password(user.password)
        
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        if database_type == "postgresql":
            # PostgreSQL con RETURNING
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO usuarios (email, password_hash, nombre) VALUES (%s, %s, %s) RETURNING id",
                (user.email, password_hash, user.nombre)
            )
            user_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
        else:
            # SQLite
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO usuarios (email, password_hash, nombre) VALUES (?, ?, ?)",
                (user.email, password_hash, user.nombre)
            )
            user_id = cursor.lastrowid
            conn.commit()
            conn.close()
        
        return {"success": True, "user_id": user_id, "message": "Usuario creado"}
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/users/login")
async def login_user(user: UserLogin):
    """Inicia sesión de usuario"""
    try:
        result = execute_query(
            "SELECT id, password_hash, nombre FROM usuarios WHERE email = ?", 
            (user.email,),
            fetch_one=True
        )
        
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(401, "Email o contraseña incorrectos")
        
        return {
            "success": True, 
            "user_id": result[0],
            "nombre": result[2],
            "message": "Login exitoso"
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    # Tipos MIME aceptados - agregamos octet-stream
    allowed_types = [
        "image/jpeg", 
        "image/png", 
        "application/pdf",
        "application/octet-stream"  # Agregado para galería
    ]
    
    # También verificar por extensión del archivo
    file_extension = file.filename.lower() if file.filename else ""
    valid_extensions = file_extension.endswith(('.jpg', '.jpeg', '.png', '.pdf'))
    
    if file.content_type not in allowed_types and not valid_extensions:
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
    """Guardar factura con productos en catálogo colaborativo (frescos + EAN)"""
    try:
        print(f"=== GUARDANDO FACTURA EN CATÁLOGO COLABORATIVO ===")
        print(f"Usuario ID: {invoice.usuario_id}")
        print(f"Establecimiento: {invoice.establecimiento}")
        print(f"Productos recibidos: {len(invoice.productos)}")
        
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cadena = detectar_cadena(invoice.establecimiento)
        print(f"Cadena detectada: {cadena}")
        
        # Insertar factura
        if database_type == "postgresql":
            cursor.execute(
                """INSERT INTO facturas (usuario_id, establecimiento, cadena, fecha_cargue) 
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (invoice.usuario_id, invoice.establecimiento, cadena, datetime.now())
            )
            factura_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                "INSERT INTO facturas (usuario_id, establecimiento, fecha_cargue) VALUES (?, ?, ?)",
                (invoice.usuario_id, invoice.establecimiento, datetime.now())
            )
            factura_id = cursor.lastrowid
        
        print(f"Factura ID: {factura_id}")
        
        productos_guardados = 0
        productos_nuevos = 0
        productos_actualizados = 0
        
        for i, producto in enumerate(invoice.productos):
            print(f"\n--- Producto {i+1}/{len(invoice.productos)} ---")
            
            codigo = producto.get('codigo')
            nombre = producto.get('nombre')
            valor = producto.get('valor')
            
            # Validar y limpiar nombre
            if not nombre or len(str(nombre).strip()) < 2:
                nombre = "Producto sin descripción"
            else:
                nombre = str(nombre).strip()
            
            # Validar valor
            if valor is None or valor == '':
                valor = 0
            
            try:
                valor_int = int(valor)
                
                # Determinar si es código válido
                codigo_valido = codigo and codigo != 'None' and len(codigo) >= 6
                
                # Detectar si es peso variable
                if codigo_valido and es_codigo_peso_variable(codigo):
                    print(f"⚠️ Código peso variable detectado: {codigo}")
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                    print(f"   Generado código único: {codigo}")
                
                # Si no hay código válido, generar
                if not codigo_valido:
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                    print(f"⚠️ Código generado: {codigo}")
                
                if database_type == "postgresql":
                    # Detectar si es producto fresco (código corto < 7 dígitos)
                    # O si es código generado por nosotros (empieza con PLU_ o AUTO_)
                    es_fresco = (
                        (len(codigo) < 7 and codigo.isdigit()) or 
                        codigo.startswith('PLU_') or 
                        codigo.startswith('AUTO_')
                    )
                    
                    if es_fresco:
                        print(f"🥬 Producto FRESCO/VARIABLE: {codigo}")
                        producto_id = manejar_producto_fresco(cursor, codigo, nombre, cadena)
                    else:
                        print(f"📦 Producto EAN: {codigo}")
                        producto_id = manejar_producto_ean(cursor, codigo, nombre)
                    
                    # Registrar precio
                    cursor.execute(
                        """INSERT INTO precios_productos 
                           (producto_id, establecimiento, cadena, precio, usuario_id, factura_id, fecha_reporte)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (producto_id, invoice.establecimiento, cadena, valor_int, invoice.usuario_id, factura_id, datetime.now())
                    )
                    
                    # Legacy
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (%s, %s, %s, %s)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                    
                    productos_guardados += 1
                    if producto_id:
                        productos_actualizados += 1
                    else:
                        productos_nuevos += 1
                    
                else:
                    # SQLite fallback
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (?, ?, ?, ?)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                    productos_guardados += 1
                
                print(f"✓ Guardado: {codigo} - {nombre} - ${valor_int}")
                
            except Exception as e:
                print(f"❌ Error guardando producto {i+1}: {e}")
                import traceback
                traceback.print_exc()
        
        conn.commit()
        conn.close()
        
        print(f"\n=== RESULTADO ===")
        print(f"Productos guardados: {productos_guardados}/{len(invoice.productos)}")
        print(f"Nuevos: {productos_nuevos}, Actualizados: {productos_actualizados}")
        
        return {
            "success": True, 
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "total_productos": len(invoice.productos),
            "productos_nuevos": productos_nuevos,
            "productos_actualizados": productos_actualizados,
            "message": f"Factura guardada: {productos_guardados} productos"
        }
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error guardando factura: {str(e)}")

def manejar_producto_ean(cursor, codigo_ean: str, nombre: str) -> int:
    """Maneja producto con código EAN (único global)"""
    # Buscar por EAN
    cursor.execute(
        "SELECT id FROM productos_catalogo WHERE codigo_ean = %s",
        (codigo_ean,)
    )
    resultado = cursor.fetchone()
    
    if resultado:
        producto_id = resultado[0]
        # Actualizar contadores
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        print(f"  ✓ Producto EAN existente: {codigo_ean}")
        return producto_id
    else:
        # Crear nuevo producto
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (codigo_ean, nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, %s, FALSE, %s, 1, %s) RETURNING id""",
            (codigo_ean, nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        print(f"  ✓ Producto EAN NUEVO: {codigo_ean}")
        return producto_id

def manejar_producto_fresco(cursor, codigo_local: str, nombre: str, cadena: str) -> int:
    """Maneja producto fresco (código local por cadena)"""
    # Buscar si existe código local para esta cadena
    cursor.execute(
        "SELECT producto_id FROM codigos_locales WHERE cadena = %s AND codigo_local = %s",
        (cadena, codigo_local)
    )
    resultado = cursor.fetchone()
    
    if resultado:
        producto_id = resultado[0]
        # Actualizar contadores
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        print(f"  ✓ Producto fresco existente: {codigo_local} en {cadena}")
        return producto_id
    else:
        # Crear nuevo producto fresco
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, TRUE, %s, 1, %s) RETURNING id""",
            (nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        
        # Crear código local
        cursor.execute(
            """INSERT INTO codigos_locales (producto_id, cadena, codigo_local)
               VALUES (%s, %s, %s)""",
            (producto_id, cadena, codigo_local)
        )
        print(f"  ✓ Producto fresco NUEVO: {codigo_local} en {cadena}")
        return producto_id

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena de supermercado"""
    establecimiento_lower = establecimiento.lower()
    
    cadenas = {
        'exito': ['exito', 'éxito', 'almacenes'],
        'carulla': ['carulla'],
        'olimpica': ['olimpica', 'olímpica'],
        'd1': ['d1', 'tiendas d1'],
        'jumbo': ['jumbo'],
        'alkosto': ['alkosto'],
        'metro': ['metro', 'makro'],
        'ara': ['ara'],
        'surtimax': ['surtimax'],
        'falabella': ['falabella']
    }
    
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    
    return 'otro'

def manejar_producto_ean(cursor, codigo_ean: str, nombre: str) -> int:
    """Maneja producto con código EAN (único global)"""
    # Buscar por EAN
    cursor.execute(
        "SELECT id FROM productos_catalogo WHERE codigo_ean = %s",
        (codigo_ean,)
    )
    resultado = cursor.fetchone()
    
    if resultado:
        producto_id = resultado[0]
        # Actualizar contadores
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        print(f"  ✓ Producto EAN existente: {codigo_ean}")
        return producto_id
    else:
        # Crear nuevo producto
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (codigo_ean, nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, %s, FALSE, %s, 1, %s) RETURNING id""",
            (codigo_ean, nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        print(f"  ✓ Producto EAN NUEVO: {codigo_ean}")
        return producto_id

def manejar_producto_fresco(cursor, codigo_local: str, nombre: str, cadena: str) -> int:
    """Maneja producto fresco (código local por cadena)"""
    # Buscar si existe código local para esta cadena
    cursor.execute(
        "SELECT producto_id FROM codigos_locales WHERE cadena = %s AND codigo_local = %s",
        (cadena, codigo_local)
    )
    resultado = cursor.fetchone()
    
    if resultado:
        producto_id = resultado[0]
        # Actualizar contadores
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        print(f"  ✓ Producto fresco existente: {codigo_local} en {cadena}")
        return producto_id
    else:
        # Crear nuevo producto fresco
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, TRUE, %s, 1, %s) RETURNING id""",
            (nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        
        # Crear código local
        cursor.execute(
            """INSERT INTO codigos_locales (producto_id, cadena, codigo_local)
               VALUES (%s, %s, %s)""",
            (producto_id, cadena, codigo_local)
        )
        print(f"  ✓ Producto fresco NUEVO: {codigo_local} en {cadena}")
        return producto_id

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena de supermercado"""
    establecimiento_lower = establecimiento.lower()
    
    cadenas = {
        'exito': ['exito', 'éxito', 'almacenes'],
        'carulla': ['carulla'],
        'olimpica': ['olimpica', 'olímpica'],
        'd1': ['d1', 'tiendas d1'],
        'jumbo': ['jumbo'],
        'alkosto': ['alkosto'],
        'metro': ['metro', 'makro'],
        'ara': ['ara'],
        'surtimax': ['surtimax'],
        'falabella': ['falabella']
    }
    
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    
    return 'otro'

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena de supermercado del establecimiento"""
    establecimiento_lower = establecimiento.lower()
    
    cadenas = {
        'exito': ['exito', 'éxito', 'almacenes'],
        'carulla': ['carulla'],
        'olimpica': ['olimpica', 'olímpica'],
        'd1': ['d1', 'tiendas d1'],
        'jumbo': ['jumbo'],
        'alkosto': ['alkosto'],
        'metro': ['metro', 'makro'],
        'ara': ['ara'],
        'surtimax': ['surtimax'],
        'falabella': ['falabella']
    }
    
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    
    return 'otro'

@app.get("/users/{user_id}/invoices")
async def get_user_invoices(user_id: int):
    """Obtiene todas las facturas de un usuario"""
    try:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        
        if database_type == "postgresql":
            query = """SELECT f.id, f.establecimiento, f.fecha_cargue, 
                       COUNT(p.id) as total_productos
                       FROM facturas f 
                       LEFT JOIN productos p ON f.id = p.factura_id
                       WHERE f.usuario_id = %s 
                       GROUP BY f.id, f.establecimiento, f.fecha_cargue
                       ORDER BY f.fecha_cargue DESC"""
        else:
            query = """SELECT f.id, f.establecimiento, f.fecha_cargue, 
                       COUNT(p.id) as total_productos
                       FROM facturas f 
                       LEFT JOIN productos p ON f.id = p.factura_id
                       WHERE f.usuario_id = ? 
                       GROUP BY f.id, f.establecimiento, f.fecha_cargue
                       ORDER BY f.fecha_cargue DESC"""
        
        result = execute_query(query, (user_id,), fetch_all=True)
        
        facturas = []
        for row in result:
            facturas.append({
                "id": row[0],
                "establecimiento": row[1], 
                "fecha_cargue": row[2],
                "total_productos": row[3]
            })
        
        return {
            "success": True,
            "facturas": facturas,
            "total": len(facturas)
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error obteniendo facturas: {str(e)}")

@app.delete("/invoices/{factura_id}")
async def delete_invoice(factura_id: int, usuario_id: int):
    """Elimina una factura y todos sus productos asociados"""
    try:
        # Verificar que la factura pertenece al usuario
        result = execute_query(
            "SELECT id FROM facturas WHERE id = ? AND usuario_id = ?", 
            (factura_id, usuario_id),
            fetch_one=True
        )
        
        if not result:
            raise HTTPException(404, "Factura no encontrada o no autorizada")
        
        # Eliminar productos asociados
        productos_eliminados = execute_query(
            "DELETE FROM productos WHERE factura_id = ?", 
            (factura_id,)
        )
        
        # Eliminar factura
        execute_query("DELETE FROM facturas WHERE id = ?", (factura_id,))
        
        return {
            "success": True,
            "message": f"Factura eliminada (incluidos {productos_eliminados} productos)"
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error eliminando factura: {str(e)}")

@app.get("/admin/verify-database")
async def verify_database():
    """
    Verifica que las tablas existan y muestra información
    """
    try:
        conn = get_db_connection()
        if not conn:
            return {
                "success": False,
                "error": "No se pudo conectar a la base de datos"
            }
        
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        
        info = {
            "database_type": database_type,
            "connection": "OK",
            "tables": {}
        }
        
        if database_type == "postgresql":
            # Verificar tablas en PostgreSQL
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            info["tables_list"] = tables
            
            # Contar registros en cada tabla
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Obtener columnas
                cursor.execute(f"""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                columns = [{"name": row[0], "type": row[1]} for row in cursor.fetchall()]
                
                info["tables"][table] = {
                    "exists": True,
                    "rows": count,
                    "columns": columns
                }
        else:
            # Verificar tablas en SQLite
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            info["tables_list"] = tables
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [{"name": row[1], "type": row[2]} for row in cursor.fetchall()]
                
                info["tables"][table] = {
                    "exists": True,
                    "rows": count,
                    "columns": columns
                }
        
        conn.close()
        
        return {
            "success": True,
            "data": info
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


@app.get("/admin/database-stats")
async def database_stats():
    """
    Estadísticas rápidas de la base de datos
    """
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(500, "Error de conexión")
        
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        
        stats = {
            "database_type": database_type,
            "timestamp": datetime.now().isoformat()
        }
        
        # Contar usuarios
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        stats["total_usuarios"] = cursor.fetchone()[0]
        
        # Contar facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        stats["total_facturas"] = cursor.fetchone()[0]
        
        # Contar productos
        cursor.execute("SELECT COUNT(*) FROM productos")
        stats["total_productos"] = cursor.fetchone()[0]
        
        # Últimas 5 facturas
        if database_type == "postgresql":
            cursor.execute("""
                SELECT id, establecimiento, fecha_cargue 
                FROM facturas 
                ORDER BY fecha_cargue DESC 
                LIMIT 5
            """)
        else:
            cursor.execute("""
                SELECT id, establecimiento, fecha_cargue 
                FROM facturas 
                ORDER BY fecha_cargue DESC 
                LIMIT 5
            """)
        
        ultimas_facturas = []
        for row in cursor.fetchall():
            ultimas_facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "fecha": str(row[2])
            })
        
        stats["ultimas_facturas"] = ultimas_facturas
        
        conn.close()
        
        return {
            "success": True,
            "stats": stats
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

def es_codigo_peso_variable(codigo):
    """Detecta códigos de peso variable o PLU temporal"""
    if not codigo or len(codigo) < 6:
        return False
    
    if codigo.startswith('29') and len(codigo) >= 12:
        return True
    
    if codigo.startswith('2') and len(codigo) == 13:
        if codigo.count('0') > 5:
            return True
    
    return False

def generar_codigo_unico(nombre, factura_id, posicion):
    """Genera código único basado en el nombre"""
    import hashlib
    
    if not nombre or len(nombre) < 3:
        return f"AUTO_{factura_id}_{posicion}"
    
    nombre_norm = nombre.upper().strip()
    hash_obj = hashlib.md5(nombre_norm.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    
    return f"PLU_{hash_hex}"

def generar_codigo_unico(nombre, factura_id, posicion):
    """Genera código único basado en el nombre"""
    import hashlib
    
    if not nombre or len(nombre) < 3:
        return f"AUTO_{factura_id}_{posicion}"
    
    nombre_norm = nombre.upper().strip()
    hash_obj = hashlib.md5(nombre_norm.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    
    return f"PLU_{hash_hex}"

@app.post("/invoices/process-async")
async def process_invoice_async(
    file: UploadFile = File(...),
    usuario_id: int = Form(...)
):
    """Procesamiento asíncrono - retorna inmediatamente"""
    try:
        # Guardar archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        # Crear factura con estado "procesando"
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """INSERT INTO facturas (usuario_id, establecimiento, estado, fecha_cargue) 
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (usuario_id, "Procesando...", "procesando", datetime.now())
            )
            factura_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                "INSERT INTO facturas (usuario_id, establecimiento, fecha_cargue) VALUES (?, ?, ?)",
                (usuario_id, "Procesando...", datetime.now())
            )
            factura_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        
        # Procesar en background
        import threading
        thread = threading.Thread(
            target=procesar_factura_background,
            args=(temp_file_path, factura_id, usuario_id)
        )
        thread.start()
        
        return {
            "success": True,
            "factura_id": factura_id,
            "estado": "procesando",
            "mensaje": "Factura en proceso. Consulta el estado en 30-60 segundos"
        }
        
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

# ENDPOINT 1: Recordatorios personales
@app.get("/recordatorios/{usuario_id}")
async def obtener_recordatorios(usuario_id: int):
    """Obtiene productos que el usuario debe comprar pronto"""
    recordatorios = obtener_recordatorios_pendientes(usuario_id)
    
    return {
        "usuario_id": usuario_id,
        "recordatorios": [
            {
                "codigo": r[0],
                "nombre": r[1],
                "frecuencia_dias": r[2],
                "ultima_compra": str(r[3]),
                "proxima_compra": str(r[4]),
                "dias_restantes": int(r[5])
            } for r in recordatorios
        ]
    }

# ENDPOINT 2: Comparar precios
@app.get("/comparar-precios/{codigo_ean}")
async def comparar_precios(codigo_ean: str):
    """Compara precios del producto en diferentes establecimientos"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Obtener producto
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT nombre, precio_promedio FROM productos_maestro WHERE codigo_ean = %s
        """, (codigo_ean,))
    else:
        cursor.execute("""
            SELECT nombre, precio_promedio FROM productos_maestro WHERE codigo_ean = ?
        """, (codigo_ean,))
    
    producto = cursor.fetchone()
    if not producto:
        conn.close()
        raise HTTPException(404, "Producto no encontrado")
    
    # Obtener precios por cadena (últimos 30 días)
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT 
                cadena,
                AVG(precio)::INTEGER as precio_promedio,
                MIN(precio) as precio_minimo,
                COUNT(*) as reportes
            FROM precios_historicos ph
            JOIN productos_maestro pm ON ph.producto_id = pm.id
            WHERE pm.codigo_ean = %s 
                AND ph.fecha_reporte >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY cadena
            ORDER BY precio_promedio ASC
        """, (codigo_ean,))
    else:
        cursor.execute("""
            SELECT 
                'GENERAL' as cadena,
                AVG(precio) as precio_promedio,
                MIN(precio) as precio_minimo,
                COUNT(*) as reportes
            FROM precios_historicos ph
            JOIN productos_maestro pm ON ph.producto_id = pm.id
            WHERE pm.codigo_ean = ?
                AND julianday('now') - julianday(ph.fecha_reporte) <= 30
        """, (codigo_ean,))
    
    precios = cursor.fetchall()
    conn.close()
    
    return {
        "codigo": codigo_ean,
        "nombre": producto[0],
        "precio_promedio_general": producto[1],
        "comparacion": [
            {
                "cadena": p[0],
                "precio_promedio": p[1],
                "precio_minimo": p[2],
                "reportes": p[3]
            } for p in precios
        ]
    }

# ENDPOINT 3: Historial personal
@app.get("/mi-historial/{usuario_id}")
async def obtener_historial_personal(usuario_id: int, limit: int = 50):
    """Obtiene el historial de compras del usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                h.precio_pagado,
                h.establecimiento,
                h.cadena,
                h.fecha_compra
            FROM historial_compras_usuario h
            JOIN productos_maestro pm ON h.producto_id = pm.id
            WHERE h.usuario_id = %s
            ORDER BY h.fecha_compra DESC
            LIMIT %s
        """, (usuario_id, limit))
    else:
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                h.precio_pagado,
                h.establecimiento,
                h.fecha_compra
            FROM historial_compras_usuario h
            JOIN productos_maestro pm ON h.producto_id = pm.id
            WHERE h.usuario_id = ?
            ORDER BY h.fecha_compra DESC
            LIMIT ?
        """, (usuario_id, limit))
    
    historial = cursor.fetchall()
    conn.close()
    
    return {
        "usuario_id": usuario_id,
        "compras": [
            {
                "codigo": h[0],
                "nombre": h[1],
                "precio": h[2],
                "establecimiento": h[3],
                "cadena": h[4] if len(h) > 4 else None,
                "fecha": str(h[5] if len(h) > 5 else h[4])
            } for h in historial
        ]
    }

from database import (
    buscar_o_crear_producto, 
    registrar_precio_historico,
    registrar_compra_personal,
    calcular_patron_compra,
    actualizar_estadisticas_usuario,
    obtener_recordatorios_pendientes
)

def procesar_factura_background(temp_file_path: str, factura_id: int, usuario_id: int):
    """Procesa factura con sistema colaborativo completo"""
    try:
        print(f"🔄 Procesando factura {factura_id}")
        
        # 1. PROCESAR CON OCR
        resultado = process_invoice_products(temp_file_path)
        
        # 2. VALIDAR TOTAL (OBLIGATORIO)
        total_factura = resultado.get("total", 0)
        if not total_factura or total_factura <= 0:
            print(f"❌ Total no detectado")
            actualizar_estado_factura(factura_id, "error", "Total de factura no detectado")
            return
        
        establecimiento = resultado.get("establecimiento", "Desconocido")
        cadena = extraer_cadena(establecimiento)
        productos_detectados = resultado.get("productos", [])
        
        # 3. FILTRAR: SOLO CÓDIGOS EAN VÁLIDOS (8+ dígitos)
        productos_validos = []
        for prod in productos_detectados:
            codigo = prod.get("codigo", "").strip()
            if codigo and len(codigo) >= 8 and codigo.isdigit():
                productos_validos.append(prod)
            else:
                print(f"⚠️ Descartado: {prod.get('nombre', 'Sin nombre')}")
        
        productos_guardados = len(productos_validos)
        productos_totales = len(productos_detectados)
        porcentaje = (productos_guardados / productos_totales * 100) if productos_totales > 0 else 0
        
        print(f"📊 Válidos: {productos_guardados}/{productos_totales} ({porcentaje:.1f}%)")
        
        # 4. GUARDAR EN BASE DE DATOS
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Actualizar factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE facturas 
                SET establecimiento = %s, cadena = %s, total_factura = %s,
                    productos_detectados = %s, productos_guardados = %s,
                    porcentaje_lectura = %s, estado = %s
                WHERE id = %s
            """, (establecimiento, cadena, total_factura, productos_totales, 
                  productos_guardados, porcentaje, "completado", factura_id))
        else:
            cursor.execute("""
                UPDATE facturas 
                SET establecimiento = ?, total_factura = ?,
                    productos_detectados = ?, productos_guardados = ?, estado = ?
                WHERE id = ?
            """, (establecimiento, total_factura, productos_totales,
                  productos_guardados, "completado", factura_id))
        
        # 5. PROCESAR CADA PRODUCTO
        for prod in productos_validos:
            codigo_ean = prod["codigo"]
            nombre = prod.get("nombre", "Sin nombre")
            precio = prod.get("precio", 0)
            
            # A. Agregar al catálogo maestro (colaborativo)
            producto_id = buscar_o_crear_producto(
                cursor, codigo_ean, nombre, precio, es_fresco=False
            )
            
            # B. Registrar precio histórico (para comparar)
            registrar_precio_historico(
                cursor, producto_id, establecimiento, cadena, 
                precio, usuario_id, factura_id
            )
            
            # C. Registrar compra personal (para recordatorios)
            registrar_compra_personal(
                cursor, usuario_id, producto_id, precio,
                establecimiento, cadena, factura_id
            )
            
            # D. Calcular patrón de compra
            patron = calcular_patron_compra(cursor, usuario_id, producto_id)
            if patron:
                print(f"📅 Patrón detectado: {nombre} cada {patron['frecuencia_dias']} días")
        
        # 6. ACTUALIZAR ESTADÍSTICAS DEL USUARIO
        actualizar_estadisticas_usuario(cursor, usuario_id, productos_guardados)
        
        conn.commit()
        conn.close()
        
        print(f"✅ Factura {factura_id} completada: {productos_guardados} productos")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        actualizar_estado_factura(factura_id, "error", str(e))
    finally:
        if os.path.exists(temp_file_path):
            os.unlink(temp_file_path)


def extraer_cadena(establecimiento: str) -> str:
    """Extrae la cadena principal del nombre del establecimiento"""
    cadenas_conocidas = {
        # SUPERMERCADOS
        "JUMBO": "JUMBO",
        "EXITO": "EXITO",
        "CARREFOUR": "CARREFOUR",
        "OLIMPICA": "OLIMPICA",
        "MAKRO": "MAKRO",
        "METRO": "METRO",
        "ALKOSTO": "ALKOSTO",
        "PRICESMART": "PRICESMART",
        
        # TIENDAS DE DESCUENTO
        "D1": "D1",
        "ARA": "ARA",
        "JUSTO Y BUENO": "JUSTO_Y_BUENO",
        "JUSTO & BUENO": "JUSTO_Y_BUENO",
        
        # DROGUERÍAS
        "CRUZ VERDE": "CRUZ_VERDE",
        "CRUZVERDE": "CRUZ_VERDE",
        "DROGAS LA REBAJA": "LA_REBAJA",
        "LA REBAJA": "LA_REBAJA",
        "CAFAM": "CAFAM",
        "LOCATEL": "LOCATEL",
        "DROGUERIA COLSUBSIDIO": "COLSUBSIDIO",
        "COLSUBSIDIO": "COLSUBSIDIO",
        "FARMATODO": "FARMATODO",
        "DROGAS COMFENALCO": "COMFENALCO",
        "COMFENALCO": "COMFENALCO",
        
        # TIENDAS ESPECIALIZADAS
        "HOME CENTER": "HOME_CENTER",
        "HOMECENTER": "HOME_CENTER",
        "FALABELLA": "FALABELLA",
        "CONSTRUCTOR": "CONSTRUCTOR",
        "EPA": "EPA",
        
        # TIENDAS ONLINE/DELIVERY
        "MERQUEO": "MERQUEO",
        "RAPPI": "RAPPI",
        "DOMICILIOS.COM": "DOMICILIOS",
        
        # TIENDAS REGIONALES
        "COLSUBSIDIO": "COLSUBSIDIO",
        "LA 14": "LA_14",
        "SUPERINTER": "SUPERINTER",
        "CAÑAVERAL": "CAÑAVERAL",
        "SURTIMAX": "SURTIMAX"
    }
    
    establecimiento_upper = establecimiento.upper().strip()
    
    # Buscar coincidencias
    for cadena, nombre_normalizado in cadenas_conocidas.items():
        if cadena in establecimiento_upper:
            return nombre_normalizado
    
    # Si no encuentra coincidencia, intentar extraer primera palabra significativa
    palabras = establecimiento_upper.split()
    if palabras:
        primera_palabra = palabras[0]
        # Excluir palabras genéricas
        if primera_palabra not in ["DROGUERIA", "DROGUERIAS", "SUPERMERCADO", "TIENDA", "ALMACEN"]:
            return primera_palabra
    
    return "OTRO"


def actualizar_estado_factura(factura_id: int, estado: str, mensaje: str = ""):
    """Actualiza el estado de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "UPDATE facturas SET estado = %s, establecimiento = %s WHERE id = %s",
                (estado, mensaje or estado, factura_id)
            )
        else:
            cursor.execute(
                "UPDATE facturas SET estado = ? WHERE id = ?",
                (estado, factura_id)
            )
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error actualizando estado: {e}")

def actualizar_estado_factura(factura_id, estado):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE facturas SET estado = %s WHERE id = %s", (estado, factura_id))
    conn.commit()
    conn.close()

from database import (
    obtener_productos_frecuentes_faltantes,
    confirmar_producto_manual
)

@app.get("/facturas/{factura_id}/sugerencias-faltantes")
async def obtener_sugerencias_productos(factura_id: int):
    """
    Después de procesar factura, retorna máximo 3 sugerencias de productos
    que el usuario probablemente compró pero no se detectaron.
    
    Totalmente opcional - el usuario puede omitir.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Obtener info de la factura
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT usuario_id, productos_guardados, establecimiento
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
    else:
        cursor.execute("""
            SELECT usuario_id, productos_guardados, establecimiento
            FROM facturas 
            WHERE id = ?
        """, (factura_id,))
    
    factura_data = cursor.fetchone()
    if not factura_data:
        conn.close()
        raise HTTPException(404, "Factura no encontrada")
    
    usuario_id = factura_data[0]
    productos_detectados_count = factura_data[1]
    
    # Obtener códigos detectados
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT pm.codigo_ean 
            FROM historial_compras_usuario hc
            JOIN productos_maestro pm ON hc.producto_id = pm.id
            WHERE hc.factura_id = %s
        """, (factura_id,))
    else:
        cursor.execute("""
            SELECT pm.codigo_ean 
            FROM historial_compras_usuario hc
            JOIN productos_maestro pm ON hc.producto_id = pm.id
            WHERE hc.factura_id = ?
        """, (factura_id,))
    
    codigos_detectados = {r[0] for r in cursor.fetchall()}
    conn.close()
    
    # Obtener sugerencias (máximo 3)
    sugerencias = obtener_productos_frecuentes_faltantes(
        usuario_id, 
        codigos_detectados,
        limite=3
    )
    
    return {
        "factura_id": factura_id,
        "productos_detectados": productos_detectados_count,
        "tiene_sugerencias": len(sugerencias) > 0,
        "sugerencias": sugerencias,
        "mensaje_ayuda": "Estos son productos que sueles comprar. ¿Los compraste en esta factura?" if sugerencias else None
    }


@app.post("/facturas/{factura_id}/confirmar-producto-faltante")
async def confirmar_producto_faltante(
    factura_id: int,
    codigo_ean: str = Form(...),
    precio: int = Form(...)
):
    """
    Usuario confirma que SÍ compró un producto que no se detectó.
    Lo agrega manualmente a la factura.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Obtener usuario_id
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("SELECT usuario_id FROM facturas WHERE id = %s", (factura_id,))
    else:
        cursor.execute("SELECT usuario_id FROM facturas WHERE id = ?", (factura_id,))
    
    resultado = cursor.fetchone()
    conn.close()
    
    if not resultado:
        raise HTTPException(404, "Factura no encontrada")
    
    usuario_id = resultado[0]
    
    # Confirmar producto
    exito = confirmar_producto_manual(factura_id, codigo_ean, precio, usuario_id)
    
    if exito:
        return {
            "success": True,
            "mensaje": "Producto agregado correctamente"
        }
    else:
        raise HTTPException(500, "Error agregando producto")


@app.get("/usuarios/{usuario_id}/recordatorios-medicamentos")
async def obtener_recordatorios_medicamentos(usuario_id: int):
    """
    Endpoint especial para recordatorios de medicamentos.
    Crítico para adultos mayores y personas con tratamientos crónicos.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    from datetime import datetime, timedelta
    hoy = datetime.now()
    ventana = hoy + timedelta(days=3)  # Alertar 3 días antes
    
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.proxima_compra_estimada,
                EXTRACT(DAY FROM (pc.proxima_compra_estimada - CURRENT_TIMESTAMP)) as dias_restantes,
                pm.precio_promedio
            FROM patrones_compra pc
            JOIN productos_maestro pm ON pc.producto_id = pm.id
            WHERE pc.usuario_id = %s 
                AND pm.categoria = 'MEDICAMENTOS'
                AND pc.recordatorio_activo = TRUE
                AND pc.proxima_compra_estimada <= %s
                AND pc.veces_comprado >= 2
            ORDER BY pc.proxima_compra_estimada ASC
        """, (usuario_id, ventana))
    else:
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.proxima_compra_estimada,
                julianday(pc.proxima_compra_estimada) - julianday('now') as dias_restantes
            FROM patrones_compra pc
            JOIN productos_maestro pm ON pc.producto_id = pm.id
            WHERE pc.usuario_id = ?
                AND pc.proxima_compra_estimada <= datetime('now', '+3 days')
                AND pc.veces_comprado >= 2
            ORDER BY pc.proxima_compra_estimada ASC
        """, (usuario_id,))
    
    recordatorios = cursor.fetchall()
    conn.close()
    
    # Clasificar por urgencia
    criticos = []  # < 1 día
    proximos = []  # 1-3 días
    
    for r in recordatorios:
        codigo = r[0]
        nombre = r[1]
        frecuencia = r[2]
        dias_restantes = int(r[5])
        precio_promedio = r[6] if len(r) > 6 else 0
        
        recordatorio = {
            "codigo": codigo,
            "nombre": nombre,
            "frecuencia_dias": frecuencia,
            "dias_restantes": dias_restantes,
            "precio_estimado": precio_promedio,
            "urgente": dias_restantes <= 0
        }
        
        if dias_restantes <= 1:
            criticos.append(recordatorio)
        else:
            proximos.append(recordatorio)
    
    return {
        "usuario_id": usuario_id,
        "medicamentos_criticos": criticos,
        "medicamentos_proximos": proximos,
        "total_recordatorios": len(criticos) + len(proximos),
        "mensaje": "Recordatorios de medicamentos activos"
    }


@app.post("/usuarios/{usuario_id}/desactivar-recordatorio-medicamento")
async def desactivar_recordatorio_medicamento(
    usuario_id: int,
    codigo_ean: str = Form(...)
):
    """
    Permite al usuario desactivar recordatorios de un medicamento específico.
    Útil cuando cambian de tratamiento.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            UPDATE patrones_compra
            SET recordatorio_activo = FALSE
            WHERE usuario_id = %s 
                AND producto_id = (
                    SELECT id FROM productos_maestro WHERE codigo_ean = %s
                )
        """, (usuario_id, codigo_ean))
    else:
        cursor.execute("""
            UPDATE patrones_compra
            SET recordatorio_activo = 0
            WHERE usuario_id = ? 
                AND producto_id = (
                    SELECT id FROM productos_maestro WHERE codigo_ean = ?
                )
        """, (usuario_id, codigo_ean))
    
    conn.commit()
    conn.close()
    
    return {
        "success": True,
        "mensaje": "Recordatorio desactivado"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

















