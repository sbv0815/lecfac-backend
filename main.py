from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import os
import tempfile
from datetime import datetime

# Importar el procesador y database
from invoice_processor import process_invoice_products
from database import create_tables, get_db_connection, hash_password, verify_password, test_database_connection

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
    """Inicialización de la aplicación"""
    print("🚀 Iniciando LecFac API...")
    
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
    """Guardar factura con productos en catálogo colaborativo"""
    try:
        print(f"=== GUARDANDO FACTURA EN CATÁLOGO COLABORATIVO ===")
        print(f"Usuario ID: {invoice.usuario_id}")
        print(f"Establecimiento: {invoice.establecimiento}")
        print(f"Productos recibidos: {len(invoice.productos)}")
        
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar cadena (éxito, carulla, olímpica, etc)
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
            
            # Generar código si falta
            if not codigo or codigo == 'None':
                codigo = f"AUTO_{factura_id}_{i+1}"
                print(f"⚠️ Código auto-generado: {codigo}")
            
            # Validar nombre
            if not nombre or len(str(nombre).strip()) < 2:
                nombre = "Producto sin descripción"
            
            # Validar valor
            if valor is None or valor == '':
                valor = 0
            
            try:
                valor_int = int(valor)
                
                if database_type == "postgresql":
                    # 1. Verificar si el producto existe en catálogo
                    cursor.execute(
                        "SELECT codigo, nombre_producto, total_reportes FROM productos_catalogo WHERE codigo = %s",
                        (codigo,)
                    )
                    producto_existente = cursor.fetchone()
                    
                    if producto_existente:
                        # Producto ya existe - actualizar contadores
                        print(f"✓ Producto existente en catálogo: {codigo}")
                        cursor.execute(
                            """UPDATE productos_catalogo 
                               SET total_reportes = total_reportes + 1,
                                   ultimo_reporte = %s
                               WHERE codigo = %s""",
                            (datetime.now(), codigo)
                        )
                        productos_actualizados += 1
                    else:
                        # Producto nuevo - agregar a catálogo
                        print(f"✓ Producto NUEVO en catálogo: {codigo}")
                        cursor.execute(
                            """INSERT INTO productos_catalogo 
                               (codigo, nombre_producto, primera_fecha_reporte, total_reportes, ultimo_reporte)
                               VALUES (%s, %s, %s, 1, %s)""",
                            (codigo, nombre, datetime.now(), datetime.now())
                        )
                        productos_nuevos += 1
                    
                    # 2. Registrar precio en el supermercado
                    cursor.execute(
                        """INSERT INTO precios_productos 
                           (codigo_producto, establecimiento, cadena, precio, usuario_id, factura_id, fecha_reporte)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (codigo, invoice.establecimiento, cadena, valor_int, invoice.usuario_id, factura_id, datetime.now())
                    )
                    
                    # 3. También guardar en tabla legacy (temporal)
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (%s, %s, %s, %s)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                    
                else:
                    # SQLite fallback
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (?, ?, ?, ?)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                
                productos_guardados += 1
                print(f"✓ Guardado: {codigo} - {nombre} - ${valor_int} en {cadena}")
                
            except Exception as e:
                print(f"❌ Error guardando producto {i+1}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n=== RESULTADO ===")
        print(f"Productos guardados: {productos_guardados}/{len(invoice.productos)}")
        print(f"Productos nuevos en catálogo: {productos_nuevos}")
        print(f"Productos actualizados: {productos_actualizados}")
        
        return {
            "success": True, 
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "total_productos": len(invoice.productos),
            "productos_nuevos": productos_nuevos,
            "productos_actualizados": productos_actualizados,
            "message": f"Factura guardada: {productos_guardados} productos ({productos_nuevos} nuevos, {productos_actualizados} actualizados)"
        }
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error guardando factura: {str(e)}")

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

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)



