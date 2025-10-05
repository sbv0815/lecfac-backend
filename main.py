from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime
import os
import tempfile
import traceback
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from database import (
    create_tables, 
    get_db_connection, 
    hash_password, 
    verify_password, 
    test_database_connection,
    obtener_productos_frecuentes_faltantes,
    confirmar_producto_manual
)
from claude_invoice import parse_invoice_with_claude
from fastapi.responses import Response, FileResponse
from admin_dashboard import router as admin_dashboard_router
from auth_routes import router as auth_router
import uuid
import json
from typing import Optional
from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends, Header, Request
from typing import List, Optional
from ocr_processor import processor, ocr_queue, processing
from audit_system import audit_scheduler, AuditSystem

processor.start()


# ========================================
# CONFIGURACIÓN DE LA APP
# ========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización y cierre de la aplicación"""
    print("🚀 Iniciando LecFac API...")
    processor.start()
    if test_database_connection():
        print("📊 Conexión a base de datos exitosa")
    else:
        print("❌ Error de conexión a base de datos")
    try:
        create_tables()
        print("🗃️ Tablas verificadas/creadas")
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")
    yield
    processor.stop()
    print("👋 Cerrando LecFac API...")

app = FastAPI(
    title="LecFac API", 
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin_dashboard_router)
app.include_router(auth_router)


# Endpoints para servir HTML


@app.get("/test")
async def test_page():
    return FileResponse("test.html")

@app.get("/dashboard")
async def dashboard():
    return FileResponse("admin_dashboard.html")

@app.get("/editor")
async def editor_page():
    return FileResponse("editor.html")

# Agregar este endpoint que falta
@app.get("/admin/facturas")
async def listar_facturas():
    """Listar todas las facturas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT f.id, f.establecimiento, f.total_factura, 
                   f.fecha_cargue, f.estado_validacion, f.cadena,
                   COUNT(pp.id) as productos
            FROM facturas f
            LEFT JOIN precios_productos pp ON f.id = pp.factura_id
            GROUP BY f.id, f.establecimiento, f.total_factura, 
                     f.fecha_cargue, f.estado_validacion, f.cadena
            ORDER BY f.id DESC
            LIMIT 100
        """)
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "total_factura": float(row[2]) if row[2] else 0,
                "fecha": row[3].isoformat() if row[3] else None,
                "estado": row[4],
                "cadena": row[5],
                "productos": row[6]
            })
        
        conn.close()
        
        return {"success": True, "facturas": facturas}
        
    except Exception as e:
        raise HTTPException(500, str(e))

# Agregar el endpoint de estadísticas
@app.get("/admin/stats")
async def get_stats():
    """Obtener estadísticas del dashboard"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Total facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        # Productos únicos
        cursor.execute("SELECT COUNT(*) FROM productos_catalogo")
        productos_unicos = cursor.fetchone()[0]
        
        # Facturas pendientes
        cursor.execute("SELECT COUNT(*) FROM facturas WHERE estado_validacion = 'pendiente'")
        facturas_pendientes = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total_facturas": total_facturas,
            "productos_unicos": productos_unicos,
            "facturas_pendientes": facturas_pendientes
        }
        
    except Exception as e:
        return {
            "total_facturas": 0,
            "productos_unicos": 0,
            "facturas_pendientes": 0,
            "error": str(e)
        }

async def get_current_user(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="No autorizado")
    # Aquí decodifica el token y obtén el user_id
    # Por ahora, simulamos:
    return {"user_id": "user123", "email": "user@example.com"}

# Función para verificar si es admin
async def require_admin(user = Depends(get_current_user)):
    # Verificar si el usuario es admin
    # Por ahora simulamos:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requiere permisos de admin")
    return user
# ========================================
# MODELOS PYDANTIC
# ========================================

class UserRegister(BaseModel):
    email: str
    password: str
    nombre: str | None = None

class UserLogin(BaseModel):
    email: str
    password: str

class SaveInvoice(BaseModel):
    usuario_id: int
    establecimiento: str
    productos: list
    temp_file_path: str = None  

class ProductoItem(BaseModel):  
    nombre: str
    cantidad: int = 1
    precio: float
    codigo: Optional[str] = None

class InvoiceConfirm(BaseModel):
    establecimiento: str
    fecha: str
    total: float
    productos: List[ProductoItem]
    user_id: Optional[str] = None
    user_email: Optional[str] = None
# ========================================
# FUNCIONES AUXILIARES
# ========================================

def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """Ejecuta queries con PostgreSQL"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de conexión a base de datos")
    try:
        cursor = conn.cursor()
        query = query.replace("?", "%s")
        cursor.execute(query, params or ())
        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        else:
            result = cursor.rowcount
        conn.commit()
        return result
    except Exception as e:
        raise e
    finally:
        try:
            conn.close()
        except:
            pass

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena del establecimiento"""
    establecimiento_lower = establecimiento.lower()
    cadenas = {
        'exito': ['exito', 'éxito'],
        'jumbo': ['jumbo'],
        'olimpica': ['olimpica', 'olímpica'],
        'd1': ['d1', 'tiendas d1'],
        'carulla': ['carulla'],
        'alkosto': ['alkosto'],
        'cruz_verde': ['cruz verde', 'cruzverde'],
        'la_rebaja': ['la rebaja', 'drogas la rebaja'],
        'cafam': ['cafam'],
    }
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    return 'otro'

def es_codigo_peso_variable(codigo):
    """Detecta códigos de peso variable"""
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
    nombre_norm = (nombre or "").upper().strip()
    hash_obj = hashlib.md5(nombre_norm.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    return f"PLU_{hash_hex}"

def manejar_producto_ean(cursor, codigo_ean: str, nombre: str) -> int:
    """Maneja producto con código EAN"""
    cursor.execute(
        "SELECT id FROM productos_catalogo WHERE codigo_ean = %s",
        (codigo_ean,)
    )
    resultado = cursor.fetchone()
    if resultado:
        producto_id = resultado[0]
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        return producto_id
    else:
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (codigo_ean, nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, %s, FALSE, %s, 1, %s) RETURNING id""",
            (codigo_ean, nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        return producto_id

def manejar_producto_fresco(cursor, codigo_local: str, nombre: str, cadena: str) -> int:
    """Maneja producto fresco"""
    cursor.execute(
        "SELECT producto_id FROM codigos_locales WHERE cadena = %s AND codigo_local = %s",
        (cadena, codigo_local)
    )
    resultado = cursor.fetchone()
    if resultado:
        producto_id = resultado[0]
        cursor.execute(
            """UPDATE productos_catalogo 
               SET total_reportes = total_reportes + 1,
                   ultimo_reporte = %s
               WHERE id = %s""",
            (datetime.now(), producto_id)
        )
        return producto_id
    else:
        cursor.execute(
            """INSERT INTO productos_catalogo 
               (nombre_producto, es_producto_fresco, primera_fecha_reporte, total_reportes, ultimo_reporte)
               VALUES (%s, TRUE, %s, 1, %s) RETURNING id""",
            (nombre, datetime.now(), datetime.now())
        )
        producto_id = cursor.fetchone()[0]
        cursor.execute(
            """INSERT INTO codigos_locales (producto_id, cadena, codigo_local)
               VALUES (%s, %s, %s)""",
            (producto_id, cadena, codigo_local)
        )
        return producto_id



# ========================================
# ENDPOINTS BÁSICOS
# ========================================

@app.get("/")
async def root():
    return {
        "message": "LecFac API funcionando", 
        "version": "2.0.0",
        "database": "postgresql",
        "ocr_engine": "openai-vision",
        "status": "active"
    }

@app.get("/health")
async def health_check():
    try:
        conn = get_db_connection()
        if conn:
            conn.close()
            db_status = "connected"
        else:
            db_status = "disconnected"
        return {
            "status": "healthy" if db_status == "connected" else "unhealthy",
            "database": db_status,
            "openai_configured": bool(os.environ.get("OPENAI_API_KEY")),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

# ========================================
# ENDPOINTS DE USUARIOS
# ========================================

@app.post("/users/register")
async def register_user(user: UserRegister):
    try:
        existing = execute_query(
            "SELECT id FROM usuarios WHERE email = %s", 
            (user.email,), 
            fetch_one=True
        )
        if existing:
            raise HTTPException(400, "El email ya está registrado")
        
        password_hash = hash_password(user.password)
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO usuarios (email, password_hash, nombre) VALUES (%s, %s, %s) RETURNING id",
            (user.email, password_hash, user.nombre)
        )
        user_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()
        
        return {"success": True, "user_id": user_id, "message": "Usuario creado"}
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.post("/users/login")
async def login_user(user: UserLogin):
    try:
        result = execute_query(
            "SELECT id, password_hash, nombre FROM usuarios WHERE email = %s", 
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

@app.get("/users/{user_id}/invoices")
async def get_user_invoices(user_id: int):
    try:
        query = """SELECT f.id, f.establecimiento, f.fecha_cargue, 
                   COUNT(p.id) as total_productos
                   FROM facturas f 
                   LEFT JOIN productos p ON f.id = p.factura_id
                   WHERE f.usuario_id = %s 
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

# ========================================
# ENDPOINTS DE FACTURAS
# ========================================

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """Procesa factura con Claude Vision"""
    try:
        # Guardar temporalmente
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        # Procesar con Claude
        result = parse_invoice_with_claude(temp_file.name)
        
        if result["success"]:
            # Guardar en BD con estado PENDIENTE
            conn = get_db_connection()
            cursor = conn.cursor()
            
            data = result["data"]
            establecimiento = data.get("establecimiento", "Desconocido")
            total = data.get("total", 0)
            
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, establecimiento, total_factura,
                    fecha_cargue, estado_validacion
                ) VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (1, establecimiento, total, datetime.now(), 'pendiente'))
            
            factura_id = cursor.fetchone()[0]
            
            # Guardar productos
            for prod in data.get("productos", []):
                cursor.execute("""
                    INSERT INTO productos (factura_id, codigo, nombre, valor)
                    VALUES (%s, %s, %s, %s)
                """, (factura_id, prod.get("codigo", ""), prod.get("nombre", ""), prod.get("precio", 0)))
            
            conn.commit()
            conn.close()
            
            # Limpiar archivo temporal
            os.unlink(temp_file.name)
            
            return {
                "success": True,
                "data": data,
                "factura_id": factura_id,
                "status": "pendiente",
                "message": "Factura procesada y pendiente de revisión"
            }
        else:
            os.unlink(temp_file.name)
            return result
            
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/invoices/save")
async def save_invoice(invoice: SaveInvoice):
    """Guardar factura con imagen en PostgreSQL"""
    try:
        print(f"=== GUARDANDO FACTURA ===")
        print(f"Usuario ID: {invoice.usuario_id}")
        print(f"Establecimiento: {invoice.establecimiento}")
        print(f"Productos: {len(invoice.productos)}")
        print(f"Imagen temporal: {invoice.temp_file_path}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cadena = detectar_cadena(invoice.establecimiento)
        
        cursor.execute(
            """INSERT INTO facturas (usuario_id, establecimiento, cadena, fecha_cargue) 
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (invoice.usuario_id, invoice.establecimiento, cadena, datetime.now())
        )
        factura_id = cursor.fetchone()[0]
        print(f"Factura ID: {factura_id}")
        
        # Guardar imagen si viene (4 espacios de indentación)
        if invoice.temp_file_path and os.path.exists(invoice.temp_file_path):
            mime = "image/jpeg" if invoice.temp_file_path.endswith(('.jpg', '.jpeg')) else "image/png"
            save_image_to_db(factura_id, invoice.temp_file_path, mime)
            os.unlink(invoice.temp_file_path)  # Limpiar temp
            print(f"✓ Imagen guardada para factura {factura_id}")

        
        if invoice.temp_file_path and os.path.exists(invoice.temp_file_path):
            mime = "image/jpeg" if invoice.temp_file_path.endswith(('.jpg', '.jpeg')) else "image/png"
            save_image_to_db(factura_id, invoice.temp_file_path, mime)
            os.unlink(invoice.temp_file_path)
            print(f"✓ Imagen guardada para factura {factura_id}")
        
        # Guardar productos
        productos_guardados = 0
        for i, producto in enumerate(invoice.productos):
            codigo = producto.get('codigo')
            nombre = producto.get('nombre') or "Producto sin descripción"
            valor = int(producto.get('valor', 0))
            
            try:
                codigo_valido = codigo and codigo != 'None' and len(codigo) >= 3
                
                if codigo_valido and es_codigo_peso_variable(codigo):
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                
                if not codigo_valido:
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                
                es_fresco = (
                    (len(codigo) < 7 and codigo.isdigit()) or 
                    codigo.startswith('PLU_') or 
                    codigo.startswith('AUTO_')
                )
                
                if es_fresco:
                    producto_id = manejar_producto_fresco(cursor, codigo, nombre, cadena)
                else:
                    producto_id = manejar_producto_ean(cursor, codigo, nombre)
                
                    # ✅ CORRECTO (sin fecha_registro)
                    cursor.execute("""
                    INSERT INTO precios_productos (
                    producto_id,
                    factura_id,
                    precio,
                    establecimiento,
                    cadena
                        )
                    VALUES (%s, %s, %s, %s, %s)
                    """, (
                    producto_id,
                    factura_id,
                    precio,
                    establecimiento,
                    cadena
                        ))
                
                cursor.execute(
                    "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (%s, %s, %s, %s)",
                    (factura_id, codigo, nombre, valor)
                )
                
                productos_guardados += 1
                
            except Exception as e:
                print(f"Error producto {i+1}: {e}")
        
        conn.commit()
        
        # Guardar imagen
        imagen_guardada = False
        if invoice.temp_file_path and os.path.exists(invoice.temp_file_path):
            mime = "image/jpeg" if invoice.temp_file_path.endswith(('.jpg', '.jpeg')) else "image/png"
            imagen_guardada = save_image_to_db(factura_id, invoice.temp_file_path, mime)
            
            # Limpiar archivo temporal
            try:
                os.unlink(invoice.temp_file_path)
                print("✓ Archivo temporal eliminado")
            except:
                pass
        
        conn.close()
        
        print(f"Productos guardados: {productos_guardados}/{len(invoice.productos)}")
        print(f"Imagen guardada: {imagen_guardada}")
        
        return {
            "success": True, 
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,
            "message": f"Factura guardada con {productos_guardados} productos"
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Error: {str(e)}")

@app.delete("/invoices/{factura_id}")
async def delete_invoice(factura_id: int, usuario_id: int):
    try:
        result = execute_query(
            "SELECT id FROM facturas WHERE id = %s AND usuario_id = %s", 
            (factura_id, usuario_id),
            fetch_one=True
        )
        if not result:
            raise HTTPException(404, "Factura no encontrada")
        
        execute_query("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        execute_query("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        return {"success": True, "message": "Factura eliminada"}
    except Exception as e:
        raise HTTPException(500, f"Error: {str(e)}")

@app.get("/admin/facturas/{factura_id}/imagen")
async def get_factura_image(factura_id: int):
    """Devuelve la imagen de una factura"""
    image_data, mime_type = get_image_from_db(factura_id)
    
    if not image_data:
        raise HTTPException(404, "Imagen no encontrada")
    
    return Response(content=image_data, media_type=mime_type or "image/jpeg")

# Agregar esto a tu main.py existente


@app.post("/api/mobile/upload-invoice")
async def mobile_upload_invoice(
    image: UploadFile = File(...),
    user_id: str = Form("mobile_user"),
    user_email: Optional[str] = Form(None),
    device_id: Optional[str] = Form(None)
):
    """Endpoint simple para recibir facturas desde Flutter"""
    try:
        # Validar imagen
        if image.size > 10 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Imagen muy grande")
        
        # Leer imagen
        image_bytes = await image.read()
        
        # Guardar temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
            tmp_file.write(image_bytes)
            temp_path = tmp_file.name
        
        # Procesar con claude_invoice.py
        result = parse_invoice_with_claude(temp_path)
        
        if result["success"]:
            ocr_data = result["data"]
            
            # Guardar en BD
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Usar ID entero para facturas
            cursor.execute("""
                INSERT INTO facturas 
                (usuario_id, establecimiento, fecha_cargue, total_factura)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (
                1,  # Usuario por defecto para móvil
                ocr_data.get("establecimiento", "Sin identificar"),
                datetime.now(),
                ocr_data.get("total", 0)
            ))
            
            factura_id = cursor.fetchone()[0]
            
            # Guardar productos
            for prod in ocr_data.get("productos", []):
                cursor.execute("""
                    INSERT INTO productos (factura_id, codigo, nombre, valor)
                    VALUES (%s, %s, %s, %s)
                """, (
                    factura_id,
                    prod.get("codigo", ""),
                    prod.get("nombre", ""),
                    prod.get("precio", 0)
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Limpiar archivo temporal
            os.remove(temp_path)
            
            return {
                "success": True,
                "invoice_id": factura_id,
                "ocr_result": ocr_data,
                "message": "Factura procesada exitosamente"
            }
        else:
            os.remove(temp_path)
            return {
                "success": False,
                "error": result.get("error", "Error en OCR")
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


@app.post("/invoices/upload")
async def upload_invoice(
    file: UploadFile = File(...),
    usuario_id: int = Form(...)
):
    """Procesar factura con OCR y guardarla"""
    try:
        print(f"=== PROCESANDO FACTURA ===")
        print(f"Archivo: {file.filename}")
        print(f"Usuario: {usuario_id}")
        
        # Guardar temporalmente
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        print(f"Archivo temporal: {temp_file.name}")
        
        # Procesar con OCR
        resultado = parse_invoice_with_claude(temp_file.name)
        
        if not resultado["success"]:
            os.unlink(temp_file.name)
            return {"success": False, "error": resultado["error"]}
        
        # Preparar datos para guardar
        establecimiento = resultado["data"].get("establecimiento", "Desconocido")
        productos = resultado["data"].get("productos", [])
        
        # Guardar en BD
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cadena = detectar_cadena(establecimiento)
        
        cursor.execute(
            """INSERT INTO facturas (usuario_id, establecimiento, cadena, fecha_cargue) 
               VALUES (%s, %s, %s, %s) RETURNING id""",
            (usuario_id, establecimiento, cadena, datetime.now())
        )
        factura_id = cursor.fetchone()[0]
        print(f"✓ Factura ID: {factura_id}")
        
        # Guardar imagen
        mime = "image/jpeg" if file.filename.endswith(('.jpg', '.jpeg')) else "image/png"
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, mime)
        print(f"✓ Imagen guardada: {imagen_guardada}")
        
        # Limpiar archivo temporal
        try:
            os.unlink(temp_file.name)
        except Exception:
            pass
        
        # Guardar productos
        productos_guardados = 0
        for prod in productos:
            codigo = prod.get("codigo", "")
            nombre = prod.get("nombre", "")
            precio = prod.get("precio", 0) or prod.get("valor", 0)
            
            if not codigo or not nombre:
                continue
            
            cursor.execute("""
                SELECT id FROM productos_catalogo 
                WHERE codigo_ean = %s
            """, (codigo,))
            
            producto = cursor.fetchone()
            
            if producto:
                producto_id = producto[0]
            else:
                cursor.execute("""
                    INSERT INTO productos_catalogo (codigo_ean, nombre_producto)
                    VALUES (%s, %s) RETURNING id
                """, (codigo, nombre))
                producto_id = cursor.fetchone()[0]
            
            # Guardar precio
            cursor.execute("""
                INSERT INTO precios_productos (
                    producto_id, factura_id, precio, establecimiento, cadena
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (producto_id, factura_id, precio, establecimiento, cadena))
            
            productos_guardados += 1
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,
            "establecimiento": establecimiento
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        if 'temp_file' in locals() and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/invoices/save-with-image")
async def save_invoice_with_image(
    file: UploadFile = File(...),
    usuario_id: int = Form(...),
    establecimiento: str = Form(...),
    total: float = Form(0),
    productos: str = Form(...)
):
    """Guardar factura con imagen + validación de calidad"""
    import json

    temp_file = None
    conn = None
    cursor = None

    try:
        print("=== GUARDANDO FACTURA CON IMAGEN ===")
        print(f"Usuario: {usuario_id}")
        print(f"Establecimiento: {establecimiento}")
        print(f"Total: {total}")

        # Parsear productos
        productos_list = json.loads(productos)
        if not isinstance(productos_list, list):
            raise ValueError("productos debe ser un array")
        
        print(f"Productos: {len(productos_list)}")

        # Guardar imagen temporalmente
        content = await file.read()
        if not content:
            raise HTTPException(400, "Imagen vacía")

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(content)
        temp_file.close()

        # Conexión BD
        conn = get_db_connection()
        cursor = conn.cursor()

        cadena = detectar_cadena(establecimiento) or ""

        # Validación
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=establecimiento,
            total=total,
            tiene_imagen=True,
            productos=productos_list,
            cadena=cadena
        )

        print(f"Validación: {puntaje}/100 - {estado}")
        for alerta in alertas:
            print(f"  - {alerta}")

        # Crear factura
        cursor.execute("""
            INSERT INTO facturas (
                usuario_id, establecimiento, cadena,
                fecha_cargue, total_factura, estado_validacion,
                puntaje_calidad, tiene_imagen
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            usuario_id, establecimiento, cadena,
            datetime.now(), total, estado,
            puntaje, True
        ))
        factura_id = cursor.fetchone()[0]
        print(f"✓ Factura ID: {factura_id}")

        # Guardar productos
        productos_guardados = 0
        for prod in productos_list:
            codigo = str(prod.get("codigo", "") or "").strip()
            nombre = str(prod.get("nombre", "") or "").strip()
            precio_val = prod.get("precio", 0)
            
            try:
                if isinstance(precio_val, str):
                    precio_val = precio_val.replace(",", ".").strip()
                precio = float(precio_val)
            except:
                precio = 0.0

            if not codigo or not nombre:
                continue

            # Buscar o crear producto
            cursor.execute("SELECT id FROM productos_catalogo WHERE codigo_ean = %s", (codigo,))
            row = cursor.fetchone()

            if row:
                producto_id = row[0]
            else:
                cursor.execute(
                    "INSERT INTO productos_catalogo (codigo_ean, nombre_producto) VALUES (%s, %s) RETURNING id",
                    (codigo, nombre)
                )
                producto_id = cursor.fetchone()[0]

            # Guardar precio
            cursor.execute("""
                INSERT INTO precios_productos (producto_id, factura_id, precio, establecimiento, cadena)
                VALUES (%s, %s, %s, %s, %s)
            """, (producto_id, factura_id, precio, establecimiento, cadena))

            productos_guardados += 1

        # COMMIT PRIMERO
        conn.commit()
        cursor.close()
        conn.close()
        conn = None
        cursor = None

        print(f"✓ {productos_guardados} productos guardados")

        # AHORA guardar imagen (después del commit)
        mime = "image/jpeg"
        if file.filename and file.filename.lower().endswith(".png"):
            mime = "image/png"
        
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, mime)

        # Limpiar temporal
        try:
            os.unlink(temp_file.name)
            temp_file = None
        except:
            pass

        print("======================================================================")

        return {
            "success": True,
            "factura_id": factura_id,
            "validacion": {
                "puntaje": puntaje,
                "estado": estado,
                "alertas": alertas
            },
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,
            "mensaje": f"Factura guardada con {productos_guardados} productos"
        }

    except HTTPException:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise
        
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise HTTPException(500, str(e))
        
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except:
                pass
@app.get("/admin/facturas/{factura_id}")
async def get_factura_detalle(factura_id: int):
    """Obtener factura completa con productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Datos de factura
        cursor.execute("""
            SELECT id, establecimiento, total_factura, fecha_cargue,
                   estado_validacion, puntaje_calidad
            FROM facturas WHERE id = %s
        """, (factura_id,))
        
        factura = cursor.fetchone()
        if not factura:
            raise HTTPException(404, "Factura no encontrada")
        
        # Productos
        cursor.execute("""
            SELECT pp.id, pc.codigo_ean, pc.nombre_producto, pp.precio
            FROM precios_productos pp
            JOIN productos_catalogo pc ON pp.producto_id = pc.id
            WHERE pp.factura_id = %s
        """, (factura_id,))
        
        productos = [
            {"id": p[0], "codigo": p[1], "nombre": p[2], "precio": float(p[3])}
            for p in cursor.fetchall()
        ]
        
        conn.close()
        
        return {
            "id": factura[0],
            "establecimiento": factura[1],
            "total_factura": float(factura[2]) if factura[2] else 0,
            "fecha_cargue": factura[3].isoformat() if factura[3] else None,
            "estado_validacion": factura[4],
            "puntaje_calidad": factura[5],
            "productos": productos
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.put("/admin/facturas/{factura_id}")
async def actualizar_factura(factura_id: int, datos: dict):
    """Actualizar factura y productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Actualizar factura
        cursor.execute("""
            UPDATE facturas
            SET establecimiento = %s, total_factura = %s
            WHERE id = %s
        """, (datos['establecimiento'], datos['total_factura'], factura_id))
        
        # Actualizar productos (borrar y recrear)
        cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
        
        for prod in datos['productos']:
            cursor.execute("""
                SELECT id FROM productos_catalogo WHERE codigo_ean = %s
            """, (prod['codigo'],))
            
            resultado = cursor.fetchone()
            if resultado:
                producto_id = resultado[0]
            else:
                cursor.execute("""
                    INSERT INTO productos_catalogo (codigo_ean, nombre_producto)
                    VALUES (%s, %s) RETURNING id
                """, (prod['codigo'], prod['nombre']))
                producto_id = cursor.fetchone()[0]
            
            cursor.execute("""
                INSERT INTO precios_productos (producto_id, factura_id, precio, establecimiento, cadena)
                VALUES (%s, %s, %s, %s, %s)
            """, (producto_id, factura_id, prod['precio'], datos['establecimiento'], 'otro'))
        
        conn.commit()
        conn.close()
        
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))


# En main.py, endpoint temporal de debug
@app.get("/admin/facturas/{factura_id}/debug")
async def debug_factura(factura_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, imagen_mime, 
               CASE WHEN imagen_bytes IS NULL THEN 'NULL' ELSE 'EXISTS' END as imagen_status,
               LENGTH(imagen_bytes) as imagen_size
        FROM facturas WHERE id = %s
    """, (factura_id,))
    result = cursor.fetchone()
    conn.close()
    
    return {
        "factura_id": result[0] if result else None,
        "mime": result[1] if result else None,
        "imagen_status": result[2] if result else None,
        "imagen_size_bytes": result[3] if result else None
    }

@app.get("/admin/facturas/{factura_id}/check-image")
async def check_image(factura_id: int):
    """Debug: verificar si imagen existe"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            id,
            imagen_mime,
            CASE 
                WHEN imagen_data IS NULL THEN 'NULL'
                ELSE 'EXISTS'
            END as status,
            LENGTH(imagen_data) as size_bytes
        FROM facturas 
        WHERE id = %s
    """, (factura_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return {"error": "Factura no encontrada"}
    
    return {
        "factura_id": result[0],
        "imagen_mime": result[1],
        "imagen_status": result[2],
        "imagen_size": result[3]
    }


# ========================================
# ENDPOINTS DE EDICIÓN Y CORRECCIÓN
# ========================================

@app.get("/admin/facturas/{factura_id}/detalle")
async def get_factura_detalle(factura_id: int):
    """Obtener factura completa para edición"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Datos de factura
        cursor.execute("""
            SELECT id, usuario_id, establecimiento, cadena, total_factura, 
                   fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen
            FROM facturas WHERE id = %s
        """, (factura_id,))
        
        factura = cursor.fetchone()
        if not factura:
            raise HTTPException(404, "Factura no encontrada")
        
        # Productos con sus precios
        cursor.execute("""
            SELECT pp.id, pc.codigo_ean, pc.nombre_producto, pp.precio, pp.producto_id
            FROM precios_productos pp
            JOIN productos_catalogo pc ON pp.producto_id = pc.id
            WHERE pp.factura_id = %s
            ORDER BY pp.id
        """, (factura_id,))
        
        productos = [{
            "id": p[0],
            "codigo": p[1],
            "nombre": p[2],
            "precio": float(p[3]),
            "producto_id": p[4]
        } for p in cursor.fetchall()]
        
        conn.close()
        
        return {
            "factura": {
                "id": factura[0],
                "usuario_id": factura[1],
                "establecimiento": factura[2],
                "cadena": factura[3],
                "total": float(factura[4]) if factura[4] else 0,
                "fecha": factura[5].isoformat() if factura[5] else None,
                "estado": factura[6],
                "puntaje": factura[7],
                "tiene_imagen": factura[8]
            },
            "productos": productos
        }
    except Exception as e:
        raise HTTPException(500, str(e))




@app.post("/admin/facturas")
async def crear_factura_desde_dashboard(datos: dict):
    """
    Crear una nueva factura desde el dashboard después del OCR
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Extraer datos
        establecimiento = datos.get('establecimiento', 'Desconocido')
        total_factura = datos.get('total_factura', 0)
        productos = datos.get('productos', [])
        usuario_id = datos.get('usuario_id', 1)  # Usuario por defecto si no se especifica
        
        # Detectar cadena
        cadena = detectar_cadena(establecimiento)
        
        # Crear factura
        cursor.execute("""
            INSERT INTO facturas (
                usuario_id, 
                establecimiento, 
                cadena,
                total_factura,
                fecha_cargue,
                estado_validacion,
                puntaje_calidad,
                tiene_imagen
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            usuario_id,
            establecimiento,
            cadena,
            total_factura,
            datetime.now(),
            'pendiente',  # Estado inicial
            80,  # Puntaje inicial
            False  # Sin imagen por ahora
        ))
        
        factura_id = cursor.fetchone()[0]
        print(f"✅ Factura creada con ID: {factura_id}")
        
        # Guardar productos
        productos_guardados = 0
        for prod in productos:
            try:
                codigo = str(prod.get('codigo_ean', prod.get('codigo', ''))).strip()
                nombre = str(prod.get('nombre_producto', prod.get('nombre', ''))).strip()
                precio = float(prod.get('precio', prod.get('valor', 0)))
                
                if not codigo or not nombre:
                    continue
                
                # Buscar o crear producto en catálogo
                cursor.execute(
                    "SELECT id FROM productos_catalogo WHERE codigo_ean = %s",
                    (codigo,)
                )
                resultado = cursor.fetchone()
                
                if resultado:
                    producto_id = resultado[0]
                else:
                    cursor.execute("""
                        INSERT INTO productos_catalogo (codigo_ean, nombre_producto)
                        VALUES (%s, %s) 
                        RETURNING id
                    """, (codigo, nombre))
                    producto_id = cursor.fetchone()[0]
                
                # Guardar precio
                cursor.execute("""
                    INSERT INTO precios_productos (
                        producto_id, 
                        factura_id, 
                        precio, 
                        establecimiento, 
                        cadena
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    producto_id, 
                    factura_id, 
                    precio, 
                    establecimiento, 
                    cadena
                ))
                
                productos_guardados += 1
                
            except Exception as e:
                print(f"Error guardando producto: {e}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ {productos_guardados} productos guardados")
        
        return {
            "success": True,
            "id": factura_id,
            "factura_id": factura_id,  # Para compatibilidad
            "productos_guardados": productos_guardados,
            "message": f"Factura #{factura_id} creada con {productos_guardados} productos"
        }
        
    except Exception as e:
        print(f"❌ Error creando factura: {e}")
        import traceback
        traceback.print_exc()
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        raise HTTPException(500, f"Error al crear factura: {str(e)}")


@app.put("/admin/facturas/{factura_id}/datos-generales")
async def actualizar_datos_generales(factura_id: int, datos: dict):
    """Actualizar establecimiento, total, fecha"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE facturas
            SET establecimiento = %s, 
                total_factura = %s,
                cadena = %s
            WHERE id = %s
        """, (
            datos.get('establecimiento'),
            datos.get('total'),
            detectar_cadena(datos.get('establecimiento', '')),
            factura_id
        ))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Datos actualizados"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.put("/admin/productos/{precio_producto_id}")
async def actualizar_producto(precio_producto_id: int, datos: dict):
    """Actualizar un producto específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Buscar o crear producto en catálogo
        codigo = datos.get('codigo')
        nombre = datos.get('nombre')
        precio = datos.get('precio')
        
        cursor.execute("SELECT id FROM productos_catalogo WHERE codigo_ean = %s", (codigo,))
        producto = cursor.fetchone()
        
        if producto:
            producto_id = producto[0]
            # Actualizar nombre si cambió
            cursor.execute(
                "UPDATE productos_catalogo SET nombre_producto = %s WHERE id = %s",
                (nombre, producto_id)
            )
        else:
            # Crear nuevo producto
            cursor.execute(
                "INSERT INTO productos_catalogo (codigo_ean, nombre_producto) VALUES (%s, %s) RETURNING id",
                (codigo, nombre)
            )
            producto_id = cursor.fetchone()[0]
        
        # Actualizar precio
        cursor.execute("""
            UPDATE precios_productos
            SET producto_id = %s, precio = %s
            WHERE id = %s
        """, (producto_id, precio, precio_producto_id))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "producto_id": producto_id}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/admin/productos/{precio_producto_id}")
async def eliminar_producto(precio_producto_id: int):
    """Eliminar un producto de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM precios_productos WHERE id = %s", (precio_producto_id,))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Producto eliminado"}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/admin/facturas/{factura_id}/productos")
async def agregar_producto(factura_id: int, datos: dict):
    """Agregar producto faltante manualmente"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener establecimiento y cadena de la factura
        cursor.execute("SELECT establecimiento, cadena FROM facturas WHERE id = %s", (factura_id,))
        factura = cursor.fetchone()
        
        if not factura:
            raise HTTPException(404, "Factura no encontrada")
        
        establecimiento, cadena = factura
        
        # Buscar o crear producto
        codigo = datos.get('codigo')
        nombre = datos.get('nombre')
        precio = datos.get('precio')
        
        cursor.execute("SELECT id FROM productos_catalogo WHERE codigo_ean = %s", (codigo,))
        producto = cursor.fetchone()
        
        if producto:
            producto_id = producto[0]
        else:
            cursor.execute(
                "INSERT INTO productos_catalogo (codigo_ean, nombre_producto) VALUES (%s, %s) RETURNING id",
                (codigo, nombre)
            )
            producto_id = cursor.fetchone()[0]
        
        # Insertar precio
        cursor.execute("""
            INSERT INTO precios_productos (producto_id, factura_id, precio, establecimiento, cadena)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (producto_id, factura_id, precio, establecimiento, cadena))
        
        nuevo_id = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return {"success": True, "id": nuevo_id, "producto_id": producto_id}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.post("/admin/facturas/{factura_id}/validar")
async def marcar_como_validada(factura_id: int):
    """Marcar factura como validada después de correcciones"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'validada',
                fecha_validacion = %s,
                puntaje_calidad = 100
            WHERE id = %s
        """, (datetime.now(), factura_id))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Factura validada"}
    except Exception as e:
        raise HTTPException(500, str(e))
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel

class ProductoItem(BaseModel):
    nombre: str
    cantidad: int = 1
    precio: float
    codigo: Optional[str] = None

class InvoiceConfirm(BaseModel):
    establecimiento: str
    fecha: str
    total: float
    productos: List[ProductoItem]
    user_id: Optional[str] = None
    user_email: Optional[str] = None

@app.post("/api/invoices/confirm")
async def confirm_invoice(invoice: InvoiceConfirm, request: Request):
    """Guarda una factura confirmada en la base de datos"""
    try:
        # Validar calidad de la factura
        tiene_imagen = True  # Asumimos que viene del flujo con imagen
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=invoice.establecimiento,
            total=invoice.total,
            tiene_imagen=tiene_imagen,
            productos=[p.dict() for p in invoice.productos]
        )
        
        # Si el puntaje es muy bajo, podemos advertir al usuario
        if puntaje < 40:
            return {
                "success": False,
                "message": "La factura tiene problemas de calidad",
                "validation": {
                    "score": puntaje,
                    "status": estado,
                    "alerts": alertas
                },
                "require_confirmation": True
            }
        
        # Guardar en la base de datos con el puntaje de calidad
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Detectar cadena del establecimiento
            cadena = detectar_cadena(invoice.establecimiento)
            
            # Insertar factura con puntaje de calidad
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, establecimiento, cadena, 
                    fecha_cargue, total_factura,
                    estado_validacion, puntaje_calidad, tiene_imagen
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                invoice.user_id, invoice.establecimiento, cadena,
                datetime.now(), invoice.total,
                estado, puntaje, tiene_imagen
            ))
            
            factura_id = cursor.fetchone()[0]
            
            # Guardar productos
            for producto in invoice.productos:
                # Buscar o crear producto en catálogo
                cursor.execute("""
                    SELECT id FROM productos_catalogo WHERE codigo_ean = %s
                """, (producto.codigo,))
                
                resultado = cursor.fetchone()
                if resultado:
                    producto_id = resultado[0]
                else:
                    cursor.execute("""
                        INSERT INTO productos_catalogo (codigo_ean, nombre_producto)
                        VALUES (%s, %s) RETURNING id
                    """, (producto.codigo, producto.nombre))
                    producto_id = cursor.fetchone()[0]
                
                # Guardar precio
                cursor.execute("""
                    INSERT INTO precios_productos (
                        producto_id, factura_id, precio, establecimiento, cadena
                    )
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    producto_id, factura_id, producto.precio, 
                    invoice.establecimiento, cadena
                ))
            
            conn.commit()
            
            return {
                "success": True,
                "message": f"Factura guardada correctamente (Calidad: {puntaje}/100)",
                "invoice_id": factura_id,
                "validation": {
                    "score": puntaje,
                    "status": estado,
                    "alerts": alertas if alertas else []
                }
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Error al guardar la factura"
        }
# ENDPOINTS PARA USUARIOS
@app.get("/api/user/my-invoices")
async def get_my_invoices(user = Depends(get_current_user)):
    """Obtiene solo las facturas del usuario autenticado"""
    user_id = user["user_id"]
    
    # TODO: Consultar la base de datos
    # SELECT * FROM invoices WHERE user_id = user_id
    
    # Por ahora, datos de ejemplo:
    return {
        "success": True,
        "invoices": [
            {
                "id": "1",
                "establecimiento": "Supermercado Éxito",
                "fecha": "2024-01-15",
                "total": 150000,
                "productos_count": 12
            }
        ]
    }

@app.get("/api/user/my-stats")
async def get_my_stats(user = Depends(get_current_user)):
    """Estadísticas personales del usuario"""
    user_id = user["user_id"]
    
    # TODO: Calcular desde la base de datos
    return {
        "total_facturas": 5,
        "gasto_total": 750000,
        "gasto_promedio": 150000,
        "establecimiento_frecuente": "Éxito"
    }

# ENDPOINTS PARA ADMIN
@app.get("/api/admin/analytics")
async def get_analytics(user = Depends(require_admin)):
    """Datos agregados para el admin - SIN información personal"""
    
    # TODO: Consultas agregadas a la BD
    return {
        "total_usuarios": 150,
        "total_facturas": 1200,
        "productos_populares": [
            {"nombre": "Leche", "frecuencia": 450},
            {"nombre": "Pan", "frecuencia": 380}
        ],
        "promedio_compra": 125000,
        "establecimientos_top": [
            {"nombre": "Éxito", "visitas": 400},
            {"nombre": "Carulla", "visitas": 350}
        ]
    }

@app.get("/api/admin/pending-reviews")
async def get_pending_reviews(user = Depends(require_admin)):
    """Facturas pendientes de revisión por el admin"""
    
    # TODO: Consultar BD por facturas con status = 'pending'
    return {
        "pending": [
            {
                "id": "123",
                "uploaded_at": "2024-01-15T10:30:00",
                "user_id": "user456",  # Solo ID, no datos personales
                "image_url": "/images/invoice123.jpg",
                "ocr_result": {...},
                "status": "pending"
            }
        ]
    }

@app.put("/api/admin/approve-invoice/{invoice_id}")
async def approve_invoice(invoice_id: str, user = Depends(require_admin)):
    """Admin aprueba una factura después de revisarla"""
    
    # TODO: Actualizar en BD
    # UPDATE invoices SET status = 'approved', reviewed_by = admin_id WHERE id = invoice_id
    
    return {"success": True, "message": "Factura aprobada"}

# Modificar el endpoint de procesamiento para guardar con estado pendiente
@app.post("/invoices/parse")
async def process_invoice(file: UploadFile = File(...), user = Depends(get_current_user)):
    """Procesa factura y la guarda como pendiente de revisión"""
    
    # Procesar con OCR (tu código existente)
    result = parse_invoice_with_claude(file)
    
    if result["success"]:
        # Guardar en BD con estado pendiente
        invoice_data = {
            "user_id": user["user_id"],
            "ocr_result": result["data"],
            "status": "pending",  # Pendiente de revisión admin
            "created_at": datetime.now()
        }
        
        # TODO: Insertar en BD
        # db.invoices.insert(invoice_data)
        
        return {
            "success": True,
            "message": "Factura procesada y pendiente de revisión",
            "data": result["data"]
        }
    
    return result
@app.post("/api/mobile/upload-auto")
async def upload_auto(
    file: UploadFile = File(...),
    user_id: Optional[int] = Form(1)
):
    """Upload rápido con procesamiento automático"""
    try:
        if file.size > 10 * 1024 * 1024:
            return {"success": False, "error": "Archivo muy grande"}
        
        content = await file.read()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(content)
        temp_file.close()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO facturas (
                usuario_id, establecimiento, estado_validacion, fecha_cargue
            ) VALUES (%s, %s, %s, %s) RETURNING id
        """, (user_id, "Procesando...", "cola", datetime.now()))
        
        factura_id = cursor.fetchone()[0]
        
        # Guardar imagen
        from storage import save_image_to_db
        save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        
        conn.commit()
        conn.close()
        
        # Agregar a cola
        processor.add_to_queue(factura_id, temp_file.name, user_id)
        
        return {
            "success": True,
            "factura_id": factura_id,
            "queue_position": processor.get_queue_position(factura_id),
            "message": "Factura en cola de procesamiento"
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/mobile/status/{factura_id}")
async def get_invoice_status(factura_id: int):
    """Obtiene estado de procesamiento"""
    
    if factura_id in processing:
        return processing[factura_id]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT estado_validacion, establecimiento, total_factura
        FROM facturas WHERE id = %s
    """, (factura_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            "status": result[0],
            "establecimiento": result[1],
            "total": float(result[2]) if result[2] else 0
        }
    
    return {"status": "not_found"}

@app.get("/ocr-stats")
async def get_ocr_stats():
    """Estadísticas del procesador"""
    return processor.get_stats()

@app.get("/admin/audit-report")
async def get_audit_report():
    """Obtiene reporte completo de auditoría"""
    audit = AuditSystem()
    return audit.generate_audit_report()

@app.post("/admin/run-audit")
async def run_manual_audit():
    """Ejecuta auditoría manual completa"""
    results = audit_scheduler.run_manual_audit()
    return {
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "results": results
    }

@app.post("/admin/improve-quality")
async def improve_quality():
    """Ejecuta mejora manual de calidad de datos"""
    quality_results = audit_scheduler.improve_quality()
    return {
        "success": True,
        "timestamp": datetime.now().isoformat(),
        "results": quality_results
    }
@app.get("/admin/audit-status")
async def get_audit_status():
    """Obtiene estado del sistema de auditoría"""
    return {
        "scheduler_running": audit_scheduler.is_running,
        "last_run": audit_scheduler.audit_system._get_recent_audit_logs()[:1],
        "system_health": audit_scheduler.audit_system._calculate_system_health(
            audit_scheduler.audit_system.assess_data_quality()
        )
    }

@app.post("/admin/clean-data")
async def clean_old_data():
    """Ejecuta limpieza manual de datos antiguos"""
    audit = AuditSystem()
    results = audit.clean_old_data()
    return {
        "success": True,
        "cleaned": results
    }

from fastapi.responses import HTMLResponse

# Añade esto temporalmente a tu código para debug
@app.get("/debug-audit-system")
   async def debug_audit_system():
       """Debug del sistema de auditoría"""
       try:
           # Verificar si audit_scheduler existe y tiene los métodos correctos
           methods = [method for method in dir(audit_scheduler) if not method.startswith('_')]
           # Verificar si los métodos nuevos están presentes
           has_improve_quality = hasattr(audit_scheduler, 'improve_quality')
           has_run_manual = hasattr(audit_scheduler, 'run_manual_audit')
           
           # Intentar crear las tablas necesarias
           tables_created = audit_scheduler.audit_system.create_missing_tables()
           
           return {
               "methods_available": methods,
               "has_improve_quality": has_improve_quality,
               "has_run_manual": has_run_manual,
               "tables_created": tables_created
           }
       except Exception as e:
           return {"error": str(e)}

@app.get("/reporte_auditoria", response_class=HTMLResponse)
async def get_reporte_auditoria():
    html_content = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Reporte de Auditoría del Sistema</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.0/font/bootstrap-icons.css">
        <style>
            .health-score {
                font-size: 3rem;
                font-weight: bold;
                width: 120px;
                height: 120px;
                border-radius: 50%;
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto;
                color: white;
            }
            .health-critical { background-color: #dc3545; }
            .health-warning { background-color: #fd7e14; }
            .health-ok { background-color: #ffc107; }
            .health-good { background-color: #28a745; }
            
            .metric-card {
                transition: transform 0.2s;
                height: 100%;
            }
            .metric-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 10px 20px rgba(0,0,0,0.1);
            }
            .anomaly-item {
                border-left: 4px solid #dc3545;
                padding-left: 15px;
                margin-bottom: 10px;
            }
            .detail-row {
                border-bottom: 1px solid #eee;
                padding: 8px 0;
            }
            .detail-row:last-child {
                border-bottom: none;
            }
            .action-taken {
                background-color: #e8f4fd;
                border-radius: 4px;
                padding: 8px 12px;
                margin-top: 5px;
            }
            .status-badge {
                font-size: 0.8em;
                padding: 3px 10px;
                border-radius: 12px;
            }
            .chart-container {
                height: 250px;
            }
            .alert-custom {
                border-left: 4px solid #fd7e14;
            }
            .section-title {
                border-bottom: 2px solid #eee;
                padding-bottom: 10px;
                margin-bottom: 20px;
            }
            #loadingOverlay {
                position: fixed;
                top: 0;
                left: 0;
                width: 100%;
                height: 100%;
                background: rgba(255, 255, 255, 0.8);
                display: flex;
                justify-content: center;
                align-items: center;
                z-index: 1000;
            }
            .spinner {
                width: 3rem;
                height: 3rem;
            }
        </style>
    </head>
    <body>
        <div id="loadingOverlay">
            <div class="spinner-border text-primary spinner" role="status">
                <span class="visually-hidden">Cargando...</span>
            </div>
        </div>

        <div class="container py-4">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h1>Reporte de Auditoría</h1>
                <span class="badge bg-secondary" id="fechaReporte"></span>
            </div>

            <!-- Resumen General -->
            <div class="row mb-4">
                <div class="col-md-4">
                    <div class="card h-100">
                        <div class="card-body text-center">
                            <h5 class="card-title">Salud del Sistema</h5>
                            <div id="healthScore" class="health-score">--</div>
                            <h6 class="mt-3" id="healthStatus">Cargando...</h6>
                            <p class="card-text text-muted">Puntaje basado en auditoría completa</p>
                        </div>
                    </div>
                </div>
                <div class="col-md-8">
                    <div class="card h-100">
                        <div class="card-body">
                            <h5 class="card-title">Resumen de Problemas</h5>
                            <div class="row g-3">
                                <div class="col-md-4">
                                    <div class="border rounded p-3 text-center">
                                        <h3 id="duplicateGroups">--</h3>
                                        <p class="mb-0">Grupos de Facturas Duplicadas</p>
                                    </div>
                                </div>
                                <div class="col-md-4">
                                    <div class="border rounded p-3 text-center">
                                        <h3 id="duplicatesProcessed">--</h3>
                                        <p class="mb-0">Duplicados Procesados</p>
                                    </div>
                                </div>
                                <div class="col-md-4">
                                    <div class="border rounded p-3 text-center">
                                        <h3 id="mathErrors">--</h3>
                                        <p class="mb-0">Errores Matemáticos</p>
                                    </div>
                                </div>
                            </div>
                            <div id="summaryAlert" class="alert alert-warning mt-3 d-none">
                                <i class="bi bi-exclamation-triangle-fill me-2"></i>
                                <span id="summaryMessage"></span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Detalles de Problemas -->
            <div class="row mb-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title section-title">Problemas Detectados y Soluciones</h5>
                            
                            <div class="mb-4" id="duplicatesSection">
                                <h6 class="fw-bold"><i class="bi bi-files me-2"></i>Facturas Duplicadas</h6>
                                <div class="table-responsive">
                                    <table class="table table-hover">
                                        <thead class="table-light">
                                            <tr>
                                                <th>Problema</th>
                                                <th>Impacto</th>
                                                <th>Acción Realizada</th>
                                                <th>Estado</th>
                                            </tr>
                                        </thead>
                                        <tbody id="duplicatesTable">
                                            <!-- Contenido dinámico -->
                                        </tbody>
                                    </table>
                                </div>
                                <div id="duplicatesAction" class="action-taken d-none">
                                    <strong>Acción Correctiva:</strong> <span id="duplicatesActionText"></span>
                                </div>
                            </div>

                            <div class="mb-4" id="mathErrorsSection">
                                <h6 class="fw-bold"><i class="bi bi-calculator me-2"></i>Validación Matemática</h6>
                                <div class="table-responsive">
                                    <table class="table table-hover">
                                        <thead class="table-light">
                                            <tr>
                                                <th>Problema</th>
                                                <th>Impacto</th>
                                                <th>Acción Realizada</th>
                                                <th>Estado</th>
                                            </tr>
                                        </thead>
                                        <tbody id="mathErrorsTable">
                                            <!-- Contenido dinámico -->
                                        </tbody>
                                    </table>
                                </div>
                                <div id="mathErrorsAction" class="action-taken d-none">
                                    <strong>Acción Correctiva:</strong> <span id="mathErrorsActionText"></span>
                                </div>
                            </div>
                            
                            <div id="anomaliesSection" class="mb-4 d-none">
                                <h6 class="fw-bold"><i class="bi bi-graph-up me-2"></i>Anomalías de Precios</h6>
                                <div class="table-responsive">
                                    <table class="table table-hover">
                                        <thead class="table-light">
                                            <tr>
                                                <th>Problema</th>
                                                <th>Impacto</th>
                                                <th>Acción Realizada</th>
                                                <th>Estado</th>
                                            </tr>
                                        </thead>
                                        <tbody id="anomaliesTable">
                                            <!-- Contenido dinámico -->
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                            
                            <div id="technicalErrorsSection" class="alert alert-info d-none">
                                <h6 class="fw-bold">Error Técnico Resuelto</h6>
                                <p class="mb-2">Se corrigió un error en el sistema de detección de duplicados:</p>
                                <div class="bg-light p-2 rounded mb-2">
                                    <code id="technicalErrorCode"></code>
                                </div>
                                <p class="mb-0" id="technicalErrorSolution"></p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Calidad de Datos -->
            <div class="row mb-4">
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title section-title">Calidad de Datos</h5>
                            <div class="row">
                                <div class="col-md-6">
                                    <div class="table-responsive">
                                        <table class="table">
                                            <tbody>
                                                <tr>
                                                    <th scope="row">Total de Facturas</th>
                                                    <td id="totalInvoices">--</td>
                                                </tr>
                                                <tr>
                                                    <th scope="row">Puntaje Promedio de Calidad</th>
                                                    <td id="avgQuality">--</td>
                                                </tr>
                                                <tr>
                                                    <th scope="row">Facturas con Imágenes</th>
                                                    <td id="withImages">--</td>
                                                </tr>
                                                <tr>
                                                    <th scope="row">Facturas Pendientes de Revisión</th>
                                                    <td id="pendingReview">--</td>
                                                </tr>
                                                <tr>
                                                    <th scope="row">Facturas con Errores</th>
                                                    <td id="withErrors">--</td>
                                                </tr>
                                                <tr>
                                                    <th scope="row">Facturas Procesadas Correctamente</th>
                                                    <td id="processedOk">--</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                                <div class="col-md-6">
                                    <div class="alert alert-custom">
                                        <h6 class="fw-bold">Recomendaciones de Mejora</h6>
                                        <ol class="mb-0" id="recommendations">
                                            <!-- Contenido dinámico -->
                                        </ol>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            
            <!-- Plan de Acción -->
            <div class="row">
                <div class="col-12">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title section-title">Plan de Acción Recomendado</h5>
                            <div class="table-responsive">
                                <table class="table table-hover">
                                    <thead class="table-light">
                                        <tr>
                                            <th>Acción</th>
                                            <th>Prioridad</th>
                                            <th>Impacto Esperado</th>
                                        </tr>
                                    </thead>
                                    <tbody id="actionPlanTable">
                                        <!-- Contenido dinámico -->
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
        <script>
            // Función para obtener datos de la auditoría
            async function cargarReporteAuditoria() {
                try {
                    // Mostrar overlay de carga
                    document.getElementById('loadingOverlay').style.display = 'flex';
                    
                    // Obtener datos de la API
                    const response = await fetch('/api/admin/audit-report');
                    
                    if (!response.ok) {
                        throw new Error(`Error al cargar datos: ${response.status}`);
                    }
                    
                    const reportData = await response.json();
                    
                    // Una vez tenemos los datos, actualizamos la UI
                    renderizarReporte(reportData);
                    
                } catch (error) {
                    console.error('Error al cargar el reporte:', error);
                    mostrarError('No se pudo cargar el reporte. Por favor, intenta nuevamente.');
                } finally {
                    // Ocultar overlay de carga
                    document.getElementById('loadingOverlay').style.display = 'none';
                }
            }
            
            // Función para renderizar los datos del reporte
            function renderizarReporte(data) {
                const timestamp = new Date(data.generated_at || data.timestamp);
                document.getElementById('fechaReporte').textContent = `Generado el ${timestamp.toLocaleString('es-ES')}`;
                
                // Renderizar calidad de datos
                if (data.data_quality) {
                    const quality = data.data_quality;
                    const healthScore = quality.health_score || 0;
                    
                    // Establecer puntaje de salud y estado
                    const healthElement = document.getElementById('healthScore');
                    healthElement.textContent = typeof healthScore === 'number' ? healthScore.toFixed(1) : healthScore.toString();
                    
                    // Ajustar color según puntaje
                    healthElement.className = 'health-score';
                    let healthStatus = '';
                    
                    if (healthScore >= 90) {
                        healthElement.classList.add('health-good');
                        healthStatus = '🟢 Excelente';
                    } else if (healthScore >= 70) {
                        healthElement.classList.add('health-ok');
                        healthStatus = '🟡 Bueno';
                    } else if (healthScore >= 50) {
                        healthElement.classList.add('health-warning');
                        healthStatus = '🟠 Regular';
                    } else {
                        healthElement.classList.add('health-critical');
                        healthStatus = '🔴 Requiere Atención';
                    }
                    
                    document.getElementById('healthStatus').textContent = healthStatus;
                    
                    // Actualizar estadísticas
                    document.getElementById('totalInvoices').textContent = quality.total_invoices || 0;
                    document.getElementById('avgQuality').textContent = `${(quality.avg_quality || 0).toFixed(1)} / 100`;
                    
                    const withImagesCount = quality.with_images || 0;
                    const withImagesPercent = quality.total_invoices ? ((withImagesCount / quality.total_invoices) * 100).toFixed(1) : 0;
                    document.getElementById('withImages').textContent = `${withImagesCount} (${withImagesPercent}%)`;
                    
                    document.getElementById('pendingReview').textContent = quality.pending_review || 0;
                    document.getElementById('withErrors').textContent = quality.errors || 0;
                    document.getElementById('processedOk').textContent = quality.processed || 0;
                }
                
                // Renderizar duplicados
                if (data.duplicates) {
                    const duplicates = data.duplicates;
                    document.getElementById('duplicateGroups').textContent = duplicates.found || 0;
                    document.getElementById('duplicatesProcessed').textContent = duplicates.processed || 0;
                    
                    const duplicatesTable = document.getElementById('duplicatesTable');
                    duplicatesTable.innerHTML = '';
                    
                    if ((duplicates.found || 0) > 0) {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>${duplicates.found} grupo${duplicates.found !== 1 ? 's' : ''} de facturas duplicadas detectadas</td>
                            <td>${duplicates.processed} registro${duplicates.processed !== 1 ? 's' : ''} afectados</td>
                            <td>Actualización automática: Estado cambiado a 'duplicado', puntaje de calidad reducido</td>
                            <td><span class="badge bg-success status-badge">Resuelto</span></td>
                        `;
                        duplicatesTable.appendChild(row);
                        
                        // Mostrar acción correctiva
                        document.getElementById('duplicatesAction').classList.remove('d-none');
                        document.getElementById('duplicatesActionText').textContent = 
                            'Los duplicados fueron marcados en la base de datos y sus puntajes de calidad fueron ajustados automáticamente. ' + 
                            'Esta acción ayuda a evitar el doble conteo en análisis financieros y reportes de precios.';
                    } else {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>No se encontraron facturas duplicadas</td>
                            <td>0 registros afectados</td>
                            <td>No se requirió acción</td>
                            <td><span class="badge bg-success status-badge">Verificado</span></td>
                        `;
                        duplicatesTable.appendChild(row);
                    }
                    
                    // Si hay error, mostrar sección de error técnico
                    if (duplicates.error) {
                        document.getElementById('technicalErrorsSection').classList.remove('d-none');
                        document.getElementById('technicalErrorCode').textContent = duplicates.error;
                        document.getElementById('technicalErrorSolution').textContent = 
                            'Solución: Se implementó una conversión explícita de tipos para asegurar que los IDs de facturas fueran procesados correctamente.';
                    }
                }
                
                // Renderizar errores matemáticos
                if (data.math_errors) {
                    const mathErrors = data.math_errors;
                    document.getElementById('mathErrors').textContent = mathErrors.errors || 0;
                    
                    const mathErrorsTable = document.getElementById('mathErrorsTable');
                    mathErrorsTable.innerHTML = '';
                    
                    if ((mathErrors.errors || 0) > 0) {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>Errores matemáticos en facturas</td>
                            <td>${mathErrors.errors} facturas con errores, ${mathErrors.warnings} con advertencias</td>
                            <td>Se ajustó el puntaje de calidad y se marcaron para revisión</td>
                            <td><span class="badge bg-warning text-dark status-badge">Requiere Revisión</span></td>
                        `;
                        mathErrorsTable.appendChild(row);
                        
                        // Mostrar acción correctiva
                        document.getElementById('mathErrorsAction').classList.remove('d-none');
                        document.getElementById('mathErrorsActionText').textContent = 
                            'Las facturas con discrepancias matemáticas fueron marcadas y su puntaje de calidad fue reducido. ' +
                            'Se recomienda una revisión manual para corregir las diferencias.';
                    } else {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>No se encontraron errores matemáticos</td>
                            <td>${mathErrors.total_checked || 0} facturas verificadas</td>
                            <td>No se requirió acción</td>
                            <td><span class="badge bg-success status-badge">Verificado</span></td>
                        `;
                        mathErrorsTable.appendChild(row);
                    }
                }
                
                // Renderizar anomalías de precios si hay datos
                if (data.price_anomalies && (data.price_anomalies.anomalies || 0) > 0) {
                    document.getElementById('anomaliesSection').classList.remove('d-none');
                    const anomalies = data.price_anomalies;
                    
                    const anomaliesTable = document.getElementById('anomaliesTable');
                    anomaliesTable.innerHTML = '';
                    
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>Anomalías de precios detectadas</td>
                        <td>${anomalies.anomalies} productos con precios anómalos</td>
                        <td>Marcados para revisión y análisis de precios</td>
                        <td><span class="badge bg-warning text-dark status-badge">Pendiente</span></td>
                    `;
                    anomaliesTable.appendChild(row);
                }
                
                // Generar recomendaciones basadas en los datos
                const recommendations = document.getElementById('recommendations');
                recommendations.innerHTML = '';
                
                // Lista de posibles recomendaciones
                const possibleRecommendations = [];
                
                // Recomendaciones basadas en duplicados
                if (data.duplicates && (data.duplicates.found || 0) > 0) {
                    possibleRecommendations.push('Revisar el proceso de carga de facturas para evitar duplicados');
                }
                
                // Recomendaciones basadas en calidad de datos
                if (data.data_quality) {
                    const quality = data.data_quality;
                    
                    if (quality.with_images < quality.total_invoices * 0.7) {
                        possibleRecommendations.push(`Aumentar el porcentaje de facturas con imágenes adjuntas (actualmente ${((quality.with_images / quality.total_invoices) * 100).toFixed(1)}%)`);
                    }
                    
                    if ((quality.errors || 0) > 0) {
                        possibleRecommendations.push('Revisar manualmente las facturas con errores y corregir los problemas');
                    }
                    
                    if (quality.health_score < 50) {
                        possibleRecommendations.push('Implementar validaciones adicionales antes de la carga de facturas');
                    }
                }
                
                // Si no hay recomendaciones específicas, agregar una genérica
                if (possibleRecommendations.length === 0) {
                    possibleRecommendations.push('Mantener el monitoreo regular del sistema');
                }
                
                // Agregar recomendaciones a la lista
                possibleRecommendations.forEach(rec => {
                    const li = document.createElement('li');
                    li.textContent = rec;
                    recommendations.appendChild(li);
                });
                
                // Crear plan de acción basado en los problemas encontrados
                const actionPlanTable = document.getElementById('actionPlanTable');
                actionPlanTable.innerHTML = '';
                
                // Determinar acciones basadas en los problemas
                const actions = [];
                
                if (data.duplicates && (data.duplicates.found || 0) > 0) {
                    actions.push({
                        action: 'Implementar validación previa a la carga para detectar facturas potencialmente duplicadas',
                        priority: 'Alta',
                        impact: 'Reducción del 90% en facturas duplicadas'
                    });
                }
                
                if (data.data_quality && data.data_quality.with_images < data.data_quality.total_invoices * 0.7) {
                    actions.push({
                        action: 'Mejorar la captura de imágenes de facturas',
                        priority: 'Media',
                        impact: 'Aumento del puntaje de calidad en aproximadamente 30 puntos'
                    });
                }
                
                if (data.price_anomalies && data.price_anomalies.checked === 0) {
                    actions.push({
                        action: 'Revisar el módulo de detección de anomalías de precios',
                        priority: 'Media',
                        impact: 'Mejor detección de inconsistencias en precios'
                    });
                }
                
                if (data.data_quality && data.data_quality.health_score < 50) {
                    actions.push({
                        action: 'Programar revisión manual de facturas con baja calidad',
                        priority: 'Baja',
                        impact: 'Corrección de errores que no pueden ser detectados automáticamente'
                    });
                }
                
                // Si no hay acciones específicas, agregar una genérica
                if (actions.length === 0) {
                    actions.push({
                        action: 'Mantener el monitoreo regular del sistema',
                        priority: 'Baja',
                        impact: 'Prevención proactiva de problemas potenciales'
                    });
                }
                
                // Agregar acciones a la tabla
                actions.forEach(action => {
                    const row = document.createElement('tr');
                    const priorityClass = 
                        action.priority === 'Alta' ? 'danger' : 
                        action.priority === 'Media' ? 'warning text-dark' : 'info';
                    
                    row.innerHTML = `
                        <td>${action.action}</td>
                        <td><span class="badge bg-${priorityClass}">${action.priority}</span></td>
                        <td>${action.impact}</td>
                    `;
                    actionPlanTable.appendChild(row);
                });
                
                // Mostrar alerta de resumen si es necesario
                const summaryAlert = document.getElementById('summaryAlert');
                const summaryMessage = document.getElementById('summaryMessage');
                
                if ((data.duplicates && data.duplicates.found > 0) || 
                    (data.math_errors && data.math_errors.errors > 0) ||
                    (data.data_quality && data.data_quality.health_score < 50)) {
                    
                    summaryAlert.classList.remove('d-none');
                    
                    if (data.data_quality && data.data_quality.health_score < 30) {
                        summaryMessage.textContent = 'Se requiere atención inmediata debido a problemas críticos en la calidad de los datos.';
                    } else if (data.duplicates && data.duplicates.found > 5) {
                        summaryMessage.textContent = 'Se requiere atención inmediata debido al alto número de facturas duplicadas detectadas.';
                    } else if (data.math_errors && data.math_errors.errors > 5) {
                        summaryMessage.textContent = 'Se requiere atención debido a múltiples errores matemáticos en las facturas.';
                    } else {
                        summaryMessage.textContent = 'Se encontraron problemas que requieren atención.';
                    }
                } else {
                    summaryAlert.classList.add('d-none');
                }
            }
            
            // Función para mostrar mensajes de error
            function mostrarError(mensaje) {
                alert(mensaje);
            }
            
            // Cargar el reporte al iniciar la página
            document.addEventListener('DOMContentLoaded', cargarReporteAuditoria);
        </script>
    </body>
    </html>
    """
    return html_content

# ========================================
# INICIO DEL SERVIDOR
# ========================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)







































































