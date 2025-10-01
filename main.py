from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime
import os
import tempfile
import traceback
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator  # ← AGREGAR ESTE
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
from fastapi.responses import Response
from admin import router as admin_router

# ========================================
# CONFIGURACIÓN DE LA APP
# ========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización y cierre de la aplicación"""
    print("🚀 Iniciando LecFac API...")
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
    temp_file_path: str = None  # Agregar este campo

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

app.include_router(admin_router)


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
        
        if not result["success"]:
            os.unlink(temp_file.name)
            return result
        
        # NO eliminar el archivo aún
        return {
            "success": True,
            "data": result["data"],  # ← ASEGÚRATE QUE ESTO ESTÉ
            "temp_file_path": temp_file.name
        }
        
    except Exception as e:
        return {"success": False, "error": str(e)}
        
    except Exception as e:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.unlink(temp_file_path)
            except:
                pass
        raise HTTPException(500, f"Error: {str(e)}")

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
    total: float = Form(0),               # ← nuevo campo
    productos: str = Form(...)
):
    """Guardar factura con imagen + validación de calidad y detalle de productos."""
    import json

    temp_file = None
    conn = None
    cursor = None

    try:
        print("=== GUARDANDO FACTURA CON IMAGEN (v2 con validación) ===")
        print(f"Usuario: {usuario_id}")
        print(f"Establecimiento: {establecimiento}")
        print(f"Archivo: {file.filename}")
        print(f"Total declarado: {total}")

        # --- Parseo de productos ---
        try:
            productos_list = json.loads(productos)
            if not isinstance(productos_list, list):
                raise ValueError("El campo 'productos' debe ser un JSON array.")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"JSON inválido en 'productos': {e}")

        print(f"Productos recibidos: {len(productos_list)}")

        # --- Guardar imagen temporalmente ---
        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="La imagen está vacía.")

        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(content)
        temp_file.close()
        print(f"Imagen temporal: {temp_file.name}")

        # --- Conexión BD ---
        conn = get_db_connection()
        cursor = conn.cursor()

        # --- Detectar cadena a partir del establecimiento ---
        cadena = detectar_cadena(establecimiento) or ""
        print(f"Cadena detectada: '{cadena}'")

        # --- VALIDACIÓN DE CALIDAD ---
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=establecimiento,
            total=total,
            tiene_imagen=True,
            productos=productos_list,
            cadena=cadena
        )

        print("=== VALIDACIÓN DE FACTURA ===")
        print(f"Puntaje: {puntaje}/100")
        print(f"Estado: {estado}")
        if alertas:
            for a in alertas:
                print(f"- {a}")
        else:
            print("- Sin alertas")

        # --- Crear factura con campos de validación ---
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

        # --- Guardar imagen en BD ---
        original = (file.filename or "").lower()
        mime = "image/jpeg"
        if original.endswith(".png"):
            mime = "image/png"
        elif original.endswith(".jpeg") or original.endswith(".jpg"):
            mime = "image/jpeg"
        else:
            # Fallback simple por extensión desconocida
            mime = "image/jpeg"

        imagen_guardada = save_image_to_db(factura_id, temp_file.name, mime)
        print(f"✓ Imagen guardada en BD: {imagen_guardada}")

        # Eliminar archivo temporal
        try:
            os.unlink(temp_file.name)
            temp_file = None
        except Exception as _:
            pass

        # Eliminar archivo temporal
        try:
            os.unlink(temp_file.name)
            temp_file = None
        except Exception as _:
            pass

        # --- Guardar productos y precios ---
        productos_guardados = 0
        for prod in productos_list:
            # Extracción robusta de campos
            codigo = str(prod.get("codigo", "") or "").strip()
            nombre = str(prod.get("nombre", "") or "").strip()

            # Precio: permitir str/float/int y normalizar
            precio_val = prod.get("precio", 0)
            try:
                # algunos OCR devuelven comas
                if isinstance(precio_val, str):
                    precio_val = precio_val.replace(",", ".").strip()
                precio = float(precio_val)
            except Exception:
                precio = 0.0

            # Saltar si faltan datos esenciales
            if not codigo or not nombre:
                print(f"⚠ Producto omitido por datos insuficientes: codigo='{codigo}', nombre='{nombre}'")
                continue

            # Buscar o crear producto en catálogo
            cursor.execute("""
                SELECT id FROM productos_catalogo
                WHERE codigo_ean = %s
            """, (codigo,))
            row = cursor.fetchone()

            if row:
                producto_id = row[0]
            else:
                cursor.execute("""
                    INSERT INTO productos_catalogo (codigo_ean, nombre_producto)
                    VALUES (%s, %s)
                    RETURNING id
                """, (codigo, nombre))
                producto_id = cursor.fetchone()[0]

            # Insertar precio asociado a la factura, establecimiento y cadena
            # (si tu tabla tiene más columnas como moneda, fecha_registro, etc., agrégalas aquí)
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

        # --- Commit ---
        conn.commit()

        print(f"✓ {productos_guardados} productos guardados")
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
        "imagen_guardada": imagen_guardada,  # ← usar la variable
        "mensaje": f"Factura guardada con {productos_guardados} productos"
        }

    except HTTPException:
        # No reempaquetar si ya es HTTPException
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    except Exception as e:
        print(f"❌ Error guardando factura: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        # Cierre de cursor/conexión
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        # Limpieza del temporal si sigue existiendo
        if temp_file and os.path.exists(temp_file.name):
            try:
                os.unlink(temp_file.name)
            except Exception:
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
# ========================================
# INICIO DEL SERVIDOR
# ========================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)

































