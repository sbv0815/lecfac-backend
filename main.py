# main.py
from fastapi import FastAPI, File, UploadFile, HTTPException, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime
import os
import tempfile
import threading
import traceback
import shutil
import subprocess

# Importar procesador y database
from invoice_processor import process_invoice_products
from database import (
    create_tables, 
    get_db_connection, 
    hash_password, 
    verify_password, 
    test_database_connection,
    obtener_productos_frecuentes_faltantes,
    confirmar_producto_manual
)

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
    version="1.0.0",
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

# ========================================
# FUNCIONES AUXILIARES
# ========================================

def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    """Ejecuta queries con manejo de diferencias entre PostgreSQL y SQLite"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Error de conexión a base de datos")
    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        if database_type == "postgresql" and query.count("?") > 0:
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
    nombre_norm = (nombre or "").upper().strip()
    hash_obj = hashlib.md5(nombre_norm.encode())
    hash_hex = hash_obj.hexdigest()[:8].upper()
    return f"PLU_{hash_hex}"

def manejar_producto_ean(cursor, codigo_ean: str, nombre: str) -> int:
    """Maneja producto con código EAN (único global) — PostgreSQL"""
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
        print(f"  ✓ Producto EAN existente: {codigo_ean}")
        return producto_id
    else:
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
    """Maneja producto fresco (código local por cadena) — PostgreSQL"""
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
        print(f"  ✓ Producto fresco existente: {codigo_local} en {cadena}")
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
        print(f"  ✓ Producto fresco NUEVO: {codigo_local} en {cadena}")
        return producto_id

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
    except Exception as e:
        print(f"Error actualizando estado: {e}")
    finally:
        try:
            conn.close()
        except:
            pass

# ========================================
# ENDPOINTS BÁSICOS
# ========================================

@app.get("/")
async def root():
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    return {
        "message": "LecFac API funcionando", 
        "version": "1.0.0",
        "database": database_type,
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
            "database_type": os.environ.get("DATABASE_TYPE", "sqlite"),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/debug")
async def debug_info():
    return {
        "environment_variables": {
            "DATABASE_TYPE": os.environ.get("DATABASE_TYPE", "sqlite"),
            "DATABASE_URL": "CONFIGURADO" if os.environ.get("DATABASE_URL") else "NO CONFIGURADO",
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "NO CONFIGURADO"),
            "DOC_AI_LOCATION": os.environ.get("DOC_AI_LOCATION", "NO CONFIGURADO"), 
            "DOC_AI_PROCESSOR_ID": os.environ.get("DOC_AI_PROCESSOR_ID", "NO CONFIGURADO"),
        },
        "python_version": os.sys.version,
        "working_directory": os.getcwd()
    }

# ========================================
# ENDPOINTS DE USUARIOS
# ========================================

@app.post("/users/register")
async def register_user(user: UserRegister):
    try:
        existing = execute_query(
            "SELECT id FROM usuarios WHERE email = ?", 
            (user.email,), 
            fetch_one=True
        )
        if existing:
            raise HTTPException(400, "El email ya está registrado")
        password_hash = hash_password(user.password)
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        if database_type == "postgresql":
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

@app.get("/users/{user_id}/invoices")
async def get_user_invoices(user_id: int):
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

# ========================================
# ENDPOINTS DE FACTURAS
# ========================================

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """Procesa una factura con OCR"""
    allowed_types = ["image/jpeg", "image/png", "application/pdf", "application/octet-stream"]
    file_extension = file.filename.lower() if file.filename else ""
    valid_extensions = file_extension.endswith(('.jpg', '.jpeg', '.png', '.pdf'))
    if file.content_type not in allowed_types and not valid_extensions:
        raise HTTPException(400, f"Tipo de archivo no soportado: {file.content_type}")
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        # Nota: process_invoice_products debe aceptar la ruta del archivo
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
        print(f"=== GUARDANDO FACTURA ===")
        print(f"Usuario ID: {invoice.usuario_id}")
        print(f"Establecimiento: {invoice.establecimiento}")
        print(f"Productos recibidos: {len(invoice.productos)}")
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        conn = get_db_connection()
        cursor = conn.cursor()
        cadena = detectar_cadena(invoice.establecimiento)
        print(f"Cadena detectada: {cadena}")
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
        for i, producto in enumerate(invoice.productos):
            print(f"\n--- Producto {i+1}/{len(invoice.productos)} ---")
            codigo = producto.get('codigo')
            nombre = producto.get('nombre') or "Producto sin descripción"
            valor = producto.get('valor', 0)
            try:
                valor_int = int(valor) if valor else 0
                codigo_valido = codigo and codigo != 'None' and len(codigo) >= 6
                if codigo_valido and es_codigo_peso_variable(codigo):
                    print(f"⚠️ Código peso variable: {codigo}")
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                if not codigo_valido:
                    codigo = generar_codigo_unico(nombre, factura_id, i)
                if database_type == "postgresql":
                    es_fresco = (
                        (len(codigo) < 7 and codigo.isdigit()) or 
                        codigo.startswith('PLU_') or 
                        codigo.startswith('AUTO_')
                    )
                    if es_fresco:
                        producto_id = manejar_producto_fresco(cursor, codigo, nombre, cadena)
                    else:
                        producto_id = manejar_producto_ean(cursor, codigo, nombre)
                    cursor.execute(
                        """INSERT INTO precios_productos 
                           (producto_id, establecimiento, cadena, precio, usuario_id, factura_id, fecha_reporte)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (producto_id, invoice.establecimiento, cadena, valor_int, invoice.usuario_id, factura_id, datetime.now())
                    )
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (%s, %s, %s, %s)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                    productos_guardados += 1
                else:
                    cursor.execute(
                        "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (?, ?, ?, ?)",
                        (factura_id, codigo, nombre, valor_int)
                    )
                    productos_guardados += 1
                print(f"✓ Guardado: {codigo} - {nombre} - ${valor_int}")
            except Exception as e:
                print(f"❌ Error guardando producto {i+1}: {e}")
        conn.commit()
        conn.close()
        print(f"\n=== RESULTADO ===")
        print(f"Productos guardados: {productos_guardados}/{len(invoice.productos)}")
        return {
            "success": True, 
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "total_productos": len(invoice.productos),
            "message": f"Factura guardada: {productos_guardados} productos"
        }
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()
        raise HTTPException(500, f"Error guardando factura: {str(e)}")

@app.delete("/invoices/{factura_id}")
async def delete_invoice(factura_id: int, usuario_id: int):
    try:
        result = execute_query(
            "SELECT id FROM facturas WHERE id = ? AND usuario_id = ?", 
            (factura_id, usuario_id),
            fetch_one=True
        )
        if not result:
            raise HTTPException(404, "Factura no encontrada o no autorizada")
        productos_eliminados = execute_query(
            "DELETE FROM productos WHERE factura_id = ?", 
            (factura_id,)
        )
        execute_query("DELETE FROM facturas WHERE id = ?", (factura_id,))
        return {
            "success": True,
            "message": f"Factura eliminada (incluidos {productos_eliminados} productos)"
        }
    except Exception as e:
        raise HTTPException(500, f"Error eliminando factura: {str(e)}")

# ========================================
# ENDPOINTS DE VALIDACIÓN INTELIGENTE
# ========================================

@app.get("/facturas/{factura_id}/sugerencias-faltantes")
async def obtener_sugerencias_productos(factura_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute(
            "SELECT usuario_id, productos_guardados FROM facturas WHERE id = %s",
            (factura_id,)
        )
    else:
        cursor.execute(
            "SELECT usuario_id, productos_guardados FROM facturas WHERE id = ?",
            (factura_id,)
        )
    factura_data = cursor.fetchone()
    if not factura_data:
        conn.close()
        raise HTTPException(404, "Factura no encontrada")
    usuario_id = factura_data[0]
    # Obtener códigos detectados
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute(
            """SELECT pm.codigo_ean FROM historial_compras_usuario hc
               JOIN productos_maestro pm ON hc.producto_id = pm.id
               WHERE hc.factura_id = %s""",
            (factura_id,)
        )
    else:
        cursor.execute(
            """SELECT codigo FROM productos WHERE factura_id = ?""",
            (factura_id,)
        )
    codigos_detectados = {r[0] for r in cursor.fetchall()}
    conn.close()
    sugerencias = obtener_productos_frecuentes_faltantes(
        usuario_id, 
        codigos_detectados,
        limite=3
    )
    return {
        "factura_id": factura_id,
        "tiene_sugerencias": len(sugerencias) > 0,
        "sugerencias": sugerencias
    }

@app.post("/facturas/{factura_id}/confirmar-producto-faltante")
async def confirmar_producto_faltante(
    factura_id: int,
    codigo_ean: str = Form(...),
    precio: int = Form(...)
):
    conn = get_db_connection()
    cursor = conn.cursor()
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("SELECT usuario_id FROM facturas WHERE id = %s", (factura_id,))
    else:
        cursor.execute("SELECT usuario_id FROM facturas WHERE id = ?", (factura_id,))
    resultado = cursor.fetchone()
    conn.close()
    if not resultado:
        raise HTTPException(404, "Factura no encontrada")
    usuario_id = resultado[0]
    exito = confirmar_producto_manual(factura_id, codigo_ean, precio, usuario_id)
    if exito:
        return {"success": True, "mensaje": "Producto agregado correctamente"}
    else:
        raise HTTPException(500, "Error agregando producto")

# ========================================
# DIAGNÓSTICO TESSERACT (ÚNICO ENDPOINT)
# ========================================

@app.get("/verify-tesseract")
def verify_tesseract():
    tcmd_env = os.getenv("TESSERACT_CMD")
    tcmd_which = shutil.which("tesseract")
    candidates = [tcmd_env, tcmd_which, "/usr/bin/tesseract", "/usr/local/bin/tesseract"]
    candidates = [c for c in candidates if c]

    found, version, langs = None, None, None
    path_env = os.getenv("PATH")

    for c in candidates:
        try:
            if c and os.path.exists(c):
                found = c
                try:
                    version = subprocess.check_output([c, "--version"], text=True).strip()
                except Exception as e:
                    version = f"Error ejecutando --version: {e}"
                try:
                    langs = subprocess.check_output([c, "--list-langs"], text=True).strip()
                except Exception:
                    langs = "No pudo listar idiomas"
                break
        except Exception:
            continue

    return {
        "tesseract_installed": bool(found),
        "tesseract_cmd": found,
        "path_env": path_env,
        "which_tesseract": tcmd_which,
        "env_TESSERACT_CMD": tcmd_env,
        "version": version,
        "langs": langs,
        "note": "Si 'tesseract_installed' es false pero version muestra algo, revisa permisos/ejecución."
    }

# ========================================
# INICIO DEL SERVIDOR
# ========================================

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)






















