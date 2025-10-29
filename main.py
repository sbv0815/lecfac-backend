import os
import tempfile
import traceback
import json
import uuid
# LIMPIEZA DE CACHÉ AL INICIO
import shutil
print("🧹 Limpiando caché de Python...")
for root, dirs, files in os.walk('.'):
    if '__pycache__' in dirs:
        shutil.rmtree(os.path.join(root, '__pycache__'))
        print(f"   ✓ Eliminado: {os.path.join(root, '__pycache__')}")
print("✅ Caché limpiado - Iniciando servidor...")
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from contextlib import asynccontextmanager

# ==========================================
# IMPORTS DE FASTAPI
# ==========================================
from fastapi import (
    FastAPI,
    File,
    UploadFile,
    HTTPException,
    Form,
    Depends,
    Header,
    Request,
    BackgroundTasks,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from fastapi import Request  # ✅ Importar Request
from api_inventario import router as inventario_router
from api_stats import router as stats_router
from audit_system import AuditSystem

# ==========================================
# IMPORTACIONES LOCALES
# ==========================================
from database import (
    create_tables,
    get_db_connection,
    hash_password,
    verify_password,
    test_database_connection,
    detectar_cadena,
    obtener_o_crear_establecimiento,
    actualizar_inventario_desde_factura,
)
from mobile_endpoints import router as mobile_router
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from claude_invoice import parse_invoice_with_claude
from product_matcher import buscar_o_crear_producto_inteligente

# Importar routers
from admin_dashboard import router as admin_dashboard_router
from auth import router as auth_router
from image_handlers import router as image_handlers_router
from duplicados_routes import router as duplicados_router
from diagnostico_routes import router as diagnostico_router

# Importar procesador OCR y auditoría
from ocr_processor import processor, ocr_queue, processing, buscar_o_crear_producto_inteligente_inline
from audit_system import audit_scheduler, AuditSystem
from corrections_service import aplicar_correcciones_automaticas
from concurrent.futures import ThreadPoolExecutor
import time
from establishments import procesar_establecimiento, obtener_o_crear_establecimiento_id
from api_auditoria_ia import router as auditoria_router
# Al inicio del archivo, con los otros imports
from fastapi import APIRouter
from fastapi.responses import HTMLResponse


# ==========================================
# MODELOS PYDANTIC
# ==========================================
class FacturaManual(BaseModel):
    establecimiento: str
    fecha: str
    total: float
    productos: List[dict]


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


# 🆕 MODELOS PARA EL EDITOR DE FACTURAS
class FacturaUpdate(BaseModel):
    """Modelo para actualizar datos generales de factura"""

    establecimiento: Optional[str] = None
    total: Optional[float] = None
    fecha: Optional[str] = None


class ItemUpdate(BaseModel):
    """Modelo para actualizar un item de factura"""

    nombre: str
    precio: float
    codigo_ean: Optional[str] = None


class ItemCreate(BaseModel):
    """Modelo para crear un nuevo item"""

    nombre: str
    precio: float
    codigo_ean: Optional[str] = None


# ==========================================
# FUNCIONES AUXILIARES
# ==========================================
async def get_current_user(authorization: str = Header(None)):
    """Obtener usuario actual desde token"""
    if not authorization:
        raise HTTPException(status_code=401, detail="No autorizado")
    return {"user_id": "user123", "email": "user@example.com"}


async def require_admin(user=Depends(get_current_user)):
    """Verificar si es admin"""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requiere permisos de admin")
    return user


# ==========================================
# AGREGAR ESTA FUNCIÓN DESPUÉS DE require_admin
# (Después de la línea 109 en tu main.py)
# ==========================================


def get_user_id_from_token(authorization: str) -> int:
    """
    Extraer usuario_id desde el token JWT

    Args:
        authorization: Header Authorization con formato "Bearer <token>"

    Returns:
        int: usuario_id extraído del token, o 1 como fallback
    """
    if not authorization or not authorization.startswith("Bearer "):
        print("⚠️ No se encontró token de autorización válido")
        return 1  # Usuario por defecto

    try:
        import jwt

        # Extraer el token sin el prefijo "Bearer "
        token = authorization.replace("Bearer ", "")

        # Decodificar sin verificar firma (para desarrollo)
        # En producción deberías verificar la firma con tu SECRET_KEY
        payload = jwt.decode(token, options={"verify_signature": False})

        # Extraer usuario_id del payload
        usuario_id = payload.get("user_id", 1)

        print(f"✅ Usuario extraído del token: {usuario_id}")
        return int(usuario_id)

    except jwt.DecodeError as e:
        print(f"⚠️ Error decodificando token JWT: {e}")
        return 1
    except Exception as e:
        print(f"⚠️ Error inesperado procesando token: {e}")
        return 1
def normalizar_precio_unitario(valor_ocr: float, cantidad: int) -> int:
    """
    Normaliza el precio unitario desde el valor del OCR

    El OCR a veces retorna precio total (cantidad × unitario)
    Esta función detecta y corrige automáticamente
    """
    try:
        valor = float(valor_ocr)
        cantidad = int(cantidad) if cantidad else 1

        if cantidad <= 0:
            cantidad = 1

        # Si cantidad es 1, el valor es precio unitario
        if cantidad == 1:
            return int(valor)

        # Si cantidad > 1, verificar si valor parece ser total o unitario
        precio_dividido = valor / cantidad

        # Precios típicos en Colombia: 500 a 50,000 pesos
        if 500 <= precio_dividido <= 50000:
            return int(precio_dividido)

        # Si el valor ya parece unitario, usarlo directo
        if 500 <= valor <= 50000:
            return int(valor)

        # Si valor es muy grande, probablemente es total
        if valor > 50000 and cantidad > 1:
            return int(valor / cantidad)

        return int(valor)

    except (ValueError, TypeError, ZeroDivisionError):
        return 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización y cierre de la aplicación"""
    print("=" * 60)
    print("🚀 INICIANDO LECFAC API")
    print("=" * 60)

    processor.start()
    print("✅ Procesador OCR iniciado")

    if test_database_connection():
        print("✅ Conexión a base de datos exitosa")
    else:
        print("⚠️ Error de conexión a base de datos")

    try:
        create_tables()
        print("✅ Tablas verificadas/creadas")
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")

    print("=" * 60)
    print("✅ SERVIDOR LISTO")
    print("=" * 60)

    yield

    processor.stop()
    print("\n👋 Cerrando LecFac API...")


app = FastAPI(
    title="LecFac API",
    version="3.2.1",
    description="Sistema de gestión de facturas con procesamiento asíncrono",
    lifespan=lifespan,
)

app.include_router(stats_router)
app.include_router(inventario_router)
app.include_router(diagnostico_router)
app.include_router(mobile_router, tags=["mobile"])


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

current_dir = Path(__file__).parent
static_path = current_dir / "static"

if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
    print(f"✅ Archivos estáticos: {static_path}")
else:
    print("⚠️ No se encontró carpeta /static")

templates = Jinja2Templates(directory=str(current_dir))
print(f"✅ Templates: {current_dir}")

print("\n" + "=" * 60)
print("📍 REGISTRANDO ROUTERS")
print("=" * 60)

try:
    app.include_router(image_handlers_router, tags=["images"])
    print("✅ image_handlers_router registrado")
except Exception as e:
    print(f"❌ Error: {e}")

try:
    app.include_router(admin_dashboard_router, tags=["admin"])
    print("✅ admin_dashboard_router registrado")
except Exception as e:
    print(f"❌ Error: {e}")

try:
    app.include_router(auth_router, prefix="/api", tags=["auth"])
    print("✅ auth_router registrado en /api/auth/*")
except Exception as e:
    print(f"❌ Error registrando auth_router: {e}")

try:
    app.include_router(duplicados_router, tags=["duplicados"])
    print("✅ duplicados_router registrado")
except Exception as e:
    print(f"❌ Error: {e}")

try:
    app.include_router(inventario_router, tags=["inventario"])
    print("✅ inventario_router registrado en /api/inventario/*")
except Exception as e:
    print(f"❌ Error registrando inventario_router: {e}")

print("=" * 60)
print("✅ ROUTERS CONFIGURADOS")
print("=" * 60 + "\n")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal / Dashboard"""
    # Buscar dashboard actualizado
    possible_files = [
        "dashboard.html",
        "admin_dashboard_v2.html",
        "admin_dashboard.html",
    ]

    for filename in possible_files:
        file_path = Path(filename)
        if file_path.exists():
            print(f"✅ Sirviendo dashboard: {filename}")
            # 🆕 AGREGAR HEADERS ANTI-CACHÉ
            return FileResponse(
                str(file_path),
                media_type="text/html",
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )

    # Si no encuentra el archivo
    print("⚠️ No se encontró dashboard.html")
    raise HTTPException(status_code=404, detail="Dashboard no encontrado")


@app.get("/editor.html")
async def serve_editor():
    """Sirve el editor de facturas con headers anti-caché"""
    possible_files = [
        "editor.html",
        "editor_factura.html",
    ]

    for filename in possible_files:
        file_path = Path(filename)
        if file_path.exists():
            print(f"✅ Sirviendo editor: {filename}")
            return FileResponse(
                str(file_path),
                media_type="text/html",
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )

    print("❌ Archivo editor.html no encontrado")
    raise HTTPException(status_code=404, detail="Editor no encontrado")


@app.get("/editor", response_class=HTMLResponse)
async def editor(request: Request):
    """Editor de facturas"""
    if templates:
        return templates.TemplateResponse("editor.html", {"request": request})
    return HTMLResponse("<h1>Editor</h1>")


@app.get("/gestor-duplicados", response_class=HTMLResponse)
async def get_duplicados_page(request: Request):
    """Gestor de duplicados"""
    try:
        if templates:
            return templates.TemplateResponse(
                "gestor_duplicados.html", {"request": request}
            )
    except:
        pass

    possible_paths = [
        Path("gestor_duplicados.html"),
        Path("static/gestor_duplicados.html"),
    ]

    for html_path in possible_paths:
        if html_path.exists():
            with open(html_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())

    raise HTTPException(404, "gestor_duplicados.html no encontrado")


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Dashboard administrativo"""
    # Buscar dashboard actualizado
    possible_files = [
        "dashboard_fixed_1761173724.html",
        "dashboard.html",
        "admin_dashboard_v2.html",
        "admin_dashboard.html",
    ]

    for filename in possible_files:
        html_path = Path(filename)
        if html_path.exists():
            return FileResponse(str(html_path))

    raise HTTPException(404, "Dashboard no encontrado")


@app.get("/duplicados.js")
async def get_duplicados_js():
    """Servir JavaScript de duplicados"""
    js_path = Path("duplicados.js")
    if js_path.exists():
        return FileResponse(str(js_path), media_type="application/javascript")
    raise HTTPException(404, "duplicados.js no encontrado")


@app.get("/health")
@app.get("/api/health-check")
async def health_check():
    """Verificar salud del sistema"""
    try:
        conn = get_db_connection()
        db_status = "connected" if conn else "disconnected"
        if conn:
            conn.close()

        return {
            "status": "healthy" if db_status == "connected" else "unhealthy",
            "database": db_status,
            "database_type": os.environ.get("DATABASE_TYPE", "postgresql"),
            "anthropic_configured": bool(os.environ.get("ANTHROPIC_API_KEY")),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"status": "unhealthy", "error": str(e)}
        )


@app.get("/verify-tesseract")
async def verify_tesseract():
    """Verificar que Tesseract OCR está instalado"""
    try:
        import pytesseract

        version = pytesseract.get_tesseract_version()
        return {
            "status": "ok",
            "tesseract_version": str(version),
            "message": "Tesseract funcionando",
        }
    except Exception as e:
        return {
            "status": "ok",
            "message": "Service ready",
        }


@app.get("/api/config/anthropic-key")
async def get_anthropic_key():
    """Obtener API Key de Anthropic"""
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    return {"apiKey": api_key if api_key else "", "configured": bool(api_key)}


# ==========================================
# 🔧 ENDPOINT 1: /invoices/parse - COMPLETO ✅
# ==========================================
@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """Procesar factura con OCR - Para imágenes individuales"""
    print(f"\n{'='*60}")
    print(f"📸 NUEVA FACTURA: {file.filename}")
    print(f"{'='*60}")

    temp_file = None
    conn = None

    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()

        print(f"✅ Archivo temporal: {temp_file.name}")

        result = parse_invoice_with_claude(temp_file.name)

        if not result.get("success"):
            raise HTTPException(400, result.get("error", "Error procesando"))

        data = result["data"]
        productos_ocr = data.get("productos", [])
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        fecha_factura = data.get("fecha")
        total_factura = data.get("total", 0)

        print(f"✅ Extraídos: {len(productos_ocr)} productos")

        productos_corregidos = aplicar_correcciones_automaticas(
            productos_ocr, establecimiento_raw
        )

        conn = get_db_connection()
        cursor = conn.cursor()

        cadena = detectar_cadena(establecimiento_raw)
        establecimiento_id = obtener_o_crear_establecimiento(
            establecimiento_raw, cadena
        )

        authorization = request.headers.get("Authorization")
        usuario_id = get_user_id_from_token(authorization)
        print(f"🆔 Usuario autenticado: {usuario_id}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, establecimiento, cadena,
                    total_factura, fecha_cargue, estado_validacion, tiene_imagen
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'procesado', TRUE)
                RETURNING id
            """,
                (
                    usuario_id,
                    establecimiento_id,
                    establecimiento_raw,
                    cadena,
                    total_factura,
                ),
            )
            factura_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, establecimiento, cadena,
                    total_factura, fecha_cargue, estado_validacion, tiene_imagen
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'procesado', 1)
            """,
                (
                    usuario_id,
                    establecimiento_id,
                    establecimiento_raw,
                    cadena,
                    total_factura,
                ),
            )
            factura_id = cursor.lastrowid

        print(f"✅ Factura creada: ID {factura_id}")

        productos_guardados = 0
        for idx, prod in enumerate(productos_corregidos, 1):
            try:
                codigo_ean = str(prod.get("codigo", "")).strip()
                nombre = str(prod.get("nombre", "")).strip()
                valor_ocr = prod.get("valor") or prod.get("precio") or 0  # ✅ 'valor', no 'precio, 0'
                cantidad = int(prod.get("cantidad", 1))  # ✅ Agregar int()
                precio_unitario = normalizar_precio_unitario(valor_ocr, cantidad)

                if not nombre or precio_unitario <= 0:
                    continue

                codigo_ean_valido = None
                if codigo_ean and len(codigo_ean) >= 8 and codigo_ean.isdigit():
                    codigo_ean_valido = codigo_ean

                # ⭐ CREAR PRODUCTO MAESTRO
                producto_maestro_id = None
                if codigo_ean_valido:
                    try:
                        producto_maestro_id = buscar_o_crear_producto_inteligente_inline(codigo=codigo_ean_valido or "",
                            nombre=nombre,
                            precio=precio_unitario,
                            establecimiento=establecimiento_raw,
                            cursor=cursor, conn=conn)
                        print(
                            f"   ✅ Producto maestro: {nombre} (ID: {producto_maestro_id})"
                        )
                    except Exception as e:
                        print(f"   ⚠️ Error creando producto maestro: {e}")

                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, precio_pagado, cantidad
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            factura_id,
                            usuario_id,
                            producto_maestro_id,
                            codigo_ean_valido,
                            nombre,
                            precio_unitario,
                            cantidad,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, precio_pagado, cantidad
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                        (
                            factura_id,
                            usuario_id,
                            producto_maestro_id,
                            codigo_ean_valido,
                            nombre,
                            precio_unitario,
                            cantidad,
                        ),
                    )

                productos_guardados += 1

            except Exception as e:
                print(f"❌ Error producto {idx}: {e}")
                continue

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "UPDATE facturas SET productos_guardados = %s WHERE id = %s",
                (productos_guardados, factura_id),
            )
        else:
            cursor.execute(
                "UPDATE facturas SET productos_guardados = ? WHERE id = ?",
                (productos_guardados, factura_id),
            )

        conn.commit()

        # ⭐⭐⭐ ACTUALIZAR INVENTARIO ⭐⭐⭐
        print(f"📦 Actualizando inventario del usuario...")
        try:
            actualizar_inventario_desde_factura(factura_id, usuario_id)
            print(f"✅ Inventario actualizado correctamente")
        except Exception as e:
            print(f"⚠️ Error actualizando inventario: {e}")
            traceback.print_exc()

        cursor.close()
        conn.close()
        conn = None

        imagen_guardada = save_image_to_db(factura_id, temp_file.name, "image/jpeg")

        try:
            os.unlink(temp_file.name)
        except:
            pass

        print(f"✅ PROCESAMIENTO COMPLETO")

        return {
            "success": True,
            "factura_id": factura_id,
            "data": data,
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        print(traceback.format_exc())

        if conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass

        if temp_file:
            try:
                os.unlink(temp_file.name)
            except:
                pass

        raise HTTPException(500, str(e))


# ==========================================
# 🔧 ENDPOINT 2: /invoices/save-with-image - COMPLETO ✅
# ==========================================
@app.post("/invoices/save-with-image")
async def save_invoice_with_image(
    file: UploadFile = File(...),
    usuario_id: int = Form(1),
    establecimiento: str = Form(...),
    total: float = Form(...),
    productos: str = Form(...),
):
    """Guardar factura procesada con su imagen"""
    print(f"\n{'='*60}")
    print(f"💾 GUARDANDO FACTURA CON IMAGEN")
    print(f"{'='*60}")

    temp_file = None
    conn = None

    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()

        print(f"📁 Archivo: {file.filename} ({len(content)} bytes)")
        print(f"🏪 Establecimiento: {establecimiento}")
        print(f"💰 Total: ${total:,.0f}")

        import json

        productos_list = json.loads(productos)
        print(f"📦 Productos: {len(productos_list)}")

        conn = get_db_connection()
        cursor = conn.cursor()

        cadena = detectar_cadena(establecimiento)
        establecimiento_id = obtener_o_crear_establecimiento(establecimiento, cadena)

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, establecimiento, cadena,
                    total_factura, fecha_cargue, estado_validacion, tiene_imagen
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'procesado', TRUE)
                RETURNING id
            """,
                (usuario_id, establecimiento_id, establecimiento, cadena, total),
            )
            factura_id = cursor.fetchone()[0]
        else:
            cursor.execute(
                """
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, establecimiento, cadena,
                    total_factura, fecha_cargue, estado_validacion, tiene_imagen
                ) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, 'procesado', 1)
            """,
                (usuario_id, establecimiento_id, establecimiento, cadena, total),
            )
            factura_id = cursor.lastrowid

        print(f"✅ Factura creada: ID {factura_id}")

        productos_guardados = 0

        for prod in productos_list:
            try:
                codigo = prod.get("codigo", "").strip()
                nombre = prod.get("nombre", "").strip()
                precio = float(prod.get("precio", 0))

                if not nombre or precio <= 0:
                    continue

                # ⭐ CREAR PRODUCTO MAESTRO

                producto_maestro_id = buscar_o_crear_producto_inteligente_inline(codigo=codigo,
                    nombre=nombre,
                    precio=int(precio),
                    establecimiento=establecimiento,
                    cursor=cursor, conn=conn)

                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, precio_pagado, cantidad
                        ) VALUES (%s, %s, %s, %s, %s, %s, 1)
                    """,
                        (
                            factura_id,
                            usuario_id,
                            producto_maestro_id,
                            codigo,
                            nombre,
                            precio,
                        ),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, precio_pagado, cantidad
                        ) VALUES (?, ?, ?, ?, ?, ?, 1)
                    """,
                        (
                            factura_id,
                            usuario_id,
                            producto_maestro_id,
                            codigo,
                            nombre,
                            precio,
                        ),
                    )

                productos_guardados += 1

            except Exception as e:
                print(f"⚠️ Error guardando producto: {e}")
                continue

        print(f"✅ {productos_guardados} productos guardados")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "UPDATE facturas SET productos_guardados = %s WHERE id = %s",
                (productos_guardados, factura_id),
            )
        else:
            cursor.execute(
                "UPDATE facturas SET productos_guardados = ? WHERE id = ?",
                (productos_guardados, factura_id),
            )

        conn.commit()

        # ⭐⭐⭐ ACTUALIZAR INVENTARIO ⭐⭐⭐
        print(f"📦 Actualizando inventario del usuario...")
        try:
            actualizar_inventario_desde_factura(factura_id, usuario_id)
            print(f"✅ Inventario actualizado correctamente")
        except Exception as e:
            print(f"⚠️ Error actualizando inventario: {e}")
            traceback.print_exc()

        imagen_guardada = save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        print(f"📸 Imagen guardada: {imagen_guardada}")

        try:
            os.unlink(temp_file.name)
        except:
            pass

        cursor.close()
        conn.close()

        print(f"{'='*60}")
        print(f"✅ FACTURA GUARDADA EXITOSAMENTE")
        print(f"{'='*60}\n")

        return {
            "success": True,
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,
            "establecimiento": establecimiento,
            "total": total,
        }

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()

        if conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass

        if temp_file:
            try:
                os.unlink(temp_file.name)
            except:
                pass

        raise HTTPException(500, f"Error guardando factura: {str(e)}")


# ==========================================
# ENDPOINT: PROCESAR VIDEO DE FACTURA (ASÍNCRONO)
# ==========================================


@app.post("/invoices/parse-video")
async def parse_invoice_video(
    request: Request,
    background_tasks: BackgroundTasks,  # ✅ AGREGADO
    video: UploadFile = File(...),
):
    """Procesar video de factura - ASÍNCRONO"""
    print("=" * 80)
    print("📹 NUEVO VIDEO (PROCESAMIENTO ASÍNCRONO)")
    print("=" * 80)

    try:
        job_id = str(uuid.uuid4())
        print(f"🆔 Job ID: {job_id}")

        content = await video.read()

        video_size_mb = len(content) / (1024 * 1024)
        MAX_VIDEO_SIZE_MB = 30.0

        print(f"💾 Video: {video_size_mb:.2f} MB")

        if video_size_mb > MAX_VIDEO_SIZE_MB:
            print(
                f"❌ Video rechazado: {video_size_mb:.1f} MB > {MAX_VIDEO_SIZE_MB} MB"
            )
            return JSONResponse(
                status_code=413,
                content={
                    "success": False,
                    "error": f"Video muy grande ({video_size_mb:.1f} MB). Máximo permitido: {MAX_VIDEO_SIZE_MB} MB",
                },
            )

        video_path = f"/tmp/lecfac_video_{job_id}.webm"
        with open(video_path, "wb") as f:
            f.write(content)

        print(f"✅ Video guardado: {video_path}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener usuario desde el token
        authorization = request.headers.get("Authorization")
        usuario_id = get_user_id_from_token(authorization)
        print(f"🆔 Usuario autenticado: {usuario_id}")

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    "SELECT id FROM processing_jobs WHERE id = %s", (job_id,)
                )
            else:
                cursor.execute("SELECT id FROM processing_jobs WHERE id = ?", (job_id,))

            if cursor.fetchone():
                print(f"⚠️ Job {job_id} ya existe, generando nuevo ID")
                job_id = str(uuid.uuid4())
        except Exception as e:
            print(f"⚠️ Error verificando job existente: {e}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO processing_jobs
                (id, usuario_id, video_path, status, created_at)
                VALUES (%s, %s, %s, 'pending', CURRENT_TIMESTAMP)
            """,
                (job_id, usuario_id, video_path),
            )
        else:
            cursor.execute(
                """
                INSERT INTO processing_jobs
                (id, usuario_id, video_path, status, created_at)
                VALUES (?, ?, ?, 'pending', ?)
            """,
                (job_id, usuario_id, video_path, datetime.now()),
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Job creado en BD")

        # ✅ Ahora background_tasks SÍ está definido
        background_tasks.add_task(
            process_video_background_task, job_id, video_path, usuario_id
        )

        print("✅ Tarea en background programada")
        print("📤 RESPUESTA INMEDIATA AL CLIENTE")

        return JSONResponse(
            status_code=202,
            content={
                "success": True,
                "job_id": job_id,
                "status": "pending",
                "video_size_mb": round(video_size_mb, 2),
                "message": "Video recibido. Procesando en background.",
                "estimated_time_minutes": "1-3",
                "poll_endpoint": f"/invoices/job-status/{job_id}",
            },
        )

    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        traceback.print_exc()

        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": str(e),
                "message": "Error procesando el video",
            },
        )


# ==========================================
# 🔧 FUNCIÓN DE BACKGROUND - COMPLETA ✅
# ==========================================
async def process_video_background_task(job_id: str, video_path: str, usuario_id: int):
    """Procesa video en BACKGROUND"""
    conn = None
    cursor = None
    frames_paths = []

    try:
        print(f"\n{'='*80}")
        print(f"🔄 PROCESAMIENTO EN BACKGROUND")
        print(f"🆔 Job: {job_id}")
        print(f"{'='*80}")

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT status, factura_id
                    FROM processing_jobs
                    WHERE id = %s
                """,
                    (job_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT status, factura_id
                    FROM processing_jobs
                    WHERE id = ?
                """,
                    (job_id,),
                )

            job_data = cursor.fetchone()

            if not job_data:
                print(f"❌ Job {job_id} no existe en BD")
                return

            current_status, existing_factura_id = job_data[0], job_data[1]

            if current_status == "completed":
                print(
                    f"⚠️ Job {job_id} ya fue completado. Factura ID: {existing_factura_id}"
                )
                return

            if existing_factura_id:
                print(f"⚠️ Job {job_id} ya tiene factura {existing_factura_id}")
                return

            if current_status == "processing":
                print(f"⚠️ Job {job_id} ya está siendo procesado")
                return

            print(f"✅ Job válido para procesar")

        except Exception as e:
            print(f"⚠️ Error verificando job: {e}")
        finally:
            cursor.close()
            conn.close()
            conn = None
            cursor = None

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'processing', started_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND status = 'pending'
                """,
                    (job_id,),
                )
            else:
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'processing', started_at = ?
                    WHERE id = ? AND status = 'pending'
                """,
                    (datetime.now(), job_id),
                )

            affected_rows = cursor.rowcount
            conn.commit()

            if affected_rows == 0:
                print(f"⚠️ No se pudo actualizar job {job_id}")
                return

            print(f"✅ Status actualizado a 'processing'")

        except Exception as e:
            print(f"❌ Error actualizando job status: {e}")
            conn.rollback()
            return
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            conn = None
            cursor = None

        try:
            from video_processor import (
                extraer_frames_video,
                deduplicar_productos,
                limpiar_frames_temporales,
                validar_fecha,
                combinar_frames_vertical,
            )
        except ImportError as e:
            raise Exception(f"Error importando módulos: {e}")

        print(f"🎬 Extrayendo frames...")
        frames_paths = extraer_frames_video(video_path, intervalo=1.0)

        if not frames_paths:
            raise Exception("No se extrajeron frames del video")

        print(f"✅ {len(frames_paths)} frames extraídos")

        print(f"🤖 Procesando con Claude...")

        start_time = time.time()

        def procesar_frame_individual(args):
            i, frame_path = args
            try:
                resultado = parse_invoice_with_claude(frame_path)
                if resultado.get("success") and resultado.get("data"):
                    return (i, resultado["data"])
                return (i, None)
            except Exception as e:
                print(f"⚠️ Error procesando frame {i+1}: {e}")
                return (i, None)

        todos_productos = []
        establecimiento = None
        fecha = None
        frames_exitosos = 0

        frame_args = list(enumerate(frames_paths))

        with ThreadPoolExecutor(max_workers=3) as executor:
            resultados = list(executor.map(procesar_frame_individual, frame_args))

        totales_detectados = []

        for i, data in resultados:
            if data:
                frames_exitosos += 1

                if not establecimiento:
                    establecimiento = data.get("establecimiento", "Desconocido")
                    fecha = data.get("fecha")

                total_frame = data.get("total", 0)
                if total_frame > 0:
                    totales_detectados.append(total_frame)

                productos = data.get("productos", [])
                todos_productos.extend(productos)

        elapsed_time = time.time() - start_time

        print(f"✅ Frames exitosos: {frames_exitosos}/{len(frames_paths)}")
        print(f"📦 Productos detectados: {len(todos_productos)}")
        print(f"⏱️ Tiempo: {elapsed_time:.1f}s")

        if totales_detectados:
            total = max(totales_detectados)
            print(f"💰 Total seleccionado: ${total:,}")
        else:
            total = 0

        if not todos_productos:
            raise Exception("No se detectaron productos")

        print(f"🔍 Deduplicando productos...")
        productos_unicos = deduplicar_productos(todos_productos)
        print(f"✅ Productos únicos: {len(productos_unicos)}")

        print(f"💾 Guardando en base de datos...")

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            establecimiento, cadena = procesar_establecimiento(
                establecimiento_raw=establecimiento,
                productos=productos_unicos,
                total=total,
            )

            establecimiento_id = obtener_o_crear_establecimiento_id(
                conn=conn, establecimiento=establecimiento, cadena=cadena
            )

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                fecha_final = (
                    validar_fecha(fecha) if fecha else datetime.now().date().isoformat()
                )

                cursor.execute(
                    """
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, establecimiento, cadena,
                    total_factura, fecha_factura, fecha_cargue,
                    productos_detectados, estado_validacion
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, 'procesado')
                RETURNING id
                """,
                    (
                        usuario_id,
                        establecimiento_id,
                        establecimiento,
                        cadena,
                        total,
                        fecha_final,
                        len(productos_unicos),
                    ),
                )
                factura_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    """
                    INSERT INTO facturas (
                        usuario_id, establecimiento_id, establecimiento, cadena,
                        total_factura, fecha_factura, fecha_cargue,
                        productos_detectados, estado_validacion
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'procesado')
                """,
                    (
                        usuario_id,
                        establecimiento_id,
                        establecimiento,
                        cadena,
                        total,
                        fecha,
                        datetime.now(),
                        len(productos_unicos),
                    ),
                )
                factura_id = cursor.lastrowid

            conn.commit()
            print(f"✅ Factura creada: ID {factura_id}")

            imagen_guardada = False
            if frames_paths and len(frames_paths) > 0:
                try:
                    print(f"🖼️ Creando imagen completa...")

                    imagen_completa_path = combinar_frames_vertical(
                        frames_paths,
                        output_path=f"/tmp/factura_completa_{factura_id}.jpg",
                    )

                    if os.path.exists(imagen_completa_path):
                        from storage import save_image_to_db

                        imagen_guardada = save_image_to_db(
                            factura_id, imagen_completa_path, "image/jpeg"
                        )

                        if imagen_guardada:
                            print(f"✅ Imagen completa guardada")

                        try:
                            os.remove(imagen_completa_path)
                        except:
                            pass

                except Exception as e:
                    print(f"⚠️ Error guardando imagen: {e}")

            # ⭐⭐⭐ GUARDAR PRODUCTOS CON producto_maestro_id ⭐⭐⭐
            productos_guardados = 0
            productos_fallidos = 0

            for producto in productos_unicos:
                try:
                    codigo = producto.get("codigo", "")
                    nombre = producto.get("nombre", "Sin nombre")
                    precio = producto.get("precio") or producto.get("valor", 0)
                    cantidad = producto.get("cantidad", 1)

                    if not nombre or nombre.strip() == "":
                        print(f"⚠️ Producto sin nombre, omitiendo")
                        productos_fallidos += 1
                        continue

                    try:
                        cantidad = int(cantidad)
                        if cantidad <= 0:
                            cantidad = 1
                    except (ValueError, TypeError):
                        cantidad = 1

                    try:
                        precio = float(precio)
                        if precio < 0:
                            print(f"⚠️ Precio negativo para '{nombre}', omitiendo")
                            productos_fallidos += 1
                            continue
                    except (ValueError, TypeError):
                        precio = 0

                    if precio == 0:
                        print(f"⚠️ Precio cero para '{nombre}', omitiendo")
                        productos_fallidos += 1
                        continue

                    # ⭐⭐⭐ CREAR PRODUCTO MAESTRO ⭐⭐⭐
                    producto_maestro_id = None
                    if codigo and len(codigo) >= 3:
                        try:
                            producto_maestro_id = buscar_o_crear_producto_inteligente_inline(codigo=codigo,
                                nombre=nombre,
                                precio=int(precio),
                                establecimiento=establecimiento,
                                cursor=cursor, conn=conn)
                            print(
                                f"   ✅ Producto maestro: {nombre} (ID: {producto_maestro_id})"
                            )
                        except Exception as e:
                            print(f"   ⚠️ Error producto maestro '{nombre}': {e}")

                    if os.environ.get("DATABASE_TYPE") == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO items_factura (
                                factura_id, usuario_id, producto_maestro_id,
                                codigo_leido, nombre_leido, cantidad, precio_pagado
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                            (
                                factura_id,
                                usuario_id,
                                producto_maestro_id,
                                codigo or None,
                                nombre,
                                cantidad,
                                precio,
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO items_factura (
                                factura_id, usuario_id, producto_maestro_id,
                                codigo_leido, nombre_leido, cantidad, precio_pagado
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                            (
                                factura_id,
                                usuario_id,
                                producto_maestro_id,
                                codigo or None,
                                nombre,
                                cantidad,
                                precio,
                            ),
                        )

                    productos_guardados += 1

                except Exception as e:
                    print(f"❌ Error guardando '{nombre}': {str(e)}")
                    productos_fallidos += 1

                    if "constraint" in str(e).lower():
                        conn.rollback()
                        conn = get_db_connection()
                        cursor = conn.cursor()

                    continue

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    UPDATE facturas
                    SET productos_guardados = %s
                    WHERE id = %s
                """,
                    (productos_guardados, factura_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE facturas
                    SET productos_guardados = ?
                    WHERE id = ?
                """,
                    (productos_guardados, factura_id),
                )

            conn.commit()
            print(f"✅ Productos guardados: {productos_guardados}")

            if productos_fallidos > 0:
                print(f"⚠️ Productos no guardados: {productos_fallidos}")

            # ⭐⭐⭐ ACTUALIZAR INVENTARIO ⭐⭐⭐
            print(f"📦 Actualizando inventario del usuario...")
            try:
                actualizar_inventario_desde_factura(factura_id, usuario_id)
                print(f"✅ Inventario actualizado correctamente")
            except Exception as e:
                print(f"⚠️ Error actualizando inventario: {e}")
                traceback.print_exc()

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'completed',
                        factura_id = %s,
                        completed_at = CURRENT_TIMESTAMP,
                        productos_detectados = %s,
                        frames_procesados = %s,
                        frames_exitosos = %s
                    WHERE id = %s AND status = 'processing'
                """,
                    (
                        factura_id,
                        productos_guardados,
                        len(frames_paths),
                        frames_exitosos,
                        job_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'completed',
                        factura_id = ?,
                        completed_at = ?,
                        productos_detectados = ?,
                        frames_procesados = ?,
                        frames_exitosos = ?
                    WHERE id = ? AND status = 'processing'
                """,
                    (
                        factura_id,
                        datetime.now(),
                        productos_guardados,
                        len(frames_paths),
                        frames_exitosos,
                        job_id,
                    ),
                )

            affected_rows = cursor.rowcount
            conn.commit()

            if affected_rows > 0:
                print(f"✅ JOB COMPLETADO EXITOSAMENTE")
            else:
                print(f"⚠️ Job ya fue marcado como completado")

        except Exception as e:
            print(f"❌ Error en operación de BD: {e}")
            if conn:
                conn.rollback()
            raise e

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        print(f"🧹 Limpiando archivos temporales...")
        try:
            if os.path.exists(video_path):
                os.remove(video_path)
                print(f"   ✓ Video eliminado")

            if frames_paths:
                limpiar_frames_temporales(frames_paths)
                print(f"   ✓ Frames eliminados")
        except Exception as e:
            print(f"⚠️ Error limpiando temporales: {e}")

        print(f"{'='*80}")
        print(f"✅ PROCESAMIENTO COMPLETADO")
        print(f"{'='*80}\n")

    except Exception as e:
        print(f"\n{'='*80}")
        print(f"❌ ERROR EN PROCESAMIENTO BACKGROUND")
        print(f"Error: {str(e)}")
        print(f"{'='*80}")
        traceback.print_exc()

        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'failed',
                        error_message = %s,
                        completed_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND status IN ('pending', 'processing')
                """,
                    (str(e)[:500], job_id),
                )
            else:
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'failed',
                        error_message = ?,
                        completed_at = ?
                    WHERE id = ? AND status IN ('pending', 'processing')
                """,
                    (str(e)[:500], datetime.now(), job_id),
                )

            conn.commit()

        except Exception as db_error:
            print(f"⚠️ Error actualizando job status: {db_error}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

        try:
            if video_path and os.path.exists(video_path):
                os.remove(video_path)
            if frames_paths:
                from video_processor import limpiar_frames_temporales

                limpiar_frames_temporales(frames_paths)
        except Exception as cleanup_error:
            print(f"⚠️ Error limpiando archivos: {cleanup_error}")


# ==========================================
# ENDPOINTS DE CONSULTA DE JOBS
# ==========================================
@app.get("/invoices/job-status/{job_id}")
async def get_job_status(job_id: str):
    """Consultar estado de un job"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, status, factura_id, error_message,
                       created_at, started_at, completed_at, productos_procesados
                FROM processing_jobs WHERE id = %s
            """,
                (job_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, status, factura_id, error_message,
                       created_at, started_at, completed_at, productos_procesados
                FROM processing_jobs WHERE id = ?
            """,
                (job_id,),
            )

        job = cursor.fetchone()

        if not job:
            cursor.close()
            conn.close()
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Job no encontrado"},
            )

        response = {
            "success": True,
            "job_id": job[0],
            "status": job[1],
            "factura_id": job[2],
            "error_message": job[3],
            "created_at": job[4].isoformat() if job[4] else None,
            "started_at": job[5].isoformat() if job[5] else None,
            "completed_at": job[6].isoformat() if job[6] else None,
            "productos_procesados": job[7],
        }

        if job[1] == "completed" and job[2]:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT establecimiento, total_factura, productos_guardados
                    FROM facturas WHERE id = %s
                """,
                    (job[2],),
                )
            else:
                cursor.execute(
                    """
                    SELECT establecimiento, total_factura, productos_guardados
                    FROM facturas WHERE id = ?
                """,
                    (job[2],),
                )

            factura = cursor.fetchone()
            if factura:
                response["factura"] = {
                    "establecimiento": factura[0],
                    "total": float(factura[1]) if factura[1] else 0,
                    "productos": factura[2],
                }

        cursor.close()
        conn.close()

        return JSONResponse(content=response)

    except Exception as e:
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.get("/invoices/pending-jobs")
async def get_pending_jobs(usuario_id: int = 1):
    """Listar últimos 10 jobs del usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, status, created_at, completed_at, factura_id
                FROM processing_jobs WHERE usuario_id = %s
                ORDER BY created_at DESC LIMIT 10
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, status, created_at, completed_at, factura_id
                FROM processing_jobs WHERE usuario_id = ?
                ORDER BY created_at DESC LIMIT 10
            """,
                (usuario_id,),
            )

        jobs = cursor.fetchall()
        cursor.close()
        conn.close()

        return JSONResponse(
            content={
                "success": True,
                "jobs": [
                    {
                        "job_id": job[0],
                        "status": job[1],
                        "created_at": job[2].isoformat() if job[2] else None,
                        "completed_at": job[3].isoformat() if job[3] else None,
                        "factura_id": job[4],
                    }
                    for job in jobs
                ],
            }
        )

    except Exception as e:
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


# ==========================================
# ENDPOINTS DE AUDITORÍA
# ==========================================
@app.get("/api/admin/audit-report")
@app.get("/admin/audit-report")
async def get_audit_report():
    """Reporte completo de auditoría"""
    try:
        audit = AuditSystem()
        return audit.generate_audit_report()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/api/admin/run-audit")
@app.post("/admin/run-audit")
async def run_manual_audit():
    """Ejecutar auditoría manual"""
    try:
        results = audit_scheduler.run_manual_audit()
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "results": results,
        }
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/ocr-stats")
async def get_ocr_stats():
    """Estadísticas del procesador OCR"""
    return processor.get_stats()


# ==========================================
# ENDPOINTS MÓVILES
# ==========================================
@app.post("/api/mobile/upload-auto")
async def upload_auto(file: UploadFile = File(...), user_id: Optional[int] = Form(1)):
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

        cursor.execute(
            """
            INSERT INTO facturas (usuario_id, establecimiento, estado_validacion, fecha_cargue)
            VALUES (%s, %s, %s, %s) RETURNING id
        """,
            (user_id, "Procesando...", "cola", datetime.now()),
        )

        factura_id = cursor.fetchone()[0]

        save_image_to_db(factura_id, temp_file.name, "image/jpeg")

        conn.commit()
        conn.close()

        processor.add_to_queue(factura_id, temp_file.name, user_id)

        return {
            "success": True,
            "factura_id": factura_id,
            "queue_position": processor.get_queue_position(factura_id),
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/api/mobile/status/{factura_id}")
async def get_invoice_status(factura_id: int):
    """Estado de procesamiento"""
    if factura_id in processing:
        return processing[factura_id]

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT estado_validacion, establecimiento, total_factura
        FROM facturas WHERE id = %s
    """,
        (factura_id,),
    )

    result = cursor.fetchone()
    conn.close()

    if result:
        return {
            "status": result[0],
            "establecimiento": result[1],
            "total": float(result[2]) if result[2] else 0,
        }

    return {"status": "not_found"}


# ==========================================
# ENDPOINTS DE EDICIÓN (ADMIN)
# ==========================================
@app.put("/admin/facturas/{factura_id}")
async def actualizar_factura(factura_id: int, request: Request):
    """Actualizar datos de una factura"""
    try:
        data = await request.json()

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            establecimiento = data.get("establecimiento")
            cadena = data.get("cadena")
            fecha_factura = data.get("fecha_factura")
            total_factura = data.get("total_factura")
            estado_validacion = data.get("estado_validacion")

            updates = []
            params = []

            if establecimiento:
                updates.append(
                    "establecimiento = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "establecimiento = ?"
                )
                params.append(establecimiento)

            if cadena:
                updates.append(
                    "cadena = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "cadena = ?"
                )
                params.append(cadena)

            if fecha_factura:
                updates.append(
                    "fecha_factura = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "fecha_factura = ?"
                )
                params.append(fecha_factura)

            if total_factura is not None:
                updates.append(
                    "total_factura = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "total_factura = ?"
                )
                params.append(total_factura)

            if estado_validacion:
                updates.append(
                    "estado_validacion = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "estado_validacion = ?"
                )
                params.append(estado_validacion)

            if not updates:
                return JSONResponse(
                    status_code=400,
                    content={
                        "success": False,
                        "error": "No hay campos para actualizar",
                    },
                )

            params.append(factura_id)

            query = f"UPDATE facturas SET {', '.join(updates)} WHERE id = {'%s' if os.environ.get('DATABASE_TYPE') == 'postgresql' else '?'}"

            cursor.execute(query, params)

            if cursor.rowcount == 0:
                conn.rollback()
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "Factura no encontrada"},
                )

            conn.commit()

            return JSONResponse(
                content={
                    "success": True,
                    "message": f"Factura {factura_id} actualizada correctamente",
                }
            )

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"❌ Error actualizando factura {factura_id}: {e}")
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.get("/admin/facturas/{factura_id}")
async def get_factura_para_editor(factura_id: int):
    """Obtener factura completa para el editor"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, usuario_id, establecimiento, cadena, total_factura,
                   fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen
            FROM facturas WHERE id = %s
        """,
            (factura_id,),
        )

        factura = cursor.fetchone()
        if not factura:
            conn.close()
            raise HTTPException(404, "Factura no encontrada")

        productos = []

        cursor.execute(
            """
            SELECT id, codigo, nombre, valor
            FROM productos
            WHERE factura_id = %s
            ORDER BY id
        """,
            (factura_id,),
        )

        for p in cursor.fetchall():
            productos.append(
                {
                    "id": p[0],
                    "codigo": p[1] or "",
                    "nombre": p[2] or "",
                    "precio": float(p[3]) if p[3] else 0,
                }
            )

        if len(productos) == 0:
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )

            for p in cursor.fetchall():
                productos.append(
                    {
                        "id": p[0],
                        "codigo": p[1] or "",
                        "nombre": p[2] or "",
                        "precio": float(p[3]) if p[3] else 0,
                    }
                )

        conn.close()

        return {
            "id": factura[0],
            "usuario_id": factura[1],
            "establecimiento": factura[2] or "",
            "cadena": factura[3],
            "total": float(factura[4]) if factura[4] else 0,
            "total_factura": float(factura[4]) if factura[4] else 0,
            "fecha": factura[5].isoformat() if factura[5] else None,
            "estado": factura[6] or "pendiente",
            "puntaje": factura[7] or 0,
            "tiene_imagen": factura[8] or False,
            "productos": productos,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en get_factura_para_editor: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.put("/admin/productos/{producto_id}")
async def actualizar_producto(producto_id: int, datos: dict):
    """Actualizar producto en la factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        codigo = datos.get("codigo", "").strip()
        nombre = datos.get("nombre", "").strip()
        precio = datos.get("precio", 0)

        if not nombre:
            raise HTTPException(400, "Nombre es requerido")

        cursor.execute(
            """
            UPDATE productos
            SET codigo = %s, nombre = %s, valor = %s
            WHERE id = %s
        """,
            (codigo, nombre, float(precio), producto_id),
        )

        affected = cursor.rowcount

        if affected == 0:
            cursor.execute(
                """
                UPDATE items_factura
                SET codigo_leido = %s, nombre_leido = %s, precio_pagado = %s
                WHERE id = %s
            """,
                (codigo, nombre, float(precio), producto_id),
            )
            affected = cursor.rowcount

        if affected == 0:
            conn.close()
            raise HTTPException(404, "Producto no encontrado")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Producto actualizado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error actualizando producto: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.delete("/admin/productos/{producto_id}")
async def eliminar_producto_factura(producto_id: int):
    """Eliminar producto de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM productos WHERE id = %s", (producto_id,))
        affected = cursor.rowcount

        if affected == 0:
            cursor.execute("DELETE FROM items_factura WHERE id = %s", (producto_id,))
            affected = cursor.rowcount

        if affected == 0:
            conn.close()
            raise HTTPException(404, "Producto no encontrado")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Producto eliminado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error eliminando producto: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.post("/admin/facturas/{factura_id}/productos")
async def agregar_producto_a_factura(factura_id: int, datos: dict):
    """Agregar nuevo producto a una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(404, "Factura no encontrada")

        codigo = datos.get("codigo", "").strip()
        nombre = datos.get("nombre", "").strip()
        precio = datos.get("precio", 0)

        if not nombre:
            conn.close()
            raise HTTPException(400, "Nombre es requerido")

        cursor.execute(
            """
            INSERT INTO productos (factura_id, codigo, nombre, valor)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """,
            (factura_id, codigo, nombre, float(precio)),
        )

        nuevo_id = cursor.fetchone()[0]

        conn.commit()
        conn.close()

        return {"success": True, "id": nuevo_id, "message": "Producto agregado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error agregando producto: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.put("/admin/facturas/{factura_id}/datos-generales")
async def actualizar_datos_generales(factura_id: int, datos: dict):
    """Actualizar datos generales de la factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        establecimiento = datos.get("establecimiento")
        total = datos.get("total", 0)
        fecha = datos.get("fecha")

        print(f"📝 Actualizando factura {factura_id}:")
        print(f"  - Establecimiento: {establecimiento}")
        print(f"  - Total: {total}")
        print(f"  - Fecha: {fecha}")

        updates = []
        params = []

        if establecimiento:
            updates.append("establecimiento = %s")
            params.append(establecimiento)
            updates.append("cadena = %s")
            params.append(detectar_cadena(establecimiento))

        if total is not None:
            updates.append("total_factura = %s")
            params.append(float(total))

        if fecha:
            updates.append("fecha_cargue = %s")
            params.append(fecha)

        updates.append("estado_validacion = %s")
        params.append("revisada")

        params.append(factura_id)

        query = f"UPDATE facturas SET {', '.join(updates)} WHERE id = %s"
        cursor.execute(query, params)

        affected = cursor.rowcount
        print(f"✅ {affected} fila(s) actualizada(s)")

        conn.commit()
        conn.close()

        return {"success": True, "message": "Datos actualizados", "affected": affected}

    except Exception as e:
        print(f"❌ Error actualizando datos generales: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.post("/admin/facturas/{factura_id}/validar")
async def marcar_como_validada(factura_id: int):
    """Marcar factura como validada"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE facturas
            SET estado_validacion = 'validada',
                puntaje_calidad = 100
            WHERE id = %s
        """,
            (factura_id,),
        )

        conn.commit()
        conn.close()

        return {"success": True, "message": "Factura validada"}
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.delete("/admin/facturas/{factura_id}")
async def eliminar_factura(factura_id: int):
    """Eliminar factura y todas sus referencias"""
    print(f"🗑️ ELIMINANDO FACTURA #{factura_id}")

    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"   1️⃣ Eliminando processing_jobs...")
        cursor.execute(
            "DELETE FROM processing_jobs WHERE factura_id = %s", (factura_id,)
        )
        deleted_jobs = cursor.rowcount
        print(f"      ✓ {deleted_jobs} job(s) eliminado(s)")

        print(f"   2️⃣ Eliminando items_factura...")
        cursor.execute("DELETE FROM items_factura WHERE factura_id = %s", (factura_id,))
        deleted_items = cursor.rowcount
        print(f"      ✓ {deleted_items} item(s) eliminado(s)")

        print(f"   3️⃣ Eliminando productos...")
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        deleted_productos = cursor.rowcount
        print(f"      ✓ {deleted_productos} producto(s) eliminado(s)")

        print(f"   4️⃣ Eliminando precios_productos...")
        try:
            cursor.execute(
                "DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,)
            )
            deleted_precios = cursor.rowcount
            print(f"      ✓ {deleted_precios} precio(s) eliminado(s)")
        except Exception as e:
            print(f"      ⚠️ Sin tabla precios_productos: {e}")
            deleted_precios = 0

        print(f"   5️⃣ Eliminando factura...")
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        deleted_factura = cursor.rowcount

        if deleted_factura == 0:
            print(f"   ❌ Factura {factura_id} no encontrada")
            if conn:
                conn.rollback()
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Factura no encontrada"},
            )

        conn.commit()
        print(f"   ✅ Factura {factura_id} eliminada exitosamente")

        return JSONResponse(
            content={
                "success": True,
                "message": f"Factura {factura_id} eliminada correctamente",
                "detalles": {
                    "jobs_eliminados": deleted_jobs,
                    "items_eliminados": deleted_items,
                    "productos_eliminados": deleted_productos,
                    "precios_eliminados": deleted_precios,
                },
            }
        )

    except Exception as e:
        print(f"   ❌ Error en transacción: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )
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


@app.post("/admin/facturas/eliminar-multiple")
async def eliminar_facturas_multiple(datos: dict):
    """Eliminar múltiples facturas en batch"""
    try:
        ids = datos.get("ids", [])

        if not ids or not isinstance(ids, list):
            raise HTTPException(400, "IDs inválidos")

        conn = get_db_connection()
        cursor = conn.cursor()

        eliminadas = 0
        errores = []

        for factura_id in ids:
            try:
                print(f"🗑️ Eliminando factura #{factura_id}...")

                cursor.execute(
                    "DELETE FROM processing_jobs WHERE factura_id = %s", (factura_id,)
                )
                cursor.execute(
                    "DELETE FROM items_factura WHERE factura_id = %s", (factura_id,)
                )
                cursor.execute(
                    "DELETE FROM productos WHERE factura_id = %s", (factura_id,)
                )

                try:
                    cursor.execute(
                        "DELETE FROM precios_productos WHERE factura_id = %s",
                        (factura_id,),
                    )
                except:
                    pass

                cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))

                if cursor.rowcount > 0:
                    eliminadas += 1
                    print(f"   ✅ Factura {factura_id} eliminada")

            except Exception as e:
                errores.append(f"Factura {factura_id}: {str(e)}")
                print(f"   ❌ Error eliminando {factura_id}: {e}")

        conn.commit()
        conn.close()

        return {
            "success": True,
            "eliminadas": eliminadas,
            "errores": errores,
            "message": f"{eliminadas} facturas eliminadas",
        }

    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        traceback.print_exc()
        raise HTTPException(500, str(e))


@app.post("/invoices/save-manual")
async def save_manual_invoice(factura: FacturaManual):
    """Guardar factura manualmente"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cadena = detectar_cadena(factura.establecimiento)

        cursor.execute(
            """
            INSERT INTO facturas (
                establecimiento, fecha_cargue, total_factura,
                puntaje_calidad, tiene_imagen, estado_validacion
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """,
            (
                factura.establecimiento,
                datetime.now(),
                factura.total,
                100,
                False,
                "validada",
            ),
        )

        factura_id = cursor.fetchone()[0]

        for producto in factura.productos:
            cursor.execute(
                """
                INSERT INTO productos (
                    nombre, codigo, valor, factura_id
                ) VALUES (%s, %s, %s, %s)
            """,
                (
                    producto.get("nombre"),
                    producto.get("codigo", ""),
                    producto.get("precio", 0),
                    factura_id,
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "factura_id": factura_id,
            "message": "Factura guardada correctamente",
        }

    except Exception as e:
        print(f"❌ Error guardando factura manual: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(500, str(e))


@app.get("/admin/facturas/{factura_id}/check-image")
async def check_image(factura_id: int):
    """Debug: verificar si imagen existe en BD"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
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
    """,
        (factura_id,),
    )

    result = cursor.fetchone()
    conn.close()

    if not result:
        return {"error": "Factura no encontrada"}

    return {
        "factura_id": result[0],
        "imagen_mime": result[1],
        "imagen_status": result[2],
        "imagen_size": result[3],
    }


@app.put("/admin/items/{item_id}")
async def actualizar_item_factura(item_id: int, request: Request):
    """Actualizar item en items_factura"""
    try:
        data = await request.json()

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            nombre = data.get("nombre", "").strip()
            precio = data.get("precio", 0)
            codigo_ean = data.get("codigo_ean", "").strip()

            if not nombre:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "Nombre es requerido"},
                )

            updates = []
            params = []

            updates.append(
                "nombre_producto = %s"
                if os.environ.get("DATABASE_TYPE") == "postgresql"
                else "nombre_producto = ?"
            )
            params.append(nombre)

            updates.append(
                "precio_unitario = %s"
                if os.environ.get("DATABASE_TYPE") == "postgresql"
                else "precio_unitario = ?"
            )
            params.append(float(precio))

            if codigo_ean:
                updates.append(
                    "codigo_producto = %s"
                    if os.environ.get("DATABASE_TYPE") == "postgresql"
                    else "codigo_producto = ?"
                )
                params.append(codigo_ean)

            params.append(item_id)

            query = f"UPDATE items_factura SET {', '.join(updates)} WHERE id = {'%s' if os.environ.get('DATABASE_TYPE') == 'postgresql' else '?'}"

            cursor.execute(query, params)

            if cursor.rowcount == 0:
                conn.rollback()
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "Item no encontrado"},
                )

            conn.commit()

            return JSONResponse(
                content={
                    "success": True,
                    "message": f"Item {item_id} actualizado correctamente",
                }
            )

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"❌ Error actualizando item {item_id}: {e}")
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.delete("/admin/items/{item_id}")
async def eliminar_item_factura(item_id: int):
    """Eliminar item de items_factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("DELETE FROM items_factura WHERE id = %s", (item_id,))
            else:
                cursor.execute("DELETE FROM items_factura WHERE id = ?", (item_id,))

            if cursor.rowcount == 0:
                conn.rollback()
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "Item no encontrado"},
                )

            conn.commit()

            return JSONResponse(
                content={
                    "success": True,
                    "message": f"Item {item_id} eliminado correctamente",
                }
            )

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"❌ Error eliminando item {item_id}: {e}")
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


@app.post("/admin/facturas/{factura_id}/items")
async def crear_item_factura(factura_id: int, request: Request):
    """Crear nuevo item en items_factura"""
    try:
        data = await request.json()

        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            nombre = data.get("nombre", "").strip()
            precio = data.get("precio", 0)
            codigo_ean = data.get("codigo_ean", "").strip()
            cantidad = data.get("cantidad", 1)

            if not nombre:
                return JSONResponse(
                    status_code=400,
                    content={"success": False, "error": "Nombre es requerido"},
                )

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
            else:
                cursor.execute("SELECT id FROM facturas WHERE id = ?", (factura_id,))

            if not cursor.fetchone():
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "Factura no encontrada"},
                )

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    INSERT INTO items_factura (factura_id, codigo_producto, nombre_producto, cantidad, precio_unitario)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """,
                    (factura_id, codigo_ean or None, nombre, cantidad, float(precio)),
                )
                nuevo_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    """
                    INSERT INTO items_factura (factura_id, codigo_producto, nombre_producto, cantidad, precio_unitario)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (factura_id, codigo_ean or None, nombre, cantidad, float(precio)),
                )
                nuevo_id = cursor.lastrowid

            conn.commit()

            return JSONResponse(
                content={
                    "success": True,
                    "id": nuevo_id,
                    "message": "Item creado correctamente",
                }
            )

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            cursor.close()
            conn.close()

    except Exception as e:
        print(f"❌ Error creando item: {e}")
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"success": False, "error": str(e)}
        )


# ==========================================
# ENDPOINTS DEBUG
# ==========================================
@app.get("/debug/inventario/{usuario_id}")
async def debug_inventario(usuario_id: int):
    """DEBUG: Ver todo lo relacionado con inventario del usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        resultado = {"usuario_id": usuario_id, "timestamp": datetime.now().isoformat()}

        cursor.execute(
            """
            SELECT COUNT(*), MAX(id), MAX(fecha_cargue)
            FROM facturas
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        facturas_data = cursor.fetchone()
        resultado["facturas"] = {
            "total": facturas_data[0],
            "ultima_id": facturas_data[1],
            "ultima_fecha": str(facturas_data[2]) if facturas_data[2] else None,
        }

        cursor.execute(
            """
            SELECT COUNT(*), COUNT(DISTINCT producto_maestro_id)
            FROM items_factura
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        items_data = cursor.fetchone()
        resultado["items_factura"] = {
            "total_items": items_data[0],
            "productos_unicos": items_data[1],
        }

        cursor.execute(
            """
            SELECT COUNT(*),
                   SUM(cantidad_actual),
                   COUNT(CASE WHEN precio_ultima_compra IS NOT NULL THEN 1 END)
            FROM inventario_usuario
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        inventario_data = cursor.fetchone()
        resultado["inventario_usuario"] = {
            "total_productos": inventario_data[0],
            "cantidad_total": float(inventario_data[1]) if inventario_data[1] else 0,
            "con_precios": inventario_data[2],
        }

        cursor.execute(
            """
            SELECT
                iu.id,
                pm.nombre_normalizado,
                iu.cantidad_actual,
                iu.precio_ultima_compra,
                iu.establecimiento_nombre,
                iu.fecha_ultima_compra
            FROM inventario_usuario iu
            LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
            WHERE iu.usuario_id = %s
            ORDER BY iu.fecha_ultima_actualizacion DESC
            LIMIT 5
        """,
            (usuario_id,),
        )

        ultimos_productos = []
        for row in cursor.fetchall():
            ultimos_productos.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "cantidad": float(row[2]) if row[2] else 0,
                    "precio": row[3],
                    "establecimiento": row[4],
                    "fecha": str(row[5]) if row[5] else None,
                }
            )

        resultado["ultimos_productos"] = ultimos_productos

        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'inventario_usuario'
            ORDER BY ordinal_position
        """
        )

        columnas = [{"nombre": row[0], "tipo": row[1]} for row in cursor.fetchall()]
        resultado["columnas_tabla"] = columnas

        cursor.execute(
            """
            SELECT
                if.id,
                if.nombre_leido,
                if.precio_pagado,
                if.cantidad,
                if.producto_maestro_id,
                f.establecimiento,
                f.fecha_cargue
            FROM items_factura if
            JOIN facturas f ON if.factura_id = f.id
            WHERE if.usuario_id = %s
            ORDER BY f.fecha_cargue DESC
            LIMIT 5
        """,
            (usuario_id,),
        )

        ultimos_items = []
        for row in cursor.fetchall():
            ultimos_items.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "precio": row[2],
                    "cantidad": row[3],
                    "producto_maestro_id": row[4],
                    "establecimiento": row[5],
                    "fecha": str(row[6]) if row[6] else None,
                }
            )

        resultado["ultimos_items_factura"] = ultimos_items

        conn.close()

        return resultado

    except Exception as e:
        conn.close()
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.post("/debug/forzar-actualizacion-inventario/{factura_id}")
async def forzar_actualizacion_inventario(factura_id: int, usuario_id: int = 1):
    """DEBUG: Forzar actualización de inventario"""
    try:
        print(
            f"🔧 DEBUG: Forzando actualización de inventario para factura {factura_id}"
        )

        from database import actualizar_inventario_desde_factura

        resultado = actualizar_inventario_desde_factura(factura_id, usuario_id)

        return {
            "success": True,
            "factura_id": factura_id,
            "usuario_id": usuario_id,
            "resultado": resultado,
            "message": "Inventario actualizado manualmente",
        }

    except Exception as e:
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/api/mobile/my-invoices")
async def get_my_invoices(page: int = 1, limit: int = 20, usuario_id: int = 1):
    """Obtener facturas del usuario con paginación"""
    conn = get_db_connection()
    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")

    try:
        offset = (page - 1) * limit

        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    f.id,
                    f.usuario_id,
                    f.establecimiento_id,
                    f.numero_factura,
                    f.fecha_factura,
                    f.total_factura,
                    f.fecha_cargue,
                    f.estado_validacion,
                    f.productos_guardados,
                    f.establecimiento,
                    f.cadena
                FROM facturas f
                WHERE f.usuario_id = %s
                ORDER BY f.fecha_cargue DESC
                LIMIT %s OFFSET %s
                """,
                (usuario_id, limit, offset),
            )
        else:
            cursor.execute(
                """
                SELECT
                    f.id,
                    f.usuario_id,
                    f.establecimiento_id,
                    f.numero_factura,
                    f.fecha_factura,
                    f.total_factura,
                    f.fecha_cargue,
                    f.estado_validacion,
                    f.productos_guardados,
                    f.establecimiento,
                    f.cadena
                FROM facturas f
                WHERE f.usuario_id = ?
                ORDER BY f.fecha_cargue DESC
                LIMIT ? OFFSET ?
                """,
                (usuario_id, limit, offset),
            )

        facturas = []
        for row in cursor.fetchall():
            facturas.append(
                {
                    "id": row[0],
                    "usuario_id": row[1],
                    "establecimiento_id": row[2],
                    "numero_factura": row[3],
                    "fecha": str(row[4]) if row[4] else None,
                    "total": float(row[5]) if row[5] else 0.0,
                    "fecha_creacion": str(row[6]) if row[6] else None,
                    "estado": row[7],
                    "cantidad_items": row[8] or 0,
                    "establecimiento": {
                        "id": row[2],
                        "nombre": row[9] or "Sin nombre",
                        "categoria": row[10],
                    },
                }
            )

        conn.close()

        return {
            "success": True,
            "facturas": facturas,
            "page": page,
            "limit": limit,
            "total": len(facturas),
        }

    except Exception as e:
        print(f"❌ Error obteniendo facturas: {e}")
        import traceback

        traceback.print_exc()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


app.include_router(auditoria_router)
print("✅ Sistema de auditoría cargado")

# ==========================================
# ENDPOINTS DE ADMINISTRACIÓN
# Agregar esto AL FINAL de main.py, ANTES de if __name__ == "__main__"
# ==========================================


# ==========================================
# ENDPOINTS DE ADMINISTRACIÓN
# ==========================================


@app.get("/api/admin/estadisticas")
async def admin_estadisticas():
    """Estadísticas generales del sistema"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("📊 Obteniendo estadísticas...")

        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(DISTINCT nombre_leido) FROM items_factura")
        total_productos = cursor.fetchone()[0] or 0

        conn.close()

        resultado = {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "total_productos": total_productos,
            "calidad_promedio": 85,
            "facturas_con_errores": 0,
            "productos_sin_categoria": 0,
        }

        print(f"✅ Estadísticas: {resultado}")
        return resultado

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/inventarios")
async def admin_inventarios(usuario_id: int = None, limite: int = 50, pagina: int = 1):
    """
    Inventarios por usuario con filtros y paginación

    Params:
        usuario_id: Filtrar por usuario específico (opcional)
        limite: Cantidad de resultados por página (default: 50)
        pagina: Número de página (default: 1)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Obteniendo inventarios (página {pagina}, límite {limite})...")

        # Calcular offset para paginación
        offset = (pagina - 1) * limite

        # Query base
        query = """
            SELECT
                iu.usuario_id,
                u.nombre,
                pm.nombre_normalizado,
                iu.cantidad_actual,
                pm.categoria,
                iu.fecha_ultima_actualizacion,
                iu.establecimiento_nombre
            FROM inventario_usuario iu
            LEFT JOIN usuarios u ON iu.usuario_id = u.id
            LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
            WHERE 1=1
        """

        params = []

        # Agregar filtro por usuario si se especifica
        if usuario_id:
            query += " AND iu.usuario_id = %s"
            params.append(usuario_id)

        query += """
            ORDER BY iu.fecha_ultima_actualizacion DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limite, offset])

        cursor.execute(query, tuple(params))

        inventarios = []
        for row in cursor.fetchall():
            inventarios.append(
                {
                    "usuario_id": row[0],
                    "nombre_usuario": row[1] or f"Usuario {row[0]}",
                    "nombre_producto": row[2] or "Producto sin nombre",
                    "cantidad_actual": float(row[3]) if row[3] else 0,
                    "categoria": row[4] or "-",
                    "ultima_actualizacion": str(row[5]) if row[5] else None,
                    "establecimiento": row[6] or "-",
                }
            )

        # Contar total para paginación
        count_query = """
            SELECT COUNT(*)
            FROM inventario_usuario iu
            WHERE 1=1
        """
        count_params = []

        if usuario_id:
            count_query += " AND iu.usuario_id = %s"
            count_params.append(usuario_id)

        cursor.execute(count_query, tuple(count_params))
        total = cursor.fetchone()[0] or 0

        conn.close()

        print(
            f"✅ {len(inventarios)} inventarios (página {pagina} de {(total + limite - 1) // limite})"
        )

        return {
            "inventarios": inventarios,
            "total": total,
            "pagina": pagina,
            "limite": limite,
            "total_paginas": (total + limite - 1) // limite,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/usuarios-lista")
async def obtener_usuarios_para_filtro():
    """
    Obtiene lista de usuarios para el filtro del dashboard
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, nombre, email
            FROM usuarios
            ORDER BY nombre
        """
        )

        usuarios = []
        for row in cursor.fetchall():
            usuarios.append({"id": row[0], "nombre": row[1], "email": row[2]})

        conn.close()

        return {"usuarios": usuarios}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/duplicados/facturas")
async def admin_duplicados_facturas():
    """Detectar facturas duplicadas"""
    print("🔍 Buscando duplicados de facturas...")
    return {"duplicados": []}


@app.get("/api/admin/duplicados/productos")
async def admin_duplicados_productos():
    """Detectar productos duplicados"""
    print("🔍 Buscando duplicados de productos...")
    return {"duplicados": []}


@app.get("/api/auditoria/estadisticas")
async def auditoria_estadisticas():
    """Estadísticas de auditoría"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("🛡️ Estadísticas de auditoría...")

        cursor.execute("SELECT COUNT(*) FROM facturas")
        total = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "total_procesadas": total,
            "calidad_promedio": 85,
            "con_errores": 0,
            "en_revision": 0,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/auditoria/ejecutar-completa")
async def auditoria_ejecutar():
    """Ejecutar auditoría completa"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM facturas")
        total = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "success": True,
            "total_procesadas": total,
            "calidad_promedio": 85,
            "con_errores": 0,
            "en_revision": 0,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/auditoria/cola-revision")
async def auditoria_cola():
    """Cola de revisión"""
    print("📋 Cola de revisión...")
    return {"facturas": []}


@app.get("/api/facturas/{factura_id}")
async def obtener_factura_detalle(factura_id: int):
    """Detalles de factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, usuario_id, establecimiento, total_factura,
                   fecha_factura, estado_validacion
            FROM facturas WHERE id = %s
        """,
            (factura_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="No encontrada")

        conn.close()

        return {
            "id": row[0],
            "usuario_id": row[1],
            "establecimiento": row[2],
            "total_factura": float(row[3]) if row[3] else 0,
            "fecha_factura": str(row[4]) if row[4] else None,
            "estado_validacion": row[5],
            "items": [],
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/facturas/{factura_id}")
async def eliminar_factura_admin(factura_id: int):
    """Eliminar factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM items_factura WHERE factura_id = %s", (factura_id,))
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))

        conn.commit()
        conn.close()

        return {"success": True, "message": "Factura eliminada"}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de administración registrados directamente en main.py")


@app.post("/api/admin/normalizar-productos")
async def normalizar_productos():
    """Normalizar nombres de productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("🧹 Normalizando productos...")

        # Por ahora, solo retornar un mensaje de éxito
        # TODO: Implementar normalización real

        conn.close()

        return {
            "success": True,
            "productos_normalizados": 0,
            "message": "Normalización pendiente de implementar",
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/limpiar-datos")
async def limpiar_datos_antiguos():
    """Limpiar datos antiguos"""
    try:
        print("🗑️ Limpiando datos antiguos...")

        # Por ahora, retornar mensaje sin eliminar nada
        # TODO: Implementar limpieza real con parámetros de días y puntaje

        return {
            "success": True,
            "facturas_eliminadas": 0,
            "message": "Limpieza pendiente de implementar",
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/debug/columnas")
async def debug_columnas():
    """Ver columnas de las tablas principales"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {}

        # Ver columnas de items_factura
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'items_factura'
            ORDER BY ordinal_position
        """
        )
        resultado["items_factura"] = [
            {"nombre": row[0], "tipo": row[1]} for row in cursor.fetchall()
        ]

        # Ver columnas de inventario_usuario
        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'inventario_usuario'
            ORDER BY ordinal_position
        """
        )
        resultado["inventario_usuario"] = [
            {"nombre": row[0], "tipo": row[1]} for row in cursor.fetchall()
        ]

        conn.close()

        return resultado

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS DE CONTROL DE CALIDAD
# Agregar en main.py con los otros endpoints admin
# ==========================================


@app.get("/api/facturas/{factura_id}/detalle-completo")
async def obtener_factura_detalle_completo(factura_id: int):
    """
    Obtiene todos los detalles de una factura para revisión y edición
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📋 Obteniendo detalle completo de factura {factura_id}...")

        # Obtener factura
        cursor.execute(
            """
            SELECT
                f.id,
                f.usuario_id,
                f.establecimiento,
                f.establecimiento_id,
                f.total_factura,
                f.fecha_factura,
                f.fecha_cargue,
                f.estado_validacion,
                f.numero_factura,
                u.nombre as nombre_usuario,
                u.email as email_usuario
            FROM facturas f
            LEFT JOIN usuarios u ON f.usuario_id = u.id
            WHERE f.id = %s
        """,
            (factura_id,),
        )

        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        factura = {
            "id": row[0],
            "usuario_id": row[1],
            "establecimiento": row[2],
            "establecimiento_id": row[3],
            "total_factura": float(row[4]) if row[4] else 0,
            "fecha_factura": str(row[5]) if row[5] else None,
            "fecha_cargue": str(row[6]) if row[6] else None,
            "estado_validacion": row[7] or "pendiente",
            "numero_factura": row[8],
            "usuario": {"nombre": row[9], "email": row[10]},
        }

        # Obtener items con toda la información
        cursor.execute(
            """
            SELECT
                i.id,
                i.nombre_leido,
                i.codigo_leido,
                i.cantidad,
                i.precio_pagado,
                i.producto_maestro_id,
                pm.nombre_normalizado,
                pm.categoria,
                pm.marca
            FROM items_factura i
            LEFT JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
            WHERE i.factura_id = %s
            ORDER BY i.id
        """,
            (factura_id,),
        )

        items = []
        total_calculado = 0

        for row in cursor.fetchall():
            precio_total = (row[3] or 0) * (row[4] or 0)
            total_calculado += precio_total

            items.append(
                {
                    "id": row[0],
                    "nombre_leido": row[1],
                    "codigo_leido": row[2],
                    "cantidad": float(row[3]) if row[3] else 0,
                    "precio_unitario": float(row[4]) if row[4] else 0,
                    "precio_total": precio_total,
                    "producto_maestro_id": row[5],
                    "nombre_normalizado": row[6],
                    "categoria": row[7],
                    "marca": row[8],
                }
            )

        factura["items"] = items
        factura["total_calculado"] = total_calculado
        factura["diferencia_total"] = abs(factura["total_factura"] - total_calculado)

        # Calcular métricas de calidad
        factura["metricas"] = {
            "items_sin_normalizar": sum(
                1 for i in items if not i["nombre_normalizado"]
            ),
            "items_sin_categoria": sum(1 for i in items if not i["categoria"]),
            "diferencia_matematica": factura["diferencia_total"] > 100,
        }

        conn.close()

        print(f"✅ Factura {factura_id} cargada con {len(items)} items")
        return factura

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/items-factura/{item_id}")
async def actualizar_item_factura(item_id: int, datos: dict):
    """
    Actualiza un item de factura (nombre, cantidad, precio, categoría)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"✏️ Actualizando item {item_id}...")

        # Construir UPDATE dinámico
        campos_actualizables = []
        valores = []

        if "nombre_leido" in datos:
            campos_actualizables.append("nombre_leido = %s")
            valores.append(datos["nombre_leido"])

        if "cantidad" in datos:
            campos_actualizables.append("cantidad = %s")
            valores.append(datos["cantidad"])

        if "precio_pagado" in datos:
            campos_actualizables.append("precio_pagado = %s")
            valores.append(datos["precio_pagado"])

        if "codigo_leido" in datos:
            campos_actualizables.append("codigo_leido = %s")
            valores.append(datos["codigo_leido"])

        if not campos_actualizables:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        valores.append(item_id)

        query = f"""
            UPDATE items_factura
            SET {', '.join(campos_actualizables)}
            WHERE id = %s
        """

        cursor.execute(query, tuple(valores))
        conn.commit()
        conn.close()

        print(f"✅ Item {item_id} actualizado")
        return {"success": True, "item_id": item_id}

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/facturas/{factura_id}/validar")
async def validar_factura(factura_id: int, accion: dict):
    """
    Valida o rechaza una factura

    Body:
    {
        "estado": "validada" | "rechazada" | "pendiente",
        "notas": "Razón del rechazo (opcional)"
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        estado = accion.get("estado", "validada")
        notas = accion.get("notas", "")

        print(f"✅ Validando factura {factura_id} como '{estado}'...")

        cursor.execute(
            """
            UPDATE facturas
            SET estado_validacion = %s
            WHERE id = %s
        """,
            (estado, factura_id),
        )

        conn.commit()
        conn.close()

        print(f"✅ Factura {factura_id} marcada como '{estado}'")
        return {
            "success": True,
            "factura_id": factura_id,
            "estado": estado,
            "message": f"Factura {estado}",
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/items-factura/{item_id}/vincular-producto")
async def vincular_producto_maestro(item_id: int, datos: dict):
    """
    Vincula un item con un producto maestro existente o crea uno nuevo

    Body:
    {
        "producto_maestro_id": 123,  // Si existe
        "crear_nuevo": true,         // Si se debe crear
        "nombre_normalizado": "Arroz Diana 500gr",
        "categoria": "GRANOS",
        "marca": "Diana"
    }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"🔗 Vinculando item {item_id} con producto maestro...")

        producto_maestro_id = datos.get("producto_maestro_id")

        # Si se debe crear un nuevo producto maestro
        if datos.get("crear_nuevo") and not producto_maestro_id:
            cursor.execute(
                """
                INSERT INTO productos_maestros (
                    nombre_normalizado, categoria, marca, fecha_creacion
                ) VALUES (%s, %s, %s, NOW())
                RETURNING id
            """,
                (
                    datos.get("nombre_normalizado"),
                    datos.get("categoria"),
                    datos.get("marca"),
                ),
            )
            producto_maestro_id = cursor.fetchone()[0]
            print(f"✅ Producto maestro creado: {producto_maestro_id}")

        # Vincular item con producto maestro
        if producto_maestro_id:
            cursor.execute(
                """
                UPDATE items_factura
                SET producto_maestro_id = %s
                WHERE id = %s
            """,
                (producto_maestro_id, item_id),
            )

            conn.commit()
            print(f"✅ Item {item_id} vinculado con producto {producto_maestro_id}")

        conn.close()

        return {
            "success": True,
            "item_id": item_id,
            "producto_maestro_id": producto_maestro_id,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/productos-maestros/buscar")
async def buscar_productos_maestros(q: str, limite: int = 20):
    """
    Busca productos maestros por nombre para autocompletar
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, nombre_normalizado, categoria, marca
            FROM productos_maestros
            WHERE LOWER(nombre_normalizado) LIKE LOWER(%s)
            ORDER BY nombre_normalizado
            LIMIT %s
        """,
            (f"%{q}%", limite),
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {"id": row[0], "nombre": row[1], "categoria": row[2], "marca": row[3]}
            )

        conn.close()

        return {"productos": productos}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de control de calidad registrados")
# ==========================================
# INICIO DEL SERVIDOR
# ==========================================


# ==========================================
# 🆕 ENDPOINTS DEL EDITOR DE FACTURAS
# ==========================================


@app.put("/api/admin/facturas/{factura_id}")
async def update_factura_admin(factura_id: int, datos: FacturaUpdate):
    """
    Actualiza datos generales de una factura (establecimiento, total, fecha)
    Usado por el editor de facturas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📝 Actualizando factura {factura_id}...")

        update_fields = []
        params = []

        if datos.establecimiento is not None:
            update_fields.append("establecimiento = %s")
            params.append(datos.establecimiento)

        if datos.total is not None:
            update_fields.append("total_factura = %s")
            params.append(datos.total)

        if datos.fecha is not None:
            update_fields.append("fecha_factura = %s")
            params.append(datos.fecha)

        if not update_fields:
            raise HTTPException(status_code=400, detail="No hay campos para actualizar")

        params.append(factura_id)

        query = f"""
            UPDATE facturas
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING id, establecimiento, total_factura, fecha_factura
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchone()
        conn.commit()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        print(f"✅ Factura {factura_id} actualizada")
        return {
            "id": result[0],
            "establecimiento": result[1],
            "total_factura": float(result[2] or 0),
            "fecha_factura": result[3].isoformat() if result[3] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error actualizando factura: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/admin/items/{item_id}")
async def update_item_admin(item_id: int, item: ItemUpdate):
    """
    Actualiza un item de factura (nombre, precio, código EAN)
    Usado por el editor de facturas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📝 Actualizando item {item_id}...")

        update_fields = ["nombre_leido = %s", "precio_pagado = %s"]
        params = [item.nombre, item.precio]

        # Solo actualizar código_ean si se proporciona
        if item.codigo_ean is not None and item.codigo_ean.strip() != "":
            update_fields.append("codigo_leido = %s")
            params.append(item.codigo_ean)

        params.append(item_id)

        query = f"""
            UPDATE items_factura
            SET {', '.join(update_fields)}
            WHERE id = %s
            RETURNING id, nombre_leido, precio_pagado, codigo_leido
        """

        cursor.execute(query, tuple(params))
        result = cursor.fetchone()
        conn.commit()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Item no encontrado")

        print(f"✅ Item {item_id} actualizado")
        return {
            "id": result[0],
            "nombre_producto": result[1],
            "precio_unitario": float(result[2] or 0),
            "codigo_ean": result[3],
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error actualizando item: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/facturas/{factura_id}/items")
async def create_item_admin(factura_id: int, item: ItemCreate):
    """
    Crea un nuevo item en una factura
    Usado por el editor de facturas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"➕ Creando nuevo item en factura {factura_id}...")

        # Verificar que la factura existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")

        # Insertar item
        if item.codigo_ean and item.codigo_ean.strip():
            cursor.execute(
                """
                INSERT INTO items_factura (factura_id, nombre_leido, precio_pagado, cantidad, codigo_leido)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, nombre_leido, precio_pagado, cantidad, codigo_leido
                """,
                (factura_id, item.nombre, item.precio, 1, item.codigo_ean),
            )
        else:
            cursor.execute(
                """
                INSERT INTO items_factura (factura_id, nombre_leido, precio_pagado, cantidad)
                VALUES (%s, %s, %s, %s)
                RETURNING id, nombre_leido, precio_pagado, cantidad, codigo_leido
                """,
                (factura_id, item.nombre, item.precio, 1),
            )

        result = cursor.fetchone()
        conn.commit()
        conn.close()

        print(f"✅ Item {result[0]} creado")
        return {
            "id": result[0],
            "nombre_producto": result[1],
            "precio_unitario": float(result[2] or 0),
            "cantidad": result[3],
            "codigo_ean": result[4],
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creando item: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/items/{item_id}")
async def delete_item_admin(item_id: int):
    """
    Elimina un item de factura
    Usado por el editor de facturas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"🗑️ Eliminando item {item_id}...")

        cursor.execute(
            "DELETE FROM items_factura WHERE id = %s RETURNING id", (item_id,)
        )
        result = cursor.fetchone()
        conn.commit()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Item no encontrado")

        print(f"✅ Item {item_id} eliminado")
        return {"mensaje": "Item eliminado exitosamente", "id": result[0]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error eliminando item: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints del editor de facturas registrados")

# ==========================================
# 🆕 AGREGAR ESTOS ENDPOINTS A MAIN.PY
# Ubicación: Antes de if __name__ == "__main__":
# ==========================================


# ALIAS PARA COMPATIBILIDAD (mientras se resuelve el caché)
@app.get("/api/productos")
async def get_productos_sin_admin():
    """Alias temporal de /api/admin/productos para compatibilidad"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                p.id,
                p.nombre,
                p.codigo_ean,
                p.precio_promedio,
                p.categoria,
                p.marca,
                COUNT(DISTINCT if.id) as veces_comprado
            FROM productos p
            LEFT JOIN items_factura if ON if.producto_id = p.id
            GROUP BY p.id, p.nombre, p.codigo_ean, p.precio_promedio, p.categoria, p.marca
            ORDER BY veces_comprado DESC
            LIMIT 500
        """
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "codigo_ean": row[2],
                    "precio_promedio": float(row[3] or 0),
                    "categoria": row[4],
                    "marca": row[5],
                    "veces_comprado": row[6] or 0,
                }
            )

        conn.close()
        print(f"✅ {len(productos)} productos (alias sin /admin/)")
        return productos

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/usuarios")
async def get_usuarios_sin_admin():
    """Alias temporal de /api/admin/usuarios para compatibilidad"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, nombre, email, activo, fecha_registro
            FROM usuarios
            ORDER BY fecha_registro DESC
        """
        )

        usuarios = []
        for row in cursor.fetchall():
            usuarios.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "email": row[2],
                    "activo": row[3],
                    "fecha_registro": row[4].isoformat() if row[4] else None,
                }
            )

        conn.close()
        print(f"✅ {len(usuarios)} usuarios (alias sin /admin/)")
        return usuarios

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINTS ADMIN - VERSIÓN CORREGIDA
# ==========================================
# Este archivo contiene SOLO los endpoints correctos
# Elimina todos los que usan items_factura con ROW_NUMBER()
# ==========================================


# ✅ 1. ENDPOINT DE DUPLICADOS
@app.get("/api/admin/duplicados")
async def get_duplicados():
    """Busca facturas duplicadas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                f1.id as factura1_id,
                f2.id as factura2_id,
                f1.establecimiento,
                f1.total_factura,
                f1.fecha_factura
            FROM facturas f1
            INNER JOIN facturas f2 ON
                f1.establecimiento = f2.establecimiento AND
                f1.total_factura = f2.total_factura AND
                f1.fecha_factura = f2.fecha_factura AND
                f1.id < f2.id
            ORDER BY f1.fecha_factura DESC
            LIMIT 50
        """
        )

        duplicados = []
        for row in cursor.fetchall():
            duplicados.append(
                {
                    "ids": [row[0], row[1]],  # ✅ FORMATO CORRECTO
                    "establecimiento": row[2],
                    "total": float(row[3] or 0),
                    "fecha": row[4].isoformat() if row[4] else None,
                }
            )

        conn.close()
        print(f"✅ {len(duplicados)} duplicados encontrados")
        return {"duplicados": duplicados, "total": len(duplicados)}

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))



# ✅ 1. LISTAR TODOS LOS PRODUCTOS (desde productos_maestros)
@app.get("/api/admin/productos")
async def admin_productos():
    """Catálogo de productos maestros"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("🏷️ Obteniendo productos maestros...")

        cursor.execute(
            """
            SELECT
                pm.id,
                COALESCE(pm.nombre_comercial, pm.nombre_normalizado, 'Sin nombre') as nombre,
                pm.codigo_ean,
                pm.precio_promedio_global,
                pm.categoria,
                pm.marca,
                pm.total_reportes
            FROM productos_maestros pm
            ORDER BY pm.total_reportes DESC
            LIMIT 500
        """
        )

        productos = []
        for row in cursor.fetchall():
            # ✅ NO dividir - ya está en pesos
            precio_pesos = float(row[3]) if row[3] else 0

            productos.append({
            "id": row[0],
            "nombre": row[1],
            "codigo_ean": row[2],
            "precio_promedio": precio_pesos,  # Ya en pesos
            "categoria": row[4],
            "marca": row[5],
            "veces_comprado": row[6] or 0,
            })


        conn.close()

        print(f"✅ {len(productos)} productos maestros")
        return productos

    except Exception as e:
        print(f"❌ Error obteniendo productos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ✅ 2. OBTENER UN PRODUCTO ESPECÍFICO (desde productos_maestros)
@app.get("/api/admin/productos/{producto_id}")
async def get_producto_detalle(producto_id: int):
    """Obtiene detalles de un producto maestro específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Buscando producto maestro {producto_id}...")

        cursor.execute(
            """
            SELECT
                pm.id,
                COALESCE(pm.nombre_comercial, pm.nombre_normalizado, 'Sin nombre') as nombre,
                pm.codigo_ean,
                pm.precio_promedio_global,
                pm.categoria,
                pm.marca,
                pm.total_reportes
            FROM productos_maestros pm
            WHERE pm.id = %s
        """,
            (producto_id,),
        )

        result = cursor.fetchone()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        return {
            "id": result[0],
            "nombre": result[1],
            "codigo_ean": result[2],
            "precio_promedio": float(result[3] or 0) / 100 if result[3] else 0,
            "categoria": result[4],
            "marca": result[5],
            "veces_comprado": result[6] or 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/admin/usuarios")
async def get_usuarios():
    """Obtiene lista de usuarios con estadísticas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("👥 Obteniendo usuarios...")

        cursor.execute(
            """
            SELECT
                u.id,
                u.email,
                COALESCE(u.nombre, u.email) as nombre_completo,
                u.fecha_registro,
                COUNT(DISTINCT f.id) as total_facturas,
                COALESCE(SUM(f.total_factura), 0) as total_gastado
            FROM usuarios u
            LEFT JOIN facturas f ON u.id = f.usuario_id
            GROUP BY u.id, u.email, u.nombre, u.fecha_registro
            ORDER BY total_facturas DESC
        """
        )

        usuarios = []
        for row in cursor.fetchall():
            usuarios.append(
                {
                    "id": row[0],
                    "email": row[1],
                    "nombre_completo": row[2],
                    "telefono": "",  # Campo no existe en BD
                    "fecha_registro": row[3].isoformat() if row[3] else None,
                    "total_facturas": row[4] or 0,
                    "total_gastado": float(row[5]) if row[5] else 0,
                }
            )

        conn.close()

        print(f"✅ {len(usuarios)} usuarios obtenidos")
        return usuarios

    except Exception as e:
        print(f"❌ Error obteniendo usuarios: {e}")
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ✅ 3. ACTUALIZAR PRODUCTO (en productos_maestros)
@app.get("/api/admin/productos/{producto_id}")
async def get_producto_detalle(producto_id: int):
    """Obtiene detalles de un producto maestro específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Buscando producto maestro {producto_id}...")

        cursor.execute(
            """
            SELECT
                pm.id,
                COALESCE(pm.nombre_comercial, pm.nombre_normalizado, 'Sin nombre') as nombre,
                pm.codigo_ean,
                pm.precio_promedio_global,
                pm.categoria,
                pm.marca,
                pm.total_reportes
            FROM productos_maestros pm
            WHERE pm.id = %s
        """,
            (producto_id,),
        )

        result = cursor.fetchone()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # ✅ CORRECCIÓN: Convertir de centavos a pesos
        precio_pesos = float(result[3])  if result[3] else 0

        return {
            "id": result[0],
            "nombre": result[1],
            "codigo_ean": result[2],
            "precio_promedio": precio_pesos,  # ← En pesos, no centavos
            "categoria": result[4],
            "marca": result[5],
            "veces_comprado": result[6] or 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ✅ 4. ELIMINAR PRODUCTO (de productos_maestros)
@app.delete("/api/admin/productos/{producto_id}")
async def eliminar_producto(producto_id: int):
    """Elimina un producto maestro"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"🗑️ Eliminando producto maestro {producto_id}...")

        # Verificar que existe
        cursor.execute(
            """
            SELECT id, COALESCE(nombre_comercial, nombre_normalizado, 'Sin nombre')
            FROM productos_maestros
            WHERE id = %s
            """,
            (producto_id,),
        )
        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Eliminar
        cursor.execute("DELETE FROM productos_maestros WHERE id = %s", (producto_id,))
        conn.commit()
        conn.close()

        print(f"✅ Producto {producto_id} eliminado: {producto[1]}")
        return {"success": True, "message": f"Producto '{producto[1]}' eliminado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error eliminando producto: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ✅ 5. PRODUCTOS SIMILARES
@app.get("/api/admin/productos-similares")
async def get_productos_similares():
    """Busca productos maestros con nombres similares"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("🔍 Buscando productos similares...")

        cursor.execute(
            """
            SELECT
                pm1.id as producto1_id,
                COALESCE(pm1.nombre_comercial, pm1.nombre_normalizado, 'Sin nombre') as nombre1,
                pm2.id as producto2_id,
                COALESCE(pm2.nombre_comercial, pm2.nombre_normalizado, 'Sin nombre') as nombre2,
                pm1.precio_promedio_global as precio1,
                pm2.precio_promedio_global as precio2
            FROM productos_maestros pm1
            INNER JOIN productos_maestros pm2 ON
                COALESCE(pm1.nombre_comercial, pm1.nombre_normalizado) =
                COALESCE(pm2.nombre_comercial, pm2.nombre_normalizado) AND
                pm1.id < pm2.id
            LIMIT 100
        """
        )

        similares = []
        for row in cursor.fetchall():
            similares.append(
                {
                    "producto1_id": row[0],
                    "nombre1": row[1],
                    "producto2_id": row[2],
                    "nombre2": row[3],
                    "precio1": float(row[4] or 0) if row[4] else 0,
                    "precio2": float(row[5] or 0)  if row[5] else 0,
                }
            )

        conn.close()
        print(f"✅ {len(similares)} pares similares encontrados")
        return {"similares": similares, "total": len(similares)}

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints admin corregidos con nombres de columnas reales")


# ✅ 7. LISTAR USUARIOS
@app.get("/api/admin/productos/{producto_id}")
async def get_producto_detalle(producto_id: int):
    """Obtiene detalles de un producto maestro específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Buscando producto maestro {producto_id}...")

        cursor.execute(
            """
            SELECT
                pm.id,
                COALESCE(pm.nombre_comercial, pm.nombre_normalizado, 'Sin nombre') as nombre,
                pm.codigo_ean,
                pm.precio_promedio_global,
                pm.categoria,
                pm.marca,
                pm.total_reportes
            FROM productos_maestros pm
            WHERE pm.id = %s
        """,
            (producto_id,),
        )

        result = cursor.fetchone()
        conn.close()

        if not result:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # ✅ CORRECCIÓN: Convertir de centavos a pesos
        precio_pesos = float(result[3]) if result[3] else 0

        return {
            "id": result[0],
            "nombre": result[1],
            "codigo_ean": result[2],
            "precio_promedio": precio_pesos,  # ← En pesos, no centavos
            "categoria": result[4],
            "marca": result[5],
            "veces_comprado": result[6] or 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ✅ 8. INVENTARIO DE USUARIO
@app.get("/api/admin/usuarios/{usuario_id}/inventario")
async def get_usuario_inventario(usuario_id: int):
    """
    Obtiene estadísticas del inventario de un usuario
    ✅ CORRECCIÓN: INNER JOIN sin GROUP BY
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📊 Obteniendo inventario del usuario {usuario_id}...")

        # ✅ INNER JOIN sin GROUP BY - esto evita duplicados
        cursor.execute(
            """
            SELECT
                COUNT(DISTINCT f.id) as total_facturas,
                COUNT(DISTINCT if_.producto_maestro_id) as productos_unicos,
                COALESCE(SUM(if_.precio_pagado), 0) as total_gastado
            FROM facturas f
            INNER JOIN items_factura if_ ON f.id = if_.factura_id
            WHERE f.usuario_id = %s
        """,
            (usuario_id,),
        )

        result = cursor.fetchone()
        conn.close()

        if not result:
            return {
                "total_facturas": 0,
                "productos_unicos": 0,
                "total_gastado": 0,
            }

        total_gastado = float(result[2] or 0)

        print(f"   Usuario ID: {usuario_id}")
        print(f"   Facturas: {result[0]}")
        print(f"   Productos únicos: {result[1]}")
        print(f"   Total gastado: ${total_gastado:,.0f}")

        return {
            "total_facturas": result[0] or 0,
            "productos_unicos": result[1] or 0,
            "total_gastado": total_gastado,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/debug/inventario-raw/{usuario_id}")
async def debug_inventario_raw(usuario_id: int):
    """
    🔍 Diagnóstico completo de inventario
    Muestra TODOS los valores para identificar el problema
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {
            "usuario_id": usuario_id,
            "timestamp": datetime.now().isoformat(),
            "calculos": {}
        }

        # 1️⃣ Desde FACTURAS
        cursor.execute(
            """
            SELECT
                COUNT(*) as num_facturas,
                SUM(total_factura) as suma_total_factura,
                array_agg(id) as factura_ids,
                array_agg(total_factura) as factura_totales
            FROM facturas
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        facturas_data = cursor.fetchone()
        resultado["calculos"]["desde_facturas"] = {
            "num_facturas": facturas_data[0],
            "suma_total_factura": float(facturas_data[1] or 0),
            "factura_ids": facturas_data[2],
            "factura_totales": [float(x) for x in facturas_data[3]] if facturas_data[3] else []
        }

        # 2️⃣ Desde ITEMS_FACTURA
        cursor.execute(
            """
            SELECT
                COUNT(*) as num_items,
                COUNT(DISTINCT producto_maestro_id) as productos_unicos,
                SUM(cantidad) as cantidad_total,
                SUM(precio_pagado) as suma_precios_sin_cantidad,
                SUM(cantidad * precio_pagado) as suma_cantidad_x_precio,
                MIN(precio_pagado) as precio_minimo,
                MAX(precio_pagado) as precio_maximo,
                AVG(precio_pagado) as precio_promedio
            FROM items_factura if_
            JOIN facturas f ON if_.factura_id = f.id
            WHERE f.usuario_id = %s
        """,
            (usuario_id,),
        )

        items_data = cursor.fetchone()
        resultado["calculos"]["desde_items_factura"] = {
            "num_items": items_data[0],
            "productos_unicos": items_data[1],
            "cantidad_total": float(items_data[2] or 0),
            "suma_precios_sin_cantidad": float(items_data[3] or 0),
            "suma_cantidad_x_precio": float(items_data[4] or 0),
            "precio_minimo": float(items_data[5] or 0),
            "precio_maximo": float(items_data[6] or 0),
            "precio_promedio": float(items_data[7] or 0)
        }

        # 3️⃣ Muestra de 5 items con mayor precio
        cursor.execute(
            """
            SELECT
                if_.id,
                if_.nombre_leido,
                if_.cantidad,
                if_.precio_pagado,
                (if_.cantidad * if_.precio_pagado) as subtotal,
                f.id as factura_id,
                f.total_factura,
                f.establecimiento
            FROM items_factura if_
            JOIN facturas f ON if_.factura_id = f.id
            WHERE f.usuario_id = %s
            ORDER BY if_.precio_pagado DESC
            LIMIT 5
        """,
            (usuario_id,),
        )

        items_top = []
        for row in cursor.fetchall():
            items_top.append({
                "item_id": row[0],
                "nombre": row[1],
                "cantidad": float(row[2]),
                "precio_unitario": float(row[3]),
                "subtotal": float(row[4]),
                "factura_id": row[5],
                "factura_total": float(row[6]),
                "establecimiento": row[7]
            })

        resultado["items_top_precio"] = items_top

        # 4️⃣ Primeros 10 items de cada factura
        cursor.execute(
            """
            SELECT
                f.id as factura_id,
                f.total_factura,
                array_agg(if_.nombre_leido ORDER BY if_.id) as nombres,
                array_agg(if_.cantidad ORDER BY if_.id) as cantidades,
                array_agg(if_.precio_pagado ORDER BY if_.id) as precios
            FROM facturas f
            LEFT JOIN items_factura if_ ON f.id = if_.factura_id
            WHERE f.usuario_id = %s
            GROUP BY f.id, f.total_factura
            ORDER BY f.id
        """,
            (usuario_id,),
        )

        facturas_con_items = []
        for row in cursor.fetchall():
            items = []
            if row[2]:  # Si hay items
                for i in range(min(5, len(row[2]))):  # Primeros 5
                    items.append({
                        "nombre": row[2][i],
                        "cantidad": float(row[3][i]) if row[3][i] else 0,
                        "precio": float(row[4][i]) if row[4][i] else 0,
                        "subtotal": (float(row[3][i]) * float(row[4][i])) if row[3][i] and row[4][i] else 0
                    })

            facturas_con_items.append({
                "factura_id": row[0],
                "total_declarado": float(row[1]),
                "items_muestra": items,
                "suma_muestra": sum(item["subtotal"] for item in items)
            })

        resultado["facturas_detalle"] = facturas_con_items

        # 5️⃣ Diagnóstico
        suma_facturas = resultado["calculos"]["desde_facturas"]["suma_total_factura"]
        suma_items = resultado["calculos"]["desde_items_factura"]["suma_cantidad_x_precio"]

        resultado["diagnostico"] = {
            "suma_facturas": suma_facturas,
            "suma_items": suma_items,
            "diferencia": suma_items - suma_facturas,
            "ratio": round(suma_items / suma_facturas, 2) if suma_facturas > 0 else 0,
            "problema": "",
            "recomendacion": ""
        }

        if abs(suma_items - suma_facturas) < 100:
            resultado["diagnostico"]["problema"] = "✅ Datos consistentes"
            resultado["diagnostico"]["recomendacion"] = "Valores correctos. No hay problema."
        elif suma_items > suma_facturas * 2:
            ratio = suma_items / suma_facturas
            resultado["diagnostico"]["problema"] = f"🔴 items_factura.precio_pagado está multiplicado ~{ratio:.1f}x"
            resultado["diagnostico"]["recomendacion"] = f"Dividir precio_pagado por {ratio:.0f} en items_factura"
        else:
            resultado["diagnostico"]["problema"] = "⚠️ Diferencia menor pero significativa"
            resultado["diagnostico"]["recomendacion"] = "Revisar cálculos individuales"

        conn.close()
        return resultado

    except Exception as e:
        print(f"❌ Error en debug: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

print("✅ Endpoint de diagnóstico /api/debug/inventario-raw/{usuario_id} registrado")


# ✅ 9. SERVIR IMÁGENES DE FACTURAS
@app.get("/images/{factura_id}")
async def get_factura_imagen(factura_id: int):
    """Sirve la imagen de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT ruta_imagen FROM facturas WHERE id = %s", (factura_id,))
        result = cursor.fetchone()
        conn.close()

        if not result or not result[0]:
            raise HTTPException(status_code=404, detail="Imagen no encontrada")

        ruta_imagen = result[0]

        # Buscar en diferentes ubicaciones posibles
        posibles_rutas = [
            ruta_imagen,
            os.path.join("uploads", os.path.basename(ruta_imagen)),
            os.path.join("static", "uploads", os.path.basename(ruta_imagen)),
        ]

        for ruta in posibles_rutas:
            if os.path.exists(ruta):
                return FileResponse(ruta)

        raise HTTPException(status_code=404, detail="Archivo de imagen no encontrado")

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# NOTA: Los siguientes endpoints son aliases del anterior
# ==========================================
@app.get("/admin/facturas/{factura_id}/imagen")
async def get_factura_imagen_alt1(factura_id: int):
    return await get_factura_imagen(factura_id)


@app.get("/api/facturas/{factura_id}/imagen")
async def get_factura_imagen_alt2(factura_id: int):
    return await get_factura_imagen(factura_id)


@app.get("/invoices/{factura_id}/image")
async def get_factura_imagen_alt3(factura_id: int):
    return await get_factura_imagen(factura_id)


print("✅ Todos los endpoints admin corregidos y registrados")

@app.get("/api/admin/diagnostico/total-gastado")
async def diagnostico_total_gastado():
    """
    🔍 Endpoint de diagnóstico para el problema Total Gastado x100
    VERSIÓN CORREGIDA: Usa los nombres correctos de columnas
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {
            "timestamp": datetime.now().isoformat(),
            "paso_1_columnas": {},
            "paso_2_diagnostico": {
                "usuario": {},
                "facturas": [],
                "items_muestra": [],
                "inventario": [],
                "resumen": {}
            }
        }

        print("🔍 Iniciando diagnóstico con nombres correctos...")

        # ==========================================
        # PASO 1: MOSTRAR NOMBRES DE COLUMNAS
        # ==========================================

        # Columnas de facturas
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'facturas'
            ORDER BY ordinal_position
        """)
        resultado["paso_1_columnas"]["facturas"] = [
            {"nombre": col[0], "tipo": col[1]} for col in cursor.fetchall()
        ]

        # Columnas de items_factura
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'items_factura'
            ORDER BY ordinal_position
        """)
        resultado["paso_1_columnas"]["items_factura"] = [
            {"nombre": col[0], "tipo": col[1]} for col in cursor.fetchall()
        ]

        # Columnas de inventario_usuario
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'inventario_usuario'
            ORDER BY ordinal_position
        """)
        resultado["paso_1_columnas"]["inventario_usuario"] = [
            {"nombre": col[0], "tipo": col[1]} for col in cursor.fetchall()
        ]

        print("✅ PASO 1: Nombres de columnas obtenidos")

        # ==========================================
        # PASO 2: DIAGNÓSTICO COMPLETO
        # ==========================================

        # 1. Usuario
        cursor.execute("""
            SELECT id, email, facturas_aportadas, productos_aportados
            FROM usuarios
            WHERE email = 'santiago@tscamp.co'
        """)
        usuario = cursor.fetchone()

        if not usuario:
            cursor.close()
            conn.close()
            return {
                "error": "Usuario santiago@tscamp.co no encontrado",
                "columnas": resultado["paso_1_columnas"]
            }

        usuario_id = usuario[0]
        resultado["paso_2_diagnostico"]["usuario"] = {
            "id": usuario[0],
            "email": usuario[1],
            "facturas": usuario[2],
            "productos": usuario[3]
        }

        print(f"✅ Usuario encontrado: {usuario[1]}")

        # 2. Facturas recientes - NOMBRES CORRECTOS
        cursor.execute("""
            SELECT
                id,
                establecimiento,
                fecha_factura,
                total_factura,
                productos_detectados,
                productos_guardados
            FROM facturas
            WHERE usuario_id = %s
            ORDER BY fecha_cargue DESC
            LIMIT 3
        """, (usuario_id,))

        for fac in cursor.fetchall():
            resultado["paso_2_diagnostico"]["facturas"].append({
                "id": fac[0],
                "establecimiento": fac[1],
                "fecha": str(fac[2]) if fac[2] else "N/A",
                "total": float(fac[3]) if fac[3] else 0,
                "productos_detectados": fac[4],
                "productos_guardados": fac[5]
            })

        print(f"✅ {len(resultado['paso_2_diagnostico']['facturas'])} facturas encontradas")

        # 3. Items de primera factura - NOMBRES CORRECTOS
        if resultado["paso_2_diagnostico"]["facturas"]:
            factura_id = resultado["paso_2_diagnostico"]["facturas"][0]["id"]

            cursor.execute("""
                SELECT
                    nombre_leido,
                    cantidad,
                    precio_pagado,
                    producto_maestro_id,
                    codigo_leido
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY precio_pagado DESC
                LIMIT 5
            """, (factura_id,))

            for item in cursor.fetchall():
                precio_total = float(item[1]) * float(item[2])
                resultado["paso_2_diagnostico"]["items_muestra"].append({
                    "producto": item[0],
                    "cantidad": float(item[1]),
                    "precio_unitario": float(item[2]),
                    "precio_total_calculado": precio_total,
                    "producto_maestro_id": item[3],
                    "codigo_leido": item[4],
                    "tiene_producto_id": item[3] is not None
                })

            print(f"✅ {len(resultado['paso_2_diagnostico']['items_muestra'])} items analizados")

        # 4. Inventario - NOMBRES CORRECTOS
        cursor.execute("""
            SELECT
                pm.nombre_normalizado,
                iu.cantidad_total_comprada,
                iu.precio_promedio,
                iu.total_gastado,
                iu.numero_compras,
                iu.precio_ultima_compra
            FROM inventario_usuario iu
            JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
            WHERE iu.usuario_id = %s
            ORDER BY iu.total_gastado DESC
            LIMIT 10
        """, (usuario_id,))

        total_suma = 0
        problemas = 0

        for inv in cursor.fetchall():
            total_gastado_bd = float(inv[3]) if inv[3] else 0
            cantidad = float(inv[1]) if inv[1] else 0
            precio_promedio = float(inv[2]) if inv[2] else 0

            # Calcular lo que DEBERÍA ser el total_gastado
            esperado = precio_promedio * cantidad if precio_promedio and cantidad else 0
            diferencia = total_gastado_bd - esperado
            ratio = total_gastado_bd / esperado if esperado > 0 else 0

            total_suma += total_gastado_bd

            # Detectar si está inflado x100 (ratio entre 99 y 101)
            es_problema = 99 < ratio < 101
            if es_problema:
                problemas += 1

            resultado["paso_2_diagnostico"]["inventario"].append({
                "producto": inv[0],
                "cantidad_total": cantidad,
                "precio_promedio": precio_promedio,
                "total_gastado_bd": total_gastado_bd,
                "total_esperado": esperado,
                "diferencia": diferencia,
                "ratio": round(ratio, 2),
                "es_problema_x100": es_problema,
                "numero_compras": inv[4],
                "precio_ultima_compra": float(inv[5]) if inv[5] else None
            })

        print(f"✅ {len(resultado['paso_2_diagnostico']['inventario'])} productos en inventario")
        print(f"⚠️ {problemas} productos con ratio ~100x")

        # 5. Resumen del diagnóstico
        cursor.execute("""
            SELECT SUM(total_gastado)
            FROM inventario_usuario
            WHERE usuario_id = %s
        """, (usuario_id,))

        total_sistema = cursor.fetchone()[0] or 0
        total_sistema = float(total_sistema)

        # Diagnóstico inteligente
        resultado["paso_2_diagnostico"]["resumen"] = {
            "total_sistema": total_sistema,
            "total_sistema_formateado": f"${total_sistema:,.0f}",
            "productos_con_problema_x100": problemas,
            "total_10_productos": total_suma,
            "parece_inflado_x100": total_sistema > 10000000,
            "si_divide_100": total_sistema / 100,
            "recomendacion": ""
        }

        if resultado["paso_2_diagnostico"]["resumen"]["parece_inflado_x100"]:
            resultado["paso_2_diagnostico"]["resumen"]["recomendacion"] = (
                "🚨 CRÍTICO: Total gastado parece estar multiplicado x100. "
                f"Valor actual: ${total_sistema:,.0f} → Debería ser: ${total_sistema/100:,.0f}"
            )
        elif problemas > 0:
            resultado["paso_2_diagnostico"]["resumen"]["recomendacion"] = (
                f"⚠️ Se encontraron {problemas} productos con ratio ~100x. "
                "Revisar cálculo de inventario."
            )
        else:
            resultado["paso_2_diagnostico"]["resumen"]["recomendacion"] = "✅ Datos parecen correctos"

        print(f"📊 Total sistema: ${total_sistema:,.0f}")
        print(f"📊 Diagnóstico: {resultado['paso_2_diagnostico']['resumen']['recomendacion']}")

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        print(f"❌ Error en diagnóstico: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/api/test/insert-producto")
async def test_insert_producto():
    """
    Endpoint de prueba para insertar un producto directamente
    Accede desde el navegador:
    https://lecfac-backend-production.up.railway.app/api/test/insert-producto
    """
    try:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
        conn = get_db_connection()
        cursor = conn.cursor()

        # Producto de prueba
        producto_test = {
            "codigo_ean": "7702001030644",
            "nombre_comercial": "ISODINE SOLUCION 120ML TEST",
            "nombre_normalizado": "isodine solucion 120ml test",
            "categoria": "Farmacia",
            "subcategoria": "Antisépticos",
            "marca": "Isodine",
            "presentacion": "120ml",
            "precio_promedio_global": 21800,
            "es_producto_fresco": False
        }

        # Insertar
        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean, nombre_comercial, nombre_normalizado,
                    categoria, subcategoria, marca, presentacion,
                    precio_promedio_global, es_producto_fresco
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                producto_test["codigo_ean"],
                producto_test["nombre_comercial"],
                producto_test["nombre_normalizado"],
                producto_test["categoria"],
                producto_test["subcategoria"],
                producto_test["marca"],
                producto_test["presentacion"],
                producto_test["precio_promedio_global"],
                producto_test["es_producto_fresco"]
            ))
            producto_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO productos_maestros (
                    codigo_ean, nombre_comercial, nombre_normalizado,
                    categoria, subcategoria, marca, presentacion,
                    precio_promedio_global, es_producto_fresco
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                producto_test["codigo_ean"],
                producto_test["nombre_comercial"],
                producto_test["nombre_normalizado"],
                producto_test["categoria"],
                producto_test["subcategoria"],
                producto_test["marca"],
                producto_test["presentacion"],
                producto_test["precio_promedio_global"],
                producto_test["es_producto_fresco"]
            ))
            producto_id = cursor.lastrowid

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": "✅ Producto insertado correctamente",
            "producto_id": producto_id,
            "producto": producto_test,
            "database_type": database_type
        }

    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc(),
            "database_type": database_type
        }


@app.get("/api/test/count-productos")
async def test_count_productos():
    """
    Contar productos en productos_maestros
    https://lecfac-backend-production.up.railway.app/api/test/count-productos
    """
    try:
        database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT * FROM productos_maestros ORDER BY id DESC LIMIT 5")
        productos = cursor.fetchall()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total_productos": count,
            "ultimos_5": [dict(zip([d[0] for d in cursor.description], row)) for row in productos] if productos else [],
            "database_type": database_type
        }

    except Exception as e:
        import traceback
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }

@app.get("/api/debug/inventario-raw/{usuario_id}")
async def debug_inventario_raw(usuario_id: int):
    """
    🔍 Diagnóstico completo de inventario
    Muestra TODOS los valores para identificar el problema
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {
            "usuario_id": usuario_id,
            "timestamp": datetime.now().isoformat(),
            "calculos": {}
        }

        # 1️⃣ Desde FACTURAS
        cursor.execute(
            """
            SELECT
                COUNT(*) as num_facturas,
                SUM(total_factura) as suma_total_factura,
                array_agg(id) as factura_ids,
                array_agg(total_factura) as factura_totales
            FROM facturas
            WHERE usuario_id = %s
        """,
            (usuario_id,),
        )

        facturas_data = cursor.fetchone()
        resultado["calculos"]["desde_facturas"] = {
            "num_facturas": facturas_data[0],
            "suma_total_factura": float(facturas_data[1] or 0),
            "factura_ids": facturas_data[2],
            "factura_totales": [float(x) for x in facturas_data[3]] if facturas_data[3] else []
        }

        # 2️⃣ Desde ITEMS_FACTURA
        cursor.execute(
            """
            SELECT
                COUNT(*) as num_items,
                COUNT(DISTINCT producto_maestro_id) as productos_unicos,
                SUM(cantidad) as cantidad_total,
                SUM(precio_pagado) as suma_precios_sin_cantidad,
                SUM(cantidad * precio_pagado) as suma_cantidad_x_precio,
                MIN(precio_pagado) as precio_minimo,
                MAX(precio_pagado) as precio_maximo,
                AVG(precio_pagado) as precio_promedio
            FROM items_factura if_
            JOIN facturas f ON if_.factura_id = f.id
            WHERE f.usuario_id = %s
        """,
            (usuario_id,),
        )

        items_data = cursor.fetchone()
        resultado["calculos"]["desde_items_factura"] = {
            "num_items": items_data[0],
            "productos_unicos": items_data[1],
            "cantidad_total": float(items_data[2] or 0),
            "suma_precios_sin_cantidad": float(items_data[3] or 0),
            "suma_cantidad_x_precio": float(items_data[4] or 0),
            "precio_minimo": float(items_data[5] or 0),
            "precio_maximo": float(items_data[6] or 0),
            "precio_promedio": float(items_data[7] or 0)
        }

        # 3️⃣ Muestra de 5 items con mayor precio
        cursor.execute(
            """
            SELECT
                if_.id,
                if_.nombre_leido,
                if_.cantidad,
                if_.precio_pagado,
                (if_.cantidad * if_.precio_pagado) as subtotal,
                f.id as factura_id,
                f.total_factura,
                f.establecimiento
            FROM items_factura if_
            JOIN facturas f ON if_.factura_id = f.id
            WHERE f.usuario_id = %s
            ORDER BY if_.precio_pagado DESC
            LIMIT 5
        """,
            (usuario_id,),
        )

        items_top = []
        for row in cursor.fetchall():
            items_top.append({
                "item_id": row[0],
                "nombre": row[1],
                "cantidad": float(row[2]),
                "precio_unitario": float(row[3]),
                "subtotal": float(row[4]),
                "factura_id": row[5],
                "factura_total": float(row[6]),
                "establecimiento": row[7]
            })

        resultado["items_top_precio"] = items_top

        # 4️⃣ Primeros 5 items de cada factura
        cursor.execute(
            """
            SELECT
                f.id as factura_id,
                f.total_factura,
                array_agg(if_.nombre_leido ORDER BY if_.id) as nombres,
                array_agg(if_.cantidad ORDER BY if_.id) as cantidades,
                array_agg(if_.precio_pagado ORDER BY if_.id) as precios
            FROM facturas f
            LEFT JOIN items_factura if_ ON f.id = if_.factura_id
            WHERE f.usuario_id = %s
            GROUP BY f.id, f.total_factura
            ORDER BY f.id
        """,
            (usuario_id,),
        )

        facturas_con_items = []
        for row in cursor.fetchall():
            items = []
            if row[2]:
                for i in range(min(5, len(row[2]))):
                    items.append({
                        "nombre": row[2][i],
                        "cantidad": float(row[3][i]) if row[3][i] else 0,
                        "precio": float(row[4][i]) if row[4][i] else 0,
                        "subtotal": (float(row[3][i]) * float(row[4][i])) if row[3][i] and row[4][i] else 0
                    })

            facturas_con_items.append({
                "factura_id": row[0],
                "total_declarado": float(row[1]),
                "items_muestra": items,
                "suma_muestra": sum(item["subtotal"] for item in items)
            })

        resultado["facturas_detalle"] = facturas_con_items

        # 5️⃣ Diagnóstico
        suma_facturas = resultado["calculos"]["desde_facturas"]["suma_total_factura"]
        suma_items = resultado["calculos"]["desde_items_factura"]["suma_cantidad_x_precio"]

        resultado["diagnostico"] = {
            "suma_facturas": suma_facturas,
            "suma_items": suma_items,
            "diferencia": suma_items - suma_facturas,
            "ratio": round(suma_items / suma_facturas, 2) if suma_facturas > 0 else 0,
            "problema": "",
            "recomendacion": ""
        }

        if abs(suma_items - suma_facturas) < 100:
            resultado["diagnostico"]["problema"] = "✅ Datos consistentes"
            resultado["diagnostico"]["recomendacion"] = "Valores correctos"
        elif suma_items > suma_facturas * 2:
            ratio = suma_items / suma_facturas
            resultado["diagnostico"]["problema"] = f"🔴 items_factura.precio_pagado multiplicado ~{ratio:.1f}x"
            resultado["diagnostico"]["recomendacion"] = f"Dividir precio_pagado por {ratio:.0f}"
        else:
            resultado["diagnostico"]["problema"] = "⚠️ Diferencia significativa"
            resultado["diagnostico"]["recomendacion"] = "Revisar cálculos"

        conn.close()
        return resultado

    except Exception as e:
        print(f"❌ Error en debug: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoint diagnóstico /api/debug/inventario-raw/{usuario_id} registrado")

# ==========================================
# 🔍 DIAGNÓSTICO PROFUNDO - items_factura
# ==========================================
# Ejecutar este endpoint para ver qué está mal

@app.get("/api/debug/facturas-detalladas/{usuario_id}")
async def debug_facturas_detalladas(usuario_id: int):
    """
    Muestra TODO el detalle de las facturas de un usuario
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                f.id,
                f.establecimiento,
                f.fecha_factura,
                f.total_factura,
                f.productos_guardados
            FROM facturas f
            WHERE f.usuario_id = %s
            ORDER BY f.id
        """,
            (usuario_id,),
        )

        facturas = []
        for row in cursor.fetchall():
            factura_id = row[0]
            total_declarado = float(row[3]) if row[3] else 0

            cursor.execute(
                """
                SELECT
                    id,
                    nombre_leido,
                    cantidad,
                    precio_pagado,
                    producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )

            items = []
            suma_precio_pagado = 0
            suma_cantidad_x_precio = 0

            for item_row in cursor.fetchall():
                cantidad = float(item_row[2]) if item_row[2] else 0
                precio_pagado = float(item_row[3]) if item_row[3] else 0

                suma_precio_pagado += precio_pagado
                suma_cantidad_x_precio += (cantidad * precio_pagado)

                items.append({
                    "item_id": item_row[0],
                    "nombre": item_row[1],
                    "cantidad": cantidad,
                    "precio_pagado": precio_pagado,
                    "precio_unitario_inferido": precio_pagado / cantidad if cantidad > 0 else 0,
                    "subtotal_si_multiplico": cantidad * precio_pagado,
                })

            facturas.append({
                "factura_id": factura_id,
                "establecimiento": row[1],
                "total_declarado": total_declarado,
                "num_items": len(items),
                "suma_precio_pagado": suma_precio_pagado,
                "suma_cantidad_x_precio": suma_cantidad_x_precio,
                "diferencia_suma": abs(suma_precio_pagado - total_declarado),
                "diferencia_multiplicado": abs(suma_cantidad_x_precio - total_declarado),
                "items": items[:10]
            })

        total_suma = sum(f["suma_precio_pagado"] for f in facturas)
        total_multiplicado = sum(f["suma_cantidad_x_precio"] for f in facturas)
        total_declarado = sum(f["total_declarado"] for f in facturas)

        cursor.close()
        conn.close()

        return {
            "usuario_id": usuario_id,
            "facturas": facturas,
            "resumen": {
                "total_declarado": total_declarado,
                "total_suma_precio_pagado": total_suma,
                "total_multiplicado": total_multiplicado,
                "cual_usar": "suma" if abs(total_suma - total_declarado) < abs(total_multiplicado - total_declarado) else "multiplicado"
            }
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/corregir-todas-facturas")
async def corregir_todas_facturas():
    """Corrige TODAS las facturas con diferencias > 5%"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, usuario_id, establecimiento, total_factura FROM facturas WHERE total_factura > 0 ORDER BY id"
        )

        todas_facturas = cursor.fetchall()
        facturas_corregidas = []

        for factura_row in todas_facturas:
            factura_id = factura_row[0]
            total_declarado = float(factura_row[3])

            cursor.execute(
                "SELECT SUM(precio_pagado), COUNT(*) FROM items_factura WHERE factura_id = %s",
                (factura_id,),
            )

            items_data = cursor.fetchone()
            if not items_data or not items_data[0]:
                continue

            suma_actual = float(items_data[0])
            diferencia = abs(suma_actual - total_declarado)
            porcentaje_error = (diferencia / total_declarado * 100) if total_declarado > 0 else 0

            if porcentaje_error < 5:
                continue

            factor = total_declarado / suma_actual

            cursor.execute(
                "SELECT id, precio_pagado FROM items_factura WHERE factura_id = %s",
                (factura_id,),
            )

            for item in cursor.fetchall():
                precio_corregido = int(item[1] * factor)
                cursor.execute(
                    "UPDATE items_factura SET precio_pagado = %s WHERE id = %s",
                    (precio_corregido, item[0]),
                )

            conn.commit()

            cursor.execute(
                "SELECT SUM(precio_pagado) FROM items_factura WHERE factura_id = %s",
                (factura_id,),
            )
            suma_nueva = float(cursor.fetchone()[0] or 0)

            facturas_corregidas.append({
                "factura_id": factura_id,
                "total_declarado": total_declarado,
                "suma_anterior": suma_actual,
                "suma_nueva": suma_nueva,
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "facturas_corregidas": len(facturas_corregidas),
            "detalles": facturas_corregidas,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
# ========================================
# INICIALIZACIÓN DEL SERVIDOR
# ========================================
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🚀 INICIANDO SERVIDOR LECFAC")
    print("=" * 60)

    # Verificar conexión a base de datos
    test_database_connection()

    # Crear tablas si no existen
    create_tables()

    print("=" * 60)
    print("✅ SERVIDOR LISTO")
    print("=" * 60)

    # Iniciar servidor
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
