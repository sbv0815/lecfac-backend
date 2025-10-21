import os
import tempfile
import traceback
import json
import uuid
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
    obtener_o_crear_producto_maestro,
    actualizar_inventario_desde_factura,
)
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from claude_invoice import parse_invoice_with_claude

# Importar routers
from admin_dashboard import router as admin_dashboard_router
from auth import router as auth_router
from image_handlers import router as image_handlers_router
from duplicados_routes import router as duplicados_router

# Importar procesador OCR y auditoría
from ocr_processor import processor, ocr_queue, processing
from audit_system import audit_scheduler, AuditSystem
from corrections_service import aplicar_correcciones_automaticas
from concurrent.futures import ThreadPoolExecutor
import time
from establishments import procesar_establecimiento, obtener_o_crear_establecimiento_id


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
    if templates:
        return templates.TemplateResponse("admin_dashboard.html", {"request": request})
    return HTMLResponse("<h1>LecFac API</h1>")


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
    html_path = Path("admin_dashboard.html")
    if html_path.exists():
        return FileResponse("admin_dashboard.html")
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
                precio = int(prod.get("valor") or prod.get("precio") or 0)
                cantidad = int(prod.get("cantidad", 1))

                if not nombre or precio <= 0:
                    continue

                codigo_ean_valido = None
                if codigo_ean and len(codigo_ean) >= 8 and codigo_ean.isdigit():
                    codigo_ean_valido = codigo_ean

                # ⭐ CREAR PRODUCTO MAESTRO
                producto_maestro_id = None
                if codigo_ean_valido:
                    try:
                        producto_maestro_id = obtener_o_crear_producto_maestro(
                            codigo_ean=codigo_ean_valido, nombre=nombre, precio=precio
                        )
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
                            precio,
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
                            precio,
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
                producto_maestro_id = None
                if codigo and len(codigo) >= 8 and codigo.isdigit():
                    try:
                        producto_maestro_id = obtener_o_crear_producto_maestro(
                            codigo_ean=codigo, nombre=nombre, precio=int(precio)
                        )
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
                            producto_maestro_id = obtener_o_crear_producto_maestro(
                                codigo_ean=codigo, nombre=nombre, precio=int(precio)
                            )
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


# ==========================================
# INICIO DEL SERVIDOR
# ==========================================
if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8080))
    print(f"🚀 Servidor iniciando en puerto: {port}")
    print(f"🔧 VERSIÓN: 2025-01-21-INVENTARIO-COMPLETO-FIX")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
