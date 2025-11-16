import os
import base64

print("=" * 80)
print("🚀 LECFAC BACKEND - VERSION 2025-11-12-15:30 - REBUILD FORZADO V2")
print("=" * 80)
import tempfile
import traceback
import json
import uuid

# LIMPIEZA DE CACHÉ AL INICIO
import shutil

print("🧹 Limpiando caché de Python...")
for root, dirs, files in os.walk("."):
    if "__pycache__" in dirs:
        shutil.rmtree(os.path.join(root, "__pycache__"))
        print(f"   ✓ Eliminado: {os.path.join(root, '__pycache__')}")
print("✅ Caché limpiado - Iniciando servidor...")

from datetime import datetime, date
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
from database import (
    create_tables,
    get_db_connection,
    hash_password,
    verify_password,
    test_database_connection,
    detectar_cadena,
    obtener_o_crear_establecimiento,
    actualizar_inventario_desde_factura as actualizar_inventario_desde_factura,
    procesar_items_factura_y_guardar_precios,
)
from analytics_updater import actualizar_todas_las_tablas_analiticas

# Importar routers
from api_inventario import router as inventario_router
from audit_system import AuditSystem
from mobile_endpoints import router as mobile_router
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from claude_invoice import parse_invoice_with_claude

# ==========================================
# FORZAR RECARGA DE MÓDULOS - NUEVO
# ==========================================
import sys
import importlib

# Eliminar product_matcher del caché si existe
if "product_matcher" in sys.modules:
    del sys.modules["product_matcher"]
    print("🔄 Módulo product_matcher removido del caché")

# ==========================================
# IMPORTACIÓN DE PRODUCT_MATCHER CON DEBUG
# ==========================================
print("\n" + "=" * 80)
print("🔍 IMPORTANDO product_matcher.py LIMPIO...")
print("=" * 80)

from product_matcher import buscar_o_crear_producto_inteligente

print("\n" + "=" * 80)
print("✅ product_matcher IMPORTADO EXITOSAMENTE")
print("=" * 80 + "\n")

# ==========================================
# VERIFICAR VERSIÓN - MEJORADO
# ==========================================
import product_matcher
import inspect

try:
    source_code = inspect.getsource(product_matcher.crear_producto_en_ambas_tablas)

    # Buscar múltiples indicadores del fix
    tiene_fix_1 = "fetchone() retornó None" in source_code
    tiene_fix_2 = "fallback manual" in source_code.lower()
    tiene_fix_3 = "if not resultado:" in source_code

    fix_completo = tiene_fix_1 or (tiene_fix_2 and tiene_fix_3)

    print(f"\n{'='*80}")
    print(f"🔧 VERIFICACIÓN DE FIX:")
    print(f"   - String 'fetchone() retornó None': {'✅' if tiene_fix_1 else '❌'}")
    print(f"   - String 'fallback manual': {'✅' if tiene_fix_2 else '❌'}")
    print(f"   - String 'if not resultado': {'✅' if tiene_fix_3 else '❌'}")
    print(
        f"   - RESULTADO FINAL: {'✅ FIX PRESENTE' if fix_completo else '❌ FIX AUSENTE'}"
    )
    print(f"{'='*80}\n")

    if not fix_completo:
        print("⚠️⚠️⚠️ WARNING: CÓDIGO DESACTUALIZADO DETECTADO ⚠️⚠️⚠️")
        print("El servidor se iniciará pero FALLARÁ al guardar productos")
        print("Verifica que product_matcher.py V6.1 esté en Git")

except Exception as e:
    print(f"❌ Error verificando fix: {e}")
    import traceback

    traceback.print_exc()

print("=" * 80 + "\n")

from comparacion_precios import router as comparacion_router

# ✅ ProductResolver removido - usando product_matcher
PRODUCT_RESOLVER_AVAILABLE = False
print("✅ product_matcher configurado")

from admin_dashboard import router as admin_dashboard_router
from auth import router as auth_router


def verify_jwt_token(token: str):
    """
    Verifica y decodifica un token JWT
    Retorna el payload si es válido, None si es inválido.
    """
    try:
        import jwt
        from auth import SECRET_KEY  # Importar la clave secreta

        # Decodificar con verificación de signature
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

        print(
            f"✅ Token válido - Usuario: {payload.get('user_id')}, Rol: {payload.get('rol')}"
        )
        return payload

    except jwt.ExpiredSignatureError:
        print("⚠️ Token expirado")
        return None
    except jwt.InvalidTokenError as e:
        print(f"⚠️ Token inválido: {e}")
        return None
    except Exception as e:
        print(f"❌ Error verificando token: {e}")
        return None


from image_handlers import router as image_handlers_router
from duplicados_routes import router as duplicados_router
from diagnostico_routes import router as diagnostico_router

# Importar procesador OCR y auditoría
from ocr_processor import processor, ocr_queue, processing
from audit_system import audit_scheduler, AuditSystem
from corrections_service import aplicar_correcciones_automaticas
from concurrent.futures import ThreadPoolExecutor
import time
from establishments import procesar_establecimiento, obtener_o_crear_establecimiento_id

# Importar AMBOS routers de auditoría con nombres diferente
from fastapi import APIRouter
from inventory_adjuster import ajustar_precios_items_por_total, limpiar_items_duplicados
from duplicate_detector import detectar_duplicados_automaticamente
from anomaly_monitor import (
    guardar_reporte_anomalia,
    obtener_estadisticas_por_establecimiento,
    obtener_anomalias_pendientes,
)
from productos_mejoras import router as productos_mejoras_router
from fastapi import FastAPI
from productos_establecimiento_endpoints import router as productos_est_router
from productos_api_v2 import router as productos_v2_router
from establecimientos_api import router as establecimientos_router
from consolidacion_productos import (
    procesar_item_con_consolidacion,
    mejorar_nombre_con_claude,
)


def extract_frames_from_video(video_path: str, max_frames: int = 10) -> List[str]:
    """Extrae frames de un video"""
    import cv2

    frames_base64 = []

    try:
        cap = cv2.VideoCapture(video_path)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        interval = max(1, total_frames // max_frames)

        frame_count = 0
        extracted = 0

        while cap.isOpened() and extracted < max_frames:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_count % interval == 0:
                _, buffer = cv2.imencode(".jpg", frame)
                frame_base64 = base64.b64encode(buffer).decode("utf-8")
                frames_base64.append(frame_base64)
                extracted += 1

            frame_count += 1

        cap.release()
        return frames_base64

    except Exception as e:
        print(f"❌ Error: {e}")
        return []


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
    return {"id": 1, "user_id": "user123", "email": "user@example.com"}


async def require_admin(user=Depends(get_current_user)):
    """Verificar si es admin"""
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requiere permisos de admin")
    return user


def get_user_id_from_token(authorization: str) -> int:
    """
    Extraer usuario_id desde el token JWT
    VERSIÓN MEJORADA - Maneja múltiples formatos de payload
    """
    print(f"🔐 [AUTH] Procesando autorización...")

    if not authorization:
        print("⚠️ [AUTH] No hay header Authorization")
        print("⚠️ [AUTH] Usando usuario_id = 2 por defecto")
        return 2  # Por ahora, usuario 2 por defecto

    if not authorization.startswith("Bearer "):
        print(f"⚠️ [AUTH] Header no empieza con 'Bearer '")
        print(f"⚠️ [AUTH] Header recibido: {authorization[:50]}...")
        print("⚠️ [AUTH] Usando usuario_id = 2 por defecto")
        return 2

    try:
        import jwt

        token = authorization.replace("Bearer ", "")

        print(f"🔐 [AUTH] Token extraído (primeros 30 chars): {token[:30]}...")

        # Decodificar SIN verificar signature (para desarrollo)
        # En producción, verifica la signature con tu SECRET_KEY
        payload = jwt.decode(token, options={"verify_signature": False})

        print(f"🔐 [AUTH] Payload completo: {payload}")

        # Intentar obtener user_id de diferentes campos posibles
        usuario_id = None

        # El backend de auth.py genera tokens con "user_id" en el payload
        if "user_id" in payload:
            usuario_id = payload["user_id"]
            print(f"✅ [AUTH] user_id encontrado: {usuario_id}")
        elif "sub" in payload:
            usuario_id = payload["sub"]
            print(f"✅ [AUTH] sub encontrado: {usuario_id}")
        elif "id" in payload:
            usuario_id = payload["id"]
            print(f"✅ [AUTH] id encontrado: {usuario_id}")
        else:
            print(f"⚠️ [AUTH] No se encontró user_id en payload")
            print(f"⚠️ [AUTH] Campos disponibles: {list(payload.keys())}")
            print("⚠️ [AUTH] Usando usuario_id = 2 por defecto")
            return 2

        # Convertir a int
        try:
            usuario_id_int = int(usuario_id)
            print(f"✅ [AUTH] Usuario autenticado: {usuario_id_int}")
            return usuario_id_int
        except (ValueError, TypeError) as e:
            print(f"⚠️ [AUTH] Error convirtiendo user_id a int: {e}")
            print(f"⚠️ [AUTH] Valor recibido: {usuario_id}")
            return 2

    except jwt.DecodeError as e:
        print(f"❌ [AUTH] Error decodificando JWT: {e}")
        print(f"❌ [AUTH] Token: {token[:50]}...")
        return 2
    except Exception as e:
        print(f"❌ [AUTH] Error inesperado: {e}")
        import traceback

        traceback.print_exc()
        return 2


# ==========================================
# FUNCIONES AUXILIARES
# ==========================================


def normalizar_fecha(fecha_str):
    """
    Convierte fecha de cualquier formato a YYYY-MM-DD para PostgreSQL
    Maneja casos especiales como 'No legible', None, strings vacíos, etc.
    """
    from datetime import datetime, date

    # Casos especiales que NO son fechas válidas
    if not fecha_str or fecha_str in [
        "No legible",
        "Desconocido",
        "N/A",
        "",
        "null",
        "None",
    ]:
        print(f"⚠️ Fecha no válida o no legible, usando fecha actual")
        return datetime.now().date()

    # Si ya es un objeto date o datetime
    if isinstance(fecha_str, datetime):
        return fecha_str.date()
    if isinstance(fecha_str, date):
        return fecha_str

    # Convertir a string y limpiar
    fecha_str = str(fecha_str).strip()

    # Verificar si contiene palabras que indican fecha inválida
    palabras_invalidas = ["legible", "desconocido", "no", "n/a", "ninguno", "error"]
    if any(palabra in fecha_str.lower() for palabra in palabras_invalidas):
        print(f"⚠️ Fecha inválida detectada: '{fecha_str}', usando fecha actual")
        return datetime.now().date()

    # Intentar diferentes formatos comunes
    formatos = [
        "%d/%m/%Y",  # 16/11/2016
        "%d-%m-%Y",  # 16-11-2016
        "%Y-%m-%d",  # 2016-11-16
        "%Y/%m/%d",  # 2016/11/16
        "%d/%m/%y",  # 16/11/16
        "%d-%m-%y",  # 16-11-16
        "%Y%m%d",  # 20161116
    ]

    for formato in formatos:
        try:
            fecha_parseada = datetime.strptime(fecha_str, formato).date()

            # Validar que la fecha tenga sentido
            hoy = datetime.now().date()
            if fecha_parseada > hoy:
                print(f"⚠️ Fecha en el futuro: {fecha_parseada}, usando fecha actual")
                return hoy

            if fecha_parseada.year < 2000:
                print(f"⚠️ Fecha muy antigua: {fecha_parseada}, usando fecha actual")
                return hoy

            return fecha_parseada
        except ValueError:
            continue

    # Si ningún formato funcionó
    print(f"⚠️ No se pudo parsear fecha '{fecha_str}', usando fecha actual")
    return datetime.now().date()


def normalizar_precio_unitario(valor_ocr: float, cantidad: int) -> int:
    """Normaliza el precio unitario desde el valor del OCR"""
    try:
        valor = float(valor_ocr)
        cantidad = int(cantidad) if cantidad else 1

        if cantidad <= 0:
            cantidad = 1

        if cantidad == 1:
            return int(valor)

        precio_dividido = valor / cantidad

        if 500 <= precio_dividido <= 50000:
            return int(precio_dividido)

        if 500 <= valor <= 50000:
            return int(valor)

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


# ==========================================
# ENDPOINT DE VERIFICACIÓN DE DEPLOY
# ==========================================
@app.get("/test-deploy-check")
async def test_deploy_check():
    """Endpoint de prueba para verificar deploys"""
    return {
        "success": True,
        "mensaje": "Railway está leyendo código actualizado",
        "timestamp": datetime.now().isoformat(),
        "version": "deploy-check-1",
    }


print("🔥 ENDPOINT /test-deploy-check REGISTRADO")


from routes import productos_admin

app.include_router(productos_admin.router)
# ==========================================
# CONFIGURAR CORS
# ==========================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
print("✅ CORS configurado")

app.include_router(productos_v2_router)
print("✅ productos_v2_router registrado PRIMERO")

# Ahora los demás routers de productos
app.include_router(inventario_router)
print("✅ inventario_router registrado")

app.include_router(diagnostico_router)
print("✅ diagnostico_router registrado")

app.include_router(comparacion_router, tags=["comparacion"])
print("✅ comparacion_router registrado")

try:
    app.include_router(productos_mejoras_router)
    print("✅ productos_mejoras_router registrado")
except Exception as e:
    print(f"❌ Error registrando productos_mejoras_router: {e}")
    import traceback

    traceback.print_exc()

app.include_router(productos_est_router)
print("✅ productos_establecimiento_router registrado")


@app.post("/invoices/parse-video")
async def parse_video(
    background_tasks: BackgroundTasks,
    video: UploadFile = File(...),
    authorization: str = Header(None),
):
    """
    Endpoint para procesar videos de facturas
    Retorna job_id inmediatamente y procesa en background
    """
    print(f"\n{'='*60}")
    print(f"🎬 VIDEO RECIBIDO: {video.filename}")
    print(f"{'='*60}")

    try:
        usuario_id = get_user_id_from_token(authorization)
        print(f"🆔 Usuario: {usuario_id}")

        temp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        content = await video.read()
        temp_video.write(content)
        temp_video.close()

        video_size_mb = len(content) / (1024 * 1024)
        print(f"💾 Tamaño: {video_size_mb:.2f} MB")

        conn = get_db_connection()
        cursor = conn.cursor()

        job_id = str(uuid.uuid4())

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                INSERT INTO processing_jobs (
                    id, usuario_id, status, created_at
                ) VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP)
            """,
                (job_id, usuario_id),
            )
        else:
            cursor.execute(
                """
                INSERT INTO processing_jobs (
                    id, usuario_id, status, created_at
                ) VALUES (?, ?, 'pending', ?)
            """,
                (job_id, usuario_id, datetime.now()),
            )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Job creado: {job_id}")

        background_tasks.add_task(
            process_video_background_task, job_id, temp_video.name, usuario_id
        )

        print(f"✅ Tarea en background agregada")
        print(f"{'='*60}\n")

        return JSONResponse(
            status_code=202,
            content={
                "success": True,
                "job_id": job_id,
                "message": "Video recibido, procesando en background",
                "status": "pending",
            },
        )

    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


app.include_router(mobile_router, tags=["mobile"])
app.include_router(establecimientos_router)

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

# ========================================


@app.get("/api/admin/usuarios/{usuario_id}/inventario")
async def get_inventario_usuario(usuario_id: int):
    """Obtener inventario de un usuario específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Obteniendo inventario del usuario {usuario_id}...")

        # Obtener productos del inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    iu.establecimiento_nombre,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 100
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    iu.establecimiento_nombre,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 100
            """,
                (usuario_id,),
            )

        inventario = []
        for row in cursor.fetchall():
            inventario.append(
                {
                    "producto_id": row[0],
                    "nombre": row[1] or "Producto sin nombre",
                    "codigo_ean": row[2] or "",
                    "cantidad": float(row[3]) if row[3] else 0,
                    "precio_ultima_compra": float(row[4]) if row[4] else 0,
                    "ultima_actualizacion": str(row[5]) if row[5] else None,
                    "establecimiento": row[6] or "-",
                    "categoria": row[7] or "-",
                }
            )

        # ✅ CORRECCIÓN: Calcular estadísticas adicionales que el dashboard necesita
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Total de facturas del usuario
            cursor.execute(
                """
                SELECT COUNT(DISTINCT id)
                FROM facturas
                WHERE usuario_id = %s
            """,
                (usuario_id,),
            )
            total_facturas = cursor.fetchone()[0] or 0

            # Total gastado (suma de todas las facturas)
            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
            """,
                (usuario_id,),
            )
            total_gastado = float(cursor.fetchone()[0] or 0)

            # Productos únicos en inventario
            cursor.execute(
                """
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = %s AND producto_maestro_id IS NOT NULL
            """,
                (usuario_id,),
            )
            productos_unicos = cursor.fetchone()[0] or 0
        else:
            # SQLite
            cursor.execute(
                """
                SELECT COUNT(DISTINCT id)
                FROM facturas
                WHERE usuario_id = ?
            """,
                (usuario_id,),
            )
            total_facturas = cursor.fetchone()[0] or 0

            cursor.execute(
                """
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
            """,
                (usuario_id,),
            )
            total_gastado = float(cursor.fetchone()[0] or 0)

            cursor.execute(
                """
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = ? AND producto_maestro_id IS NOT NULL
            """,
                (usuario_id,),
            )
            productos_unicos = cursor.fetchone()[0] or 0

        conn.close()

        print(f"✅ {len(inventario)} productos en inventario")
        print(
            f"📊 Stats: {total_facturas} facturas, ${total_gastado:,.0f} gastado, {productos_unicos} productos únicos"
        )

        # ✅ CORRECCIÓN: Retornar la estructura que el dashboard espera
        return {
            "success": True,
            "usuario_id": usuario_id,
            "inventario": inventario,
            "total_productos": len(inventario),
            "total_facturas": total_facturas,  # ← AGREGADO
            "total_gastado": total_gastado,  # ← AGREGADO
            "productos_unicos": productos_unicos,  # ← AGREGADO
        }

    except Exception as e:
        print(f"❌ Error obteniendo inventario del usuario {usuario_id}: {e}")
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/inventarios")
async def get_todos_inventarios(limite: int = 50, pagina: int = 1):
    """Obtener todos los inventarios con paginación"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        offset = (pagina - 1) * limite

        print(f"📦 Obteniendo inventarios (página {pagina})...")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.usuario_id,
                    u.email,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN usuarios u ON iu.usuario_id = u.id
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT %s OFFSET %s
            """,
                (limite, offset),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.usuario_id,
                    u.email,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN usuarios u ON iu.usuario_id = u.id
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT ? OFFSET ?
            """,
                (limite, offset),
            )

        inventarios = []
        for row in cursor.fetchall():
            inventarios.append(
                {
                    "usuario_id": row[0],
                    "email": row[1] or f"Usuario {row[0]}",
                    "producto": row[2] or "Sin nombre",
                    "cantidad": float(row[3]) if row[3] else 0,
                    "precio": float(row[4]) if row[4] else 0,
                    "ultima_actualizacion": str(row[5]) if row[5] else None,
                    "categoria": row[6] or "-",
                }
            )

        # Contar total
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT COUNT(*) FROM inventario_usuario")
        else:
            cursor.execute("SELECT COUNT(*) FROM inventario_usuario")

        total = cursor.fetchone()[0] or 0

        conn.close()

        print(f"✅ {len(inventarios)} inventarios obtenidos")

        return {
            "success": True,
            "inventarios": inventarios,
            "total": total,
            "pagina": pagina,
            "limite": limite,
            "total_paginas": (total + limite - 1) // limite,
        }

    except Exception as e:
        print(f"❌ Error obteniendo inventarios: {e}")
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints admin de inventario agregados")


@app.get("/api/admin/estadisticas")
async def get_admin_stats():
    """Estadísticas del dashboard admin"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0] or 0

        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0] or 0

        cursor.execute(
            "SELECT COUNT(DISTINCT producto_maestro_id) FROM items_factura WHERE producto_maestro_id IS NOT NULL"
        )
        total_productos = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "total_productos": total_productos,
        }
    except Exception as e:
        print(f"❌ Error en estadísticas: {e}")
        raise HTTPException(500, str(e))


@app.get("/api/admin/usuarios")
async def get_admin_usuarios():
    """Lista de usuarios"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT u.id, u.email, u.nombre, u.fecha_registro,
                   COUNT(f.id) as total_facturas
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
                    "nombre": row[2] or row[1],
                    "fecha_registro": str(row[3]) if row[3] else None,
                    "total_facturas": row[4] or 0,
                }
            )

        conn.close()
        return usuarios

    except Exception as e:
        print(f"❌ Error obteniendo usuarios: {e}")
        raise HTTPException(500, str(e))


@app.get("/consolidacion.html", response_class=HTMLResponse)
async def serve_consolidacion():
    """Servir página de consolidación de productos"""
    try:
        print("✅ Sirviendo página de consolidación: consolidacion.html")
        return FileResponse("consolidacion.html")
    except Exception as e:
        print(f"❌ Error sirviendo consolidacion.html: {e}")
        raise HTTPException(status_code=404, detail="Página no encontrada")


@app.get("/api/admin/productos")
async def get_admin_productos():
    """Catálogo de productos maestros con información completa"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print("🏷️ Obteniendo productos maestros con información completa...")

        cursor.execute(
            """
            SELECT
                pm.id,
                pm.codigo_ean,
                COALESCE(pm.nombre_comercial, pm.nombre_consolidado, 'Sin nombre') as nombre,
                pm.marca,
                pm.categoria,
                pm.subcategoria,
                pm.precio_promedio_global,
                pm.total_reportes,
                pm.primera_vez_reportado,
                pm.ultima_actualizacion,
                -- Contar cuántos usuarios lo han comprado
                COUNT(DISTINCT if.usuario_id) as usuarios_compraron,
                -- Contar cuántas facturas lo incluyen
                COUNT(DISTINCT if.factura_id) as facturas_incluyen
            FROM productos_maestros pm
            LEFT JOIN items_factura if ON if.producto_maestro_id = pm.id
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.nombre_comercial,
                     pm.marca, pm.categoria, pm.subcategoria, pm.precio_promedio_global,
                     pm.total_reportes, pm.primera_vez_reportado, pm.ultima_actualizacion
            ORDER BY pm.total_reportes DESC, pm.id DESC
            LIMIT 500
        """
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo_ean": row[1] or "",
                    "nombre": row[2],
                    "marca": row[3] or "Sin marca",
                    "categoria": row[4] or "Sin categoría",
                    "subcategoria": row[5] or "",
                    "precio_promedio": float(row[6]) if row[6] else 0,
                    "veces_comprado": row[7] or 0,
                    "primera_vez": str(row[8]) if row[8] else None,
                    "ultima_actualizacion": str(row[9]) if row[9] else None,
                    "usuarios_compraron": row[10] or 0,
                    "facturas_incluyen": row[11] or 0,
                }
            )

        conn.close()

        print(f"✅ {len(productos)} productos maestros obtenidos")
        print(
            f"📊 Productos con marca: {sum(1 for p in productos if p['marca'] != 'Sin marca')}"
        )
        print(
            f"📊 Productos con categoría: {sum(1 for p in productos if p['categoria'] != 'Sin categoría')}"
        )

        return productos

    except Exception as e:
        print(f"❌ Error obteniendo productos: {e}")
        traceback.print_exc()
        if conn:
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/admin/duplicados")
async def get_admin_duplicados():
    """Buscar duplicados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT f1.id, f2.id, f1.establecimiento, f1.total_factura, f1.fecha_factura
            FROM facturas f1
            INNER JOIN facturas f2 ON
                f1.establecimiento = f2.establecimiento AND
                ABS(f1.total_factura - f2.total_factura) < 100 AND
                f1.fecha_factura = f2.fecha_factura AND
                f1.id < f2.id
            LIMIT 50
        """
        )

        duplicados = []
        for row in cursor.fetchall():
            duplicados.append(
                {
                    "ids": [row[0], row[1]],
                    "establecimiento": row[2],
                    "total": float(row[3]) if row[3] else 0,
                    "fecha": str(row[4]) if row[4] else None,
                }
            )

        conn.close()
        return {"duplicados": duplicados, "total": len(duplicados)}

    except Exception as e:
        print(f"❌ Error buscando duplicados: {e}")
        raise HTTPException(500, str(e))


print("✅ Endpoints admin registrados directamente en main.py")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal / Dashboard"""
    possible_files = [
        "dashboard.html",
        "admin_dashboard_v2.html",
        "admin_dashboard.html",
    ]

    for filename in possible_files:
        file_path = Path(filename)
        if file_path.exists():
            print(f"✅ Sirviendo dashboard: {filename}")
            return FileResponse(
                str(file_path),
                media_type="text/html",
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )

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


@app.get("/productos.html", response_class=HTMLResponse)
async def serve_productos():
    """Servir página de gestión de productos v2"""
    try:
        return FileResponse("static/productos.html", media_type="text/html")
    except Exception as e:
        print(f"❌ Error sirviendo productos.html: {e}")
        raise HTTPException(status_code=404, detail=f"Productos no disponible: {e}")


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


@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...), request: Request = None):
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

        # Detectar duplicados automáticamente
        productos_parseados = []
        for p in productos_corregidos:
            productos_parseados.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "valor": float(p.get("valor") or p.get("precio", 0)),
                }
            )

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados, total_factura
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(f"✅ Después de detección: {len(productos_finales)} productos")

        # Convertir de vuelta al formato original
        productos_corregidos = []
        for p in productos_finales:
            productos_corregidos.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "precio": p.get("valor", 0),
                    "valor": p.get("valor", 0),
                    "cantidad": p.get("cantidad", 1),
                }
            )

        conn = get_db_connection()
        cursor = conn.cursor()

        cadena = detectar_cadena(establecimiento_raw)
        establecimiento_id = obtener_o_crear_establecimiento(
            establecimiento_raw, cadena
        )

        # Obtener usuario_id del token
        authorization = request.headers.get("Authorization") if request else None
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

        # Guardar reporte de anomalías si hubo correcciones
        if resultado_deteccion.get("duplicados_detectados"):
            metricas = resultado_deteccion.get("metricas", {})
            metricas["productos_originales"] = len(productos_parseados)
            metricas["productos_corregidos"] = len(productos_finales)
            metricas["productos_eliminados_detalle"] = resultado_deteccion.get(
                "productos_eliminados", []
            )

            guardar_reporte_anomalia(factura_id, establecimiento_raw, metricas)

        productos_guardados = 0
        for idx, prod in enumerate(productos_corregidos, 1):
            try:
                codigo_ean = str(prod.get("codigo", "")).strip()
                nombre = str(prod.get("nombre", "")).strip()

                valor_ocr = prod.get("valor") or prod.get("precio") or 0
                cantidad = int(prod.get("cantidad", 1))

                precio_unitario = normalizar_precio_unitario(valor_ocr, cantidad)

                if not nombre or precio_unitario <= 0:
                    continue

                codigo_ean_valido = None
                if codigo_ean and len(codigo_ean) >= 8 and codigo_ean.isdigit():
                    codigo_ean_valido = codigo_ean

                # ========================================
                # ✅ NUEVO: Usar ProductResolver
                # ========================================
                # ✅ CAMBIO B: Usar buscar_o_crear_producto_inteligente
                producto_maestro_id = buscar_o_crear_producto_inteligente(
                    codigo=codigo_ean_valido or "",
                    nombre=nombre,
                    precio=precio_unitario,
                    establecimiento=establecimiento_raw,
                    cursor=cursor,
                    conn=conn,
                    establecimiento_id=establecimiento_id,  # ← AGREGAR ESTO
                )

                print(f"   ✅ Producto Maestro ID: {producto_maestro_id} - {nombre}")

                # Guardar en items_factura
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

        # Actualizar inventario
        print(f"📦 Actualizando inventario del usuario...")
        try:
            actualizar_inventario_desde_factura(factura_id, usuario_id)
            print(f"✅ Inventario actualizado correctamente")
        except Exception as e:
            print(f"⚠️ Error actualizando inventario: {e}")
            traceback.print_exc()

        print(f"💰 Guardando precios para comparación...")
        try:
            stats = procesar_items_factura_y_guardar_precios(factura_id, usuario_id)
            if stats.get("error"):
                print(f"⚠️ Error guardando precios: {stats['error']}")
            else:
                print(
                    f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos"
                )
        except Exception as e:
            print(f"⚠️ Error guardando precios: {e}")
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
            "deteccion_duplicados": resultado_deteccion.get("metricas", {}),
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
        print(f"📦 Productos recibidos: {len(productos_list)}")

        # Detector automático de duplicados
        productos_parseados = []
        for p in productos_list:
            productos_parseados.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "valor": float(p.get("precio", 0)),
                }
            )

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados, total
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(f"✅ Productos después de detección: {len(productos_finales)}")

        # Convertir de vuelta al formato original
        productos_list = []
        for p in productos_finales:
            productos_list.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "precio": p.get("valor", 0),
                }
            )

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

                # ========================================
                # ✅ NUEVO: Usar ProductResolver
                # ========================================
                # ✅ CAMBIO C: Usar buscar_o_crear_producto_inteligente
                # ✅ CAMBIO C: Usar buscar_o_crear_producto_inteligente
                producto_maestro_id = buscar_o_crear_producto_inteligente(
                    codigo=codigo,
                    nombre=nombre,
                    precio=int(precio),
                    establecimiento=establecimiento,
                    cursor=cursor,
                    conn=conn,
                    establecimiento_id=establecimiento_id,  # ← AGREGAR ESTO
                )

                print(f"   ✅ Producto Maestro ID: {producto_maestro_id} - {nombre}")

                # Guardar en items_factura
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
                traceback.print_exc()
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

        # Guardar reporte de anomalías
        if resultado_deteccion.get("duplicados_detectados"):
            metricas = resultado_deteccion.get("metricas", {})
            metricas["productos_originales"] = len(productos_parseados)
            metricas["productos_corregidos"] = len(productos_finales)
            metricas["productos_eliminados_detalle"] = resultado_deteccion.get(
                "productos_eliminados", []
            )

            guardar_reporte_anomalia(factura_id, establecimiento, metricas)

        # Ajustar precios por descuentos/duplicados
        print(f"🔧 Ajustando precios por descuentos...")
        try:
            duplicados_eliminados = limpiar_items_duplicados(factura_id, conn)
            ajuste_exitoso = ajustar_precios_items_por_total(factura_id, conn)

            if ajuste_exitoso:
                print(f"✅ Precios ajustados correctamente")
            else:
                print(f"⚠️ No se pudo ajustar precios automáticamente")

        except Exception as e:
            print(f"⚠️ Error en ajuste: {e}")
            traceback.print_exc()

        # Actualizar inventario
        print(f"📦 Actualizando inventario del usuario...")
        try:
            actualizar_inventario_desde_factura(factura_id, usuario_id)
            print(f"✅ Inventario actualizado correctamente")
        except Exception as e:
            print(f"⚠️ Error actualizando inventario: {e}")
            traceback.print_exc()

        # ✅ NUEVO: Actualizar tablas analíticas
        print(f"📊 Actualizando tablas analíticas...")
        try:
            # Preparar items procesados desde items_factura
            cursor.execute(
                """
                SELECT producto_maestro_id, codigo_leido
                FROM items_factura
                WHERE factura_id = %s
            """,
                (factura_id,),
            )

            items_data = []
            for row in cursor.fetchall():
                if row[0]:  # Solo si tiene producto_maestro_id
                    items_data.append(
                        {"producto_maestro_id": row[0], "codigo_leido": row[1] or ""}
                    )
            productos_ids = [
                item["producto_maestro_id"]
                for item in items_data
                if item.get("producto_maestro_id")
            ]
            resultado_analytics = actualizar_todas_las_tablas_analiticas(
                cursor=cursor,
                conn=conn,
                factura_id=factura_id,
                usuario_id=usuario_id,
                establecimiento_id=establecimiento_id,
                productos_ids=productos_ids,
            )

            print(f"✅ Tablas analíticas actualizadas:")
            print(f"   - Códigos: {resultado_analytics['codigos_actualizados']}")
            print(f"   - Historial: {resultado_analytics['historial_compras']}")
            print(f"   - Patrones: {resultado_analytics['patrones_compra']}")
            print(
                f"   - Precios: {resultado_analytics['productos_por_establecimiento']}"
            )

        except Exception as e:
            print(f"⚠️ Error actualizando tablas analíticas: {e}")
            import traceback

            traceback.print_exc()

        print(f"💰 Guardando precios para comparación...")
        try:
            stats = procesar_items_factura_y_guardar_precios(factura_id, usuario_id)
            if stats.get("error"):
                print(f"⚠️ Error guardando precios: {stats['error']}")
            else:
                print(
                    f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos"
                )
        except Exception as e:
            print(f"⚠️ Error guardando precios: {e}")
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


@app.get("/api/establecimientos")
async def get_establecimientos():
    """
    Obtener lista de establecimientos
    Endpoint directo sin redirect para evitar problemas con HTTPS
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT id, nombre_normalizado, cadena, activo
            FROM establecimientos
            WHERE activo = TRUE
            ORDER BY nombre_normalizado
        """
        )

        establecimientos = []
        for row in cursor.fetchall():
            establecimientos.append(
                {
                    "id": row[0],
                    "nombre_normalizado": row[1],
                    "nombre": row[1],  # Alias para compatibilidad
                    "cadena": row[2] if len(row) > 2 else None,
                    "activo": row[3] if len(row) > 3 else True,
                }
            )

        cursor.close()
        conn.close()

        return establecimientos

    except Exception as e:
        print(f"❌ Error obteniendo establecimientos: {e}")
        # Retornar lista vacía en lugar de error para no romper el frontend
        return []


@app.post("/api/establecimientos")
async def crear_establecimiento(request: Request):
    """
    Crear un nuevo establecimiento
    """
    try:
        data = await request.json()
        nombre = data.get("nombre", "").strip()

        if not nombre:
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Nombre es requerido"},
            )

        # Normalizar nombre (convertir a mayúsculas)
        nombre_normalizado = nombre.upper()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si ya existe
        cursor.execute(
            "SELECT id FROM establecimientos WHERE UPPER(nombre_normalizado) = %s",
            (nombre_normalizado,),
        )

        existente = cursor.fetchone()

        if existente:
            cursor.close()
            conn.close()
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"El establecimiento '{nombre}' ya existe",
                },
            )

        # Insertar nuevo establecimiento
        cursor.execute(
            """
            INSERT INTO establecimientos (nombre_normalizado, cadena, activo)
            VALUES (%s, %s, TRUE)
            RETURNING id
            """,
            (nombre_normalizado, None),
        )

        nuevo_id = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Establecimiento creado: {nombre_normalizado} (ID: {nuevo_id})")

        return {
            "success": True,
            "establecimiento": {
                "id": nuevo_id,
                "nombre": nombre_normalizado,
                "nombre_normalizado": nombre_normalizado,
                "cadena": None,
                "activo": True,
            },
        }

    except Exception as e:
        print(f"❌ Error creando establecimiento: {e}")
        import traceback

        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Error al crear establecimiento: {str(e)}",
            },
        )


# FUNCIÓN DE BACKGROUND - COMPLETA CON ESTABLECIMIENTO
# ==========================================
async def process_video_background_task(
    job_id: str,
    video_path: str,
    usuario_id: int,
    establecimiento_nombre: str = None,  # ← NUEVO
    establecimiento_id: int = None,  # ← NUEVO
):
    """Procesa video en BACKGROUND con establecimiento confirmado"""
    conn = None
    cursor = None
    frames_paths = []

    try:
        print(f"\n{'='*80}")
        print(f"🔄 PROCESAMIENTO EN BACKGROUND")
        print(f"🆔 Job: {job_id}")
        print(f"👤 Usuario: {usuario_id}")
        if establecimiento_nombre:
            print(
                f"🏪 Establecimiento: {establecimiento_nombre} (ID: {establecimiento_id})"
            )
        print(f"{'='*80}")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar job
        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    "SELECT status, factura_id FROM processing_jobs WHERE id = %s",
                    (job_id,),
                )
            else:
                cursor.execute(
                    "SELECT status, factura_id FROM processing_jobs WHERE id = ?",
                    (job_id,),
                )

            job_data = cursor.fetchone()

            if not job_data:
                print(f"❌ Job {job_id} no existe en BD")
                return

            current_status, existing_factura_id = job_data[0], job_data[1]

            if current_status == "completed":
                print(f"⚠️ Job {job_id} ya completado. Factura: {existing_factura_id}")
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

        # Actualizar status a processing
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

        # Importar módulos de video
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

        # Extraer frames
        # Extraer frames - OPTIMIZADO
        print(f"🎬 Extrayendo frames...")
        frames_paths = extraer_frames_video(video_path, intervalo=5.0)

        # LIMITAR a máximo 5 frames para evitar duplicados y rate limits
        MAX_FRAMES = 5
        if len(frames_paths) > MAX_FRAMES:
            print(f"⚠️ Limitando de {len(frames_paths)} a {MAX_FRAMES} frames")
            # Tomar frames distribuidos uniformemente
            indices = [
                int(i * len(frames_paths) / MAX_FRAMES) for i in range(MAX_FRAMES)
            ]
            frames_paths = [frames_paths[i] for i in indices]

        if not frames_paths:
            raise Exception("No se extrajeron frames del video")

        print(f"✅ {len(frames_paths)} frames a procesar")

        # Procesar frames con Claude
        print(f"🤖 Procesando con Claude...")
        start_time = time.time()

        def procesar_frame_individual(args):
            i, frame_path = args
            try:
                # ✅ CRÍTICO: Pasar establecimiento_nombre a Claude
                resultado = parse_invoice_with_claude(
                    frame_path,
                    establecimiento_preseleccionado=establecimiento_nombre,  # ← NUEVO
                )
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

        # Deduplicar
        print(f"🔍 Deduplicando productos...")
        productos_unicos = todos_productos
        print(f"📦 {len(todos_productos)} productos detectados (sin pre-filtrar)")

        # Detector automático de duplicados
        print(f"🔍 Aplicando detector inteligente de duplicados...")

        productos_parseados = []
        for p in productos_unicos:
            productos_parseados.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "valor": float(p.get("precio") or p.get("valor", 0)),
                }
            )

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados, total
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(
            f"✅ Después de detección inteligente: {len(productos_finales)} productos"
        )

        # Convertir de vuelta
        productos_unicos = []
        for p in productos_finales:
            productos_unicos.append(
                {
                    "codigo": p.get("codigo", ""),
                    "nombre": p.get("nombre", ""),
                    "precio": p.get("valor", 0),
                    "valor": p.get("valor", 0),
                    "cantidad": p.get("cantidad", 1),
                }
            )

        # Guardar en BD
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

            # Guardar reporte de anomalías
            if resultado_deteccion.get("duplicados_detectados"):
                print(f"📊 Guardando reporte de anomalías...")

                metricas = resultado_deteccion.get("metricas", {})
                metricas["productos_originales"] = len(productos_parseados)
                metricas["productos_corregidos"] = len(productos_finales)
                metricas["productos_eliminados_detalle"] = resultado_deteccion.get(
                    "productos_eliminados", []
                )

                guardar_reporte_anomalia(factura_id, establecimiento, metricas)
                print(f"✅ Reporte de anomalías guardado")

            # Guardar imagen completa
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

            productos_guardados = 0
            productos_fallidos = 0

            for producto in productos_unicos:
                try:
                    codigo = producto.get("codigo", "")
                    nombre = producto.get("nombre", "Sin nombre")
                    precio = producto.get("precio") or producto.get("valor", 0)
                    cantidad = producto.get("cantidad", 1)

                    # Validación: producto sin nombre
                    if not nombre or nombre.strip() == "":
                        print(f"⚠️ Producto sin nombre, omitiendo")
                        productos_fallidos += 1
                        continue

                    # Validación: cantidad válida
                    try:
                        cantidad = int(cantidad)
                        if cantidad <= 0:
                            cantidad = 1
                    except (ValueError, TypeError):
                        cantidad = 1

                    # Validación: precio válido
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

                    # ==========================================
                    # BUSCAR O CREAR PRODUCTO MAESTRO
                    # ==========================================
                    producto_maestro_id = None

                    if codigo and len(codigo) >= 3:
                        producto_maestro_id = buscar_o_crear_producto_inteligente(
                            codigo=codigo,
                            nombre=nombre,
                            precio=int(precio),
                            establecimiento=establecimiento,
                            cursor=cursor,
                            conn=conn,
                            establecimiento_id=establecimiento_id,  # ← AGREGAR ESTO
                        )

                        # ✅ FIX: Validar que se creó correctamente
                        if not producto_maestro_id:
                            print(
                                f"   ⚠️ SKIP: No se pudo crear producto maestro para '{nombre}'"
                            )
                            productos_fallidos += 1
                            continue

                        print(
                            f"   ✅ Producto Maestro ID: {producto_maestro_id} - {nombre}"
                        )

                    # Guardar en items_factura
                    if os.environ.get("DATABASE_TYPE") == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO items_factura (
                                factura_id, usuario_id, producto_maestro_id,
                                codigo_leido, nombre_leido, cantidad, precio_pagado
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
                    import traceback

                    traceback.print_exc()
                    productos_fallidos += 1

                    if "constraint" in str(e).lower():
                        conn.rollback()
                        conn = get_db_connection()
                        cursor = conn.cursor()

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
            print(f"✅ Productos guardados: {productos_guardados}")

            if productos_fallidos > 0:
                print(f"⚠️ Productos no guardados: {productos_fallidos}")

            # Ajustar precios
            print(f"🔧 Ajustando precios por descuentos...")
            try:
                duplicados_eliminados = limpiar_items_duplicados(factura_id, conn)
                if duplicados_eliminados > 0:
                    print(f"   ✅ {duplicados_eliminados} items duplicados eliminados")

                ajuste_exitoso = ajustar_precios_items_por_total(factura_id, conn)

                if ajuste_exitoso:
                    print(f"   ✅ Precios ajustados correctamente")
                else:
                    print(f"   ⚠️ No se requirió ajuste de precios")

            except Exception as e:
                print(f"   ⚠️ Error en ajuste automático: {e}")
                traceback.print_exc()

            # Actualizar inventario
            print(f"📦 Actualizando inventario del usuario...")
            try:
                actualizar_inventario_desde_factura(factura_id, usuario_id)
                print(f"✅ Inventario actualizado correctamente")
            except Exception as e:
                print(f"⚠️ Error actualizando inventario: {e}")
                traceback.print_exc()

                # ✅ NUEVO: Actualizar tablas analíticas
            print(f"📊 Actualizando tablas analíticas...")
            try:
                # Preparar items procesados
                items_data = []
                for producto in productos_unicos:
                    # Buscar producto_maestro_id de cada item guardado
                    cursor.execute(
                        """
                        SELECT producto_maestro_id, codigo_leido
                        FROM items_factura
                        WHERE factura_id = %s
                        AND nombre_leido = %s
                        LIMIT 1
                    """,
                        (factura_id, producto.get("nombre", "")),
                    )

                    item_data = cursor.fetchone()
                    if item_data:
                        items_data.append(
                            {
                                "producto_maestro_id": item_data[0],
                                "codigo_leido": item_data[1],
                            }
                        )

                resultado_analytics = actualizar_todas_las_tablas_analiticas(
                    cursor=cursor,
                    conn=conn,
                    factura_id=factura_id,
                    usuario_id=usuario_id,
                    establecimiento_id=establecimiento_id,
                    productos_ids=productos_ids,
                )

                print(f"✅ Tablas analíticas actualizadas:")
                print(f"   - Códigos: {resultado_analytics['codigos_actualizados']}")
                print(f"   - Historial: {resultado_analytics['historial_compras']}")
                print(f"   - Patrones: {resultado_analytics['patrones_compra']}")
                print(
                    f"   - Precios: {resultado_analytics['productos_por_establecimiento']}"
                )

            except Exception as e:
                print(f"⚠️ Error actualizando tablas analíticas: {e}")
                import traceback

                traceback.print_exc()
            print(f"💰 Guardando precios para comparación...")
            try:
                stats = procesar_items_factura_y_guardar_precios(factura_id, usuario_id)
                if stats.get("error"):
                    print(f"⚠️ Error guardando precios: {stats['error']}")
                else:
                    print(
                        f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos"
                    )
            except Exception as e:
                print(f"⚠️ Error guardando precios: {e}")
                traceback.print_exc()

            # Actualizar job como completado
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

        # Limpiar archivos temporales
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
# RESTO DE ENDPOINTS (simplificados por espacio)
# ==========================================


@app.get("/api/admin/anomalias")
async def get_anomalias(usuario: dict = Depends(get_current_user)):
    """Dashboard de anomalías detectadas"""
    usuario_id = usuario.get("user_id", 1)
    stats = obtener_estadisticas_por_establecimiento()
    anomalias = obtener_anomalias_pendientes(limit=50)

    return {
        "success": True,
        "estadisticas": [
            {
                "establecimiento": s[0],
                "num_facturas": s[1],
                "ratio_promedio": float(s[2]) if s[2] else 1.0,
                "facturas_corregidas": s[3] or 0,
                "promedio_duplicados": float(s[4]) if s[4] else 0,
            }
            for s in stats
        ],
        "anomalias_pendientes": anomalias,
    }


print("✅ Sistema de auditoría cargado")


# ==========================================
# INICIALIZACIÓN DEL SERVIDOR
# ==========================================

from typing import Optional

print("=" * 80)
print("📦 CARGANDO ENDPOINTS DE AUDITORÍA DIRECTAMENTE EN MAIN.PY")
print("=" * 80)


# Modelos Pydantic para Auditoría
class ProductoBaseAuditoria(BaseModel):
    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None


class ProductoCreateAuditoria(ProductoBaseAuditoria):
    pass


class ProductoUpdateAuditoria(ProductoBaseAuditoria):
    pass


class AuditoriaLoginRequest(BaseModel):
    email: str
    password: str


# ============================================================================
# ENDPOINT: LOGIN AUDITORÍA (SEPARADO DEL LOGIN PRINCIPAL)
# ============================================================================

# ============================================================
# ENDPOINT: Login de Auditoría (Separado del login principal)
# ============================================================


@app.post("/setup/make-admin")
async def make_admin(email: str = "santiago@tscamp.co"):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = %s
        RETURNING id, email, rol
    """,
        (email,),
    )
    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {
            "success": True,
            "user": {"id": user[0], "email": user[1], "rol": user[2]},
        }
    return {"success": False, "error": "Usuario no encontrado"}


# ============================================================
# ENDPOINTS AUDITORÍA - PRODUCTOS REFERENCIA
# ============================================================


@app.get("/api/productos-referencia/{codigo_ean}")
async def get_producto_referencia(codigo_ean: str, authorization: str = Header(None)):
    """Obtener producto de referencia por código EAN"""
    print(f"🔍 [AUDITORÍA] Buscando producto referencia: {codigo_ean}")

    # Validar token JWT
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace("Bearer ", "")
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get("rol") not in ["admin", "auditor"]:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre,
                marca,
                categoria,
                presentacion,
                unidad_medida,
                created_at,
                updated_at
            FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        print(f"✅ [AUDITORÍA] Producto encontrado: {producto[2]}")

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre": producto[2],
            "marca": producto[3],
            "categoria": producto[4],
            "presentacion": producto[5],
            "unidad_medida": producto[6],
            "created_at": producto[7].isoformat() if producto[7] else None,
            "updated_at": producto[8].isoformat() if producto[8] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al buscar producto: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/productos-referencia")
async def crear_producto_referencia(
    request: Request, authorization: str = Header(None)
):
    """Crear nuevo producto de referencia"""
    print(f"📝 [AUDITORÍA] Creando nuevo producto referencia")

    # Validar token JWT
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace("Bearer ", "")
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get("rol") not in ["admin", "auditor"]:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    # Obtener datos del body
    body = await request.json()

    codigo_ean = body.get("codigo_ean")
    nombre = body.get("nombre")
    marca = body.get("marca", "")
    categoria = body.get("categoria", "")
    presentacion = body.get("presentacion", "")
    unidad_medida = body.get("unidad_medida", "")

    if not codigo_ean or not nombre:
        raise HTTPException(
            status_code=400, detail="Código EAN y nombre son requeridos"
        )

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si ya existe
        cursor.execute(
            """
            SELECT id FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Producto ya existe")

        # Insertar nuevo producto
        cursor.execute(
            """
            INSERT INTO productos_referencia
            (codigo_ean, nombre, marca, categoria, presentacion, unidad_medida)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, codigo_ean, nombre, created_at
        """,
            (codigo_ean, nombre, marca, categoria, presentacion, unidad_medida),
        )

        producto = cursor.fetchone()
        conn.commit()

        print(f"✅ [AUDITORÍA] Producto creado: {producto[2]} (ID: {producto[0]})")

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre": producto[2],
            "marca": marca,
            "categoria": categoria,
            "presentacion": presentacion,
            "unidad_medida": unidad_medida,
            "created_at": producto[3].isoformat() if producto[3] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al crear producto: {str(e)}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.put("/api/productos-referencia/{codigo_ean}")
async def actualizar_producto_referencia(
    codigo_ean: str, request: Request, authorization: str = Header(None)
):
    """Actualizar producto de referencia existente"""
    print(f"✏️ [AUDITORÍA] Actualizando producto: {codigo_ean}")

    # Validar token JWT
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace("Bearer ", "")
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get("rol") not in ["admin", "auditor"]:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    body = await request.json()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si existe
        cursor.execute(
            """
            SELECT id FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Actualizar campos proporcionados
        updates = []
        params = []

        if "nombre" in body:
            updates.append("nombre = %s")
            params.append(body["nombre"])
        if "marca" in body:
            updates.append("marca = %s")
            params.append(body["marca"])
        if "categoria" in body:
            updates.append("categoria = %s")
            params.append(body["categoria"])
        if "presentacion" in body:
            updates.append("presentacion = %s")
            params.append(body["presentacion"])
        if "unidad_medida" in body:
            updates.append("unidad_medida = %s")
            params.append(body["unidad_medida"])

        if not updates:
            raise HTTPException(status_code=400, detail="No hay datos para actualizar")

        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(codigo_ean)

        query = f"""
            UPDATE productos_referencia
            SET {', '.join(updates)}
            WHERE codigo_ean = %s
            RETURNING id, codigo_ean, nombre, marca, categoria, presentacion, unidad_medida, updated_at
        """

        cursor.execute(query, params)
        producto = cursor.fetchone()
        conn.commit()

        print(f"✅ [AUDITORÍA] Producto actualizado: {producto[2]}")

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre": producto[2],
            "marca": producto[3],
            "categoria": producto[4],
            "presentacion": producto[5],
            "unidad_medida": producto[6],
            "updated_at": producto[7].isoformat() if producto[7] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al actualizar producto: {str(e)}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/api/productos/{producto_id}/marcar-revisado")
async def marcar_producto_revisado(producto_id: int):
    """Marca un producto como revisado por admin"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión")

    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE productos_maestros
            SET revisado_admin = TRUE,
                fecha_revision = CURRENT_TIMESTAMP,
                notas_revision = 'Revisado desde dashboard admin'
            WHERE id = %s
            RETURNING id, nombre_normalizado
        """,
            (producto_id,),
        )

        resultado = cursor.fetchone()

        if not resultado:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "producto_id": resultado[0],
            "nombre": resultado[1],
            "mensaje": "Producto marcado como revisado",
        }

    except Exception as e:
        conn.rollback()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fix-rol")
async def fix_rol():
    """Endpoint temporal para arreglar rol del usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = 'santiago@tscamp.co'
        RETURNING id, email, rol
    """,
    )

    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {
            "success": True,
            "user": {"id": user[0], "email": user[1], "rol": user[2]},
        }
    return {"success": False, "error": "Usuario no encontrado"}


from consolidacion_productos import procesar_item_con_consolidacion
import psycopg2.extras

# ============================================================================
# REEMPLAZAR ENDPOINT /api/v2/procesar-factura EN main.py
# Buscar desde @app.post("/api/v2/procesar-factura") hasta el siguiente @app
# ============================================================================

# ============================================================================
# ENDPOINT V2 CON AUTENTICACIÓN JWT CORREGIDA
# Reemplazar en main.py desde @app.post("/api/v2/procesar-factura")
# ============================================================================


@app.post("/api/v2/procesar-factura")
async def procesar_factura_v2(request: Request, background_tasks: BackgroundTasks):
    """
    Endpoint V2 - Acepta VIDEOS o JSON
    ✅ Compatible con formato móvil
    """
    conn = None

    try:
        print("\n" + "=" * 80)
        print("📥 RECIBIENDO SOLICITUD /api/v2/procesar-factura")
        print("=" * 80)

        # 1. AUTENTICACIÓN
        token = request.headers.get("Authorization", "").replace("Bearer ", "")
        if not token:
            raise HTTPException(status_code=401, detail="Token no proporcionado")

        import jwt
        from auth import SECRET_KEY

        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            user_id = payload.get("user_id")
            if not user_id:
                raise HTTPException(status_code=401, detail="Token inválido")
            print(f"👤 Usuario autenticado: ID {user_id}")
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expirado")
        except jwt.InvalidTokenError as e:
            raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")

        # 2. DETECTAR TIPO DE CONTENIDO
        content_type = request.headers.get("content-type", "")
        print(f"📦 Content-Type: {content_type}")

        # 3. ✅ SI ES MULTIPART (VIDEO)
        if "multipart/form-data" in content_type:
            print("🎬 Detectado: VIDEO")

            form_data = await request.form()
            print(f"   Form keys: {list(form_data.keys())}")

            # ✅ VALIDAR CAMPOS REQUERIDOS
            if "video" not in form_data:
                raise HTTPException(
                    status_code=400,
                    detail="Se esperaba un archivo 'video' en el form-data",
                )

            # ✅ NUEVO: Extraer establecimiento_id del form
            establecimiento_id = form_data.get("establecimiento_id")
            if not establecimiento_id:
                raise HTTPException(
                    status_code=400,
                    detail="Se requiere 'establecimiento_id' en el form-data",
                )

            try:
                establecimiento_id = int(establecimiento_id)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="'establecimiento_id' debe ser un número",
                )

            print(f"🏪 Establecimiento ID: {establecimiento_id}")

            # ✅ NUEVO: Obtener nombre del establecimiento desde la BD
            conn = get_db_connection()
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT nombre_normalizado, cadena
                FROM establecimientos
                WHERE id = %s
            """,
                (establecimiento_id,),
            )

            resultado = cursor.fetchone()

            if not resultado:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"Establecimiento ID {establecimiento_id} no encontrado",
                )

            establecimiento_nombre = resultado[0]
            cadena = resultado[1]

            print(f"🏪 Establecimiento: {establecimiento_nombre} (Cadena: {cadena})")

            video = form_data.get("video")

            temp_video = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
            content = await video.read()
            temp_video.write(content)
            temp_video.close()

            video_size_mb = len(content) / (1024 * 1024)
            print(f"💾 Video guardado: {video_size_mb:.2f} MB")

            job_id = str(uuid.uuid4())

            cursor.execute(
                """
                INSERT INTO processing_jobs (
                    id, usuario_id, status, created_at
                ) VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP)
            """,
                (job_id, user_id),
            )

            conn.commit()
            cursor.close()
            conn.close()

            print(f"✅ Job creado: {job_id}")

            # ✅ NUEVO: Pasar establecimiento_nombre y establecimiento_id a la tarea
            background_tasks.add_task(
                process_video_background_task,
                job_id,
                temp_video.name,
                user_id,
                establecimiento_nombre,  # ← NUEVO
                establecimiento_id,  # ← NUEVO
            )

            return {
                "success": True,
                "job_id": job_id,
                "message": "Video recibido, procesando en background",
                "status": "pending",
                "establecimiento": establecimiento_nombre,
            }

        # 4. ✅ SI ES JSON (IMÁGENES INDIVIDUALES)
        elif "application/json" in content_type:
            print("📄 Detectado: JSON (imagen escaneada)")

            datos_factura = await request.json()

            if "items" not in datos_factura:
                raise HTTPException(status_code=400, detail="Falta el campo 'items'")

            items = datos_factura.get("items", [])
            establecimiento = datos_factura.get("establecimiento", "Desconocido")
            total = float(datos_factura.get("total", 0))
            fecha = datos_factura.get("fecha")

            print(f"📦 Items recibidos: {len(items)}")
            print(f"🏪 Establecimiento: {establecimiento}")
            print(f"💰 Total: ${total:,}")

            # Procesar establecimiento
            establecimiento, cadena = procesar_establecimiento(
                establecimiento_raw=establecimiento, productos=items, total=total
            )

            establecimiento_id = obtener_o_crear_establecimiento_id(
                conn=None, establecimiento=establecimiento, cadena=cadena
            )

            # Crear factura
            conn = get_db_connection()
            cursor = conn.cursor()

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
                    user_id,
                    establecimiento_id,
                    establecimiento,
                    cadena,
                    total,
                    fecha or datetime.now().date(),
                    len(items),
                ),
            )

            factura_id = cursor.fetchone()[0]
            conn.commit()

            print(f"✅ Factura creada: ID {factura_id}")

            # Procesar items
            items_procesados = 0
            items_fallidos = 0

            for item in items:
                try:
                    codigo = item.get("codigo", "")
                    nombre = item.get("nombre", "")
                    precio = float(item.get("precio", 0))
                    cantidad = int(item.get("cantidad", 1))

                    if not nombre or precio <= 0:
                        items_fallidos += 1
                        continue

                    # Buscar o crear producto
                    producto_maestro_id = None

                    if codigo and len(codigo) >= 3:
                        producto_maestro_id = buscar_o_crear_producto_inteligente(
                            codigo=codigo,
                            nombre=nombre,
                            precio=int(precio),
                            establecimiento=establecimiento,
                            cursor=cursor,
                            conn=conn,
                            establecimiento_id=establecimiento_id,  # ← AGREGAR ESTO
                        )

                        if not producto_maestro_id:
                            print(f"   ⚠️ No se pudo crear producto: {nombre}")
                            items_fallidos += 1
                            continue

                    # Guardar item
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, cantidad, precio_pagado
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        (
                            factura_id,
                            user_id,
                            producto_maestro_id,
                            codigo or None,
                            nombre,
                            cantidad,
                            precio,
                        ),
                    )

                    items_procesados += 1

                except Exception as e:
                    print(f"❌ Error procesando item: {e}")
                    items_fallidos += 1
                    continue

            # Actualizar contador
            cursor.execute(
                """
                UPDATE facturas
                SET productos_guardados = %s
                WHERE id = %s
            """,
                (items_procesados, factura_id),
            )

            conn.commit()
            cursor.close()
            conn.close()

            print(f"✅ Procesamiento completado:")
            print(f"   Items procesados: {items_procesados}")
            print(f"   Items fallidos: {items_fallidos}")

            return {
                "success": True,
                "factura_id": factura_id,
                "items_procesados": items_procesados,
                "items_fallidos": items_fallidos,
                "message": "Factura procesada correctamente",
            }

        else:
            raise HTTPException(
                status_code=400, detail=f"Content-Type no soportado: {content_type}"
            )

    except HTTPException:
        raise
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 2: CREAR PRODUCTO - ✅ CORREGIDO
# ==========================================
@app.post("/api/admin/auditoria/producto")
async def crear_producto_auditoria(
    producto: ProductoCreateAuditoria, current_user: dict = Depends(get_current_user)
):
    """Crear un nuevo producto en la base de datos"""
    print(f"➕ [AUDITORÍA] Creando producto: {producto.codigo_ean} - {producto.nombre}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si ya existe
        cursor.execute(
            "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
            (producto.codigo_ean,),
        )
        if cursor.fetchone():
            cursor.close()
            conn.close()
            print(f"⚠️ [AUDITORÍA] Producto ya existe: {producto.codigo_ean}")
            raise HTTPException(status_code=400, detail="Producto ya existe")

        # Crear producto
        cursor.execute(
            """
            INSERT INTO productos_maestros (
            codigo_ean, nombre_normalizado, marca, categoria,
            auditado_manualmente, validaciones_manuales
            ) VALUES (%s, %s, %s, %s, TRUE, 1)
            RETURNING id, codigo_ean, nombre_normalizado, marca, categoria, auditado_manualmente, validaciones_manuales
            """,
            (producto.codigo_ean, producto.nombre, producto.marca, producto.categoria),
        )

        row = cursor.fetchone()

        # Registrar auditoría
        cursor.execute(
            """
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'crear', %s)
        """,
            (current_user["id"], row[0], datetime.now()),
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto creado exitosamente: ID {row[0]}")

        return {
            "mensaje": "Producto creado",
            "producto": {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "categoria": row[4],
                "auditado_manualmente": row[5],
                "validaciones_manuales": row[6],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en crear_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 3: ACTUALIZAR PRODUCTO - ✅ CORREGIDO
# ==========================================
@app.put("/api/admin/auditoria/producto/{producto_id}")
async def actualizar_producto_auditoria(
    producto_id: int,
    producto: ProductoUpdateAuditoria,
    current_user: dict = Depends(get_current_user),
):
    """Actualizar un producto existente"""
    print(f"✏️ [AUDITORÍA] Actualizando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE productos_maestros
            SET nombre_normalizado = %s, marca = %s, categoria = %s
            WHERE id = %s
            RETURNING id, codigo_ean, nombre_normalizado, marca, categoria, auditado_manualmente, validaciones_manuales
            """,
            (producto.nombre, producto.marca, producto.categoria, producto_id),
        )

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")

        # Registrar auditoría
        cursor.execute(
            """
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'actualizar', %s)
        """,
            (current_user["id"], producto_id, datetime.now()),
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto actualizado: {row[2]}")

        return {
            "mensaje": "Actualizado",
            "producto": {
                "id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "categoria": row[4],
                "auditado_manualmente": row[5],
                "validaciones_manuales": row[6],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en actualizar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 4: VALIDAR PRODUCTO
# ==========================================
@app.post("/api/admin/auditoria/validar/{producto_id}")
async def validar_producto_auditoria(
    producto_id: int, current_user: dict = Depends(get_current_user)
):
    """Validar manualmente un producto"""
    print(f"✅ [AUDITORÍA] Validando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE productos_maestros
            SET auditado_manualmente = TRUE,
                validaciones_manuales = validaciones_manuales + 1,
                ultima_validacion = %s
            WHERE id = %s
        """,
            (datetime.now(), producto_id),
        )

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")

        # Registrar auditoría
        cursor.execute(
            """
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'validar', %s)
        """,
            (current_user["id"], producto_id, datetime.now()),
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto validado exitosamente")

        return {"mensaje": "Validado"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en validar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 5: ESTADÍSTICAS
# ==========================================
@app.get("/api/admin/auditoria/estadisticas")
async def obtener_estadisticas_auditoria(
    current_user: dict = Depends(get_current_user),
):
    """Obtener estadísticas de auditoría del usuario"""
    print(f"📊 [AUDITORÍA] Obteniendo estadísticas para usuario {current_user['id']}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT accion, COUNT(*)
            FROM auditoria_productos
            WHERE usuario_id = %s
            GROUP BY accion
        """,
            (current_user["id"],),
        )

        stats = {
            "productos_creados": 0,
            "productos_actualizados": 0,
            "productos_validados": 0,
            "total_acciones": 0,
        }

        for row in cursor.fetchall():
            if row[0] == "crear":
                stats["productos_creados"] = row[1]
            elif row[0] == "actualizar":
                stats["productos_actualizados"] = row[1]
            elif row[0] == "validar":
                stats["productos_validados"] = row[1]
            stats["total_acciones"] += row[1]

        cursor.execute(
            """
            SELECT accion, producto_maestro_id, fecha
            FROM auditoria_productos
            WHERE usuario_id = %s
            ORDER BY fecha DESC
            LIMIT 10
        """,
            (current_user["id"],),
        )

        stats["ultimas_acciones"] = [
            {
                "accion": r[0],
                "producto_maestro_id": r[1],
                "fecha": r[2].isoformat() if r[2] else None,
            }
            for r in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Estadísticas obtenidas")

        return stats

    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en obtener_estadisticas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("=" * 80)
print("✅ ENDPOINTS DE AUDITORÍA CORREGIDOS Y CARGADOS")
print("=" * 80)


@app.get("/admin/force-update-inventario/{factura_id}/{usuario_id}")
async def force_update_inventario(factura_id: int, usuario_id: int):
    """
    Endpoint de emergencia para forzar actualización de inventario
    """
    print("=" * 80)
    print(f"🔧 FORZANDO ACTUALIZACIÓN DE INVENTARIO")
    print(f"   Factura ID: {factura_id}")
    print(f"   Usuario ID: {usuario_id}")
    print("=" * 80)

    try:
        # Verificar que la factura existe
        conn = get_db_connection()
        cursor = conn.cursor()

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, establecimiento, total_factura, productos_guardados
                FROM facturas
                WHERE id = %s AND usuario_id = %s
            """,
                (factura_id, usuario_id),
            )
        else:
            cursor.execute(
                """
                SELECT id, establecimiento, total_factura, productos_guardados
                FROM facturas
                WHERE id = ? AND usuario_id = ?
            """,
                (factura_id, usuario_id),
            )

        factura = cursor.fetchone()

        if not factura:
            cursor.close()
            conn.close()
            return {
                "success": False,
                "error": f"Factura {factura_id} no encontrada para usuario {usuario_id}",
            }

        print(f"✅ Factura encontrada:")
        print(f"   Establecimiento: {factura[1]}")
        print(f"   Total: ${factura[2]:,}")
        print(f"   Productos guardados: {factura[3]}")

        # Verificar items
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(producto_maestro_id)
                FROM items_factura
                WHERE factura_id = %s
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*), COUNT(producto_maestro_id)
                FROM items_factura
                WHERE factura_id = ?
            """,
                (factura_id,),
            )

        items_stats = cursor.fetchone()
        print(f"📦 Items en BD:")
        print(f"   Total items: {items_stats[0]}")
        print(f"   Con producto_maestro_id: {items_stats[1]}")

        cursor.close()
        conn.close()

        if items_stats[1] == 0:
            return {
                "success": False,
                "error": f"La factura {factura_id} no tiene items con producto_maestro_id",
                "factura": {
                    "id": factura[0],
                    "establecimiento": factura[1],
                    "total": float(factura[2]) if factura[2] else 0,
                    "productos_guardados": factura[3],
                },
                "total_items": items_stats[0],
                "items_con_producto_maestro": items_stats[1],
            }

        # Forzar actualización
        print(f"\n🔄 Ejecutando actualizar_inventario_desde_factura...")
        resultado = actualizar_inventario_desde_factura(factura_id, usuario_id)

        if resultado:
            print(f"✅ Inventario actualizado exitosamente")

            # Verificar inventario
            conn = get_db_connection()
            cursor = conn.cursor()

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM inventario_usuario
                    WHERE usuario_id = %s
                """,
                    (usuario_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM inventario_usuario
                    WHERE usuario_id = ?
                """,
                    (usuario_id,),
                )

            total_inventario = cursor.fetchone()[0]
            cursor.close()
            conn.close()

            return {
                "success": True,
                "message": "Inventario actualizado correctamente",
                "factura": {
                    "id": factura[0],
                    "establecimiento": factura[1],
                    "total": float(factura[2]) if factura[2] else 0,
                    "productos_guardados": factura[3],
                },
                "items_procesados": items_stats[1],
                "productos_en_inventario": total_inventario,
            }
        else:
            return {
                "success": False,
                "error": "actualizar_inventario_desde_factura retornó False",
                "mensaje": "Revisa los logs para ver el error específico",
            }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/admin/debug-inventario/{usuario_id}")
async def debug_inventario(usuario_id: int):
    """
    Ver estado actual del inventario de un usuario
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Contar productos en inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM inventario_usuario
                WHERE usuario_id = %s
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM inventario_usuario
                WHERE usuario_id = ?
            """,
                (usuario_id,),
            )

        total_inventario = cursor.fetchone()[0]

        # Obtener últimas facturas
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    f.id,
                    f.establecimiento,
                    f.total_factura,
                    f.productos_guardados,
                    f.fecha_cargue,
                    COUNT(i.id) as items_guardados,
                    COUNT(i.producto_maestro_id) as items_con_producto_maestro
                FROM facturas f
                LEFT JOIN items_factura i ON f.id = i.factura_id
                WHERE f.usuario_id = %s
                GROUP BY f.id, f.establecimiento, f.total_factura, f.productos_guardados, f.fecha_cargue
                ORDER BY f.fecha_cargue DESC
                LIMIT 5
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    f.id,
                    f.establecimiento,
                    f.total_factura,
                    f.productos_guardados,
                    f.fecha_cargue,
                    COUNT(i.id) as items_guardados,
                    COUNT(i.producto_maestro_id) as items_con_producto_maestro
                FROM facturas f
                LEFT JOIN items_factura i ON f.id = i.factura_id
                WHERE f.usuario_id = ?
                GROUP BY f.id, f.establecimiento, f.total_factura, f.productos_guardados, f.fecha_cargue
                ORDER BY f.fecha_cargue DESC
                LIMIT 5
            """,
                (usuario_id,),
            )

        facturas = []
        for row in cursor.fetchall():
            facturas.append(
                {
                    "id": row[0],
                    "establecimiento": row[1],
                    "total": float(row[2]) if row[2] else 0,
                    "productos_guardados": row[3],
                    "fecha": str(row[4]) if row[4] else None,
                    "items_en_bd": row[5],
                    "items_con_producto_maestro": row[6],
                }
            )

        # Ver muestra del inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.establecimiento,
                    iu.fecha_ultima_compra
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 10
            """,
                (usuario_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_consolidado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.establecimiento,
                    iu.fecha_ultima_compra
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 10
            """,
                (usuario_id,),
            )

        inventario_muestra = []
        for row in cursor.fetchall():
            inventario_muestra.append(
                {
                    "producto_maestro_id": row[0],
                    "nombre": row[1],
                    "cantidad": float(row[2]) if row[2] else 0,
                    "precio": float(row[3]) if row[3] else 0,
                    "establecimiento": row[4],
                    "fecha_compra": str(row[5]) if row[5] else None,
                }
            )

        cursor.close()
        conn.close()

        return {
            "success": True,
            "usuario_id": usuario_id,
            "total_productos_inventario": total_inventario,
            "total_facturas": len(facturas),
            "facturas_recientes": facturas,
            "inventario_muestra": inventario_muestra,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": str(e)}


print("✅ Endpoints de mantenimiento agregados:")
print("   GET /admin/force-update-inventario/{factura_id}/{usuario_id}")
print("   GET /admin/debug-inventario/{usuario_id}")


@app.post("/api/auditoria/login")
async def auditoria_login(request: Request):
    """
    Login exclusivo para auditores y administradores
    """
    try:
        body = await request.json()
        email = body.get("email")
        password = body.get("password")

        print(f"🔐 [AUDITORÍA] Intento de login: {email}")

        if not email or not password:
            raise HTTPException(status_code=400, detail="Email y contraseña requeridos")

        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar usuario
        cursor.execute(
            """
            SELECT id, email, password_hash, nombre, rol
            FROM usuarios
            WHERE email = %s
        """,
            (email,),
        )

        usuario = cursor.fetchone()
        conn.close()

        if not usuario:
            print(f"⚠️ [AUDITORÍA] Usuario no encontrado: {email}")
            raise HTTPException(status_code=401, detail="Usuario no encontrado")

        user_id, user_email, password_hash, nombre, rol = usuario

        # Verificar contraseña
        from database import verify_password

        if not verify_password(password, password_hash):
            print(f"⚠️ [AUDITORÍA] Contraseña incorrecta: {email}")
            raise HTTPException(status_code=401, detail="Contraseña incorrecta")

        # Verificar rol (debe ser admin o auditor)
        if rol not in ["admin", "auditor"]:
            print(f"⚠️ [AUDITORÍA] Usuario sin permisos: {email} (rol: {rol})")
            raise HTTPException(
                status_code=403,
                detail=f"Sin permisos de auditoría. Tu rol es: {rol}. Contacta al administrador.",
            )

        # Generar token JWT
        from auth import create_jwt_token

        token = create_jwt_token(user_id, user_email, rol)

        print(f"✅ [AUDITORÍA] Login exitoso: {email} (rol: {rol})")

        return {
            "success": True,
            "message": "Login exitoso",
            "token": token,
            "user": {"id": user_id, "email": user_email, "nombre": nombre, "rol": rol},
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en login: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fix-mi-rol")
async def fix_mi_rol():
    """Endpoint temporal para cambiar rol a admin"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = 'santiago@tscamp.co'
        RETURNING id, email, rol
    """
    )

    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {
            "success": True,
            "mensaje": f"Usuario actualizado: {user[1]} → rol: {user[2]}",
        }
    return {"success": False, "error": "Usuario no encontrado"}


# ============================================================================
# ENDPOINTS PARA CONSULTAR CÓDIGOS POR ESTABLECIMIENTO
# Agregar a main.py
# ============================================================================


@app.get("/api/v2/productos/{producto_id}/codigos")
async def get_codigos_producto(producto_id: int):
    """
    Obtener todos los códigos de un producto en diferentes establecimientos
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                ce.id,
                ce.codigo_local,
                ce.tipo_codigo,
                e.nombre_normalizado as establecimiento,
                ce.veces_visto,
                ce.primera_vez_visto,
                ce.ultima_vez_visto
            FROM codigos_establecimiento ce
            JOIN establecimientos e ON ce.establecimiento_id = e.id
            WHERE ce.producto_maestro_id = %s
              AND ce.activo = TRUE
            ORDER BY ce.veces_visto DESC
        """,
            (producto_id,),
        )

        codigos = []
        for row in cursor.fetchall():
            codigos.append(
                {
                    "id": row[0],
                    "codigo": row[1],
                    "tipo": row[2],
                    "establecimiento": row[3],
                    "veces_visto": row[4],
                    "primera_vez": row[5].isoformat() if row[5] else None,
                    "ultima_vez": row[6].isoformat() if row[6] else None,
                }
            )

        cursor.close()
        conn.close()

        return {
            "success": True,
            "producto_id": producto_id,
            "codigos": codigos,
            "total": len(codigos),
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v2/productos/{producto_id}")
async def obtener_producto_detalle(producto_id: int):
    """Obtiene detalles de un producto específico con sus PLUs"""
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión")

    try:
        cursor = conn.cursor()

        # Obtener producto
        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre_normalizado,
                marca,
                categoria,
                precio_promedio_global,
                total_reportes
            FROM productos_maestros
            WHERE id = %s
        """,
            (producto_id,),
        )

        producto = cursor.fetchone()

        if not producto:
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Obtener PLUs
        cursor.execute(
            """
            SELECT
                ppe.codigo_plu,
                e.nombre_normalizado as establecimiento,
                ppe.precio_unitario,
                ppe.fecha_actualizacion
            FROM productos_por_establecimiento ppe
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            WHERE ppe.producto_maestro_id = %s
        """,
            (producto_id,),
        )

        plus_rows = cursor.fetchall()

        plus = []
        for plu in plus_rows:
            plus.append(
                {
                    "codigo_plu": plu[0],
                    "nombre_establecimiento": plu[1],
                    "precio": plu[2],
                    "ultima_vez_visto": plu[3].strftime("%Y-%m-%d") if plu[3] else None,
                }
            )

        cursor.close()
        conn.close()

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre_normalizado": producto[2],
            "nombre_comercial": producto[2],  # Por ahora igual
            "marca": producto[3],
            "categoria": producto[4],
            "subcategoria": None,
            "presentacion": None,
            "precio_promedio": producto[5],
            "veces_comprado": producto[6],
            "num_establecimientos": len(plus),
            "plus": plus,
        }

    except HTTPException:
        raise
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v2/establecimientos/{establecimiento_id}/codigos")
async def get_codigos_establecimiento(establecimiento_id: int, limite: int = 100):
    """
    Obtener todos los códigos locales de un establecimiento
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Info del establecimiento
        cursor.execute(
            """
            SELECT nombre_normalizado, cadena
            FROM establecimientos
            WHERE id = %s
        """,
            (establecimiento_id,),
        )

        establecimiento = cursor.fetchone()
        if not establecimiento:
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        # Códigos del establecimiento
        cursor.execute(
            """
            SELECT
                ce.codigo_local,
                ce.tipo_codigo,
                pm.nombre_consolidado as producto,
                pm.codigo_ean,
                ce.veces_visto,
                ce.ultima_vez_visto
            FROM codigos_establecimiento ce
            JOIN productos_maestros_v2 pm ON ce.producto_maestro_id = pm.id
            WHERE ce.establecimiento_id = %s
            AND ce.activo = TRUE
            ORDER BY ce.veces_visto DESC
            LIMIT %s
        """,
            (establecimiento_id, limite),
        )

        codigos = []
        for row in cursor.fetchall():
            codigos.append(
                {
                    "codigo_local": row[0],
                    "tipo": row[1],
                    "producto": row[2],
                    "codigo_ean": row[3] or "N/A",
                    "veces_visto": row[4],
                    "ultima_vez": row[5].isoformat() if row[5] else None,
                }
            )

        # Estadísticas
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN tipo_codigo = 'plu_local' THEN 1 END) as plu_locales,
                COUNT(CASE WHEN tipo_codigo = 'plu_estandar' THEN 1 END) as plu_estandares,
                COUNT(CASE WHEN tipo_codigo = 'ean' THEN 1 END) as eans
            FROM codigos_establecimiento
            WHERE establecimiento_id = %s AND activo = TRUE
        """,
            (establecimiento_id,),
        )

        stats = cursor.fetchone()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "establecimiento": {
                "id": establecimiento_id,
                "nombre": establecimiento[0],
                "cadena": establecimiento[1],
            },
            "codigos": codigos,
            "estadisticas": {
                "total_codigos": stats[0],
                "plu_locales": stats[1],
                "plu_estandares": stats[2],
                "eans": stats[3],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v2/productos/buscar-por-codigo/{codigo}")
async def buscar_producto_por_codigo(codigo: str, establecimiento_id: int = None):
    """
    Buscar producto por cualquier código (EAN o PLU local)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Buscar por EAN primero (universal)
        cursor.execute(
            """
            SELECT id, nombre_consolidado, codigo_ean, marca
            FROM productos_maestros_v2
            WHERE codigo_ean = %s
            LIMIT 1
        """,
            (codigo,),
        )

        producto = cursor.fetchone()

        if producto:
            cursor.close()
            conn.close()
            return {
                "success": True,
                "encontrado": True,
                "tipo_busqueda": "ean",
                "producto": {
                    "id": producto[0],
                    "nombre": producto[1],
                    "codigo_ean": producto[2],
                    "marca": producto[3],
                },
            }

        # Buscar por código local
        if establecimiento_id:
            cursor.execute(
                """
                SELECT
                    pm.id,
                    pm.nombre_consolidado,
                    pm.codigo_ean,
                    pm.marca,
                    ce.tipo_codigo
                FROM codigos_establecimiento ce
                JOIN productos_maestros_v2 pm ON ce.producto_maestro_id = pm.id
                WHERE ce.codigo_local = %s
                AND ce.establecimiento_id = %s
                AND ce.activo = TRUE
                LIMIT 1
            """,
                (codigo, establecimiento_id),
            )
        else:
            cursor.execute(
                """
                SELECT
                    pm.id,
                    pm.nombre_consolidado,
                    pm.codigo_ean,
                    pm.marca,
                    ce.tipo_codigo
                FROM codigos_establecimiento ce
                JOIN productos_maestros_v2 pm ON ce.producto_maestro_id = pm.id
                WHERE ce.codigo_local = %s
                AND ce.activo = TRUE
                LIMIT 1
            """,
                (codigo,),
            )

        producto = cursor.fetchone()
        cursor.close()
        conn.close()

        if producto:
            return {
                "success": True,
                "encontrado": True,
                "tipo_busqueda": "codigo_local",
                "producto": {
                    "id": producto[0],
                    "nombre": producto[1],
                    "codigo_ean": producto[2] or "N/A",
                    "marca": producto[3],
                    "tipo_codigo": producto[4],
                },
            }

        return {
            "success": True,
            "encontrado": False,
            "mensaje": f"No se encontró producto con código {codigo}",
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de códigos por establecimiento registrados")

# ============================================================================
# ENDPOINT TEMPORAL PARA FORZAR CREACIÓN
# Agregar a main.py temporalmente
# ============================================================================


@app.get("/admin/setup-codigos-establecimiento")
async def setup_codigos_establecimiento():
    """
    Endpoint de emergencia para crear el sistema de códigos
    Ejecutar visitando: https://tu-app.railway.app/admin/setup-codigos-establecimiento
    """
    try:
        print("\n" + "=" * 70)
        print("🔧 FORZANDO CREACIÓN DE SISTEMA DE CÓDIGOS")
        print("=" * 70)

        from database import (
            crear_tabla_codigos_establecimiento,
            verificar_sistema_codigos,
        )

        # Intentar crear
        resultado = crear_tabla_codigos_establecimiento()

        if resultado:
            print("✅ Creación exitosa, verificando...")
            verificacion = verificar_sistema_codigos()

            return {
                "success": True,
                "mensaje": "Sistema de códigos instalado correctamente",
                "verificacion": verificacion,
            }
        else:
            return {
                "success": False,
                "error": "No se pudo crear el sistema (ver logs del servidor)",
            }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/admin/verificar-codigos")
async def verificar_codigos():
    """
    Verificar estado del sistema de códigos
    """
    try:
        from database import verificar_sistema_codigos

        existe = verificar_sistema_codigos()

        return {
            "success": True,
            "sistema_instalado": existe,
            "mensaje": (
                "Sistema instalado correctamente" if existe else "Sistema NO instalado"
            ),
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoints de setup agregados")


@app.get("/admin/setup-categorias")
async def setup_categorias():
    """
    Endpoint para crear tabla categorias (VERSIÓN CORREGIDA)
    Para tablas que YA TIENEN categoria_id pero la tabla categorias no existe
    """
    try:
        print("\n" + "=" * 70)
        print("🏗️ CREANDO TABLA CATEGORIAS (Versión Simple)")
        print("=" * 70)

        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Crear tabla categorias
        print("1️⃣ Creando tabla categorias...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS categorias (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) UNIQUE NOT NULL,
                descripcion TEXT,
                icono VARCHAR(50),
                color VARCHAR(20),
                orden INTEGER DEFAULT 0,
                activo BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.commit()
        print("   ✅ Tabla creada")

        # 2. Insertar categorías básicas
        print("2️⃣ Insertando categorías básicas...")
        categorias_basicas = [
            ("Lácteos", "Leche, yogurt, queso y derivados", "🥛", 1),
            ("Carnes", "Carnes rojas, pollo, pescado", "🥩", 2),
            ("Frutas y Verduras", "Productos frescos", "🍎", 3),
            ("Panadería", "Pan, pasteles, repostería", "🍞", 4),
            ("Bebidas", "Jugos, gaseosas, agua", "🥤", 5),
            ("Despensa", "Granos, pastas, enlatados", "🥫", 6),
            ("Aseo Personal", "Jabones, shampoo, cuidado personal", "🧴", 7),
            ("Aseo Hogar", "Limpieza y desinfección", "🧹", 8),
            ("Snacks", "Galletas, papas, dulces", "🍪", 9),
            ("Congelados", "Productos congelados", "🧊", 10),
            ("Farmacia", "Medicamentos y cuidado de salud", "💊", 11),
            ("Bebé", "Productos para bebés", "👶", 12),
            ("Mascotas", "Alimentos y cuidado de mascotas", "🐕", 13),
            ("Licores", "Bebidas alcohólicas", "🍺", 14),
            ("Otros", "Productos varios", "📦", 99),
        ]

        insertadas = 0
        for nombre, desc, icono, orden in categorias_basicas:
            try:
                cursor.execute(
                    """
                    INSERT INTO categorias (nombre, descripcion, icono, orden)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (nombre) DO NOTHING
                """,
                    (nombre, desc, icono, orden),
                )
                if cursor.rowcount > 0:
                    insertadas += 1
            except Exception as e:
                print(f"   ⚠️ Error con {nombre}: {e}")

        conn.commit()
        print(f"   ✅ {insertadas} categorías insertadas")

        # 3. Verificar columna categoria_id (ya debe existir)
        print("3️⃣ Verificando columna categoria_id...")
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros_v2'
            AND column_name = 'categoria_id'
        """
        )

        if cursor.fetchone():
            print("   ✓ Columna categoria_id ya existe")
        else:
            print("   ➕ Agregando columna categoria_id...")
            cursor.execute(
                """
                ALTER TABLE productos_maestros_v2
                ADD COLUMN categoria_id INTEGER REFERENCES categorias(id)
            """
            )
            conn.commit()
            print("   ✅ Columna agregada")

        # 4. Crear índice
        print("4️⃣ Creando índices...")
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_productos_v2_categoria
            ON productos_maestros_v2(categoria_id)
        """
        )
        conn.commit()
        print("   ✅ Índice creado")

        # 5. Crear vista
        print("5️⃣ Creando vista v_productos_con_categoria...")
        cursor.execute(
            """
            CREATE OR REPLACE VIEW v_productos_con_categoria AS
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                c.nombre as categoria,
                c.icono as categoria_icono,
                pm.categoria_id,
                pm.veces_visto,
                pm.fecha_creacion
            FROM productos_maestros_v2 pm
            LEFT JOIN categorias c ON pm.categoria_id = c.id
        """
        )
        conn.commit()
        print("   ✅ Vista creada")

        # 6. Obtener estadísticas
        cursor.execute("SELECT COUNT(*) FROM categorias WHERE activo = TRUE")
        total_categorias = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM productos_maestros_v2 WHERE categoria_id IS NOT NULL"
        )
        productos_con_categoria = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM productos_maestros_v2 WHERE categoria_id IS NULL"
        )
        productos_sin_categoria = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        print("\n" + "=" * 70)
        print("✅ SETUP COMPLETADO")
        print("=" * 70)

        return {
            "success": True,
            "mensaje": "Tabla categorias creada correctamente",
            "nota": "Tu tabla ya tenía categoria_id, solo se creó el catálogo de categorías",
            "estadisticas": {
                "total_categorias": total_categorias,
                "productos_con_categoria": productos_con_categoria,
                "productos_sin_categoria": productos_sin_categoria,
                "categorias_insertadas": insertadas,
            },
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


print("✅ Endpoint de categorías corregido (para tablas con categoria_id existente)")


@app.get("/admin/verificar-categorias")
async def verificar_categorias():
    """
    Verificar estado del sistema de categorías
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar tabla
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'categorias'
            )
        """
        )
        tabla_existe = cursor.fetchone()[0]

        if not tabla_existe:
            cursor.close()
            conn.close()
            return {
                "success": False,
                "instalado": False,
                "mensaje": "Tabla categorias no existe. Ejecuta /admin/setup-categorias",
            }

        # Obtener estadísticas
        cursor.execute("SELECT COUNT(*) FROM categorias WHERE activo = TRUE")
        total_categorias = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM productos_maestros_v2 WHERE categoria_id IS NOT NULL"
        )
        productos_con_categoria = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_maestros_v2")
        total_productos = cursor.fetchone()[0]

        # Listar categorías
        cursor.execute(
            """
            SELECT c.nombre, COUNT(pm.id) as productos
            FROM categorias c
            LEFT JOIN productos_maestros_v2 pm ON c.id = pm.categoria_id
            WHERE c.activo = TRUE
            GROUP BY c.nombre
            ORDER BY productos DESC
            LIMIT 10
        """
        )

        top_categorias = []
        for row in cursor.fetchall():
            top_categorias.append({"nombre": row[0], "productos": row[1]})

        cursor.close()
        conn.close()

        porcentaje_categorizados = (
            (productos_con_categoria / total_productos * 100)
            if total_productos > 0
            else 0
        )

        return {
            "success": True,
            "instalado": True,
            "estadisticas": {
                "total_categorias": total_categorias,
                "total_productos": total_productos,
                "productos_con_categoria": productos_con_categoria,
                "productos_sin_categoria": total_productos - productos_con_categoria,
                "porcentaje_categorizados": round(porcentaje_categorizados, 1),
            },
            "top_categorias": top_categorias,
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoints de categorías agregados:")
print("   GET /admin/setup-categorias")
print("   GET /admin/verificar-categorias")

# ============================================================================
# AGREGAR ESTE ENDPOINT A main.py - VERSIÓN ULTRA SIMPLE
# ============================================================================


@app.get("/admin/fix-categorias-final")
async def fix_categorias_final():
    """
    Versión ultra simple: solo crea tabla e inserta datos
    NO crea la vista (eso se hace después si es necesario)
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        resultados = []

        # 1. Crear tabla
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS categorias (
                id SERIAL PRIMARY KEY,
                nombre VARCHAR(100) UNIQUE NOT NULL,
                descripcion TEXT,
                icono VARCHAR(50),
                orden INTEGER DEFAULT 0,
                activo BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        conn.commit()
        resultados.append("✅ Tabla categorias verificada")

        # 2. Insertar categorías
        categorias = [
            ("Lácteos", "🥛", 1),
            ("Carnes", "🥩", 2),
            ("Frutas y Verduras", "🍎", 3),
            ("Panadería", "🍞", 4),
            ("Bebidas", "🥤", 5),
            ("Despensa", "🥫", 6),
            ("Aseo Personal", "🧴", 7),
            ("Aseo Hogar", "🧹", 8),
            ("Snacks", "🍪", 9),
            ("Congelados", "🧊", 10),
            ("Farmacia", "💊", 11),
            ("Bebé", "👶", 12),
            ("Mascotas", "🐕", 13),
            ("Licores", "🍺", 14),
            ("Otros", "📦", 99),
        ]

        insertadas = 0
        for nombre, icono, orden in categorias:
            cursor.execute(
                """
                INSERT INTO categorias (nombre, icono, orden)
                VALUES (%s, %s, %s)
                ON CONFLICT (nombre) DO NOTHING
            """,
                (nombre, icono, orden),
            )
            if cursor.rowcount > 0:
                insertadas += 1

        conn.commit()
        resultados.append(f"✅ {insertadas} categorías nuevas insertadas")

        # 3. Crear índice
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_productos_v2_categoria
            ON productos_maestros_v2(categoria_id)
        """
        )
        conn.commit()
        resultados.append("✅ Índice creado")

        # 4. Verificar
        cursor.execute("SELECT COUNT(*) FROM categorias")
        total = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT COUNT(*) FROM productos_maestros_v2
            WHERE categoria_id IS NOT NULL
        """
        )
        con_cat = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Tabla categorias configurada correctamente",
            "resultados": resultados,
            "estadisticas": {
                "total_categorias": total,
                "productos_con_categoria": con_cat,
                "categorias_insertadas": insertadas,
            },
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


from inventory_consolidator import (
    obtener_inventario_consolidado,
    obtener_estadisticas_stock,
)


@app.get("/api/v2/inventario-consolidado/{usuario_id}")
async def get_inventario_consolidado(usuario_id: int):
    """
    🆕 ENDPOINT NUEVO - Obtiene inventario consolidado del usuario

    Agrupa productos duplicados y muestra 1 línea por producto único

    Args:
        usuario_id: ID del usuario

    Returns:
        {
            "productos": [...],
            "estadisticas": {...},
            "total": 50,
            "consolidados": 10
        }
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener inventario consolidado
        inventario = obtener_inventario_consolidado(usuario_id, cursor)

        # Obtener estadísticas
        estadisticas = obtener_estadisticas_stock(inventario)

        cursor.close()
        conn.close()

        return {
            "success": True,
            "productos": inventario,
            "estadisticas": estadisticas,
            "total": len(inventario),
            "consolidados": estadisticas["productos_consolidados"],
        }

    except Exception as e:
        print(f"❌ Error en get_inventario_consolidado: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ═══════════════════════════════════════════════════════════════
# MODIFICACIÓN OPCIONAL DEL ENDPOINT EXISTENTE
# ═══════════════════════════════════════════════════════════════

# Si quieres agregar un parámetro opcional al endpoint existente:


@app.get("/api/v2/inventario/{usuario_id}")
async def get_inventario(usuario_id: int, consolidado: bool = False):
    """
    Obtiene inventario del usuario

    Args:
        usuario_id: ID del usuario
        consolidado: Si es True, devuelve inventario consolidado

    Returns:
        Lista de productos (consolidados o sin consolidar)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # ✅ NUEVO: Si piden consolidado, usar el nuevo módulo
        if consolidado:
            inventario = obtener_inventario_consolidado(usuario_id, cursor)
            estadisticas = obtener_estadisticas_stock(inventario)

            cursor.close()
            conn.close()

            return {
                "success": True,
                "productos": inventario,
                "estadisticas": estadisticas,
                "consolidado": True,
            }

        # ❌ CÓDIGO EXISTENTE - NO MODIFICAR
        # ... código actual del endpoint ...

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


from fastapi import HTTPException, Depends

# ============================================================================
# MODELOS PYDANTIC
# ============================================================================


class ProductoReferenciaCreate(BaseModel):
    """Modelo para crear producto de referencia"""

    codigo_ean: str
    nombre: str
    marca: Optional[str] = None
    categoria: Optional[str] = None
    presentacion: Optional[str] = None
    unidad_medida: Optional[str] = "unidades"


class ProductoReferenciaUpdate(BaseModel):
    """Modelo para actualizar producto de referencia"""

    nombre: Optional[str] = None
    marca: Optional[str] = None
    categoria: Optional[str] = None
    presentacion: Optional[str] = None


# ============================================================================
# ENDPOINT 1: BUSCAR PRODUCTO POR EAN
# ============================================================================


@app.get("/api/productos-referencia/{codigo_ean}")
async def buscar_producto_referencia(
    codigo_ean: str, current_user: dict = Depends(get_current_user)
):
    """
    Busca un producto en productos_referencia por su código EAN

    Returns:
        - 200: Producto encontrado
        - 404: Producto no encontrado
        - 401: No autenticado
    """

    print(f"🔍 Buscando producto EAN: {codigo_ean}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre,
                marca,
                categoria,
                presentacion,
                unidad_medida,
                created_at,
                updated_at
            FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        producto = cursor.fetchone()

        if not producto:
            print(f"   ❌ Producto {codigo_ean} NO encontrado")
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        print(f"   ✅ Producto {codigo_ean} encontrado: {producto[2]}")

        return {
            "id": producto[0],
            "codigo_ean": producto[1],
            "nombre": producto[2],
            "marca": producto[3],
            "categoria": producto[4],
            "presentacion": producto[5],
            "unidad_medida": producto[6],
            "created_at": producto[7].isoformat() if producto[7] else None,
            "updated_at": producto[8].isoformat() if producto[8] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error buscando producto: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT 2: CREAR PRODUCTO NUEVO
# ============================================================================


@app.post("/api/productos-referencia")
async def crear_producto_referencia(
    producto: ProductoReferenciaCreate, current_user: dict = Depends(get_current_user)
):
    """
    Crea un nuevo producto en productos_referencia

    Returns:
        - 200: Producto creado exitosamente
        - 409: Producto ya existe (código EAN duplicado)
        - 401: No autenticado
    """

    print(f"💾 Creando producto: {producto.nombre} (EAN: {producto.codigo_ean})")

    # Validaciones
    if len(producto.codigo_ean) < 8:
        raise HTTPException(
            status_code=400, detail="Código EAN inválido (mínimo 8 dígitos)"
        )

    if len(producto.nombre.strip()) < 2:
        raise HTTPException(
            status_code=400, detail="Nombre muy corto (mínimo 2 caracteres)"
        )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    cursor = conn.cursor()

    try:
        # Verificar si ya existe
        cursor.execute(
            """
            SELECT id, nombre
            FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (producto.codigo_ean,),
        )

        existente = cursor.fetchone()

        if existente:
            print(f"   ⚠️ Producto {producto.codigo_ean} YA existe: {existente[1]}")
            raise HTTPException(
                status_code=409, detail=f"Producto ya existe: {existente[1]}"
            )

        # Crear producto
        cursor.execute(
            """
            INSERT INTO productos_referencia (
                codigo_ean,
                nombre,
                marca,
                categoria,
                presentacion,
                unidad_medida,
                created_at,
                updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            RETURNING id, codigo_ean, nombre, marca, categoria,
                      presentacion, unidad_medida, created_at
        """,
            (
                producto.codigo_ean,
                producto.nombre.strip().upper(),
                producto.marca.strip() if producto.marca else None,
                producto.categoria.strip() if producto.categoria else None,
                producto.presentacion.strip() if producto.presentacion else None,
                producto.unidad_medida,
            ),
        )

        nuevo_producto = cursor.fetchone()
        conn.commit()

        print(f"   ✅ Producto creado ID: {nuevo_producto[0]}")

        # Registrar en auditoría
        try:
            from database import registrar_auditoria

            registrar_auditoria(
                usuario_id=current_user["id"],
                accion="crear",
                datos_nuevos={
                    "codigo_ean": producto.codigo_ean,
                    "nombre": producto.nombre,
                    "marca": producto.marca,
                    "categoria": producto.categoria,
                },
                razon="Producto creado desde app de escaneo",
            )
        except:
            pass  # No fallar si auditoría falla

        return {
            "id": nuevo_producto[0],
            "codigo_ean": nuevo_producto[1],
            "nombre": nuevo_producto[2],
            "marca": nuevo_producto[3],
            "categoria": nuevo_producto[4],
            "presentacion": nuevo_producto[5],
            "unidad_medida": nuevo_producto[6],
            "created_at": nuevo_producto[7].isoformat() if nuevo_producto[7] else None,
            "message": "Producto creado exitosamente",
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Error creando producto: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT 3: ACTUALIZAR PRODUCTO
# ============================================================================


@app.put("/api/productos-referencia/{codigo_ean}")
async def actualizar_producto_referencia(
    codigo_ean: str,
    producto: ProductoReferenciaUpdate,
    current_user: dict = Depends(get_current_user),
):
    """
    Actualiza un producto existente en productos_referencia

    Returns:
        - 200: Producto actualizado
        - 404: Producto no encontrado
        - 401: No autenticado
    """

    print(f"✏️ Actualizando producto EAN: {codigo_ean}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    cursor = conn.cursor()

    try:
        # Verificar que existe
        cursor.execute(
            """
            SELECT id, nombre, marca, categoria, presentacion
            FROM productos_referencia
            WHERE codigo_ean = %s
        """,
            (codigo_ean,),
        )

        existente = cursor.fetchone()

        if not existente:
            print(f"   ❌ Producto {codigo_ean} NO existe")
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Preparar datos anteriores para auditoría
        datos_anteriores = {
            "nombre": existente[1],
            "marca": existente[2],
            "categoria": existente[3],
            "presentacion": existente[4],
        }

        # Construir UPDATE dinámico (solo campos proporcionados)
        campos = []
        valores = []

        if producto.nombre is not None:
            campos.append("nombre = %s")
            valores.append(producto.nombre.strip().upper())

        if producto.marca is not None:
            campos.append("marca = %s")
            valores.append(producto.marca.strip() if producto.marca else None)

        if producto.categoria is not None:
            campos.append("categoria = %s")
            valores.append(producto.categoria.strip() if producto.categoria else None)

        if producto.presentacion is not None:
            campos.append("presentacion = %s")
            valores.append(
                producto.presentacion.strip() if producto.presentacion else None
            )

        if not campos:
            raise HTTPException(status_code=400, detail="No hay datos para actualizar")

        # Agregar updated_at
        campos.append("updated_at = CURRENT_TIMESTAMP")
        valores.append(codigo_ean)

        # Ejecutar UPDATE
        query = f"""
            UPDATE productos_referencia
            SET {', '.join(campos)}
            WHERE codigo_ean = %s
            RETURNING id, codigo_ean, nombre, marca, categoria,
                      presentacion, unidad_medida, updated_at
        """

        cursor.execute(query, valores)
        actualizado = cursor.fetchone()
        conn.commit()

        print(f"   ✅ Producto {codigo_ean} actualizado")

        # Registrar en auditoría
        try:
            from database import registrar_auditoria

            datos_nuevos = {
                "nombre": actualizado[2],
                "marca": actualizado[3],
                "categoria": actualizado[4],
                "presentacion": actualizado[5],
            }

            registrar_auditoria(
                usuario_id=current_user["id"],
                accion="actualizar",
                datos_anteriores=datos_anteriores,
                datos_nuevos=datos_nuevos,
                razon="Producto actualizado desde app de escaneo",
            )
        except:
            pass

        return {
            "id": actualizado[0],
            "codigo_ean": actualizado[1],
            "nombre": actualizado[2],
            "marca": actualizado[3],
            "categoria": actualizado[4],
            "presentacion": actualizado[5],
            "unidad_medida": actualizado[6],
            "updated_at": actualizado[7].isoformat() if actualizado[7] else None,
            "message": "Producto actualizado exitosamente",
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ Error actualizando producto: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# ENDPOINT BONUS: LISTAR PRODUCTOS (para debugging)
# ============================================================================


@app.get("/api/productos-referencia")
async def listar_productos_referencia(
    limite: int = 20, current_user: dict = Depends(get_current_user)
):
    """Lista los últimos productos de referencia creados"""

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Error de conexión a BD")

    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                id, codigo_ean, nombre, marca, categoria,
                presentacion, created_at
            FROM productos_referencia
            ORDER BY created_at DESC
            LIMIT %s
        """,
            (limite,),
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "categoria": row[4],
                    "presentacion": row[5],
                    "created_at": row[6].isoformat() if row[6] else None,
                }
            )

        return {"total": len(productos), "productos": productos}

    except Exception as e:
        print(f"❌ Error listando productos: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ============================================================================
# INSTRUCCIONES DE INSTALACIÓN
# ============================================================================

print("=" * 80)
print("✅ Endpoints productos_referencia cargados:")
print("   GET    /api/productos-referencia/{ean}  - Buscar producto")
print("   POST   /api/productos-referencia        - Crear producto")
print("   PUT    /api/productos-referencia/{ean}  - Actualizar producto")
print("   GET    /api/productos-referencia        - Listar productos")
print("=" * 80)


print("✅ Endpoint /admin/fix-categorias-final agregado")


@app.get("/admin/migrar-aprendizaje")
async def migrar_aprendizaje():
    """
    Ejecutar migración del constraint de aprendizaje (VERSIÓN CORREGIDA)
    Visitar: https://tu-app.railway.app/admin/migrar-aprendizaje
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {"success": False, "pasos": []}

        # Verificar tabla
        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'correcciones_aprendidas'
            )
        """
        )

        if not cursor.fetchone()[0]:
            resultado["error"] = "Tabla correcciones_aprendidas no existe"
            cursor.close()
            conn.close()
            return resultado

        resultado["pasos"].append("✅ Tabla existe")

        # ELIMINAR constraint viejo si existe
        cursor.execute(
            """
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'correcciones_aprendidas'
              AND constraint_type = 'UNIQUE'
        """
        )

        constraints = cursor.fetchall()
        for (constraint_name,) in constraints:
            cursor.execute(
                f"ALTER TABLE correcciones_aprendidas DROP CONSTRAINT IF EXISTS {constraint_name}"
            )
            resultado["pasos"].append(
                f"🗑️  Eliminado constraint viejo: {constraint_name}"
            )

        # Agregar columna computed si no existe
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'correcciones_aprendidas'
              AND column_name = 'establecimiento_key'
        """
        )

        if not cursor.fetchone():
            cursor.execute(
                """
                ALTER TABLE correcciones_aprendidas
                ADD COLUMN establecimiento_key VARCHAR(100)
                GENERATED ALWAYS AS (COALESCE(establecimiento, '')) STORED
            """
            )
            resultado["pasos"].append("➕ Agregada columna establecimiento_key")
        else:
            resultado["pasos"].append("✅ Columna establecimiento_key ya existe")

        # Limpiar duplicados
        cursor.execute(
            """
            DELETE FROM correcciones_aprendidas a
            USING correcciones_aprendidas b
            WHERE a.id < b.id
              AND a.ocr_normalizado = b.ocr_normalizado
              AND COALESCE(a.establecimiento, '') = COALESCE(b.establecimiento, '')
        """
        )

        duplicados = cursor.rowcount
        resultado["pasos"].append(f"🗑️  {duplicados} duplicados eliminados")

        # Crear constraint correcto
        cursor.execute(
            """
            ALTER TABLE correcciones_aprendidas
            ADD CONSTRAINT unique_correccion
            UNIQUE (ocr_normalizado, establecimiento_key)
        """
        )

        conn.commit()
        resultado["pasos"].append("✅ Constraint creado correctamente")

        # Verificar
        cursor.execute(
            """
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'correcciones_aprendidas'
              AND constraint_name = 'unique_correccion'
        """
        )

        if cursor.fetchone():
            resultado["success"] = True
            resultado["mensaje"] = "✅ Migración completada correctamente"
            resultado["pasos"].append("✅ Constraint verificado")
        else:
            resultado["error"] = "Constraint no se creó"

        cursor.close()
        conn.close()

        return resultado

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/admin/verificar-aprendizaje")
async def verificar_aprendizaje():
    """Verificar estado del sistema de aprendizaje"""
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar constraint
        cursor.execute(
            """
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints
            WHERE table_name = 'correcciones_aprendidas'
              AND constraint_name = 'unique_correccion'
        """
        )

        constraint = cursor.fetchone()

        # Contar registros
        cursor.execute(
            "SELECT COUNT(*) FROM correcciones_aprendidas WHERE activo = TRUE"
        )
        total = cursor.fetchone()[0]

        # Ver últimos 5
        cursor.execute(
            """
            SELECT ocr_normalizado, nombre_validado, establecimiento, confianza, veces_confirmado
            FROM correcciones_aprendidas
            WHERE activo = TRUE
            ORDER BY fecha_ultima_confirmacion DESC
            LIMIT 5
        """
        )

        ultimos = []
        for row in cursor.fetchall():
            ultimos.append(
                {
                    "ocr": row[0],
                    "validado": row[1],
                    "establecimiento": row[2],
                    "confianza": float(row[3]) if row[3] else 0,
                    "veces": row[4],
                }
            )

        cursor.close()
        conn.close()

        return {
            "success": True,
            "constraint_existe": constraint is not None,
            "productos_aprendidos": total,
            "sistema_listo": constraint is not None,
            "ultimos_5": ultimos,
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.post("/admin/corregir-aprendizaje/{aprendizaje_id}")
async def corregir_aprendizaje(aprendizaje_id: int, nombre_correcto: str):
    """
    Corregir un producto mal aprendido

    Ejemplo:
    POST /admin/corregir-aprendizaje/3
    Body: { "nombre_correcto": "ZANAHORIA GRANEL" }
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Actualizar el nombre validado
        cursor.execute(
            """
            UPDATE correcciones_aprendidas
            SET nombre_validado = %s,
                fecha_ultima_confirmacion = CURRENT_TIMESTAMP
            WHERE id = %s
            RETURNING ocr_normalizado, establecimiento
        """,
            (nombre_correcto.upper().strip(), aprendizaje_id),
        )

        resultado = cursor.fetchone()

        if not resultado:
            return {
                "success": False,
                "error": f"No se encontró aprendizaje con ID {aprendizaje_id}",
            }

        conn.commit()

        ocr_norm, establecimiento = resultado

        # Actualizar productos_maestros que usen este nombre
        cursor.execute(
            """
            UPDATE productos_maestros pm
            SET nombre_normalizado = %s
            FROM items_factura if_
            WHERE pm.id = if_.producto_maestro_id
            AND if_.nombre_leido = %s
        """,
            (nombre_correcto.upper().strip(), ocr_norm),
        )

        productos_actualizados = cursor.rowcount
        conn.commit()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": f"Aprendizaje corregido: '{ocr_norm}' → '{nombre_correcto}'",
            "productos_actualizados": productos_actualizados,
            "establecimiento": establecimiento,
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


# ============================================================
# ADMIN: PANEL DE ANOMALÍAS Y CORRECCIÓN
# ============================================================


@app.get("/api/admin/anomalias")
async def obtener_anomalias():
    """
    Obtiene todos los productos con problemas de datos
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre_normalizado,
                marca,
                categoria,
                precio_promedio_global,
                total_reportes,
                LENGTH(nombre_normalizado) as chars_nombre,
                CASE
                    WHEN LENGTH(nombre_normalizado) < 8 THEN 'nombre_corto'
                    WHEN nombre_normalizado ~ '^[A-Z0-9 ]+$' AND LENGTH(nombre_normalizado) < 15 THEN 'nombre_truncado'
                    WHEN marca IS NULL OR marca = '' THEN 'sin_marca'
                    WHEN categoria IS NULL OR categoria = '' THEN 'sin_categoria'
                    WHEN codigo_ean IS NULL OR codigo_ean = '' THEN 'sin_ean'
                    WHEN LENGTH(codigo_ean) NOT IN (8, 12, 13, 14) THEN 'ean_invalido'
                    WHEN precio_promedio_global = 0 THEN 'sin_precio'
                    ELSE 'ok'
                END as tipo_problema
            FROM productos_maestros
            ORDER BY
                CASE
                    WHEN LENGTH(nombre_normalizado) < 8 THEN 1
                    WHEN marca IS NULL THEN 2
                    WHEN categoria IS NULL THEN 3
                    ELSE 4
                END,
                id DESC
        """
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "categoria": row[4],
                    "precio_promedio": row[5],
                    "veces_comprado": row[6],
                    "chars_nombre": row[7],
                    "tipo_problema": row[8],
                }
            )

        # Estadísticas
        total = len(productos)
        problematicos = len([p for p in productos if p["tipo_problema"] != "ok"])
        sin_marca = len([p for p in productos if p["tipo_problema"] == "sin_marca"])
        sin_categoria = len(
            [p for p in productos if p["tipo_problema"] == "sin_categoria"]
        )
        nombres_cortos = len(
            [
                p
                for p in productos
                if p["tipo_problema"] in ["nombre_corto", "nombre_truncado"]
            ]
        )
        sin_ean = len([p for p in productos if p["tipo_problema"] == "sin_ean"])

        return {
            "success": True,
            "productos": productos,
            "estadisticas": {
                "total": total,
                "problematicos": problematicos,
                "completos": total - problematicos,
                "sin_marca": sin_marca,
                "sin_categoria": sin_categoria,
                "nombres_cortos": nombres_cortos,
                "sin_ean": sin_ean,
                "porcentaje_calidad": (
                    round((total - problematicos) / total * 100, 1) if total > 0 else 0
                ),
            },
        }

    except Exception as e:
        print(f"❌ Error obteniendo anomalías: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


@app.put("/api/admin/producto/{producto_id}/corregir")
async def corregir_producto(producto_id: int, datos: dict):
    """
    Corrige un producto específico
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Construir query dinámico
        campos = []
        valores = []

        if "nombre_normalizado" in datos:
            campos.append("nombre_normalizado = %s")
            valores.append(datos["nombre_normalizado"])

        if "marca" in datos:
            campos.append("marca = %s")
            valores.append(datos["marca"])

        if "categoria" in datos:
            campos.append("categoria = %s")
            valores.append(datos["categoria"])

        if "codigo_ean" in datos:
            campos.append("codigo_ean = %s")
            valores.append(datos["codigo_ean"])

        if "verificado" in datos:
            campos.append("verificado = %s")
            valores.append(datos["verificado"])

        if not campos:
            return {"success": False, "error": "No hay campos para actualizar"}

        campos.append("ultima_actualizacion = CURRENT_TIMESTAMP")
        valores.append(producto_id)

        query = f"""
            UPDATE productos_maestros
            SET {', '.join(campos)}
            WHERE id = %s
        """

        cursor.execute(query, valores)
        conn.commit()

        print(f"✅ Producto {producto_id} corregido")

        return {
            "success": True,
            "mensaje": f"Producto {producto_id} actualizado correctamente",
        }

    except Exception as e:
        conn.rollback()
        print(f"❌ Error corrigiendo producto: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


@app.post("/api/admin/correccion-masiva")
async def correccion_masiva(datos: dict):
    """
    Aplica correcciones a múltiples productos
    Ejemplo: {"ids": [1,2,3], "marca": "ALPINA", "categoria": "Lácteos"}
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        ids = datos.get("ids", [])
        if not ids:
            return {"success": False, "error": "No se especificaron IDs"}

        campos = []
        valores = []

        if "marca" in datos and datos["marca"]:
            campos.append("marca = %s")
            valores.append(datos["marca"])

        if "categoria" in datos and datos["categoria"]:
            campos.append("categoria = %s")
            valores.append(datos["categoria"])

        if "verificado" in datos:
            campos.append("verificado = %s")
            valores.append(datos["verificado"])

        if not campos:
            return {"success": False, "error": "No hay campos para actualizar"}

        campos.append("ultima_actualizacion = CURRENT_TIMESTAMP")

        # Crear placeholders para los IDs
        id_placeholders = ",".join(["%s"] * len(ids))

        query = f"""
            UPDATE productos_maestros
            SET {', '.join(campos)}
            WHERE id IN ({id_placeholders})
        """

        cursor.execute(query, valores + ids)
        affected = cursor.rowcount
        conn.commit()

        print(f"✅ {affected} productos actualizados masivamente")

        return {"success": True, "productos_actualizados": affected}

    except Exception as e:
        conn.rollback()
        print(f"❌ Error en corrección masiva: {e}")
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


@app.get("/api/admin/sugerencias-marca")
async def sugerencias_marca():
    """
    Obtiene las marcas más comunes para autocompletar
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT marca, COUNT(*) as cantidad
            FROM productos_maestros
            WHERE marca IS NOT NULL AND marca != ''
            GROUP BY marca
            ORDER BY cantidad DESC
            LIMIT 50
        """
        )

        marcas = [{"marca": row[0], "cantidad": row[1]} for row in cursor.fetchall()]

        return {"success": True, "marcas": marcas}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


@app.get("/api/admin/sugerencias-categoria")
async def sugerencias_categoria():
    """
    Obtiene las categorías más comunes
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT categoria, COUNT(*) as cantidad
            FROM productos_maestros
            WHERE categoria IS NOT NULL AND categoria != ''
            GROUP BY categoria
            ORDER BY cantidad DESC
            LIMIT 50
        """
        )

        categorias = [
            {"categoria": row[0], "cantidad": row[1]} for row in cursor.fetchall()
        ]

        return {"success": True, "categorias": categorias}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()
        conn.close()


print("✅ Endpoints de administración registrados")


@app.get("/admin/listar-aprendizaje")
async def listar_aprendizaje(
    limite: int = 20, orden: str = "fecha"  # "fecha" o "confianza" o "veces"
):
    """
    Listar productos aprendidos con opción de filtrado
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Determinar orden
        if orden == "confianza":
            order_by = "confianza DESC"
        elif orden == "veces":
            order_by = "veces_confirmado DESC"
        else:
            order_by = "fecha_ultima_confirmacion DESC"

        cursor.execute(
            f"""
            SELECT
                id,
                ocr_original,
                ocr_normalizado,
                nombre_validado,
                establecimiento,
                codigo_ean,
                confianza,
                veces_confirmado,
                veces_rechazado,
                fecha_primera_vez,
                fecha_ultima_confirmacion,
                activo
            FROM correcciones_aprendidas
            WHERE activo = TRUE
            ORDER BY {order_by}
            LIMIT %s
        """,
            (limite,),
        )

        productos = []
        for row in cursor.fetchall():
            productos.append(
                {
                    "id": row[0],
                    "ocr_original": row[1],
                    "ocr_normalizado": row[2],
                    "nombre_validado": row[3],
                    "establecimiento": row[4],
                    "codigo_ean": row[5],
                    "confianza": float(row[6]) if row[6] else 0,
                    "veces_confirmado": row[7],
                    "veces_rechazado": row[8],
                    "fecha_primera_vez": row[9].isoformat() if row[9] else None,
                    "fecha_ultima_confirmacion": (
                        row[10].isoformat() if row[10] else None
                    ),
                    "activo": row[11],
                }
            )

        cursor.close()
        conn.close()

        return {"success": True, "total": len(productos), "productos": productos}

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.delete("/admin/eliminar-aprendizaje/{aprendizaje_id}")
async def eliminar_aprendizaje(aprendizaje_id: int):
    """
    Desactivar (soft delete) un aprendizaje incorrecto
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE correcciones_aprendidas
            SET activo = FALSE
            WHERE id = %s
            RETURNING ocr_normalizado, nombre_validado
        """,
            (aprendizaje_id,),
        )

        resultado = cursor.fetchone()

        if not resultado:
            return {
                "success": False,
                "error": f"No se encontró aprendizaje con ID {aprendizaje_id}",
            }

        conn.commit()
        cursor.close()
        conn.close()

        ocr_norm, nombre_val = resultado

        return {
            "success": True,
            "mensaje": f"Aprendizaje desactivado: '{ocr_norm}' → '{nombre_val}'",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/admin/reparar-secuencia")
async def reparar_secuencia():
    """
    Repara la secuencia de IDs de productos_maestros
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener el ID máximo actual
        cursor.execute("SELECT MAX(id) FROM productos_maestros")
        max_id = cursor.fetchone()[0] or 0

        # Reiniciar la secuencia
        cursor.execute(
            f"""
            SELECT setval('productos_maestros_id_seq', {max_id}, true)
        """
        )

        # Verificar
        cursor.execute("SELECT last_value FROM productos_maestros_id_seq")
        nuevo_valor = cursor.fetchone()[0]

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": "Secuencia reparada",
            "max_id_actual": max_id,
            "siguiente_id": nuevo_valor + 1,
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.get("/admin/encontrar-duplicados")
async def encontrar_duplicados(limite: int = 50):
    """
    Encuentra productos duplicados por similitud de nombre
    """
    try:
        from database import get_db_connection
        from product_matcher import calcular_similitud

        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener todos los productos
        cursor.execute(
            """
            SELECT id, nombre_normalizado, codigo_ean, precio_promedio_global
            FROM productos_maestros
            ORDER BY id DESC
            LIMIT %s
        """,
            (limite * 2,),
        )

        productos = cursor.fetchall()
        duplicados = []

        # Comparar cada producto con los demás
        for i in range(len(productos)):
            for j in range(i + 1, len(productos)):
                id1, nombre1, ean1, precio1 = productos[i]
                id2, nombre2, ean2, precio2 = productos[j]

                # Mismo EAN = duplicado
                if ean1 and ean2 and ean1 == ean2:
                    duplicados.append(
                        {
                            "id1": id1,
                            "nombre1": nombre1,
                            "id2": id2,
                            "nombre2": nombre2,
                            "similitud": 1.0,
                            "razon": "Mismo EAN",
                        }
                    )
                    continue

                # Similitud de nombre
                sim = calcular_similitud(nombre1, nombre2)

                if sim >= 0.90:
                    duplicados.append(
                        {
                            "id1": id1,
                            "nombre1": nombre1,
                            "id2": id2,
                            "nombre2": nombre2,
                            "similitud": round(sim, 2),
                            "razon": "Nombre similar",
                        }
                    )

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total_duplicados": len(duplicados),
            "duplicados": duplicados[:limite],
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


@app.post("/admin/fusionar-productos")
async def fusionar_productos(
    producto_principal_id: int, productos_duplicados: list[int]
):
    """
    Fusiona productos duplicados en uno solo

    Ejemplo:
    POST /admin/fusionar-productos
    Body: {
        "producto_principal_id": 150,
        "productos_duplicados": [228, 230, 231]
    }
    """
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        fusionados = 0

        for dup_id in productos_duplicados:
            # Actualizar items_factura
            cursor.execute(
                """
                UPDATE items_factura
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """,
                (producto_principal_id, dup_id),
            )

            items_actualizados = cursor.rowcount

            # Actualizar inventario_usuario
            cursor.execute(
                """
                UPDATE inventario_usuario
                SET producto_maestro_id = %s
                WHERE producto_maestro_id = %s
            """,
                (producto_principal_id, dup_id),
            )

            # Eliminar producto duplicado
            cursor.execute(
                """
                DELETE FROM productos_maestros
                WHERE id = %s
            """,
                (dup_id,),
            )

            fusionados += 1

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "mensaje": f"{fusionados} productos fusionados en ID {producto_principal_id}",
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


# ==========================================
# ENDPOINTS DE DEPURACIÓN - DEBEN ESTAR ANTES DEL MAIN
# ==========================================


@app.post("/admin/reprocesar-todas-facturas")
async def reprocesar_todas_facturas(limite: int = 1000):
    """
    Reprocesa TODAS las facturas existentes para actualizar tablas analíticas
    ⚠️ Proceso pesado - usar con cuidado
    """
    try:
        from database import get_db_connection
        from analytics_updater import actualizar_todas_las_tablas_analiticas

        conn = get_db_connection()
        cursor = conn.cursor()

        print("\n" + "=" * 80)
        print("🔄 REPROCESAMIENTO MASIVO DE FACTURAS")
        print("=" * 80)

        # Obtener todas las facturas
        cursor.execute(
            """
            SELECT f.id, f.usuario_id, f.establecimiento_id
            FROM facturas f
            ORDER BY f.id DESC
            LIMIT %s
        """,
            (limite,),
        )

        facturas = cursor.fetchall()
        total_facturas = len(facturas)

        print(f"📊 Total de facturas a procesar: {total_facturas}")

        procesadas = 0
        errores = 0

        for factura_id, usuario_id, establecimiento_id in facturas:
            try:
                print(f"\n🔄 Procesando factura {factura_id}...")

                # Obtener items de la factura
                cursor.execute(
                    """
                    SELECT producto_maestro_id, codigo_leido
                    FROM items_factura
                    WHERE factura_id = %s AND producto_maestro_id IS NOT NULL
                """,
                    (factura_id,),
                )

                items_data = []
                for row in cursor.fetchall():
                    items_data.append(
                        {"producto_maestro_id": row[0], "codigo_leido": row[1] or ""}
                    )

                if not items_data:
                    print(f"   ⚠️ Factura {factura_id} sin items válidos")
                    continue

                productos_ids = [item["producto_maestro_id"] for item in items_data]
                # Actualizar tablas analíticas
                resultado = actualizar_todas_las_tablas_analiticas(
                    cursor=cursor,
                    conn=conn,
                    factura_id=factura_id,
                    usuario_id=usuario_id,
                    establecimiento_id=establecimiento_id,
                    productos_ids=productos_ids,
                )

                procesadas += 1

                if procesadas % 10 == 0:
                    print(f"\n📊 Progreso: {procesadas}/{total_facturas} facturas")
                    conn.commit()  # Commit cada 10 facturas

            except Exception as e:
                errores += 1
                print(f"❌ Error en factura {factura_id}: {e}")
                continue

        # Commit final
        conn.commit()
        cursor.close()
        conn.close()

        print("\n" + "=" * 80)
        print("✅ REPROCESAMIENTO COMPLETADO")
        print(f"   Total: {total_facturas}")
        print(f"   Procesadas: {procesadas}")
        print(f"   Errores: {errores}")
        print("=" * 80)

        return {
            "success": True,
            "total_facturas": total_facturas,
            "procesadas": procesadas,
            "errores": errores,
            "porcentaje": (
                round((procesadas / total_facturas * 100), 2)
                if total_facturas > 0
                else 0
            ),
        }

    except Exception as e:
        print(f"❌ Error en reprocesamiento masivo: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": str(e)}


@app.get("/admin/diagnostico-general")
async def diagnostico_general():
    """Estado general del sistema"""
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        stats = {}

        # Total facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        stats["total_facturas"] = cursor.fetchone()[0]

        # Items con producto_maestro_id
        cursor.execute(
            """
            SELECT
                COUNT(*) as total,
                COUNT(producto_maestro_id) as con_producto
            FROM items_factura
        """
        )
        row = cursor.fetchone()
        stats["total_items"] = row[0]
        stats["items_con_producto"] = row[1]
        stats["items_sin_producto"] = row[0] - row[1]

        # Productos maestros
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        stats["total_productos_maestros"] = cursor.fetchone()[0]

        # Tablas analíticas
        cursor.execute("SELECT COUNT(*) FROM historial_compras_usuario")
        stats["registros_historial"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM patrones_compra")
        stats["registros_patrones"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM productos_por_establecimiento")
        stats["registros_productos_est"] = cursor.fetchone()[0]

        # Productos con códigos actualizados
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM productos_maestros
            WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
        """
        )
        stats["productos_con_codigo"] = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {"success": True, "timestamp": datetime.now().isoformat(), **stats}

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoint /admin/diagnostico-general registrado")


@app.get("/admin/verificar-analytics")
async def verificar_analytics():
    """Verificar estado de tablas analíticas"""
    try:
        from database import get_db_connection

        conn = get_db_connection()
        cursor = conn.cursor()

        resultado = {}

        # Historial de compras por usuario
        cursor.execute(
            """
            SELECT usuario_id, COUNT(*) as compras
            FROM historial_compras_usuario
            GROUP BY usuario_id
            ORDER BY compras DESC
            LIMIT 10
        """
        )
        resultado["top_usuarios"] = [
            {"usuario_id": row[0], "compras": row[1]} for row in cursor.fetchall()
        ]

        # ✅ CORRECCIÓN: Usar columnas correctas
        # Patrones de compra más frecuentes
        cursor.execute(
            """
            SELECT
                pm.nombre_consolidado,
                pc.veces_comprado,
                pc.ultima_compra,
                pc.frecuencia_dias
            FROM patrones_compra pc
            JOIN productos_maestros pm ON pc.producto_maestro_id = pm.id
            ORDER BY pc.veces_comprado DESC
            LIMIT 10
        """
        )
        resultado["productos_frecuentes"] = [
            {
                "producto": row[0],
                "veces_comprado": row[1],
                "ultima_compra": str(row[2]) if row[2] else None,
                "frecuencia_dias": row[3],
            }
            for row in cursor.fetchall()
        ]

        # Productos por establecimiento
        cursor.execute(
            """
            SELECT
                e.nombre_normalizado,
                COUNT(DISTINCT ppe.producto_maestro_id) as productos_unicos
            FROM productos_por_establecimiento ppe
            JOIN establecimientos e ON ppe.establecimiento_id = e.id
            GROUP BY e.nombre_normalizado
            ORDER BY productos_unicos DESC
        """
        )
        resultado["productos_por_tienda"] = [
            {"establecimiento": row[0], "productos": row[1]}
            for row in cursor.fetchall()
        ]

        cursor.close()
        conn.close()

        return {"success": True, **resultado}

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoints de depuración agregados")
print("✅ Endpoint /admin/verificar-analytics registrado")


@app.post("/admin/limpiar-productos-huerfanos")
async def limpiar_productos_huerfanos():
    """
    Elimina productos que NO están vinculados a ninguna factura
    ⚠️ PELIGROSO - Solo usar en desarrollo
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Eliminar productos NO usados en items_factura
        cursor.execute(
            """
            DELETE FROM productos_maestros
            WHERE id NOT IN (
                SELECT DISTINCT producto_maestro_id
                FROM items_factura
                WHERE producto_maestro_id IS NOT NULL
            )
        """
        )

        productos_eliminados = cursor.rowcount
        conn.commit()

        # Contar restantes
        cursor.execute("SELECT COUNT(*) FROM productos_maestros")
        productos_restantes = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        return {
            "success": True,
            "productos_eliminados": productos_eliminados,
            "productos_restantes": productos_restantes,
            "mensaje": f"✅ Limpieza completada: {productos_eliminados} productos huérfanos eliminados",
        }

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoint /admin/limpiar-productos-huerfanos registrado")


@app.get("/admin/diagnostico-items-sin-producto")
async def diagnostico_items_sin_producto():
    """
    Ver cuáles items NO tienen producto_maestro_id
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Items sin producto
        cursor.execute(
            """
            SELECT
                i.id,
                i.factura_id,
                i.codigo_leido,
                i.nombre_leido,
                i.precio_pagado,
                f.establecimiento
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            WHERE i.producto_maestro_id IS NULL
            ORDER BY i.factura_id, i.id
            LIMIT 50
        """
        )

        items_sin_producto = []
        for row in cursor.fetchall():
            items_sin_producto.append(
                {
                    "item_id": row[0],
                    "factura_id": row[1],
                    "codigo": row[2] or "SIN CODIGO",
                    "nombre": row[3],
                    "precio": float(row[4]) if row[4] else 0,
                    "establecimiento": row[5],
                }
            )

        # Contar por factura
        cursor.execute(
            """
            SELECT
                factura_id,
                COUNT(*) as items_sin_producto
            FROM items_factura
            WHERE producto_maestro_id IS NULL
            GROUP BY factura_id
            ORDER BY factura_id
        """
        )

        por_factura = []
        for row in cursor.fetchall():
            por_factura.append({"factura_id": row[0], "items_sin_producto": row[1]})

        cursor.close()
        conn.close()

        return {
            "success": True,
            "total_items_sin_producto": len(items_sin_producto),
            "items_sin_producto": items_sin_producto,
            "resumen_por_factura": por_factura,
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


print("✅ Endpoint /admin/diagnostico-items-sin-producto registrado")


@app.get("/admin/verificar-schema-patrones")
async def verificar_schema_patrones():
    """Ver estructura de tabla patrones_compra"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'patrones_compra'
            ORDER BY ordinal_position
        """
        )

        columnas = []
        for row in cursor.fetchall():
            columnas.append({"columna": row[0], "tipo": row[1]})

        cursor.close()
        conn.close()

        return {"success": True, "tabla": "patrones_compra", "columnas": columnas}

    except Exception as e:
        return {"success": False, "error": str(e)}


print("✅ Endpoint /admin/verificar-schema-patrones registrado")


@app.post("/admin/vincular-items-sin-codigo")
async def vincular_items_sin_codigo():
    """
    Crea productos maestros para items que NO tienen código EAN
    y los vincula automáticamente
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Obtener items sin producto_maestro_id
        cursor.execute(
            """
            SELECT
                i.id as item_id,
                i.nombre_leido,
                i.precio_pagado,
                i.factura_id,
                f.establecimiento,
                f.establecimiento_id
            FROM items_factura i
            JOIN facturas f ON i.factura_id = f.id
            WHERE i.producto_maestro_id IS NULL
            ORDER BY i.nombre_leido
        """
        )

        items_sin_producto = cursor.fetchall()

        print(f"\n🔍 Encontrados {len(items_sin_producto)} items sin producto maestro")

        vinculados = 0
        creados = 0
        errores = 0

        for item in items_sin_producto:
            item_id, nombre, precio, factura_id, establecimiento, establecimiento_id = (
                item
            )

            try:
                nombre_normalizado = nombre.strip().upper()

                # Buscar si ya existe un producto con este nombre
                cursor.execute(
                    """
                    SELECT id
                    FROM productos_maestros
                    WHERE nombre_normalizado = %s
                    LIMIT 1
                """,
                    (nombre_normalizado,),
                )

                producto_existente = cursor.fetchone()

                if producto_existente:
                    # Vincular al producto existente
                    producto_id = producto_existente[0]
                    print(
                        f"   ✓ Vinculando '{nombre}' a producto existente ID {producto_id}"
                    )
                else:
                    # Crear nuevo producto maestro
                    cursor.execute(
                        """
                        INSERT INTO productos_maestros (
                            nombre_normalizado,
                            precio_promedio_global,
                            total_reportes,
                            primera_vez_reportado,
                            ultima_actualizacion
                        ) VALUES (%s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        RETURNING id
                    """,
                        (nombre_normalizado, int(precio)),
                    )

                    producto_id = cursor.fetchone()[0]
                    creados += 1
                    print(f"   + Creado producto ID {producto_id}: '{nombre}'")

                # Vincular item al producto
                cursor.execute(
                    """
                    UPDATE items_factura
                    SET producto_maestro_id = %s
                    WHERE id = %s
                """,
                    (producto_id, item_id),
                )

                vinculados += 1

            except Exception as e:
                errores += 1
                print(f"   ❌ Error procesando item {item_id}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        return {
            "success": True,
            "items_procesados": len(items_sin_producto),
            "productos_creados": creados,
            "items_vinculados": vinculados,
            "errores": errores,
            "mensaje": f"✅ Vinculación completada: {creados} productos nuevos, {vinculados} items vinculados",
        }

    except Exception as e:
        import traceback

        return {"success": False, "error": str(e), "traceback": traceback.format_exc()}


print("✅ Endpoint /admin/vincular-items-sin-codigo registrado")


@app.get("/admin/test-reprocesar-una-factura/{factura_id}")
async def test_reprocesar_una_factura(factura_id: int):
    """
    Probar reprocesamiento de UNA factura con logs detallados
    """
    try:
        from database import get_db_connection
        from analytics_updater import actualizar_todas_las_tablas_analiticas

        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"\n{'='*60}")
        print(f"🔄 TEST: Reprocesando factura {factura_id}")
        print(f"{'='*60}")

        # Obtener datos de la factura
        cursor.execute(
            """
            SELECT f.id, f.usuario_id, f.establecimiento_id, f.establecimiento
            FROM facturas f
            WHERE f.id = %s
        """,
            (factura_id,),
        )

        factura = cursor.fetchone()

        if not factura:
            return {"success": False, "error": f"Factura {factura_id} no encontrada"}

        fid, usuario_id, establecimiento_id, establecimiento = factura

        print(f"   📋 Factura: {fid}")
        print(f"   👤 Usuario: {usuario_id}")
        print(f"   🏪 Establecimiento: {establecimiento} (ID: {establecimiento_id})")

        # ✅ CORRECCIÓN: Obtener solo los IDs de productos
        cursor.execute(
            """
            SELECT producto_maestro_id
            FROM items_factura
            WHERE factura_id = %s AND producto_maestro_id IS NOT NULL
        """,
            (factura_id,),
        )

        productos_ids = [row[0] for row in cursor.fetchall()]

        print(f"   📦 Productos IDs encontrados: {len(productos_ids)}")
        print(f"   📦 IDs: {productos_ids[:10]}...")  # Primeros 10

        if not productos_ids:
            return {
                "success": False,
                "error": f"Factura {factura_id} no tiene items con producto_maestro_id",
            }

        # ✅ CORRECCIÓN: Llamar con productos_ids
        print(f"   🔄 Actualizando tablas analíticas...")

        productos_ids = [item["producto_maestro_id"] for item in items_data]
        resultado = actualizar_todas_las_tablas_analiticas(
            cursor=cursor,
            conn=conn,
            factura_id=fid,
            usuario_id=usuario_id,
            establecimiento_id=establecimiento_id,
            productos_ids=productos_ids,  # ← Parámetro correcto
        )

        conn.commit()
        cursor.close()
        conn.close()

        print(f"   ✅ Actualización completada")
        print(f"   📊 Resultado: {resultado}")
        print(f"{'='*60}\n")

        return {
            "success": True,
            "factura_id": fid,
            "productos_procesados": len(productos_ids),
            "resultado": resultado,
        }

    except Exception as e:
        import traceback

        error_trace = traceback.format_exc()

        print(f"   ❌ ERROR: {e}")
        print(f"   Traceback:\n{error_trace}")

        return {"success": False, "error": str(e), "traceback": error_trace}


print("✅ Endpoint /admin/test-reprocesar-una-factura registrado (v3 - corregido)")


print("✅ Endpoint /admin/test-reprocesar-una-factura registrado")

if __name__ == "__main__":  # ← AGREGAR :
    print("\n" + "=" * 60)
    print("🚀 INICIANDO SERVIDOR LECFAC")

    test_database_connection()
    create_tables()

    print("=" * 60)
    print("✅ SERVIDOR LISTO")
    print("=" * 60)

    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)


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
    """Inventarios por usuario con filtros y paginación"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Obteniendo inventarios (página {pagina}, límite {limite})...")

        offset = (pagina - 1) * limite

        query = """
            SELECT
                iu.usuario_id,
                u.nombre,
                pm.nombre_consolidado,
                iu.cantidad_actual,
                pm.categoria,
                iu.fecha_ultima_actualizacion,
                iu.establecimiento_nombre
            FROM inventario_usuario iu
            LEFT JOIN usuarios u ON iu.usuario_id = u.id
            LEFT JOIN productos_maestros_v2 pm ON iu.producto_maestro_id = pm.id
            WHERE 1=1
        """

        params = []

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
                    "telefono": "",
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
                COALESCE(pm.nombre_comercial, pm.nombre_consolidado, 'Sin nombre') as nombre,
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
            precio_pesos = float(row[3]) if row[3] else 0

            productos.append(
                {
                    "id": row[0],
                    "nombre": row[1],
                    "codigo_ean": row[2],
                    "precio_promedio": precio_pesos,
                    "categoria": row[4],
                    "marca": row[5],
                    "veces_comprado": row[6] or 0,
                }
            )

        conn.close()

        print(f"✅ {len(productos)} productos maestros")
        return productos

    except Exception as e:
        print(f"❌ Error obteniendo productos: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


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
                    "ids": [row[0], row[1]],
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


@app.get("/api/admin/aprendizaje/stats")
async def get_aprendizaje_stats():
    """Estadísticas del sistema de aprendizaje"""
    try:
        from database import get_db_connection
        from aprendizaje_manager import AprendizajeManager

        conn = get_db_connection()
        cursor = conn.cursor()

        mgr = AprendizajeManager(cursor, conn)
        stats = mgr.obtener_estadisticas()

        # Calcular ahorro
        if stats.get("total_confirmaciones", 0) > 0:
            # Cada confirmación = $0.005 USD ahorrado
            ahorro_usd = (
                stats["total_confirmaciones"] - stats["total_activas"]
            ) * 0.005
            stats["ahorro_estimado_usd"] = round(ahorro_usd, 2)

        cursor.close()
        conn.close()

        return {"success": True, **stats}

    except Exception as e:
        return {"success": False, "error": str(e)}


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

        conn.close()
        return resultado

    except Exception as e:
        conn.close()
        return {"error": str(e), "traceback": traceback.format_exc()}


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
        traceback.print_exc()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Todos los endpoints administrativos y de debug cargados")


@app.get("/debug/fix-status")
async def check_fix_status():
    """Verificar si el fix está presente"""
    try:
        import product_matcher
        import inspect

        source_code = inspect.getsource(product_matcher.crear_producto_en_ambas_tablas)

        tiene_fix_1 = "fetchone() retornó None" in source_code
        tiene_fix_2 = "fallback manual" in source_code.lower()
        tiene_fix_3 = "if not resultado:" in source_code

        fix_completo = tiene_fix_1 or (tiene_fix_2 and tiene_fix_3)

        # Intentar leer archivo de status
        status_file = None
        try:
            with open("/tmp/fix_status.txt", "r") as f:
                status_file = f.read()
        except:
            pass

        return {
            "fix_presente": fix_completo,
            "verificaciones": {
                "string_fetchone": tiene_fix_1,
                "string_fallback": tiene_fix_2,
                "string_if_not": tiene_fix_3,
            },
            "status_file": status_file,
            "version_esperada": "V6.1",
            "mensaje": "✅ FIX PRESENTE" if fix_completo else "❌ FIX AUSENTE",
        }

    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}
