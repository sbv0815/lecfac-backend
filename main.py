import os
import base64
print("=" * 80)
print("🚀 LECFAC BACKEND - VERSION 2025-10-30-21:00 - REBUILD FORZADO")
print("=" * 80)
import tempfile
import traceback
import json
import uuid
# Al inicio de main.py, donde están los imports:
from claude_invoice import parse_invoice_with_claude as procesar_factura_con_claude
# LIMPIEZA DE CACHÉ AL INICIO
import shutil
print("🧹 Limpiando caché de Python...")
for root, dirs, files in os.walk('.'):
    if '__pycache__' in dirs:
        shutil.rmtree(os.path.join(root, '__pycache__'))
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

# Importar routers
from api_inventario import router as inventario_router
from audit_system import AuditSystem
from mobile_endpoints import router as mobile_router
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from claude_invoice import parse_invoice_with_claude
from product_matcher import buscar_o_crear_producto_inteligente
from comparacion_precios import router as comparacion_router

# ✅ ProductResolver removido - usando product_matcher
PRODUCT_RESOLVER_AVAILABLE = False
print("✅ product_matcher configurado")

from admin_dashboard import router as admin_dashboard_router
from auth import router as auth_router

def verify_jwt_token(token: str):
    """
    Verifica y decodifica un token JWT
    Retorna el payload si es válido, None si es inválido
    """
    try:
        import jwt
        from auth import SECRET_KEY  # Importar la clave secreta

        # Decodificar con verificación de signature
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])

        print(f"✅ Token válido - Usuario: {payload.get('user_id')}, Rol: {payload.get('rol')}")
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
from anomaly_monitor import guardar_reporte_anomalia, obtener_estadisticas_por_establecimiento, obtener_anomalias_pendientes
from productos_mejoras import router as productos_mejoras_router
from fastapi import FastAPI
from productos_establecimiento_endpoints import router as productos_est_router
from productos_api_v2 import router as productos_v2_router
from establecimientos_api import router as establecimientos_router
from consolidacion_productos import (
    procesar_item_con_consolidacion,
    mejorar_nombre_con_claude
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
                _, buffer = cv2.imencode('.jpg', frame)
                frame_base64 = base64.b64encode(buffer).decode('utf-8')
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
    if not fecha_str or fecha_str in ['No legible', 'Desconocido', 'N/A', '', 'null', 'None']:
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
    palabras_invalidas = ['legible', 'desconocido', 'no', 'n/a', 'ninguno', 'error']
    if any(palabra in fecha_str.lower() for palabra in palabras_invalidas):
        print(f"⚠️ Fecha inválida detectada: '{fecha_str}', usando fecha actual")
        return datetime.now().date()

    # Intentar diferentes formatos comunes
    formatos = [
        '%d/%m/%Y',    # 16/11/2016
        '%d-%m-%Y',    # 16-11-2016
        '%Y-%m-%d',    # 2016-11-16
        '%Y/%m/%d',    # 2016/11/16
        '%d/%m/%y',    # 16/11/16
        '%d-%m-%y',    # 16-11-16
        '%Y%m%d',      # 20161116
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
    authorization: str = Header(None)
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

        temp_video = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4')
        content = await video.read()
        temp_video.write(content)
        temp_video.close()

        video_size_mb = len(content) / (1024 * 1024)
        print(f"💾 Tamaño: {video_size_mb:.2f} MB")

        conn = get_db_connection()
        cursor = conn.cursor()

        job_id = str(uuid.uuid4())

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO processing_jobs (
                    id, usuario_id, status, created_at
                ) VALUES (%s, %s, 'pending', CURRENT_TIMESTAMP)
            """, (job_id, usuario_id))
        else:
            cursor.execute("""
                INSERT INTO processing_jobs (
                    id, usuario_id, status, created_at
                ) VALUES (?, ?, 'pending', ?)
            """, (job_id, usuario_id, datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ Job creado: {job_id}")

        background_tasks.add_task(
            process_video_background_task,
            job_id,
            temp_video.name,
            usuario_id
        )

        print(f"✅ Tarea en background agregada")
        print(f"{'='*60}\n")

        return JSONResponse(
            status_code=202,
            content={
                "success": True,
                "job_id": job_id,
                "message": "Video recibido, procesando en background",
                "status": "pending"
            }
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

#========================================

@app.get("/api/admin/usuarios/{usuario_id}/inventario")
async def get_inventario_usuario(usuario_id: int):
    """Obtener inventario de un usuario específico"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        print(f"📦 Obteniendo inventario del usuario {usuario_id}...")

        # Obtener productos del inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_normalizado,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    iu.establecimiento_nombre,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 100
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_normalizado,
                    pm.codigo_ean,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    iu.establecimiento_nombre,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 100
            """, (usuario_id,))

        inventario = []
        for row in cursor.fetchall():
            inventario.append({
                "producto_id": row[0],
                "nombre": row[1] or "Producto sin nombre",
                "codigo_ean": row[2] or "",
                "cantidad": float(row[3]) if row[3] else 0,
                "precio_ultima_compra": float(row[4]) if row[4] else 0,
                "ultima_actualizacion": str(row[5]) if row[5] else None,
                "establecimiento": row[6] or "-",
                "categoria": row[7] or "-"
            })

        # ✅ CORRECCIÓN: Calcular estadísticas adicionales que el dashboard necesita
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Total de facturas del usuario
            cursor.execute("""
                SELECT COUNT(DISTINCT id)
                FROM facturas
                WHERE usuario_id = %s
            """, (usuario_id,))
            total_facturas = cursor.fetchone()[0] or 0

            # Total gastado (suma de todas las facturas)
            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = %s
            """, (usuario_id,))
            total_gastado = float(cursor.fetchone()[0] or 0)

            # Productos únicos en inventario
            cursor.execute("""
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = %s AND producto_maestro_id IS NOT NULL
            """, (usuario_id,))
            productos_unicos = cursor.fetchone()[0] or 0
        else:
            # SQLite
            cursor.execute("""
                SELECT COUNT(DISTINCT id)
                FROM facturas
                WHERE usuario_id = ?
            """, (usuario_id,))
            total_facturas = cursor.fetchone()[0] or 0

            cursor.execute("""
                SELECT COALESCE(SUM(total_factura), 0)
                FROM facturas
                WHERE usuario_id = ?
            """, (usuario_id,))
            total_gastado = float(cursor.fetchone()[0] or 0)

            cursor.execute("""
                SELECT COUNT(DISTINCT producto_maestro_id)
                FROM inventario_usuario
                WHERE usuario_id = ? AND producto_maestro_id IS NOT NULL
            """, (usuario_id,))
            productos_unicos = cursor.fetchone()[0] or 0

        conn.close()

        print(f"✅ {len(inventario)} productos en inventario")
        print(f"📊 Stats: {total_facturas} facturas, ${total_gastado:,.0f} gastado, {productos_unicos} productos únicos")

        # ✅ CORRECCIÓN: Retornar la estructura que el dashboard espera
        return {
            "success": True,
            "usuario_id": usuario_id,
            "inventario": inventario,
            "total_productos": len(inventario),
            "total_facturas": total_facturas,          # ← AGREGADO
            "total_gastado": total_gastado,            # ← AGREGADO
            "productos_unicos": productos_unicos       # ← AGREGADO
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
            cursor.execute("""
                SELECT
                    iu.usuario_id,
                    u.email,
                    pm.nombre_normalizado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN usuarios u ON iu.usuario_id = u.id
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT %s OFFSET %s
            """, (limite, offset))
        else:
            cursor.execute("""
                SELECT
                    iu.usuario_id,
                    u.email,
                    pm.nombre_normalizado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.fecha_ultima_actualizacion,
                    pm.categoria
                FROM inventario_usuario iu
                LEFT JOIN usuarios u ON iu.usuario_id = u.id
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT ? OFFSET ?
            """, (limite, offset))

        inventarios = []
        for row in cursor.fetchall():
            inventarios.append({
                "usuario_id": row[0],
                "email": row[1] or f"Usuario {row[0]}",
                "producto": row[2] or "Sin nombre",
                "cantidad": float(row[3]) if row[3] else 0,
                "precio": float(row[4]) if row[4] else 0,
                "ultima_actualizacion": str(row[5]) if row[5] else None,
                "categoria": row[6] or "-"
            })

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
            "total_paginas": (total + limite - 1) // limite
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

        cursor.execute("SELECT COUNT(DISTINCT producto_maestro_id) FROM items_factura WHERE producto_maestro_id IS NOT NULL")
        total_productos = cursor.fetchone()[0] or 0

        conn.close()

        return {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "total_productos": total_productos
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

        cursor.execute("""
            SELECT u.id, u.email, u.nombre, u.fecha_registro,
                   COUNT(f.id) as total_facturas
            FROM usuarios u
            LEFT JOIN facturas f ON u.id = f.usuario_id
            GROUP BY u.id, u.email, u.nombre, u.fecha_registro
            ORDER BY total_facturas DESC
        """)

        usuarios = []
        for row in cursor.fetchall():
            usuarios.append({
                "id": row[0],
                "email": row[1],
                "nombre": row[2] or row[1],
                "fecha_registro": str(row[3]) if row[3] else None,
                "total_facturas": row[4] or 0
            })

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

        cursor.execute("""
            SELECT
                pm.id,
                pm.codigo_ean,
                COALESCE(pm.nombre_comercial, pm.nombre_normalizado, 'Sin nombre') as nombre,
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
            GROUP BY pm.id, pm.codigo_ean, pm.nombre_normalizado, pm.nombre_comercial,
                     pm.marca, pm.categoria, pm.subcategoria, pm.precio_promedio_global,
                     pm.total_reportes, pm.primera_vez_reportado, pm.ultima_actualizacion
            ORDER BY pm.total_reportes DESC, pm.id DESC
            LIMIT 500
        """)

        productos = []
        for row in cursor.fetchall():
            productos.append({
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
                "facturas_incluyen": row[11] or 0
            })

        conn.close()

        print(f"✅ {len(productos)} productos maestros obtenidos")
        print(f"📊 Productos con marca: {sum(1 for p in productos if p['marca'] != 'Sin marca')}")
        print(f"📊 Productos con categoría: {sum(1 for p in productos if p['categoria'] != 'Sin categoría')}")

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

        cursor.execute("""
            SELECT f1.id, f2.id, f1.establecimiento, f1.total_factura, f1.fecha_factura
            FROM facturas f1
            INNER JOIN facturas f2 ON
                f1.establecimiento = f2.establecimiento AND
                ABS(f1.total_factura - f2.total_factura) < 100 AND
                f1.fecha_factura = f2.fecha_factura AND
                f1.id < f2.id
            LIMIT 50
        """)

        duplicados = []
        for row in cursor.fetchall():
            duplicados.append({
                "ids": [row[0], row[1]],
                "establecimiento": row[2],
                "total": float(row[3]) if row[3] else 0,
                "fecha": str(row[4]) if row[4] else None
            })

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
            productos_parseados.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "valor": float(p.get("valor") or p.get("precio", 0))
            })

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados,
            total_factura
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(f"✅ Después de detección: {len(productos_finales)} productos")

        # Convertir de vuelta al formato original
        productos_corregidos = []
        for p in productos_finales:
            productos_corregidos.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "precio": p.get("valor", 0),
                "valor": p.get("valor", 0),
                "cantidad": p.get("cantidad", 1)
            })

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
            metricas["productos_eliminados_detalle"] = resultado_deteccion.get("productos_eliminados", [])

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
                    conn=conn
                )

                # ✅ CAMBIO B: Usar buscar_o_crear_producto_inteligente
                producto_maestro_id = buscar_o_crear_producto_inteligente(
                    codigo=codigo_ean_valido or "",
                    nombre=nombre,
                    precio=precio_unitario,
                    establecimiento=establecimiento_raw,
                    cursor=cursor,
                    conn=conn
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
            if stats.get('error'):
                print(f"⚠️ Error guardando precios: {stats['error']}")
            else:
                print(f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos")
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
            "deteccion_duplicados": resultado_deteccion.get("metricas", {})
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
            productos_parseados.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "valor": float(p.get("precio", 0))
            })

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados,
            total
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(f"✅ Productos después de detección: {len(productos_finales)}")

        # Convertir de vuelta al formato original
        productos_list = []
        for p in productos_finales:
            productos_list.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "precio": p.get("valor", 0)
            })

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
                    conn=conn
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
                        (factura_id, usuario_id, producto_maestro_id,
                         codigo, nombre, precio),
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO items_factura (
                            factura_id, usuario_id, producto_maestro_id,
                            codigo_leido, nombre_leido, precio_pagado, cantidad
                        ) VALUES (?, ?, ?, ?, ?, ?, 1)
                    """,
                        (factura_id, usuario_id, producto_maestro_id,
                         codigo, nombre, precio),
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
            metricas["productos_eliminados_detalle"] = resultado_deteccion.get("productos_eliminados", [])

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

        print(f"💰 Guardando precios para comparación...")
        try:
            stats = procesar_items_factura_y_guardar_precios(factura_id, usuario_id)
            if stats.get('error'):
                print(f"⚠️ Error guardando precios: {stats['error']}")
            else:
                print(f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos")
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

        cursor.execute("""
            SELECT id, nombre_normalizado, cadena, activo
            FROM establecimientos
            WHERE activo = TRUE
            ORDER BY nombre_normalizado
        """)

        establecimientos = []
        for row in cursor.fetchall():
            establecimientos.append({
                "id": row[0],
                "nombre_normalizado": row[1],
                "nombre": row[1],  # Alias para compatibilidad
                "cadena": row[2] if len(row) > 2 else None,
                "activo": row[3] if len(row) > 3 else True
            })

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
                content={"success": False, "error": "Nombre es requerido"}
            )

        # Normalizar nombre (convertir a mayúsculas)
        nombre_normalizado = nombre.upper()

        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si ya existe
        cursor.execute(
            "SELECT id FROM establecimientos WHERE UPPER(nombre_normalizado) = %s",
            (nombre_normalizado,)
        )

        existente = cursor.fetchone()

        if existente:
            cursor.close()
            conn.close()
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"El establecimiento '{nombre}' ya existe"
                }
            )

        # Insertar nuevo establecimiento
        cursor.execute(
            """
            INSERT INTO establecimientos (nombre_normalizado, cadena, activo)
            VALUES (%s, %s, TRUE)
            RETURNING id
            """,
            (nombre_normalizado, None)
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
                "activo": True
            }
        }

    except Exception as e:
        print(f"❌ Error creando establecimiento: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Error al crear establecimiento: {str(e)}"
            }
        )
# FUNCIÓN DE BACKGROUND - COMPLETA
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

        # Verificar job
        try:
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    "SELECT status, factura_id FROM processing_jobs WHERE id = %s",
                    (job_id,)
                )
            else:
                cursor.execute(
                    "SELECT status, factura_id FROM processing_jobs WHERE id = ?",
                    (job_id,)
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
                    (job_id,)
                )
            else:
                cursor.execute(
                    """
                    UPDATE processing_jobs
                    SET status = 'processing', started_at = ?
                    WHERE id = ? AND status = 'pending'
                    """,
                    (datetime.now(), job_id)
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
        print(f"🎬 Extrayendo frames...")
        frames_paths = extraer_frames_video(video_path, intervalo=1.0)

        if not frames_paths:
            raise Exception("No se extrajeron frames del video")

        print(f"✅ {len(frames_paths)} frames extraídos")

        # Procesar frames con Claude
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

        # Deduplicar
        print(f"🔍 Deduplicando productos...")
        productos_unicos = deduplicar_productos(todos_productos)
        print(f"✅ Productos únicos: {len(productos_unicos)}")

        # Detector automático de duplicados
        print(f"🔍 Aplicando detector inteligente de duplicados...")

        productos_parseados = []
        for p in productos_unicos:
            productos_parseados.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "valor": float(p.get("precio") or p.get("valor", 0))
            })

        resultado_deteccion = detectar_duplicados_automaticamente(
            productos_parseados,
            total
        )

        productos_finales = resultado_deteccion["productos_limpios"]

        print(f"✅ Después de detección inteligente: {len(productos_finales)} productos")

        # Convertir de vuelta
        productos_unicos = []
        for p in productos_finales:
            productos_unicos.append({
                "codigo": p.get("codigo", ""),
                "nombre": p.get("nombre", ""),
                "precio": p.get("valor", 0),
                "valor": p.get("valor", 0),
                "cantidad": p.get("cantidad", 1)
            })

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
                metricas["productos_eliminados_detalle"] = resultado_deteccion.get("productos_eliminados", [])

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

            # Guardar productos
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

                    # ========================================
                    # ✅ NUEVO: Usar ProductResolver
                    # ========================================
                    # ✅ CAMBIO D: Usar buscar_o_crear_producto_inteligente
                    # ✅ CAMBIO D: Usar buscar_o_crear_producto_inteligente
                    producto_maestro_id = None

                    if codigo and len(codigo) >= 3:
                        producto_maestro_id = buscar_o_crear_producto_inteligente(
                            codigo=codigo,
                            nombre=nombre,
                            precio=int(precio),
                            establecimiento=establecimiento,
                            cursor=cursor,
                            conn=conn
                        )
                        print(f"   ✅ Producto Maestro ID: {producto_maestro_id} - {nombre}")

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

            print(f"💰 Guardando precios para comparación...")
            try:
                stats = procesar_items_factura_y_guardar_precios(factura_id, usuario_id)
                if stats.get('error'):
                    print(f"⚠️ Error guardando precios: {stats['error']}")
                else:
                    print(f"✅ Guardados {stats.get('precios_guardados', 0)} precios en precios_productos")
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
                "promedio_duplicados": float(s[4]) if s[4] else 0
            }
            for s in stats
        ],
        "anomalias_pendientes": anomalias
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
    cursor.execute("""
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = %s
        RETURNING id, email, rol
    """, (email,))
    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {"success": True, "user": {"id": user[0], "email": user[1], "rol": user[2]}}
    return {"success": False, "error": "Usuario no encontrado"}

# ============================================================
# ENDPOINTS AUDITORÍA - PRODUCTOS REFERENCIA
# ============================================================

@app.get("/api/productos-referencia/{codigo_ean}")
async def get_producto_referencia(
    codigo_ean: str,
    authorization: str = Header(None)
):
    """Obtener producto de referencia por código EAN"""
    print(f"🔍 [AUDITORÍA] Buscando producto referencia: {codigo_ean}")

    # Validar token JWT
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace('Bearer ', '')
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get('rol') not in ['admin', 'auditor']:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
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
        """, (codigo_ean,))

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
            "updated_at": producto[8].isoformat() if producto[8] else None
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
    request: Request,
    authorization: str = Header(None)
):
    """Crear nuevo producto de referencia"""
    print(f"📝 [AUDITORÍA] Creando nuevo producto referencia")

    # Validar token JWT
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace('Bearer ', '')
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get('rol') not in ['admin', 'auditor']:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    # Obtener datos del body
    body = await request.json()

    codigo_ean = body.get('codigo_ean')
    nombre = body.get('nombre')
    marca = body.get('marca', '')
    categoria = body.get('categoria', '')
    presentacion = body.get('presentacion', '')
    unidad_medida = body.get('unidad_medida', '')

    if not codigo_ean or not nombre:
        raise HTTPException(status_code=400, detail="Código EAN y nombre son requeridos")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si ya existe
        cursor.execute("""
            SELECT id FROM productos_referencia
            WHERE codigo_ean = %s
        """, (codigo_ean,))

        if cursor.fetchone():
            raise HTTPException(status_code=409, detail="Producto ya existe")

        # Insertar nuevo producto
        cursor.execute("""
            INSERT INTO productos_referencia
            (codigo_ean, nombre, marca, categoria, presentacion, unidad_medida)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id, codigo_ean, nombre, created_at
        """, (codigo_ean, nombre, marca, categoria, presentacion, unidad_medida))

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
            "created_at": producto[3].isoformat() if producto[3] else None
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
    codigo_ean: str,
    request: Request,
    authorization: str = Header(None)
):
    """Actualizar producto de referencia existente"""
    print(f"✏️ [AUDITORÍA] Actualizando producto: {codigo_ean}")

    # Validar token JWT
    if not authorization or not authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Token no proporcionado")

    token = authorization.replace('Bearer ', '')
    user_data = verify_jwt_token(token)

    if not user_data:
        raise HTTPException(status_code=401, detail="Token inválido")

    # Validar rol de auditor/admin
    if user_data.get('rol') not in ['admin', 'auditor']:
        raise HTTPException(status_code=403, detail="Sin permisos de auditoría")

    body = await request.json()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Verificar si existe
        cursor.execute("""
            SELECT id FROM productos_referencia
            WHERE codigo_ean = %s
        """, (codigo_ean,))

        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail="Producto no encontrado")

        # Actualizar campos proporcionados
        updates = []
        params = []

        if 'nombre' in body:
            updates.append("nombre = %s")
            params.append(body['nombre'])
        if 'marca' in body:
            updates.append("marca = %s")
            params.append(body['marca'])
        if 'categoria' in body:
            updates.append("categoria = %s")
            params.append(body['categoria'])
        if 'presentacion' in body:
            updates.append("presentacion = %s")
            params.append(body['presentacion'])
        if 'unidad_medida' in body:
            updates.append("unidad_medida = %s")
            params.append(body['unidad_medida'])

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
            "updated_at": producto[7].isoformat() if producto[7] else None
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error al actualizar producto: {str(e)}")
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/fix-rol")
async def fix_rol():
    """Endpoint temporal para arreglar rol del usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = 'santiago@tscamp.co'
        RETURNING id, email, rol
    """, )

    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {
            "success": True,
            "user": {
                "id": user[0],
                "email": user[1],
                "rol": user[2]
            }
        }
    return {"success": False, "error": "Usuario no encontrado"}



from consolidacion_productos import procesar_item_con_consolidacion
import psycopg2.extras

@app.post("/api/v2/procesar-factura")
async def procesar_factura_v2(
    video: UploadFile = File(None),
    imagen: UploadFile = File(None),
    user_id: int = Form(...),
    establecimiento_id: int = Form(...)
):
    """
    Nuevo endpoint v2 - CON EXTRACCIÓN DE FRAMES
    """
    print("\n" + "="*70)
    print("🚀 PROCESAMIENTO DE FACTURA V2 (Sistema de Consolidación)")
    print("="*70)

    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        # 1. Procesar archivo
        print("\n📹 Procesando medios...")

        frames_para_ocr = []

        if video:
            print("   ✓ Video recibido")

            # Guardar video temporalmente
            temp_video_path = f"/tmp/video_{user_id}_{datetime.now().timestamp()}.mp4"
            with open(temp_video_path, "wb") as f:
                f.write(await video.read())

            video_size_mb = os.path.getsize(temp_video_path) / (1024 * 1024)
            print(f"   ℹ️  Tamaño: {video_size_mb:.2f} MB")

            # ✅ EXTRAER FRAMES (esto resuelve el problema de 5 MB)
            print("   📸 Extrayendo frames del video...")
            frames_base64 = extract_frames_from_video(temp_video_path, max_frames=10)

            # Eliminar video temporal
            os.remove(temp_video_path)

            if not frames_base64:
                raise HTTPException(status_code=400, detail="No se pudieron extraer frames del video")

            print(f"   ✓ {len(frames_base64)} frames extraídos")

            # Guardar frames como imágenes temporales para Claude
            for idx, frame_b64 in enumerate(frames_base64):
                frame_path = f"/tmp/frame_{user_id}_{idx}_{datetime.now().timestamp()}.jpg"

                # Decodificar base64 y guardar
                with open(frame_path, "wb") as f:
                    f.write(base64.b64decode(frame_b64))

                # Verificar tamaño del frame
                frame_size_mb = os.path.getsize(frame_path) / (1024 * 1024)

                if frame_size_mb < 5:  # Solo frames menores a 5 MB
                    frames_para_ocr.append(frame_path)
                else:
                    print(f"   ⚠️  Frame {idx} muy grande ({frame_size_mb:.2f} MB), omitiendo")
                    os.remove(frame_path)

            print(f"   ✓ {len(frames_para_ocr)} frames listos para OCR")

        elif imagen:
            print("   ✓ Imagen recibida")

            # Guardar imagen temporalmente
            temp_img_path = f"/tmp/imagen_{user_id}_{datetime.now().timestamp()}.jpg"
            with open(temp_img_path, "wb") as f:
                f.write(await imagen.read())

            img_size_mb = os.path.getsize(temp_img_path) / (1024 * 1024)
            print(f"   ℹ️  Tamaño: {img_size_mb:.2f} MB")

            if img_size_mb > 5:
                os.remove(temp_img_path)
                raise HTTPException(
                    status_code=400,
                    detail=f"Imagen muy grande ({img_size_mb:.2f} MB). Máximo 5 MB"
                )

            frames_para_ocr = [temp_img_path]

        else:
            raise HTTPException(status_code=400, detail="Debe proporcionar video o imagen")

        # 2. OCR - Procesar cada frame y consolidar resultados
        print(f"\n🤖 Ejecutando OCR con Claude Vision ({len(frames_para_ocr)} frames)...")

        todos_los_items = []
        total_acumulado = 0
        fecha_factura = None

        # ✅ LOOP DE PROCESAMIENTO DE FRAMES
        for idx, frame_path in enumerate(frames_para_ocr, 1):
            print(f"   Frame {idx}/{len(frames_para_ocr)}...")

            try:
                resultado = parse_invoice_with_claude(frame_path)

                if resultado.get('success'):
                    # ✅ CORRECCIÓN: Los productos están en resultado['data']['productos']
                    data = resultado.get('data', {})
                    productos = data.get('productos', [])

                    if productos:
                        # Normalizar formato para que coincida con el resto del código
                        for prod in productos:
                            item_normalizado = {
                                'descripcion': prod.get('nombre', 'PRODUCTO SIN NOMBRE'),
                                'codigo': str(prod.get('codigo', '')) if prod.get('codigo') else None,
                                'precio_unitario': float(prod.get('precio', 0)),
                                'cantidad': int(prod.get('cantidad', 1)),
                                'subtotal': float(prod.get('precio', 0)) * int(prod.get('cantidad', 1))
                            }
                            todos_los_items.append(item_normalizado)

                        print(f"      → {len(productos)} items detectados")
                    else:
                        print(f"      ⚠️  Frame sin productos")

                    # Tomar el total y fecha del último frame que los tenga
                    if data.get('total'):
                        total_acumulado = data['total']
                    if data.get('fecha'):
                        fecha_factura = data['fecha']
                else:
                    print(f"      ⚠️  Frame sin datos válidos")

            except Exception as e:
                print(f"      ❌ Error procesando frame: {e}")
                import traceback
                traceback.print_exc()

            # Eliminar frame temporal
            if os.path.exists(frame_path):
                os.remove(frame_path)

        datos_factura = {
            'success': True,
            'items': todos_los_items,
            'total': total_acumulado or sum(item.get('subtotal', 0) for item in todos_los_items),
            'fecha': normalizar_fecha(fecha_factura)
        }

        print(f"   ✓ Total de items detectados: {len(todos_los_items)}")
        print(f"   • Total: ${datos_factura['total']:,.0f}")
        print(f"   • Fecha: {datos_factura['fecha']}")

        if not datos_factura['items']:
            raise HTTPException(
                status_code=400,
                detail="No se detectaron productos en la factura"
            )

        # ✅ Validar que el establecimiento existe
        # ✅ Validar que el establecimiento existe
        print("\n🏪 Validando establecimiento...")

        cursor.execute(
            "SELECT id, nombre_normalizado, cadena FROM establecimientos WHERE id = %s",
            (establecimiento_id,)
            )
        establecimiento_db = cursor.fetchone()

        if not establecimiento_db:
            print(f"   ❌ Establecimiento ID {establecimiento_id} no existe")

            raise HTTPException(
                status_code=400,
        detail=f"Establecimiento ID {establecimiento_id} no encontrado. Por favor, créalo primero desde la app."
            )

        print(f"   ✅ Establecimiento válido:")
        print(f"      • ID: {establecimiento_db['id']}")
        print(f"      • Nombre: {establecimiento_db['nombre_normalizado']}")
        print(f"      • Cadena: {establecimiento_db['cadena']}")
        print(f"   ℹ️  El establecimiento seleccionado por el usuario tiene prioridad")
        print(f"   ℹ️  Cualquier establecimiento detectado por OCR será IGNORADO")

# ============================================================================
# ENDPOINT V2 SIMPLIFICADO - USA FUNCIONES DE database.py
# Reemplazar en main.py desde "# 3. Crear factura" hasta el final del endpoint
# ============================================================================

        # 3. Crear factura
        print("\n💾 Creando registro de factura...")
        cursor.execute(
            """INSERT INTO facturas
               (usuario_id, establecimiento_id, fecha_factura, total_factura, estado_validacion, establecimiento, cadena)
               VALUES (%s, %s, %s, %s, 'procesado', %s, %s)
               RETURNING id, fecha_factura""",
            (
                user_id,
                establecimiento_id,
                datos_factura['fecha'],
                datos_factura['total'],
                establecimiento_db['nombre_normalizado'],
                establecimiento_db['cadena']
            )
        )
        factura = cursor.fetchone()
        factura_id = factura['id']
        print(f"   ✅ Factura #{factura_id} creada")
        print(f"   📍 Establecimiento: {establecimiento_db['nombre_normalizado']}")

        # ✅ COMMIT: Factura debe existir antes de procesar items
        conn.commit()
        print(f"   ✅ Factura confirmada en BD")

        # 4. Consolidación inteligente
        print("\n" + "="*70)
        print("🧠 CONSOLIDACIÓN INTELIGENTE DE PRODUCTOS")
        print("="*70)

        items_procesados = []
        errores = []

        for idx, item in enumerate(datos_factura['items'], 1):
            try:
                print(f"\n[{idx}/{len(datos_factura['items'])}]")
                print(f"📦 Procesando: {item.get('descripcion', 'Sin nombre')}")

                codigo_ocr = item.get('codigo', '')
                if codigo_ocr:
                    print(f"   📟 Código: {codigo_ocr}")

                # Procesar con consolidación inteligente
                producto_id = procesar_item_con_consolidacion(
                    cursor=cursor,
                    item_ocr={
                        'nombre': item.get('descripcion', 'PRODUCTO SIN NOMBRE'),
                        'codigo': codigo_ocr,
                        'precio': item.get('precio_unitario', 0),
                        'cantidad': item.get('cantidad', 1)
                    },
                    factura_id=factura_id,
                    establecimiento_id=establecimiento_id
                )

                # ✅ Sincronizar a productos_maestros (tabla legacy)
                try:
                    cursor.execute("SELECT id FROM productos_maestros WHERE id = %s", (producto_id,))

                    if not cursor.fetchone():
                        print(f"   🔄 Sincronizando producto {producto_id}...")

                        cursor.execute("""
                            SELECT nombre_consolidado, codigo_ean, marca
                            FROM productos_maestros_v2
                            WHERE id = %s
                        """, (producto_id,))

                        producto_v2 = cursor.fetchone()

                        if producto_v2:
                            cursor.execute("""
                                INSERT INTO productos_maestros
                                (id, nombre_normalizado, codigo_ean, marca, auditado_manualmente, validaciones_manuales)
                                VALUES (%s, %s, %s, %s, FALSE, 0)
                                ON CONFLICT (id) DO UPDATE SET
                                    nombre_normalizado = EXCLUDED.nombre_normalizado,
                                    codigo_ean = EXCLUDED.codigo_ean,
                                    marca = EXCLUDED.marca
                            """, (
                                producto_id,
                                producto_v2['nombre_consolidado'],
                                producto_v2['codigo_ean'],
                                producto_v2['marca']
                            ))
                            print(f"   ✅ Sincronizado a productos_maestros")
                except Exception as sync_error:
                    print(f"   ⚠️  Error sync: {sync_error}")

                # ✅ NUEVO: Registrar código usando función de database.py
                if codigo_ocr and len(codigo_ocr) >= 4:
                    from database import registrar_codigo_producto

                    if registrar_codigo_producto(producto_id, establecimiento_id, codigo_ocr):
                        # Obtener tipo de código
                        cursor.execute("""
                            SELECT tipo_codigo
                            FROM codigos_establecimiento
                            WHERE producto_maestro_id = %s
                              AND establecimiento_id = %s
                              AND codigo_local = %s
                            LIMIT 1
                        """, (producto_id, establecimiento_id, codigo_ocr))

                        tipo = cursor.fetchone()
                        if tipo:
                            print(f"   ✅ Código registrado: {codigo_ocr} (tipo: {tipo[0]})")
                    else:
                        print(f"   ℹ️  Código {codigo_ocr} no registrado (inválido)")

                # ✅ COMMIT después de registrar producto y código
                conn.commit()
                print(f"   ✅ Producto ID {producto_id} confirmado")

                # Insertar item en factura
                cursor.execute(
                    """INSERT INTO items_factura
                       (factura_id, usuario_id, producto_maestro_id, nombre_leido, codigo_leido,
                        cantidad, precio_pagado)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       RETURNING id""",
                    (
                        factura_id,
                        user_id,
                        producto_id,
                        item.get('descripcion'),
                        codigo_ocr,
                        item.get('cantidad', 1),
                        item.get('precio_unitario', 0)
                    )
                )
                item_id = cursor.fetchone()['id']

                # ✅ COMMIT después de insertar item
                conn.commit()
                print(f"   ✅ Item #{item_id} guardado")

                items_procesados.append({
                    'item_id': item_id,
                    'producto_id': producto_id,
                    'nombre': item.get('descripcion'),
                    'codigo': codigo_ocr,
                    'precio': item.get('precio_unitario')
                })

            except Exception as e:
                conn.rollback()
                cursor.close()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                error_msg = f"Error en item '{item.get('descripcion', 'N/A')}': {str(e)}"
                print(f"   ❌ {error_msg}")
                errores.append(error_msg)
                continue

        # 5. Actualizar contador
        cursor.execute(
            "UPDATE facturas SET productos_guardados = %s WHERE id = %s",
            (len(items_procesados), factura_id)
        )

        # 6. Estadísticas
        cursor.execute("""
            SELECT
                COUNT(*) as total_productos,
                COUNT(CASE WHEN codigo_ean IS NOT NULL THEN 1 END) as con_ean,
                COUNT(CASE WHEN estado = 'verificado' THEN 1 END) as verificados,
                COUNT(CASE WHEN estado = 'pendiente' THEN 1 END) as pendientes,
                COALESCE(AVG(confianza_datos), 0) as confianza_promedio
            FROM productos_maestros_v2
        """)
        stats = cursor.fetchone()

        # Estadísticas de códigos
        cursor.execute("""
            SELECT
                COUNT(*) as total_codigos,
                COUNT(DISTINCT producto_maestro_id) as productos_con_codigos,
                COUNT(CASE WHEN tipo_codigo = 'plu_local' THEN 1 END) as plu_locales,
                COUNT(CASE WHEN tipo_codigo = 'plu_estandar' THEN 1 END) as plu_estandares,
                COUNT(CASE WHEN tipo_codigo = 'ean' THEN 1 END) as eans
            FROM codigos_establecimiento
            WHERE establecimiento_id = %s AND activo = TRUE
        """, (establecimiento_id,))
        stats_codigos = cursor.fetchone()

        print("\n" + "="*70)
        print("📈 ESTADÍSTICAS")
        print("="*70)
        print(f"Productos maestros: {stats['total_productos']}")
        print(f"  • Con EAN: {stats['con_ean']}")
        print(f"  • Verificados: {stats['verificados']}")
        print(f"\nCódigos en {establecimiento_db['nombre_normalizado']}:")
        print(f"  • Total: {stats_codigos['total_codigos']}")
        print(f"  • PLU locales: {stats_codigos['plu_locales']}")
        print(f"  • PLU estándar: {stats_codigos['plu_estandares']}")
        print(f"  • EANs: {stats_codigos['eans']}")

        # ✅ COMMIT final
        conn.commit()

        # 7. ✅ CRÍTICO: Actualizar inventario
        print(f"\n📦 Actualizando inventario del usuario {user_id}...")
        try:
            from database import actualizar_inventario_desde_factura
            actualizar_inventario_desde_factura(factura_id, user_id)
            print(f"✅ Inventario actualizado")
        except Exception as e:
            print(f"⚠️  Error inventario: {e}")
            import traceback
            traceback.print_exc()

        print("="*70)
        print("✅ PROCESAMIENTO COMPLETADO")
        print("="*70 + "\n")

        return {
            "success": True,
            "factura_id": factura_id,
            "items_procesados": len(items_procesados),
            "items_con_errores": len(errores),
            "errores": errores,
            "items": items_procesados,
            "estadisticas": {
                "total_productos": int(stats['total_productos']),
                "con_ean": int(stats['con_ean']),
                "verificados": int(stats['verificados']),
                "pendientes": int(stats['pendientes']),
                "confianza_promedio": float(stats['confianza_promedio']),
                "codigos": {
                    "total": int(stats_codigos['total_codigos']),
                    "productos_con_codigos": int(stats_codigos['productos_con_codigos']),
                    "plu_locales": int(stats_codigos['plu_locales']),
                    "plu_estandares": int(stats_codigos['plu_estandares']),
                    "eans": int(stats_codigos['eans'])
                }
            }
        }

    except Exception as e:
        conn.rollback()
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        cursor.close()
        conn.close()

@app.get("/api/v2/productos/pendientes")
async def productos_pendientes_revision(limite: int = 50):
    """
    Lista productos que necesitan revisión humana
    Ordenados por frecuencia (los más escaneados primero)
    """
    conn = await get_db_connection()

    try:
        productos = await conn.fetch("""
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_consolidado,
                pm.marca,
                pm.confianza_datos,
                pm.veces_visto,
                pm.estado,
                COUNT(DISTINCT ph.factura_id) as num_facturas,
                array_agg(DISTINCT vn.nombre_variante) as variantes_nombres,
                array_agg(DISTINCT e.nombre) as establecimientos
            FROM productos_maestros_v2 pm
            LEFT JOIN precios_historicos_v2 ph ON pm.id = ph.producto_maestro_id
            LEFT JOIN variantes_nombres vn ON pm.id = vn.producto_maestro_id
            LEFT JOIN establecimientos e ON ph.establecimiento_id = e.id
            WHERE pm.estado IN ('pendiente', 'conflicto')
            GROUP BY pm.id
            ORDER BY pm.veces_visto DESC, pm.confianza_datos ASC
            LIMIT $1
        """, limite)

        return {
            "total": len(productos),
            "productos": [
                {
                    "id": p['id'],
                    "ean": p['codigo_ean'],
                    "nombre": p['nombre_consolidado'],
                    "marca": p['marca'],
                    "confianza": float(p['confianza_datos']),
                    "veces_visto": p['veces_visto'],
                    "num_facturas": p['num_facturas'],
                    "estado": p['estado'],
                    "variantes": p['variantes_nombres'],
                    "establecimientos": p['establecimientos']
                }
                for p in productos
            ]
        }

    finally:
        await conn.close()


@app.post("/api/v2/productos/{producto_id}/verificar")
async def verificar_producto_manualmente(
    producto_id: int,
    nombre_correcto: str,
    codigo_ean: str = None,
    marca: str = None
):
    """
    Permite verificar/corregir un producto manualmente
    Esto entrena al sistema para futuras consolidaciones
    """
    conn = await get_db_connection()

    try:
        # Actualizar producto
        await conn.execute("""
            UPDATE productos_maestros_v2
            SET nombre_consolidado = $1,
                codigo_ean = $2,
                marca = $3,
                estado = 'verificado',
                confianza_datos = 1.0,
                fecha_ultima_actualizacion = NOW()
            WHERE id = $4
        """, nombre_correcto, codigo_ean, marca, producto_id)

        # Si ahora tiene EAN, agregarlo a productos_referencia
        if codigo_ean:
            await conn.execute("""
                INSERT INTO productos_referencia
                (codigo_ean, nombre_oficial, marca, verificado, fuente)
                VALUES ($1, $2, $3, TRUE, 'manual')
                ON CONFLICT (codigo_ean)
                DO UPDATE SET
                    nombre_oficial = $2,
                    marca = $3,
                    fecha_actualizacion = NOW()
            """, codigo_ean, nombre_correcto, marca)

        return {
            "success": True,
            "mensaje": "Producto verificado correctamente"
        }

    finally:
        await conn.close()

# ==========================================
# ENDPOINT 1: VERIFICAR PRODUCTO - ✅ CORREGIDO
# ==========================================


@app.get("/api/admin/auditoria/verificar/{codigo_ean}")
async def verificar_producto_auditoria(codigo_ean: str, current_user: dict = Depends(get_current_user)):
    """Verificar si un producto existe en la base de datos"""
    print(f"🔍 [AUDITORÍA] Verificando producto: {codigo_ean}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id, codigo_ean, nombre_normalizado, marca, categoria, auditado_manualmente, validaciones_manuales
            FROM productos_maestros
            WHERE codigo_ean = %s
            LIMIT 1
            """, (codigo_ean,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if row:
            print(f"✅ [AUDITORÍA] Producto encontrado: {row[2]}")
            return {
                'existe': True,
                'producto': {
                    'id': row[0],
                    'codigo_ean': row[1],
                    'nombre': row[2],
                    'marca': row[3],
                    'categoria': row[4],
                    'auditado_manualmente': row[5],
                    'validaciones_manuales': row[6]
                }
            }
        else:
            print(f"⚠️ [AUDITORÍA] Producto NO encontrado: {codigo_ean}")
            return {
                'existe': False,
                'producto': None,
                'mensaje': 'Producto no encontrado'
            }

    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en verificar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 2: CREAR PRODUCTO - ✅ CORREGIDO
# ==========================================
@app.post("/api/admin/auditoria/producto")
async def crear_producto_auditoria(producto: ProductoCreateAuditoria, current_user: dict = Depends(get_current_user)):
    """Crear un nuevo producto en la base de datos"""
    print(f"➕ [AUDITORÍA] Creando producto: {producto.codigo_ean} - {producto.nombre}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Verificar si ya existe
        cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = %s", (producto.codigo_ean,))
        if cursor.fetchone():
            cursor.close()
            conn.close()
            print(f"⚠️ [AUDITORÍA] Producto ya existe: {producto.codigo_ean}")
            raise HTTPException(status_code=400, detail="Producto ya existe")

        # Crear producto
        cursor.execute("""
            INSERT INTO productos_maestros (
            codigo_ean, nombre_normalizado, marca, categoria,
            auditado_manualmente, validaciones_manuales
            ) VALUES (%s, %s, %s, %s, TRUE, 1)
            RETURNING id, codigo_ean, nombre_normalizado, marca, categoria, auditado_manualmente, validaciones_manuales
            """, (producto.codigo_ean, producto.nombre, producto.marca, producto.categoria))

        row = cursor.fetchone()

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'crear', %s)
        """, (current_user['id'], row[0], datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto creado exitosamente: ID {row[0]}")

        return {
            'mensaje': 'Producto creado',
            'producto': {
                'id': row[0],
                'codigo_ean': row[1],
                'nombre': row[2],
                'marca': row[3],
                'categoria': row[4],
                'auditado_manualmente': row[5],
                'validaciones_manuales': row[6]
            }
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
async def actualizar_producto_auditoria(producto_id: int, producto: ProductoUpdateAuditoria, current_user: dict = Depends(get_current_user)):
    """Actualizar un producto existente"""
    print(f"✏️ [AUDITORÍA] Actualizando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE productos_maestros
            SET nombre_normalizado = %s, marca = %s, categoria = %s
            WHERE id = %s
            RETURNING id, codigo_ean, nombre_normalizado, marca, categoria, auditado_manualmente, validaciones_manuales
            """, (producto.nombre, producto.marca, producto.categoria, producto_id))

        row = cursor.fetchone()

        if not row:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'actualizar', %s)
        """, (current_user['id'], producto_id, datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto actualizado: {row[2]}")

        return {
            'mensaje': 'Actualizado',
            'producto': {
                'id': row[0],
                'codigo_ean': row[1],
                'nombre': row[2],
                'marca': row[3],
                'categoria': row[4],
                'auditado_manualmente': row[5],
                'validaciones_manuales': row[6]
            }
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
async def validar_producto_auditoria(producto_id: int, current_user: dict = Depends(get_current_user)):
    """Validar manualmente un producto"""
    print(f"✅ [AUDITORÍA] Validando producto ID: {producto_id}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE productos_maestros
            SET auditado_manualmente = TRUE,
                validaciones_manuales = validaciones_manuales + 1,
                ultima_validacion = %s
            WHERE id = %s
        """, (datetime.now(), producto_id))

        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="No encontrado")

        # Registrar auditoría
        cursor.execute("""
            INSERT INTO auditoria_productos (usuario_id, producto_maestro_id, accion, fecha)
            VALUES (%s, %s, 'validar', %s)
        """, (current_user['id'], producto_id, datetime.now()))

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ [AUDITORÍA] Producto validado exitosamente")

        return {'mensaje': 'Validado'}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ [AUDITORÍA] Error en validar_producto: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ENDPOINT 5: ESTADÍSTICAS
# ==========================================
@app.get("/api/admin/auditoria/estadisticas")
async def obtener_estadisticas_auditoria(current_user: dict = Depends(get_current_user)):
    """Obtener estadísticas de auditoría del usuario"""
    print(f"📊 [AUDITORÍA] Obteniendo estadísticas para usuario {current_user['id']}")
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT accion, COUNT(*)
            FROM auditoria_productos
            WHERE usuario_id = %s
            GROUP BY accion
        """, (current_user['id'],))

        stats = {
            'productos_creados': 0,
            'productos_actualizados': 0,
            'productos_validados': 0,
            'total_acciones': 0
        }

        for row in cursor.fetchall():
            if row[0] == 'crear':
                stats['productos_creados'] = row[1]
            elif row[0] == 'actualizar':
                stats['productos_actualizados'] = row[1]
            elif row[0] == 'validar':
                stats['productos_validados'] = row[1]
            stats['total_acciones'] += row[1]

        cursor.execute("""
            SELECT accion, producto_maestro_id, fecha
            FROM auditoria_productos
            WHERE usuario_id = %s
            ORDER BY fecha DESC
            LIMIT 10
        """, (current_user['id'],))

        stats['ultimas_acciones'] = [
            {
                'accion': r[0],
                'producto_maestro_id': r[1],
                'fecha': r[2].isoformat() if r[2] else None
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
            cursor.execute("""
                SELECT id, establecimiento, total_factura, productos_guardados
                FROM facturas
                WHERE id = %s AND usuario_id = %s
            """, (factura_id, usuario_id))
        else:
            cursor.execute("""
                SELECT id, establecimiento, total_factura, productos_guardados
                FROM facturas
                WHERE id = ? AND usuario_id = ?
            """, (factura_id, usuario_id))

        factura = cursor.fetchone()

        if not factura:
            cursor.close()
            conn.close()
            return {
                "success": False,
                "error": f"Factura {factura_id} no encontrada para usuario {usuario_id}"
            }

        print(f"✅ Factura encontrada:")
        print(f"   Establecimiento: {factura[1]}")
        print(f"   Total: ${factura[2]:,}")
        print(f"   Productos guardados: {factura[3]}")

        # Verificar items
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT COUNT(*), COUNT(producto_maestro_id)
                FROM items_factura
                WHERE factura_id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*), COUNT(producto_maestro_id)
                FROM items_factura
                WHERE factura_id = ?
            """, (factura_id,))

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
                    "productos_guardados": factura[3]
                },
                "total_items": items_stats[0],
                "items_con_producto_maestro": items_stats[1]
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
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM inventario_usuario
                    WHERE usuario_id = %s
                """, (usuario_id,))
            else:
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM inventario_usuario
                    WHERE usuario_id = ?
                """, (usuario_id,))

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
                    "productos_guardados": factura[3]
                },
                "items_procesados": items_stats[1],
                "productos_en_inventario": total_inventario
            }
        else:
            return {
                "success": False,
                "error": "actualizar_inventario_desde_factura retornó False",
                "mensaje": "Revisa los logs para ver el error específico"
            }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e),
            "traceback": traceback.format_exc()
        }


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
            cursor.execute("""
                SELECT COUNT(*)
                FROM inventario_usuario
                WHERE usuario_id = %s
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT COUNT(*)
                FROM inventario_usuario
                WHERE usuario_id = ?
            """, (usuario_id,))

        total_inventario = cursor.fetchone()[0]

        # Obtener últimas facturas
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
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
            """, (usuario_id,))
        else:
            cursor.execute("""
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
            """, (usuario_id,))

        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "total": float(row[2]) if row[2] else 0,
                "productos_guardados": row[3],
                "fecha": str(row[4]) if row[4] else None,
                "items_en_bd": row[5],
                "items_con_producto_maestro": row[6]
            })

        # Ver muestra del inventario
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_normalizado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.establecimiento,
                    iu.fecha_ultima_compra
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = %s
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 10
            """, (usuario_id,))
        else:
            cursor.execute("""
                SELECT
                    iu.producto_maestro_id,
                    pm.nombre_normalizado,
                    iu.cantidad_actual,
                    iu.precio_ultima_compra,
                    iu.establecimiento,
                    iu.fecha_ultima_compra
                FROM inventario_usuario iu
                LEFT JOIN productos_maestros pm ON iu.producto_maestro_id = pm.id
                WHERE iu.usuario_id = ?
                ORDER BY iu.fecha_ultima_actualizacion DESC
                LIMIT 10
            """, (usuario_id,))

        inventario_muestra = []
        for row in cursor.fetchall():
            inventario_muestra.append({
                "producto_maestro_id": row[0],
                "nombre": row[1],
                "cantidad": float(row[2]) if row[2] else 0,
                "precio": float(row[3]) if row[3] else 0,
                "establecimiento": row[4],
                "fecha_compra": str(row[5]) if row[5] else None
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "usuario_id": usuario_id,
            "total_productos_inventario": total_inventario,
            "total_facturas": len(facturas),
            "facturas_recientes": facturas,
            "inventario_muestra": inventario_muestra
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": str(e)
        }


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
        cursor.execute("""
            SELECT id, email, password_hash, nombre, rol
            FROM usuarios
            WHERE email = %s
        """, (email,))

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
        if rol not in ['admin', 'auditor']:
            print(f"⚠️ [AUDITORÍA] Usuario sin permisos: {email} (rol: {rol})")
            raise HTTPException(
                status_code=403,
                detail=f"Sin permisos de auditoría. Tu rol es: {rol}. Contacta al administrador."
            )

        # Generar token JWT
        from auth import create_jwt_token
        token = create_jwt_token(user_id, user_email, rol)

        print(f"✅ [AUDITORÍA] Login exitoso: {email} (rol: {rol})")

        return {
            "success": True,
            "message": "Login exitoso",
            "token": token,
            "user": {
                "id": user_id,
                "email": user_email,
                "nombre": nombre,
                "rol": rol
            }
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

    cursor.execute("""
        UPDATE usuarios
        SET rol = 'admin'
        WHERE email = 'santiago@tscamp.co'
        RETURNING id, email, rol
    """)

    user = cursor.fetchone()
    conn.commit()
    conn.close()

    if user:
        return {
            "success": True,
            "mensaje": f"Usuario actualizado: {user[1]} → rol: {user[2]}"
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

        cursor.execute("""
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
        """, (producto_id,))

        codigos = []
        for row in cursor.fetchall():
            codigos.append({
                "id": row[0],
                "codigo": row[1],
                "tipo": row[2],
                "establecimiento": row[3],
                "veces_visto": row[4],
                "primera_vez": row[5].isoformat() if row[5] else None,
                "ultima_vez": row[6].isoformat() if row[6] else None
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "producto_id": producto_id,
            "codigos": codigos,
            "total": len(codigos)
        }

    except Exception as e:
        print(f"❌ Error: {e}")
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
        cursor.execute("""
            SELECT nombre_normalizado, cadena
            FROM establecimientos
            WHERE id = %s
        """, (establecimiento_id,))

        establecimiento = cursor.fetchone()
        if not establecimiento:
            raise HTTPException(status_code=404, detail="Establecimiento no encontrado")

        # Códigos del establecimiento
        cursor.execute("""
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
        """, (establecimiento_id, limite))

        codigos = []
        for row in cursor.fetchall():
            codigos.append({
                "codigo_local": row[0],
                "tipo": row[1],
                "producto": row[2],
                "codigo_ean": row[3] or "N/A",
                "veces_visto": row[4],
                "ultima_vez": row[5].isoformat() if row[5] else None
            })

        # Estadísticas
        cursor.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN tipo_codigo = 'plu_local' THEN 1 END) as plu_locales,
                COUNT(CASE WHEN tipo_codigo = 'plu_estandar' THEN 1 END) as plu_estandares,
                COUNT(CASE WHEN tipo_codigo = 'ean' THEN 1 END) as eans
            FROM codigos_establecimiento
            WHERE establecimiento_id = %s AND activo = TRUE
        """, (establecimiento_id,))

        stats = cursor.fetchone()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "establecimiento": {
                "id": establecimiento_id,
                "nombre": establecimiento[0],
                "cadena": establecimiento[1]
            },
            "codigos": codigos,
            "estadisticas": {
                "total_codigos": stats[0],
                "plu_locales": stats[1],
                "plu_estandares": stats[2],
                "eans": stats[3]
            }
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
        cursor.execute("""
            SELECT id, nombre_consolidado, codigo_ean, marca
            FROM productos_maestros_v2
            WHERE codigo_ean = %s
            LIMIT 1
        """, (codigo,))

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
                    "marca": producto[3]
                }
            }

        # Buscar por código local
        if establecimiento_id:
            cursor.execute("""
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
            """, (codigo, establecimiento_id))
        else:
            cursor.execute("""
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
            """, (codigo,))

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
                    "tipo_codigo": producto[4]
                }
            }

        return {
            "success": True,
            "encontrado": False,
            "mensaje": f"No se encontró producto con código {codigo}"
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


print("✅ Endpoints de códigos por establecimiento registrados")


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
# SEGUNDA PARTE - ENDPOINTS ADMINISTRATIVOS Y DEBUG
# Agregar después de los endpoints básicos en main.py
# ==========================================

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
            precio_pesos = float(row[3]) if row[3] else 0

            productos.append({
                "id": row[0],
                "nombre": row[1],
                "codigo_ean": row[2],
                "precio_promedio": precio_pesos,
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


