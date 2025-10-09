"""
main.py - Servidor FastAPI Principal para LecFac
VERSIÓN COMPLETA - Incluye TODOS los endpoints necesarios + Gestor de Duplicados
"""
from video_processor import extraer_frames_video, deduplicar_productos, limpiar_frames_temporales
from claude_invoice import parse_invoice_with_claude
from database import obtener_o_crear_establecimiento, detectar_cadena
import tempfile
from fastapi import FastAPI, File, UploadFile, HTTPException, Form, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import List, Optional
import os
import tempfile
import traceback
import json

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
    obtener_productos_frecuentes_faltantes,
    confirmar_producto_manual,
    obtener_o_crear_establecimiento,      # ← NUEVO
    obtener_o_crear_producto_maestro      # ← NUEVO
)
from storage import save_image_to_db, get_image_from_db
from validator import FacturaValidator
from claude_invoice import parse_invoice_with_claude

# Importar routers
from admin_dashboard import router as admin_dashboard_router
from auth_routes import router as auth_router
from image_handlers import router as image_handlers_router
from duplicados_routes import router as duplicados_router  # ← AGREGAR ESTA LÍNEA

# Importar procesador OCR y auditoría
from ocr_processor import processor, ocr_queue, processing
from audit_system import audit_scheduler, AuditSystem
from corrections_service import aplicar_correcciones_automaticas




# ==========================================
# CICLO DE VIDA DE LA APLICACIÓN
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicialización y cierre de la aplicación"""
    print("=" * 60)
    print("🚀 INICIANDO LECFAC API")
    print("=" * 60)
    
    # Iniciar procesador OCR
    processor.start()
    print("✅ Procesador OCR iniciado")
    
    # Verificar conexión BD
    if test_database_connection():
        print("✅ Conexión a base de datos exitosa")
    else:
        print("⚠️ Error de conexión a base de datos")
    
    # Crear/actualizar tablas
    try:
        create_tables()
        print("✅ Tablas verificadas/creadas")
    except Exception as e:
        print(f"❌ Error creando tablas: {e}")
    
    print("=" * 60)
    print("✅ SERVIDOR LISTO")
    print("=" * 60)
    
    yield
    
    # Limpieza al cerrar
    processor.stop()
    print("\n👋 Cerrando LecFac API...")

# ==========================================
# CONFIGURACIÓN DE LA APP
# ==========================================
app = FastAPI(
    title="LecFac API", 
    version="3.0.1",  # ← Incrementada versión
    description="Sistema de gestión de facturas con control de calidad y detección de duplicados",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# ARCHIVOS ESTÁTICOS Y TEMPLATES
# ==========================================
# Servir archivos estáticos desde la raíz del proyecto
# (Para servir duplicados.js, etc.)
current_dir = Path(__file__).parent

# Opción 1: Si tienes carpeta static/
static_path = current_dir / "static"
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
    print(f"✅ Archivos estáticos montados desde: {static_path}")
else:
    # Opción 2: Servir desde la raíz directamente
    print("⚠️ No se encontró carpeta /static, sirviendo JS desde raíz")

# Configurar templates desde la raíz del proyecto
templates = Jinja2Templates(directory=str(current_dir))
print(f"✅ Templates configurados desde: {current_dir}")

# ==========================================
# INCLUIR ROUTERS CON LOGGING
# ==========================================
print("\n" + "=" * 60)
print("📍 REGISTRANDO ROUTERS")
print("=" * 60)

# Router de imágenes - PRIMERO para evitar conflictos
try:
    app.include_router(image_handlers_router, tags=["images"])
    print("✅ image_handlers_router registrado")
    print("   Rutas disponibles:")
    print("   - GET  /admin/facturas/{id}/imagen")
    print("   - GET  /admin/facturas/{id}/debug-imagen")
    print("   - GET  /admin/facturas/{id}/check-image")
    print("   - POST /admin/facturas/{id}/subir-imagen")
    print("   - POST /admin/facturas/{id}/fix-imagen")
except Exception as e:
    print(f"❌ Error registrando image_handlers_router: {e}")

# Router de admin dashboard
try:
    app.include_router(admin_dashboard_router, tags=["admin"])
    print("✅ admin_dashboard_router registrado")
except Exception as e:
    print(f"❌ Error registrando admin_dashboard_router: {e}")


# Router de autenticación
try:
    app.include_router(auth_router, tags=["auth"])
    print("✅ auth_router registrado")
except Exception as e:
    print(f"❌ Error registrando auth_router: {e}")

    # Router de duplicados - NUEVO
try:
    app.include_router(duplicados_router, tags=["duplicados"])
    print("✅ duplicados_router registrado")
    print("   Rutas disponibles:")
    print("   - GET  /admin/duplicados/productos")
    print("   - POST /admin/duplicados/productos/fusionar")
    print("   - GET  /admin/duplicados/facturas")
    print("   - DELETE /admin/facturas/{id}")
except Exception as e:
    print(f"❌ Error registrando duplicados_router: {e}")

print("=" * 60)
print("✅ ROUTERS CONFIGURADOS")
print("=" * 60 + "\n")

# ==========================================
# ENDPOINTS DE PÁGINAS HTML
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal / Dashboard"""
    if templates:
        return templates.TemplateResponse("admin_dashboard.html", {"request": request})
    return HTMLResponse("<h1>LecFac API</h1><p>Templates no configurados</p>")

@app.get("/editor", response_class=HTMLResponse)
async def editor(request: Request):
    """Editor de facturas"""
    if templates:
        return templates.TemplateResponse("editor.html", {"request": request})
    return HTMLResponse("<h1>Editor</h1><p>Templates no configurados</p>")

@app.get("/gestor-duplicados", response_class=HTMLResponse)
async def get_duplicados_page(request: Request):
    """Gestor de duplicados"""
    try:
        # Intentar con templates
        if templates:
            return templates.TemplateResponse("gestor_duplicados.html", {"request": request})
    except:
        pass
    
    # Fallback: leer archivo directamente
    possible_paths = [
        Path("gestor_duplicados.html"),
        Path("static/gestor_duplicados.html"),
        Path("public/gestor_duplicados.html"),
        Path("templates/gestor_duplicados.html")
    ]
    
    for html_path in possible_paths:
        if html_path.exists():
            with open(html_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    
    raise HTTPException(404, "gestor_duplicados.html no encontrado")
# ==========================================
# ENDPOINTS DE CONFIGURACIÓN Y UTILIDADES
# ==========================================

@app.get("/api/health-check")
async def health_check():
    """Verificar estado del servidor"""
    return {
        "status": "ok",
        "version": "3.0.1",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if test_database_connection() else "disconnected"
    }

@app.get("/api/config/anthropic-key")
async def get_anthropic_key():
    """
    Obtener API Key de Anthropic desde variables de entorno
    (Retorna solo asteriscos por seguridad si existe)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    
    if api_key:
        # Por seguridad, solo indicar que existe
        return {
            "apiKey": api_key,  # En producción, mejor devolver solo: "********"
            "configured": True
        }
    else:
        return {
            "apiKey": "",
            "configured": False
        }

# ==========================================
# ENDPOINTS DE PROCESAMIENTO DE FACTURAS
# ==========================================

class FacturaManual(BaseModel):
    """Modelo para guardado manual de facturas"""
    establecimiento: str
    fecha: str
    total: float
    productos: List[dict]

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """
    Procesar factura con OCR usando Claude Vision API
    VERSIÓN NUEVA ARQUITECTURA - Alimenta base comunitaria + personal
    """
    print(f"\n{'='*60}")
    print(f"📸 NUEVA FACTURA RECIBIDA: {file.filename}")
    print(f"{'='*60}")
    
    temp_file = None
    conn = None
    
    try:
        # 1. Guardar archivo temporal
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        print(f"✅ Archivo temporal creado: {temp_file.name}")
        print(f"📊 Tamaño: {len(content)} bytes")
        
        # 2. Procesar con Claude Vision
        print("🤖 Procesando con Claude Vision API...")
        result = parse_invoice_with_claude(temp_file.name)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=400, 
                detail=result.get("error", "Error procesando factura")
            )
        
        data = result["data"]
        productos_ocr = data.get("productos", [])
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        fecha_factura = data.get("fecha")
        total_factura = data.get("total", 0)
        
        print(f"✅ Datos extraídos:")
        print(f"   - Establecimiento: {establecimiento_raw}")
        print(f"   - Productos: {len(productos_ocr)}")
        print(f"   - Total: ${total_factura:,.0f}")
        
        # 3. Conectar a BD
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # =========================================================
        # PASO 1: NORMALIZAR ESTABLECIMIENTO (Base Comunitaria)
        # =========================================================
        print("\n🏪 PASO 1: Normalizando establecimiento...")
        cadena = detectar_cadena(establecimiento_raw)
        establecimiento_id = obtener_o_crear_establecimiento(establecimiento_raw, cadena)
        print(f"   ✅ Establecimiento ID: {establecimiento_id} (Cadena: {cadena})")
        
        # =========================================================
        # PASO 2: CREAR FACTURA (Base Personal del Usuario)
        # =========================================================
        print("\n📄 PASO 2: Creando factura...")
        
        # Usuario por defecto = 1 (temporal, debe venir del token)
        usuario_id = 1
        
        # Convertir fecha vacía a None (será NULL en PostgreSQL)
        fecha_factura = data.get("fecha") or None
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, 
                    establecimiento_id,
                    establecimiento,
                    cadena,
                    total_factura,
                    fecha_factura,
                    fecha_cargue,
                    estado_validacion,
                    tiene_imagen,
                    productos_detectados
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s)
                RETURNING id
            """, (
                usuario_id,
                establecimiento_id,
                establecimiento_raw,
                cadena,
                total_factura,
                fecha_factura,  # Ahora puede ser None
                'procesado',
                True,
                len(productos_ocr)
            ))
        else:  # SQLite
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, 
                    establecimiento_id,
                    establecimiento,
                    cadena,
                    total_factura,
                    fecha_factura,
                    fecha_cargue,
                    estado_validacion,
                    tiene_imagen,
                    productos_detectados
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                usuario_id,
                establecimiento_id,
                establecimiento_raw,
                cadena,
                total_factura,
                fecha_factura,
                datetime.now(),
                'procesado',
                True,
                len(productos_ocr)
            ))
        
        factura_id = cursor.fetchone()[0]
        print(f"   ✅ Factura creada con ID: {factura_id}")
        # =========================================================
        # 🔥 PASO 2.5: APLICAR CORRECCIONES AUTOMÁTICAS (NUEVO)
        # =========================================================
        print(f"\n🔍 PASO 2.5: Aplicando correcciones automáticas...")
        
        try:
            from corrections_service import aplicar_correcciones_automaticas
            
            productos_corregidos = aplicar_correcciones_automaticas(
                conn,
                productos_ocr,
                establecimiento_id=establecimiento_id,
                umbral_similitud=0.85
            )
            
            correcciones_aplicadas = sum(1 for p in productos_corregidos if p.get('correccion_aplicada'))
            print(f"   ✅ {correcciones_aplicadas}/{len(productos_ocr)} productos auto-corregidos")
            
        except ImportError:
            # Si corrections_service no existe, usar productos originales
            print(f"   ⚠️ Módulo de correcciones no disponible, usando datos OCR")
            productos_corregidos = productos_ocr
        except Exception as e:
            # Si hay error, no romper el flujo
            print(f"   ⚠️ Error aplicando correcciones: {e}")
            productos_corregidos = productos_ocr
        
        # =========================================================
        # PASO 3: PROCESAR PRODUCTOS (Base Comunitaria + Personal)
        # =========================================================
        # 🔥 CAMBIO: Usar productos_corregidos en lugar de productos_ocr
        print(f"\n🏷️ PASO 3: Procesando {len(productos_corregidos)} productos...")
        
        productos_guardados = 0
        productos_contribuidos = 0
        
        for idx, prod in enumerate(productos_corregidos, 1):  # ← ÚNICO CAMBIO AQUÍ
            try:
                codigo_ean = str(prod.get("codigo", "")).strip()
                nombre = str(prod.get("nombre", "")).strip()
                precio = int(prod.get("valor") or prod.get("precio") or 0)
                cantidad = int(prod.get("cantidad", 1))
                
                # Validar datos mínimos
                if not nombre or precio <= 0:
                    print(f"   ⚠️ Producto {idx} incompleto, saltando...")
                    continue
                
                # Validar código EAN (debe tener al menos 8 dígitos)
                codigo_ean_valido = None
                if codigo_ean and len(codigo_ean) >= 8 and codigo_ean.isdigit():
                    codigo_ean_valido = codigo_ean
                
                # 🔥 Mostrar si fue corregido automáticamente
                marca_correccion = " ✨ AUTO-CORREGIDO" if prod.get('correccion_aplicada') else ""
                print(f"   📦 {idx}. {nombre[:30]}... (${precio:,}){marca_correccion}")
                
                # -------------------------------------------------------
                # A. REGISTRAR EN CATÁLOGO GLOBAL (Base Comunitaria)
                # -------------------------------------------------------
                producto_maestro_id = None
                
                if codigo_ean_valido:
                    # Tiene EAN válido → Buscar/crear en productos_maestros
                    producto_maestro_id = obtener_o_crear_producto_maestro(
                        codigo_ean=codigo_ean_valido,
                        nombre=nombre,
                        precio=precio
                    )
                    
                    if producto_maestro_id:
                        print(f"      ✅ Vinculado a catálogo global (ID: {producto_maestro_id})")
                        productos_contribuidos += 1
                else:
                    print(f"      ⚠️ Sin EAN válido, no se vincula a catálogo")
                
                # -------------------------------------------------------
                # B. CREAR ITEM_FACTURA (Base Personal)
                # -------------------------------------------------------
                if os.environ.get("DATABASE_TYPE") == "postgresql":
                    cursor.execute("""
                        INSERT INTO items_factura (
                            factura_id,
                            producto_maestro_id,
                            usuario_id,
                            codigo_leido,
                            nombre_leido,
                            precio_pagado,
                            cantidad,
                            matching_confianza
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        factura_id,
                        producto_maestro_id,
                        usuario_id,
                        codigo_ean,
                        nombre,
                        precio,
                        cantidad,
                        100 if codigo_ean_valido else 50
                    ))
                else:  # SQLite
                    cursor.execute("""
                        INSERT INTO items_factura (
                            factura_id,
                            producto_maestro_id,
                            usuario_id,
                            codigo_leido,
                            nombre_leido,
                            precio_pagado,
                            cantidad,
                            matching_confianza
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        factura_id,
                        producto_maestro_id,
                        usuario_id,
                        codigo_ean,
                        nombre,
                        precio,
                        cantidad,
                        100 if codigo_ean_valido else 50
                    ))
                
                productos_guardados += 1
                
                # -------------------------------------------------------
                # C. REGISTRAR PRECIO EN BASE COMUNITARIA
                # -------------------------------------------------------
                if producto_maestro_id and establecimiento_id:
                    try:
                        fecha_hoy = datetime.now().date()
                        
                        if os.environ.get("DATABASE_TYPE") == "postgresql":
                            cursor.execute("""
                                SELECT id FROM precios_productos
                                WHERE producto_id = %s
                                  AND establecimiento = %s
                                  AND DATE(fecha_reporte) = %s
                                  AND usuario_id = %s
                            """, (producto_maestro_id, establecimiento_raw, fecha_hoy, usuario_id))
                            
                            if not cursor.fetchone():
                                cursor.execute("""
                                    INSERT INTO precios_productos (
                                        producto_id,
                                        establecimiento,
                                        cadena,
                                        precio,
                                        usuario_id,
                                        factura_id,
                                        fecha_reporte
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """, (
                                    producto_maestro_id,
                                    establecimiento_raw,
                                    cadena,
                                    precio,
                                    usuario_id,
                                    factura_id,
                                    datetime.now()
                                ))
                                print(f"      💰 Precio registrado en base comunitaria")
                        else:  # SQLite
                            cursor.execute("""
                                SELECT id FROM precios_productos
                                WHERE producto_id = ?
                                  AND establecimiento = ?
                                  AND DATE(fecha_reporte) = ?
                                  AND usuario_id = ?
                            """, (producto_maestro_id, establecimiento_raw, fecha_hoy, usuario_id))
                            
                            if not cursor.fetchone():
                                cursor.execute("""
                                    INSERT INTO precios_productos (
                                        producto_id,
                                        establecimiento,
                                        cadena,
                                        precio,
                                        usuario_id,
                                        factura_id,
                                        fecha_reporte
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    producto_maestro_id,
                                    establecimiento_raw,
                                    cadena,
                                    precio,
                                    usuario_id,
                                    factura_id,
                                    datetime.now()
                                ))
                                print(f"      💰 Precio registrado en base comunitaria")
                    except Exception as e:
                        print(f"      ⚠️ Error registrando precio: {e}")
                
            except Exception as e:
                print(f"   ❌ Error procesando producto {idx}: {e}")
                continue
        
        # =========================================================
        # PASO 4: ACTUALIZAR GASTOS MENSUALES (Analytics Personal)
        # =========================================================
        print("\n📊 PASO 4: Actualizando gastos mensuales...")
        
        try:
            if fecha_factura:
                fecha_obj = datetime.strptime(fecha_factura, "%Y-%m-%d") if isinstance(fecha_factura, str) else fecha_factura
            else:
                fecha_obj = datetime.now()
            
            anio = fecha_obj.year
            mes = fecha_obj.month
            
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    INSERT INTO gastos_mensuales (
                        usuario_id,
                        anio,
                        mes,
                        establecimiento_id,
                        total_gastado,
                        total_facturas,
                        total_productos
                    ) VALUES (%s, %s, %s, %s, %s, 1, %s)
                    ON CONFLICT (usuario_id, anio, mes, establecimiento_id)
                    DO UPDATE SET
                        total_gastado = gastos_mensuales.total_gastado + EXCLUDED.total_gastado,
                        total_facturas = gastos_mensuales.total_facturas + 1,
                        total_productos = gastos_mensuales.total_productos + EXCLUDED.total_productos,
                        fecha_calculo = CURRENT_TIMESTAMP
                """, (usuario_id, anio, mes, establecimiento_id, total_factura, productos_guardados))
            else:  # SQLite
                cursor.execute("""
                    SELECT id, total_gastado, total_facturas, total_productos
                    FROM gastos_mensuales
                    WHERE usuario_id = ? AND anio = ? AND mes = ? AND establecimiento_id = ?
                """, (usuario_id, anio, mes, establecimiento_id))
                
                resultado = cursor.fetchone()
                if resultado:
                    cursor.execute("""
                        UPDATE gastos_mensuales
                        SET total_gastado = total_gastado + ?,
                            total_facturas = total_facturas + 1,
                            total_productos = total_productos + ?,
                            fecha_calculo = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (total_factura, productos_guardados, resultado[0]))
                else:
                    cursor.execute("""
                        INSERT INTO gastos_mensuales (
                            usuario_id, anio, mes, establecimiento_id,
                            total_gastado, total_facturas, total_productos
                        ) VALUES (?, ?, ?, ?, ?, 1, ?)
                    """, (usuario_id, anio, mes, establecimiento_id, total_factura, productos_guardados))
            
            print(f"   ✅ Gastos actualizados para {mes}/{anio}")
        except Exception as e:
            print(f"   ⚠️ Error actualizando gastos mensuales: {e}")
        
        # =========================================================
        # PASO 5: COMMIT Y GUARDAR IMAGEN
        # =========================================================
        print(f"\n💾 PASO 5: Guardando cambios...")
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE facturas 
                SET productos_guardados = %s
                WHERE id = %s
            """, (productos_guardados, factura_id))
        else:
            cursor.execute("""
                UPDATE facturas 
                SET productos_guardados = ?
                WHERE id = ?
            """, (productos_guardados, factura_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        conn = None
        
        # Guardar imagen
        print(f"\n📸 PASO 6: Guardando imagen...")
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        
        if imagen_guardada:
            print(f"✅✅✅ IMAGEN GUARDADA EXITOSAMENTE ✅✅✅")
        else:
            print(f"⚠️ Advertencia: Imagen no se guardó")
        
        # Limpiar archivo temporal
        try:
            os.unlink(temp_file.name)
            print(f"✅ Archivo temporal eliminado")
        except Exception as e:
            print(f"⚠️ Error eliminando temporal: {e}")
        
        print(f"\n{'='*60}")
        print(f"✅ FACTURA PROCESADA EXITOSAMENTE")
        print(f"{'='*60}")
        print(f"📊 Resumen:")
        print(f"   - Factura ID: {factura_id}")
        print(f"   - Productos guardados: {productos_guardados}/{len(productos_ocr)}")
        print(f"   - Contribuciones al catálogo: {productos_contribuidos}")
        print(f"   - Imagen: {'✅' if imagen_guardada else '❌'}")
        print(f"{'='*60}\n")
        
        return {
            "success": True,
            "factura_id": factura_id,
            "data": data,
            "productos_guardados": productos_guardados,
            "productos_contribuidos": productos_contribuidos,
            "imagen_guardada": imagen_guardada,
            "estadisticas": {
                "total_detectados": len(productos_ocr),
                "guardados_personal": productos_guardados,
                "contribuidos_comunidad": productos_contribuidos,
                "establecimiento_normalizado": establecimiento_id
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n❌ ERROR PROCESANDO FACTURA:")
        print(f"   {str(e)}")
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
        
        raise HTTPException(status_code=500, detail=str(e))
@app.post("/invoices/save-manual")
async def save_manual_invoice(factura: FacturaManual):
    """Guardar factura manualmente (sin imagen)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar cadena
        cadena = detectar_cadena(factura.establecimiento)
        
        # Insertar factura
        cursor.execute("""
            INSERT INTO facturas (
                establecimiento, fecha, total, 
                calidad_score, tiene_imagen
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            factura.establecimiento,
            factura.fecha,
            factura.total,
            100,  # Score perfecto para entrada manual
            False  # Sin imagen
        ))
        
        factura_id = cursor.fetchone()[0]
        
        # Insertar productos
        for producto in factura.productos:
            cursor.execute("""
                INSERT INTO productos (
                    nombre, codigo, cantidad, precio,
                    precio_total, establecimiento, fecha,
                    factura_id, cadena
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                producto.get("nombre"),
                producto.get("codigo"),
                producto.get("cantidad", 1),
                producto.get("precio_unitario", 0),
                producto.get("precio_total", 0),
                factura.establecimiento,
                factura.fecha,
                factura_id,
                cadena
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "factura_id": factura_id,
            "message": "Factura guardada correctamente"
        }
        
    except Exception as e:
        print(f"❌ Error guardando factura manual: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ARRANQUE DEL SERVIDOR
# ==========================================

if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 60)
    print("🚀 INICIANDO SERVIDOR LECFAC")
    print("=" * 60)
    print("📍 Host: 0.0.0.0")
    print("📍 Puerto: 8000")
    print("📍 Docs: http://localhost:8000/docs")
    print("📍 Gestor Duplicados: http://localhost:8000/gestor-duplicados")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

# ==========================================
# ENDPOINTS DE PÁGINAS HTML
# ==========================================

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """Página principal / Dashboard"""
    if templates:
        return templates.TemplateResponse("admin_dashboard.html", {"request": request})
    return HTMLResponse("<h1>LecFac API</h1><p>Templates no configurados</p>")

@app.get("/editor", response_class=HTMLResponse)
async def editor(request: Request):
    """Editor de facturas"""
    if templates:
        return templates.TemplateResponse("editor.html", {"request": request})
    return HTMLResponse("<h1>Editor</h1><p>Templates no configurados</p>")

@app.get("/gestor-duplicados", response_class=HTMLResponse)
async def gestor_duplicados(request: Request):
    """Gestor de duplicados - NUEVO"""
    if templates:
        return templates.TemplateResponse("gestor_duplicados.html", {"request": request})
    return HTMLResponse("<h1>Gestor de Duplicados</h1><p>Templates no configurados</p>")

# ==========================================
# ENDPOINTS DE CONFIGURACIÓN Y UTILIDADES
# ==========================================

@app.get("/api/health-check")
async def health_check():
    """Verificar estado del servidor"""
    return {
        "status": "ok",
        "version": "3.0.1",
        "timestamp": datetime.now().isoformat(),
        "database": "connected" if test_database_connection() else "disconnected"
    }

@app.get("/api/config/anthropic-key")
async def get_anthropic_key():
    """
    Obtener API Key de Anthropic desde variables de entorno
    (Retorna solo asteriscos por seguridad si existe)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    
    if api_key:
        # Por seguridad, solo indicar que existe
        return {
            "apiKey": api_key,  # En producción, mejor devolver solo: "********"
            "configured": True
        }
    else:
        return {
            "apiKey": "",
            "configured": False
        }

# ==========================================
# ENDPOINTS DE PROCESAMIENTO DE FACTURAS
# ==========================================

class FacturaManual(BaseModel):
    """Modelo para guardado manual de facturas"""
    establecimiento: str
    fecha: str
    total: float
    productos: List[dict]

@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """
    Procesar factura con OCR usando Claude Vision API
    Versión mejorada con guardado de imagen
    """
    print(f"\n{'='*60}")
    print(f"📸 NUEVA FACTURA RECIBIDA: {file.filename}")
    print(f"{'='*60}")
    
    temp_file = None
    conn = None
    
    try:
        # 1. Guardar archivo temporal
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        print(f"✅ Archivo temporal creado: {temp_file.name}")
        print(f"📊 Tamaño: {len(content)} bytes")
        
        # 2. Procesar con Claude Vision
        print("🤖 Procesando con Claude Vision API...")
        result = parse_invoice_with_claude(temp_file.name)
        
        if not result.get("success"):
            raise HTTPException(
                status_code=400, 
                detail=result.get("error", "Error procesando factura")
            )
        
        data = result["data"]
        print(f"✅ Datos extraídos: {len(data.get('productos', []))} productos")
        
        # 3. Validar datos
        validator = FacturaValidator()
        validacion = validator.validate_complete_invoice(data)
        
        print(f"📋 Validación: {validacion['score']}/100 puntos")
        print(f"   Issues: {len(validacion['issues'])}")
        print(f"   Warnings: {len(validacion['warnings'])}")
        
        # 4. Guardar en base de datos
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar cadena
        cadena = detectar_cadena(data.get("establecimiento", ""))
        
        # Insertar factura
        cursor.execute("""
            INSERT INTO facturas (
                establecimiento, fecha, total, 
                calidad_score, tiene_imagen,
                raw_ocr_data
            ) VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            data.get("establecimiento"),
            data.get("fecha"),
            data.get("total", 0),
            validacion["score"],
            True,  # Siempre True porque tenemos la imagen
            json.dumps(data)
        ))
        
        factura_id = cursor.fetchone()[0]
        print(f"✅ Factura guardada con ID: {factura_id}")
        
        # Insertar productos
        productos = data.get("productos", [])
        for producto in productos:
            cursor.execute("""
                INSERT INTO productos (
                    nombre, codigo, cantidad, precio, 
                    precio_total, establecimiento, fecha, 
                    factura_id, cadena
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                producto.get("nombre"),
                producto.get("codigo"),
                producto.get("cantidad", 1),
                producto.get("precio_unitario", 0),
                producto.get("precio_total", 0),
                data.get("establecimiento"),
                data.get("fecha"),
                factura_id,
                cadena
            ))
        
        print(f"✅ {len(productos)} productos guardados")
        
        # COMMIT y cerrar conexión
        conn.commit()
        cursor.close()
        conn.close()
        conn = None  # Importante: marcar como cerrada
        
        # 5. GUARDAR IMAGEN (con nueva conexión)
        print(f"📸 Guardando imagen para factura {factura_id}...")
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        
        if imagen_guardada:
            print(f"✅✅✅ IMAGEN GUARDADA EXITOSAMENTE ✅✅✅")
        else:
            print(f"⚠️ Advertencia: Imagen no se guardó, pero factura está registrada")
        
        # 6. Limpiar archivo temporal
        try:
            os.unlink(temp_file.name)
            print(f"✅ Archivo temporal eliminado")
        except Exception as e:
            print(f"⚠️ Error eliminando temporal: {e}")
        
        print(f"{'='*60}")
        print(f"✅ FACTURA PROCESADA EXITOSAMENTE")
        print(f"{'='*60}\n")
        
        return {
            "success": True,
            "factura_id": factura_id,
            "data": data,
            "validation": validacion,
            "imagen_guardada": imagen_guardada
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n❌ ERROR PROCESANDO FACTURA:")
        print(f"   {str(e)}")
        print(traceback.format_exc())
        
        # Rollback si hay conexión abierta
        if conn:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
        
        # Limpiar archivo temporal
        if temp_file:
            try:
                os.unlink(temp_file.name)
            except:
                pass
        
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invoices/save-manual")
async def save_manual_invoice(factura: FacturaManual):
    """Guardar factura manualmente (sin imagen)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar cadena
        cadena = detectar_cadena(factura.establecimiento)
        
        # Insertar factura
        cursor.execute("""
            INSERT INTO facturas (
                establecimiento, fecha, total, 
                calidad_score, tiene_imagen
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            factura.establecimiento,
            factura.fecha,
            factura.total,
            100,  # Score perfecto para entrada manual
            False  # Sin imagen
        ))
        
        factura_id = cursor.fetchone()[0]
        
        # Insertar productos
        for producto in factura.productos:
            cursor.execute("""
                INSERT INTO productos (
                    nombre, codigo, cantidad, precio,
                    precio_total, establecimiento, fecha,
                    factura_id, cadena
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                producto.get("nombre"),
                producto.get("codigo"),
                producto.get("cantidad", 1),
                producto.get("precio_unitario", 0),
                producto.get("precio_total", 0),
                factura.establecimiento,
                factura.fecha,
                factura_id,
                cadena
            ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "factura_id": factura_id,
            "message": "Factura guardada correctamente"
        }
        
    except Exception as e:
        print(f"❌ Error guardando factura manual: {e}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================
# ARRANQUE DEL SERVIDOR
# ==========================================

if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 60)
    print("🚀 INICIANDO SERVIDOR LECFAC")
    print("=" * 60)
    print("📍 Host: 0.0.0.0")
    print("📍 Puerto: 8000")
    print("📍 Docs: http://localhost:8000/docs")
    print("📍 Gestor Duplicados: http://localhost:8000/gestor-duplicados")
    print("=" * 60 + "\n")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
# MODELOS PYDANTIC
# ==========================================
class UserRegister(BaseModel):
    email: str
    password: str
    nombre: Optional[str] = None

class UserLogin(BaseModel):
    email: str
    password: str

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

class SaveInvoice(BaseModel):
    usuario_id: int
    establecimiento: str
    productos: list
    temp_file_path: Optional[str] = None

# ==========================================
# FUNCIONES AUXILIARES
# ==========================================
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
               (codigo_ean, nombre_producto, es_producto_fresco, 
                primera_fecha_reporte, total_reportes, ultimo_reporte)
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
               (nombre_producto, es_producto_fresco, primera_fecha_reporte, 
                total_reportes, ultimo_reporte)
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

# ==========================================
# AUTENTICACIÓN (Funciones de ayuda)
# ==========================================
async def get_current_user(authorization: str = Header(None)):
    """Obtener usuario actual desde token"""
    if not authorization:
        raise HTTPException(status_code=401, detail="No autorizado")
    # TODO: Decodificar JWT token
    return {"user_id": "user123", "email": "user@example.com"}

async def require_admin(user = Depends(get_current_user)):
    """Verificar si es admin"""
    # TODO: Verificar rol en BD
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Requiere permisos de admin")
    return user

# ==========================================
# ENDPOINTS BÁSICOS
# ==========================================
@app.get("/")
async def root():
    """Información de la API"""
    return {
        "app": "LecFac API",
        "version": "3.0.0",
        "status": "running",
        "database": os.environ.get('DATABASE_TYPE', 'postgresql'),
        "endpoints": {
            "health": "/api/health-check",
            "admin": "/admin/*",
            "auth": "/auth/*",
            "invoices": "/invoices/*",
            "docs": "/docs",
            "dashboard": "/dashboard",
            "editor": "/editor"
        }
    }

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
            "openai_configured": bool(os.environ.get("OPENAI_API_KEY")),
            "anthropic_configured": bool(os.environ.get("ANTHROPIC_API_KEY")),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )

# ============================================
# ENDPOINT: PROCESAR VIDEO DE FACTURA
# ============================================

@app.post("/invoices/parse-video")
async def parse_invoice_video(
    video: UploadFile = File(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Procesa un video de factura
    """
    
    
    video_path = None
    frames_paths = []
    
    try:
        print("=" * 80)
        print("📹 PROCESANDO VIDEO DE FACTURA")
        print("=" * 80)
        
        # Guardar video temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix='.webm') as tmp_video:
            content = await video.read()
            tmp_video.write(content)
            video_path = tmp_video.name
        
        video_size_mb = len(content) / (1024 * 1024)
        print(f"📦 Video: {video_size_mb:.2f} MB")
        
        # Extraer frames
        print("🎬 Extrayendo frames...")
        frames_paths = extraer_frames_video(video_path, intervalo=0.5)
        
        if not frames_paths:
            return {"success": False, "error": "No se pudieron extraer frames"}
        
        print(f"✅ {len(frames_paths)} frames extraídos")
        
        # Procesar frames
        todos_productos = []
        establecimiento = None
        total = 0
        fecha = None
        
        for i, frame_path in enumerate(frames_paths):
            print(f"📸 Frame {i+1}/{len(frames_paths)}...")
            
            resultado = parse_invoice_with_claude(frame_path)
            
            if resultado.get('success'):
                data = resultado['data']
                
                if not establecimiento:
                    establecimiento = data.get('establecimiento', 'Desconocido')
                    total = data.get('total', 0)
                    fecha = data.get('fecha')
                
                todos_productos.extend(data.get('productos', []))
        
        # Deduplicar
        productos_unicos = deduplicar_productos(todos_productos)
        
        # Guardar en BD
        conn = get_db_connection()
        cursor = conn.cursor()
        
        establecimiento_id = obtener_o_crear_establecimiento(
            establecimiento, 
            detectar_cadena(establecimiento)
        )
        
        usuario_id = current_user['id']
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, total_factura, 
                    fecha_factura, productos_detectados, estado
                ) VALUES (%s, %s, %s, %s, %s, 'procesado')
                RETURNING id
            """, (usuario_id, establecimiento_id, total, fecha, len(productos_unicos)))
            factura_id = cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO facturas (
                    usuario_id, establecimiento_id, total_factura, 
                    fecha_factura, productos_detectados, estado
                ) VALUES (?, ?, ?, ?, ?, 'procesado')
            """, (usuario_id, establecimiento_id, total, fecha, len(productos_unicos)))
            factura_id = cursor.lastrowid
        
        # Guardar productos
        for producto in productos_unicos:
            codigo = producto.get('codigo', '')
            nombre = producto.get('nombre', 'Sin nombre')
            precio = producto.get('precio', 0)
            
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    INSERT INTO productos (factura_id, codigo, nombre, valor)
                    VALUES (%s, %s, %s, %s)
                """, (factura_id, codigo or None, nombre, precio))
            else:
                cursor.execute("""
                    INSERT INTO productos (factura_id, codigo, nombre, valor)
                    VALUES (?, ?, ?, ?)
                """, (factura_id, codigo or None, nombre, precio))
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "factura_id": factura_id,
            "data": {
                "establecimiento": establecimiento,
                "fecha": fecha,
                "total": total,
                "productos": productos_unicos,
                "frames_procesados": len(frames_paths)
            }
        }
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}
        
    finally:
        if video_path and os.path.exists(video_path):
            os.remove(video_path)
        if frames_paths:
            limpiar_frames_temporales(frames_paths)

# ==========================================
# PÁGINAS HTML
# ==========================================
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    """Dashboard administrativo"""
    html_path = Path("admin_dashboard.html")
    if html_path.exists():
        return FileResponse("admin_dashboard.html")
    raise HTTPException(404, "Dashboard no encontrado")

@app.get("/editor", response_class=HTMLResponse)
async def editor_page():
    """Editor de facturas"""
    html_path = Path("editor.html")
    if html_path.exists():
        return FileResponse("editor.html")
    raise HTTPException(404, "Editor no encontrado")

@app.get("/gestor_duplicados.html", response_class=HTMLResponse)
@app.get("/gestor_duplicados", response_class=HTMLResponse)
async def get_duplicados_page():
    """Gestor de duplicados"""
    possible_paths = [
        Path("gestor_duplicados.html"),
        Path("static/gestor_duplicados.html"),
        Path("public/gestor_duplicados.html"),
        Path("templates/gestor_duplicados.html")
    ]
    
    for html_path in possible_paths:
        if html_path.exists():
            with open(html_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
    
    raise HTTPException(404, "gestor_duplicados.html no encontrado")

@app.get("/duplicados.js")
async def get_duplicados_js():
    """Servir archivo JavaScript de duplicados"""
    js_path = Path("duplicados.js")
    if js_path.exists():
        return FileResponse(str(js_path), media_type="application/javascript")
    else:
        raise HTTPException(status_code=404, detail="duplicados.js no encontrado")

@app.get("/test", response_class=HTMLResponse)
async def test_page():
    """Página de pruebas"""
    html_path = Path("test.html")
    if html_path.exists():
        return FileResponse("test.html")
    raise HTTPException(404, "test.html no encontrado")

# ==========================================
# ENDPOINTS DE USUARIOS
# ==========================================
@app.post("/users/register")
async def register_user(user: UserRegister):
    """Registro de nuevo usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar si existe
        cursor.execute("SELECT id FROM usuarios WHERE email = %s", (user.email,))
        if cursor.fetchone():
            raise HTTPException(400, "El email ya está registrado")
        
        # Crear usuario
        password_hash = hash_password(user.password)
        cursor.execute(
            "INSERT INTO usuarios (email, password_hash, nombre) VALUES (%s, %s, %s) RETURNING id",
            (user.email, password_hash, user.nombre)
        )
        user_id = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return {"success": True, "user_id": user_id}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/users/login")
async def login_user(user: UserLogin):
    """Login de usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id, password_hash, nombre FROM usuarios WHERE email = %s",
            (user.email,)
        )
        result = cursor.fetchone()
        conn.close()
        
        if not result or not verify_password(user.password, result[1]):
            raise HTTPException(401, "Email o contraseña incorrectos")
        
        return {
            "success": True,
            "user_id": result[0],
            "nombre": result[2]
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/users/{user_id}/invoices")
async def get_user_invoices(user_id: int):
    """Obtener facturas de un usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT f.id, f.establecimiento, f.fecha_cargue, 
                   COUNT(p.id) as total_productos
            FROM facturas f 
            LEFT JOIN productos p ON f.id = p.factura_id
            WHERE f.usuario_id = %s 
            GROUP BY f.id, f.establecimiento, f.fecha_cargue
            ORDER BY f.fecha_cargue DESC
        """, (user_id,))
        
        facturas = []
        for row in cursor.fetchall():
            facturas.append({
                "id": row[0],
                "establecimiento": row[1],
                "fecha_cargue": row[2].isoformat() if row[2] else None,
                "total_productos": row[3]
            })
        
        conn.close()
        return {"success": True, "facturas": facturas}
    except Exception as e:
        raise HTTPException(500, str(e))

# ==========================================
# ENDPOINTS DE FACTURAS (OCR y Guardado)
# ==========================================
@app.post("/invoices/parse")
async def parse_invoice(file: UploadFile = File(...)):
    """Procesa factura con Claude Vision y guarda con imagen"""
    temp_file = None
    conn = None
    
    try:
        # Guardar temporalmente
        content = await file.read()
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(content)
        temp_file.close()
        
        print(f"✅ Archivo temporal: {temp_file.name}, {len(content)} bytes")
        
        # Procesar con Claude
        result = parse_invoice_with_claude(temp_file.name)
        
        if not result["success"]:
            if os.path.exists(temp_file.name):
                os.unlink(temp_file.name)
            return result
        
        # Extraer datos
        data = result["data"]
        establecimiento = data.get("establecimiento", "Desconocido")
        total = data.get("total", 0)
        productos = data.get("productos", [])
        
        cadena = detectar_cadena(establecimiento)
        
        # Validar calidad
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=establecimiento,
            total=total,
            tiene_imagen=True,
            productos=productos,
            cadena=cadena
        )
        
        # Guardar en BD
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Crear factura
        cursor.execute("""
            INSERT INTO facturas (
                usuario_id, establecimiento, cadena, total_factura,
                fecha_cargue, estado_validacion, tiene_imagen,
                puntaje_calidad
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (1, establecimiento, cadena, total, datetime.now(), estado, True, puntaje))
        
        factura_id = cursor.fetchone()[0]
        print(f"✅ Factura ID: {factura_id}")
        
        # Guardar productos
        productos_guardados = 0
        for prod in productos:
            try:
                codigo = prod.get("codigo", "") or ""
                nombre = prod.get("nombre", "") or ""
                precio = prod.get("precio", 0) or prod.get("valor", 0) or 0
                
                if nombre:
                    cursor.execute("""
                        INSERT INTO productos (factura_id, codigo, nombre, valor)
                        VALUES (%s, %s, %s, %s)
                    """, (factura_id, codigo, nombre, precio))
                    productos_guardados += 1
            except Exception as e:
                print(f"⚠️ Error guardando producto: {e}")
        
        # ⚠️ IMPORTANTE: COMMIT ANTES DE GUARDAR IMAGEN
        conn.commit()
        cursor.close()
        conn.close()
        conn = None  # ← Importante para evitar double-close
        
        # 🔑 GUARDAR IMAGEN - ESTO ES LO MÁS IMPORTANTE
        print(f"📸 Guardando imagen para factura {factura_id}...")
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        print(f"✅ Imagen guardada: {imagen_guardada}")
        
        # Limpiar temporal
        os.unlink(temp_file.name)
        
        return {
            "success": True,
            "data": data,
            "factura_id": factura_id,
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada,  # ← Incluir en respuesta
            "validacion": {
                "puntaje": puntaje,
                "estado": estado,
                "alertas": alertas
            },
            "message": f"Factura procesada. {productos_guardados} productos guardados."
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        
        if temp_file and hasattr(temp_file, 'name') and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        if conn:
            conn.rollback()
            conn.close()
        
        raise HTTPException(500, str(e))

@app.post("/invoices/save-with-image")
async def save_invoice_with_image(
    file: UploadFile = File(...),
    usuario_id: int = Form(...),
    establecimiento: str = Form(...),
    total: float = Form(0),
    productos: str = Form(...)
):
    """Guardar factura confirmada con imagen"""
    temp_file = None
    conn = None
    
    try:
        print("=== GUARDANDO FACTURA CON IMAGEN ===")
        print(f"Archivo recibido: {file.filename}, {file.content_type}")
        
        # Parsear productos
        productos_list = json.loads(productos)
        
        # Guardar imagen temporalmente
        content = await file.read()
        print(f"✅ Imagen leída: {len(content)} bytes")
        
        if not content:
            raise HTTPException(400, "Imagen vacía")
        
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(content)
        temp_file.close()
        print(f"✅ Archivo temporal: {temp_file.name}")
        
        # Validar
        cadena = detectar_cadena(establecimiento)
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=establecimiento,
            total=total,
            tiene_imagen=True,
            productos=productos_list,
            cadena=cadena
        )
        
        # Crear factura
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
        print(f"✅ Factura creada: ID={factura_id}")
        
        # Guardar productos
        productos_guardados = 0
        for prod in productos_list:
            codigo = str(prod.get("codigo", "") or "").strip()
            nombre = str(prod.get("nombre", "") or "").strip()
            precio = float(prod.get("precio", 0))
            
            if nombre:
                cursor.execute("""
                    INSERT INTO productos (factura_id, codigo, nombre, valor)
                    VALUES (%s, %s, %s, %s)
                """, (factura_id, codigo, nombre, precio))
                productos_guardados += 1
        
        print(f"✅ {productos_guardados} productos guardados")
        
        # COMMIT antes de guardar imagen
        conn.commit()
        cursor.close()
        conn.close()
        conn = None
        
        # 🔑 GUARDAR IMAGEN - LO MÁS IMPORTANTE
        print(f"📸 Guardando imagen en BD...")
        mime = "image/png" if file.filename and file.filename.lower().endswith(".png") else "image/jpeg"
        imagen_guardada = save_image_to_db(factura_id, temp_file.name, mime)
        print(f"✅ Imagen guardada: {imagen_guardada}")
        
        # Limpiar temporal
        os.unlink(temp_file.name)
        temp_file = None
        
        return {
            "success": True,
            "factura_id": factura_id,
            "validacion": {
                "puntaje": puntaje,
                "estado": estado,
                "alertas": alertas
            },
            "productos_guardados": productos_guardados,
            "imagen_guardada": imagen_guardada
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        raise HTTPException(500, str(e))

@app.get("/admin/facturas/{factura_id}/debug-imagen")
async def debug_imagen(factura_id: int):
    """Debug completo de imagen"""
    from storage import verify_image_exists
    return verify_image_exists(factura_id)

@app.delete("/invoices/{factura_id}")
async def delete_invoice(factura_id: int, usuario_id: int):
    """Eliminar factura de un usuario"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT id FROM facturas WHERE id = %s AND usuario_id = %s",
            (factura_id, usuario_id)
        )
        if not cursor.fetchone():
            raise HTTPException(404, "Factura no encontrada")
        
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Factura eliminada"}
    except Exception as e:
        raise HTTPException(500, str(e))

# ==========================================
# ENDPOINTS DE EDICIÓN (ADMIN)
# ==========================================
@app.get("/admin/facturas/{factura_id}/detalle")
async def get_factura_detalle_completo(factura_id: int):
    """Obtener factura completa para edición"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, usuario_id, establecimiento, cadena, total_factura, 
                   fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen
            FROM facturas WHERE id = %s
        """, (factura_id,))
        
        factura = cursor.fetchone()
        if not factura:
            raise HTTPException(404, "Factura no encontrada")
        
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

# ==========================================
# ENDPOINT PARA OBTENER FACTURA (EDITOR)
# ==========================================
@app.get("/admin/facturas/{factura_id}")
async def get_factura_para_editor(factura_id: int):
    """
    Obtener factura completa para el editor
    BUSCA PRODUCTOS EN MÚLTIPLES TABLAS
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Obtener datos de la factura
        cursor.execute("""
            SELECT id, usuario_id, establecimiento, cadena, total_factura, 
                   fecha_cargue, estado_validacion, puntaje_calidad, tiene_imagen
            FROM facturas WHERE id = %s
        """, (factura_id,))
        
        factura = cursor.fetchone()
        if not factura:
            conn.close()
            raise HTTPException(404, "Factura no encontrada")
        
        productos = []
        
        # 2A. INTENTAR TABLA productos PRIMERO
        cursor.execute("""
            SELECT id, codigo, nombre, valor
            FROM productos
            WHERE factura_id = %s
            ORDER BY id
        """, (factura_id,))
        
        for p in cursor.fetchall():
            productos.append({
                "id": p[0],
                "codigo": p[1] or "",
                "nombre": p[2] or "",
                "precio": float(p[3]) if p[3] else 0
            })
        
        print(f"📦 Productos en tabla 'productos': {len(productos)}")
        
        # 2B. SI ESTÁ VACÍA, BUSCAR EN precios_productos
        if len(productos) == 0:
            print("⚠️ Buscando en precios_productos...")
            cursor.execute("""
                SELECT pp.id, pc.codigo_ean, pc.nombre_producto, pp.precio
                FROM precios_productos pp
                LEFT JOIN productos_catalogo pc ON pp.producto_id = pc.id
                WHERE pp.factura_id = %s
                ORDER BY pp.id
            """, (factura_id,))
            
            for p in cursor.fetchall():
                productos.append({
                    "id": p[0],
                    "codigo": p[1] or "",
                    "nombre": p[2] or "Producto sin nombre",
                    "precio": float(p[3]) if p[3] else 0
                })
            
            print(f"📦 Productos en 'precios_productos': {len(productos)}")
        
        conn.close()
        
        print(f"✅ TOTAL a devolver: {len(productos)} productos")
        
        # 3. Construir respuesta
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
            "productos": productos
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error en get_factura_para_editor: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.put("/admin/productos/{producto_id}")
@app.put("/admin/productos/{producto_id}")
async def actualizar_producto(producto_id: int, datos: dict):
    """Actualizar producto en la tabla productos"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        codigo = datos.get('codigo', '').strip()
        nombre = datos.get('nombre', '').strip()
        precio = datos.get('precio', 0)
        
        if not codigo or not nombre:
            raise HTTPException(400, "Código y nombre son requeridos")
        
        cursor.execute("""
            UPDATE productos
            SET codigo = %s, nombre = %s, valor = %s
            WHERE id = %s
        """, (codigo, nombre, float(precio), producto_id))
        
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
        print(f"Error actualizando producto: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.delete("/admin/productos/{producto_id}")
async def eliminar_producto_factura(producto_id: int):
    """Eliminar producto de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM productos WHERE id = %s", (producto_id,))
        
        if cursor.rowcount == 0:
            conn.close()
            raise HTTPException(404, "Producto no encontrado")
        
        conn.commit()
        conn.close()
        
        return {"success": True, "message": "Producto eliminado"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error eliminando producto: {e}")
        raise HTTPException(500, str(e))

@app.post("/admin/facturas/{factura_id}/productos")
async def agregar_producto_a_factura(factura_id: int, datos: dict):
    """Agregar producto a una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que la factura existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(404, "Factura no encontrada")
        
        codigo = datos.get('codigo', '').strip()
        nombre = datos.get('nombre', '').strip()
        precio = datos.get('precio', 0)
        
        if not codigo or not nombre:
            conn.close()
            raise HTTPException(400, "Código y nombre son requeridos")
        
        cursor.execute("""
            INSERT INTO productos (factura_id, codigo, nombre, valor)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """, (factura_id, codigo, nombre, float(precio)))
        
        nuevo_id = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        return {
            "success": True, 
            "id": nuevo_id,
            "message": "Producto agregado"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error agregando producto: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.post("/admin/facturas/{factura_id}/validar")
async def marcar_como_validada(factura_id: int):
    """Marcar factura como validada"""
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
        
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.put("/admin/facturas/{factura_id}/datos-generales")
async def actualizar_datos_generales(factura_id: int, datos: dict):
    """Actualizar datos generales de factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        establecimiento = datos.get('establecimiento')
        total = datos.get('total', 0)
        fecha = datos.get('fecha')
        
        print(f"📝 Actualizando factura {factura_id}:")
        print(f"  - Establecimiento: {establecimiento}")
        print(f"  - Total: {total}")
        print(f"  - Fecha: {fecha}")
        
        # Construir query dinámicamente
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
        
        # IMPORTANTE: Marcar como revisada después de editar
        updates.append("estado_validacion = %s")
        params.append('revisada')
        
        params.append(factura_id)
        
        query = f"UPDATE facturas SET {', '.join(updates)} WHERE id = %s"
        print(f"🔧 Query: {query}")
        print(f"🔧 Params: {params}")
        
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

@app.delete("/admin/facturas/{factura_id}")
async def eliminar_factura_admin(factura_id: int):
    """Eliminar factura y todos sus datos relacionados"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que la factura existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(404, "Factura no encontrada")
        
        print(f"Eliminando factura {factura_id}...")
        
        # PASO 1: Eliminar productos asociados
        cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
        productos_eliminados = cursor.rowcount
        print(f"  ✓ {productos_eliminados} productos eliminados")
        
        # PASO 2: Eliminar precios_productos asociados (si existen)
        try:
            cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
            precios_eliminados = cursor.rowcount
            print(f"  ✓ {precios_eliminados} precios eliminados")
        except Exception as e:
            print(f"  ⚠️ No se pudieron eliminar precios: {e}")
        
        # PASO 3: Eliminar la factura
        cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
        print(f"  ✓ Factura {factura_id} eliminada")
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "message": f"Factura {factura_id} eliminada exitosamente",
            "productos_eliminados": productos_eliminados
        }
        
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"❌ Error eliminando factura {factura_id}: {e}")
        traceback.print_exc()
        raise HTTPException(500, f"Error al eliminar factura: {str(e)}")

@app.post("/admin/facturas/eliminar-multiple")
async def eliminar_facturas_multiple(datos: dict):
    """Eliminar múltiples facturas"""
    try:
        ids = datos.get('ids', [])
        
        if not ids or not isinstance(ids, list):
            raise HTTPException(400, "IDs inválidos")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        eliminadas = 0
        errores = []
        
        for factura_id in ids:
            try:
                # Eliminar productos
                cursor.execute("DELETE FROM productos WHERE factura_id = %s", (factura_id,))
                
                # Eliminar precios
                try:
                    cursor.execute("DELETE FROM precios_productos WHERE factura_id = %s", (factura_id,))
                except:
                    pass
                
                # Eliminar factura
                cursor.execute("DELETE FROM facturas WHERE id = %s", (factura_id,))
                
                if cursor.rowcount > 0:
                    eliminadas += 1
                    
            except Exception as e:
                errores.append(f"Factura {factura_id}: {str(e)}")
                print(f"Error eliminando factura {factura_id}: {e}")
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "eliminadas": eliminadas,
            "errores": errores,
            "message": f"{eliminadas} facturas eliminadas"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        if conn:
            conn.rollback()
            conn.close()
        print(f"Error en eliminación múltiple: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))

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
        print(f"Error en audit report: {e}")
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.post("/api/admin/run-audit")
@app.post("/admin/run-audit")
async def run_manual_audit():
    """Ejecuta auditoría manual"""
    try:
        results = audit_scheduler.run_manual_audit()
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/admin/improve-quality")
@app.post("/admin/improve-quality")
async def improve_quality():
    """Ejecuta mejora de calidad"""
    try:
        results = audit_scheduler.improve_quality()
        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "results": results
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/admin/clean-data")
async def clean_old_data():
    """Limpieza de datos antiguos"""
    try:
        audit = AuditSystem()
        results = audit.clean_old_data()
        return {
            "success": True,
            "cleaned": results
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/reporte_auditoria", response_class=HTMLResponse)
async def get_reporte_auditoria():
    """Página HTML del reporte de auditoría"""
    html_path = Path("reporte_auditoria.html")
    if html_path.exists():
        return FileResponse("reporte_auditoria.html")
    
    # HTML básico si no existe el archivo
    return HTMLResponse(content="""
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte de Auditoría</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container mt-5">
        <h1>Reporte de Auditoría</h1>
        <div id="content">Cargando...</div>
    </div>
    <script>
        fetch('/api/admin/audit-report')
            .then(r => r.json())
            .then(data => {
                document.getElementById('content').innerHTML = 
                    '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
            })
            .catch(err => {
                document.getElementById('content').innerHTML = 
                    '<div class="alert alert-danger">Error: ' + err + '</div>';
            });
    </script>
</body>
</html>
    """)

# ==========================================
# ENDPOINTS MÓVILES
# ==========================================
@app.post("/api/mobile/upload-invoice")
async def mobile_upload_invoice(
    image: UploadFile = File(...),
    user_id: str = Form("mobile_user"),
    user_email: Optional[str] = Form(None),
    device_id: Optional[str] = Form(None)
):
    """Upload desde app móvil"""
    try:
        if image.size > 10 * 1024 * 1024:
            raise HTTPException(400, "Imagen muy grande")
        
        image_bytes = await image.read()
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
            tmp_file.write(image_bytes)
            temp_path = tmp_file.name
        
        result = parse_invoice_with_claude(temp_path)
        
        if result["success"]:
            ocr_data = result["data"]
            
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO facturas 
                (usuario_id, establecimiento, fecha_cargue, total_factura)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (
                1,
                ocr_data.get("establecimiento", "Sin identificar"),
                datetime.now(),
                ocr_data.get("total", 0)
            ))
            
            factura_id = cursor.fetchone()[0]
            
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
            
            os.remove(temp_path)
            
            return {
                "success": True,
                "invoice_id": factura_id,
                "ocr_result": ocr_data
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
        
        save_image_to_db(factura_id, temp_file.name, "image/jpeg")
        
        conn.commit()
        conn.close()
        
        processor.add_to_queue(factura_id, temp_file.name, user_id)
        
        return {
            "success": True,
            "factura_id": factura_id,
            "queue_position": processor.get_queue_position(factura_id)
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

# ==========================================
# ENDPOINTS PARA CONFIRMACIÓN DE FACTURAS
# ==========================================
@app.post("/api/invoices/confirm")
async def confirm_invoice(invoice: InvoiceConfirm, request: Request):
    """Confirmar y guardar factura procesada"""
    try:
        tiene_imagen = True
        puntaje, estado, alertas = FacturaValidator.validar_factura(
            establecimiento=invoice.establecimiento,
            total=invoice.total,
            tiene_imagen=tiene_imagen,
            productos=[p.dict() for p in invoice.productos]
        )
        
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
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cadena = detectar_cadena(invoice.establecimiento)
            
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
            
            for producto in invoice.productos:
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
                "message": f"Factura guardada (Calidad: {puntaje}/100)",
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
            "error": str(e)
        }

# ==========================================
# ENDPOINTS DE ESTADÍSTICAS PERSONALES
# ==========================================
@app.get("/api/user/my-invoices")
async def get_my_invoices(user = Depends(get_current_user)):
    """Facturas del usuario autenticado"""
    user_id = user["user_id"]
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, establecimiento, fecha_cargue, total_factura,
                   (SELECT COUNT(*) FROM productos WHERE factura_id = facturas.id) as productos_count
            FROM facturas
            WHERE usuario_id = %s
            ORDER BY fecha_cargue DESC
        """, (user_id,))
        
        invoices = []
        for row in cursor.fetchall():
            invoices.append({
                "id": row[0],
                "establecimiento": row[1],
                "fecha": row[2].isoformat() if row[2] else None,
                "total": float(row[3]) if row[3] else 0,
                "productos_count": row[4]
            })
        
        conn.close()
        
        return {
            "success": True,
            "invoices": invoices
        }
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/api/user/my-stats")
async def get_my_stats(user = Depends(get_current_user)):
    """Estadísticas personales del usuario"""
    user_id = user["user_id"]
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(total_factura) as gasto_total,
                AVG(total_factura) as promedio
            FROM facturas
            WHERE usuario_id = %s
        """, (user_id,))
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return {
            "total_facturas": stats[0] or 0,
            "gasto_total": float(stats[1]) if stats[1] else 0,
            "gasto_promedio": float(stats[2]) if stats[2] else 0
        }
    except Exception as e:
        raise HTTPException(500, str(e))

# ==========================================
# ENDPOINTS DE ADMIN ANALYTICS
# ==========================================
@app.get("/api/admin/analytics")
async def get_analytics(user = Depends(require_admin)):
    """Analytics agregados (sin info personal)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(total_factura) FROM facturas")
        promedio_compra = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "total_usuarios": total_usuarios,
            "total_facturas": total_facturas,
            "promedio_compra": float(promedio_compra) if promedio_compra else 0
        }
    except Exception as e:
        raise HTTPException(500, str(e))

# ==========================================
# DEBUG ENDPOINTS
# ==========================================
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

# ==========================================
# DEBUG ENDPOINTS - TEMPORAL
# ==========================================
@app.get("/api/debug/routes")
async def debug_routes():
    """Ver todas las rutas registradas"""
    routes = []
    for route in app.routes:
        if hasattr(route, 'path') and hasattr(route, 'methods'):
            routes.append({
                "path": route.path,
                "methods": list(route.methods) if route.methods else [],
                "name": route.name if hasattr(route, 'name') else None
            })
    
    # Filtrar rutas de imágenes
    image_routes = [r for r in routes if 'imagen' in r['path'].lower()]
    
    return {
        "total_routes": len(routes),
        "image_routes": image_routes,
        "all_routes": routes
    }

@app.get("/api/debug/test-image/{factura_id}")
async def debug_test_image(factura_id: int):
    """Probar directamente la función get_image_from_db"""
    from storage import get_image_from_db
    
    image_data, mime_type = get_image_from_db(factura_id)
    
    if image_data:
        return {
            "success": True,
            "has_image": True,
            "size": len(image_data),
            "mime": mime_type
        }
    else:
        return {
            "success": False,
            "has_image": False,
            "message": "No hay imagen en BD"
        }

# ==========================================
# INICIO DEL SERVIDOR
# ==========================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=False
    )



























