# ================ mobile_ocr_integrated.py ================
# Sistema móvil que usa tu claude_invoice.py existente

from fastapi import APIRouter, UploadFile, File, HTTPException, Form, BackgroundTasks
from typing import Optional
import uuid
import asyncio
from datetime import datetime
import json
import tempfile
import os
from enum import Enum

# Importar tu procesador existente
from claude_invoice import parse_invoice_with_claude

from database import get_db_connection

router = APIRouter(prefix="/api/mobile", tags=["Mobile OCR"])

# Estados del procesamiento
class ProcessingStatus(Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    OCR_COMPLETE = "ocr_complete"
    SAVED = "saved"
    ERROR = "error"

# Cola en memoria (si no usas Redis)
processing_queue = []
processing_lock = asyncio.Lock()

# ================ ENDPOINT PRINCIPAL PARA FLUTTER ================

@router.post("/upload-invoice")
async def upload_invoice_from_mobile(
    background_tasks: BackgroundTasks,
    image: UploadFile = File(...),
    user_id: str = Form(...),
    user_email: Optional[str] = Form(None),
    device_id: Optional[str] = Form(None),
    location: Optional[str] = Form(None)
):
    """
    Endpoint optimizado para recibir facturas desde Flutter
    """
    try:
        # Validar archivo
        if image.size > 10 * 1024 * 1024:  # 10MB máximo
            raise HTTPException(status_code=400, detail="Imagen muy grande (máx 10MB)")
        
        if not image.content_type.startswith('image/'):
            raise HTTPException(status_code=400, detail="Solo se aceptan imágenes")
        
        # Generar ID único
        invoice_id = str(uuid.uuid4())[:8]  # ID corto para facilidad
        timestamp = datetime.utcnow()
        
        # Leer imagen
        image_bytes = await image.read()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Obtener posición en cola
            async with processing_lock:
                queue_position = len([item for item in processing_queue 
                                    if item['status'] in ['queued', 'processing']]) + 1
            
            # Crear registro en BD
            cursor.execute("""
                INSERT INTO mobile_invoice_queue 
                (id, user_id, user_email, device_id, location, status, 
                 uploaded_at, image_size, mime_type, queue_position)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                invoice_id, user_id, user_email, device_id, location,
                ProcessingStatus.QUEUED.value, timestamp,
                len(image_bytes), image.content_type, queue_position
            ))
            
            # Guardar imagen
            cursor.execute("""
                INSERT INTO mobile_invoice_images (invoice_id, image_data)
                VALUES (%s, %s)
            """, (invoice_id, image_bytes))
            
            conn.commit()
            
            # Agregar a cola de procesamiento
            queue_item = {
                "invoice_id": invoice_id,
                "user_id": user_id,
                "user_email": user_email,
                "timestamp": timestamp.isoformat(),
                "status": "queued",
                "position": queue_position
            }
            
            async with processing_lock:
                processing_queue.append(queue_item)
            
            # Iniciar procesamiento en background
            background_tasks.add_task(process_next_in_queue)
            
            # Calcular tiempo estimado (15 segundos por factura aprox)
            estimated_time = queue_position * 15
            
            return {
                "success": True,
                "invoice_id": invoice_id,
                "status": ProcessingStatus.QUEUED.value,
                "queue_position": queue_position,
                "estimated_time_seconds": estimated_time,
                "message": f"Factura {invoice_id} en cola. Posición: {queue_position}"
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"❌ Error en upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/invoice-status/{invoice_id}")
async def get_invoice_status(invoice_id: str):
    """
    Obtener estado del procesamiento
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT status, queue_position, ocr_result, error_message,
                   processing_started_at, processing_completed_at
            FROM mobile_invoice_queue
            WHERE id = %s
        """, (invoice_id,))
        
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        status, queue_pos, ocr_result, error_msg, started_at, completed_at = result
        
        # Buscar en cola activa
        current_position = None
        async with processing_lock:
            for item in processing_queue:
                if item['invoice_id'] == invoice_id and item['status'] == 'queued':
                    current_position = item['position']
                    break
        
        response = {
            "invoice_id": invoice_id,
            "status": status,
            "queue_position": current_position or queue_pos
        }
        
        # Tiempo de procesamiento
        if started_at and completed_at:
            processing_time = (completed_at - started_at).total_seconds()
            response["processing_time_seconds"] = round(processing_time, 1)
        
        # Resultados del OCR
        if status == ProcessingStatus.OCR_COMPLETE.value and ocr_result:
            response["ocr_result"] = json.loads(ocr_result) if isinstance(ocr_result, str) else ocr_result
        
        # Error si existe
        if status == ProcessingStatus.ERROR.value:
            response["error"] = error_msg
        
        # Tiempo estimado
        if status == ProcessingStatus.QUEUED.value and current_position:
            response["estimated_time_seconds"] = current_position * 15
        
        return response
        
    finally:
        cursor.close()
        conn.close()

@router.post("/confirm-invoice/{invoice_id}")
async def confirm_invoice_data(
    invoice_id: str, 
    corrections: Optional[dict] = None
):
    """
    Confirmar datos del OCR y guardar en BD definitiva
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Obtener resultados
        cursor.execute("""
            SELECT user_id, ocr_result, status
            FROM mobile_invoice_queue
            WHERE id = %s
        """, (invoice_id,))
        
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        user_id, ocr_result, status = result
        
        if status != ProcessingStatus.OCR_COMPLETE.value:
            raise HTTPException(status_code=400, detail="OCR no completado")
        
        invoice_data = json.loads(ocr_result) if isinstance(ocr_result, str) else ocr_result
        
        # Aplicar correcciones
        if corrections:
            if 'establecimiento' in corrections:
                invoice_data['establecimiento'] = corrections['establecimiento']
            if 'total' in corrections:
                invoice_data['total'] = corrections['total']
            if 'productos' in corrections:
                invoice_data['productos'] = corrections['productos']
        
        # Guardar en tabla definitiva
        cursor.execute("""
            INSERT INTO facturas 
            (id, usuario_id, establecimiento, fecha_emision, total_factura, 
             productos, estado_validacion, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'confirmado', NOW())
        """, (
            invoice_id,
            user_id,
            invoice_data.get("establecimiento", "Sin identificar"),
            invoice_data.get("fecha") or datetime.now().date(),
            invoice_data.get("total", 0),
            json.dumps(invoice_data.get("productos", []))
        ))
        
        # Actualizar catálogo comunitario
        for producto in invoice_data.get("productos", []):
            if producto.get("nombre"):
                # Verificar si existe
                cursor.execute("""
                    SELECT id FROM productos_catalogo 
                    WHERE nombre_producto = %s
                """, (producto["nombre"],))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Actualizar existente
                    cursor.execute("""
                        UPDATE productos_catalogo 
                        SET veces_visto = veces_visto + 1,
                            ultimo_precio = %s,
                            ultima_actualizacion = NOW()
                        WHERE nombre_producto = %s
                    """, (producto.get("valor", 0), producto["nombre"]))
                else:
                    # Crear nuevo
                    cursor.execute("""
                        INSERT INTO productos_catalogo 
                        (codigo_ean, nombre_producto, primera_vista, 
                         ultima_actualizacion, ultimo_precio, veces_visto)
                        VALUES (%s, %s, NOW(), NOW(), %s, 1)
                    """, (
                        producto.get("codigo", ""),
                        producto["nombre"],
                        producto.get("valor", 0)
                    ))
        
        # Actualizar estado
        cursor.execute("""
            UPDATE mobile_invoice_queue 
            SET status = %s, confirmed_at = NOW()
            WHERE id = %s
        """, (ProcessingStatus.SAVED.value, invoice_id))
        
        conn.commit()
        
        print(f"✅ Factura {invoice_id} guardada exitosamente")
        
        return {
            "success": True,
            "message": "Factura guardada exitosamente",
            "invoice_id": invoice_id,
            "establecimiento": invoice_data.get("establecimiento"),
            "total_saved": invoice_data.get("total", 0),
            "products_count": len(invoice_data.get("productos", []))
        }
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error confirmando: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# ================ PROCESAMIENTO CON claude_invoice.py ================

async def process_next_in_queue():
    """
    Procesar siguiente factura en la cola
    """
    async with processing_lock:
        # Buscar siguiente factura pendiente
        next_item = None
        for item in processing_queue:
            if item['status'] == 'queued':
                next_item = item
                item['status'] = 'processing'
                break
        
        if not next_item:
            return  # No hay nada que procesar
    
    invoice_id = next_item['invoice_id']
    print(f"🔄 Procesando factura {invoice_id}")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Actualizar estado
        cursor.execute("""
            UPDATE mobile_invoice_queue 
            SET status = %s, processing_started_at = NOW(), queue_position = NULL
            WHERE id = %s
        """, (ProcessingStatus.PROCESSING.value, invoice_id))
        conn.commit()
        
        # Obtener imagen
        cursor.execute("""
            SELECT image_data FROM mobile_invoice_images 
            WHERE invoice_id = %s
        """, (invoice_id,))
        
        result = cursor.fetchone()
        if not result:
            raise Exception("Imagen no encontrada")
        
        image_bytes = result[0]
        
        # Guardar temporalmente para procesamiento
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_file:
            tmp_file.write(image_bytes)
            temp_path = tmp_file.name
        
        try:
            # USAR TU FUNCIÓN DE claude_invoice.py
            print(f"📸 Procesando con Claude Vision: {invoice_id}")
            result = parse_invoice_with_claude(temp_path)
            
            if result["success"]:
                ocr_data = result["data"]
                
                # Guardar resultados
                cursor.execute("""
                    UPDATE mobile_invoice_queue 
                    SET status = %s, 
                        ocr_result = %s,
                        processing_completed_at = NOW()
                    WHERE id = %s
                """, (
                    ProcessingStatus.OCR_COMPLETE.value,
                    json.dumps(ocr_data),
                    invoice_id
                ))
                
                print(f"✅ OCR completado para {invoice_id}")
                print(f"   Establecimiento: {ocr_data.get('establecimiento')}")
                print(f"   Total: ${ocr_data.get('total', 0):,}")
                print(f"   Productos: {len(ocr_data.get('productos', []))}")
                
            else:
                # Error en OCR
                error_msg = result.get("error", "Error desconocido en OCR")
                cursor.execute("""
                    UPDATE mobile_invoice_queue 
                    SET status = %s, 
                        error_message = %s,
                        processing_completed_at = NOW()
                    WHERE id = %s
                """, (ProcessingStatus.ERROR.value, error_msg, invoice_id))
                
                print(f"❌ Error OCR para {invoice_id}: {error_msg}")
            
            conn.commit()
            
        finally:
            # Limpiar archivo temporal
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        # Remover de cola
        async with processing_lock:
            processing_queue[:] = [item for item in processing_queue 
                                  if item['invoice_id'] != invoice_id]
            
            # Actualizar posiciones
            queued_items = [item for item in processing_queue 
                           if item['status'] == 'queued']
            for i, item in enumerate(queued_items):
                item['position'] = i + 1
        
        # Procesar siguiente si hay
        if any(item['status'] == 'queued' for item in processing_queue):
            await asyncio.sleep(1)  # Pequeña pausa
            await process_next_in_queue()
        
    except Exception as e:
        print(f"❌ Error procesando {invoice_id}: {str(e)}")
        
        # Marcar como error
        cursor.execute("""
            UPDATE mobile_invoice_queue 
            SET status = %s, 
                error_message = %s,
                processing_completed_at = NOW()
            WHERE id = %s
        """, (ProcessingStatus.ERROR.value, str(e), invoice_id))
        conn.commit()
        
        # Remover de cola
        async with processing_lock:
            processing_queue[:] = [item for item in processing_queue 
                                  if item['invoice_id'] != invoice_id]
        
    finally:
        cursor.close()
        conn.close()

# ================ ENDPOINTS DE MONITOREO ================

@router.get("/queue-status")
async def get_queue_status():
    """
    Estado actual de la cola de procesamiento
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Facturas en cola
        cursor.execute("""
            SELECT COUNT(*) FROM mobile_invoice_queue 
            WHERE status = %s
        """, (ProcessingStatus.QUEUED.value,))
        queued_count = cursor.fetchone()[0]
        
        # Procesándose
        cursor.execute("""
            SELECT COUNT(*) FROM mobile_invoice_queue 
            WHERE status = %s
        """, (ProcessingStatus.PROCESSING.value,))
        processing_count = cursor.fetchone()[0]
        
        # Completadas hoy
        cursor.execute("""
            SELECT COUNT(*) FROM mobile_invoice_queue 
            WHERE status IN (%s, %s) 
            AND DATE(processing_completed_at) = CURRENT_DATE
        """, (ProcessingStatus.OCR_COMPLETE.value, ProcessingStatus.SAVED.value))
        completed_today = cursor.fetchone()[0]
        
        # Errores hoy
        cursor.execute("""
            SELECT COUNT(*) FROM mobile_invoice_queue 
            WHERE status = %s 
            AND DATE(processing_completed_at) = CURRENT_DATE
        """, (ProcessingStatus.ERROR.value,))
        errors_today = cursor.fetchone()[0]
        
        # Tiempo promedio
        cursor.execute("""
            SELECT AVG(EXTRACT(EPOCH FROM (processing_completed_at - processing_started_at)))
            FROM mobile_invoice_queue
            WHERE status IN (%s, %s) 
            AND processing_completed_at > NOW() - INTERVAL '24 hours'
        """, (ProcessingStatus.OCR_COMPLETE.value, ProcessingStatus.SAVED.value))
        
        avg_time = cursor.fetchone()[0] or 0
        
        # Estado de la cola en memoria
        async with processing_lock:
            queue_details = [
                {
                    "invoice_id": item['invoice_id'],
                    "position": item.get('position', 0),
                    "status": item['status']
                }
                for item in processing_queue[:10]  # Solo los primeros 10
            ]
        
        return {
            "queue_stats": {
                "queued": queued_count,
                "processing": processing_count,
                "completed_today": completed_today,
                "errors_today": errors_today
            },
            "performance": {
                "average_processing_time_seconds": round(avg_time, 1),
                "estimated_wait_time": queued_count * 15
            },
            "current_queue": queue_details,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    finally:
        cursor.close()
        conn.close()

@router.get("/recent-invoices")
async def get_recent_invoices(limit: int = 20):
    """
    Obtener facturas recientes procesadas
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, user_id, status, queue_position, 
                   processing_started_at, processing_completed_at,
                   ocr_result
            FROM mobile_invoice_queue 
            ORDER BY uploaded_at DESC 
            LIMIT %s
        """, (limit,))
        
        invoices = []
        for row in cursor.fetchall():
            invoice = {
                "id": row[0],
                "user_id": row[1],
                "status": row[2],
                "queue_position": row[3]
            }
            
            if row[4] and row[5]:  # Si tiene tiempos de procesamiento
                processing_time = (row[5] - row[4]).total_seconds()
                invoice["processing_time_seconds"] = round(processing_time, 1)
            
            if row[6]:  # Si tiene resultados OCR
                ocr_data = json.loads(row[6]) if isinstance(row[6], str) else row[6]
                invoice["establecimiento"] = ocr_data.get("establecimiento", "N/A")
                invoice["total"] = ocr_data.get("total", 0)
                invoice["productos_count"] = len(ocr_data.get("productos", []))
            
            invoices.append(invoice)
        
        return {
            "invoices": invoices,
            "count": len(invoices)
        }
        
    finally:
        cursor.close()
        conn.close()

# ================ TABLAS DE BD NECESARIAS ================

CREATE_TABLES_SQL = """
-- Tabla principal de cola móvil (simplificada)
CREATE TABLE IF NOT EXISTS mobile_invoice_queue (
    id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    user_email VARCHAR(255),
    device_id VARCHAR(100),
    location VARCHAR(255),
    status VARCHAR(20) DEFAULT 'queued',
    queue_position INTEGER,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_started_at TIMESTAMP,
    processing_completed_at TIMESTAMP,
    confirmed_at TIMESTAMP,
    image_size INTEGER,
    mime_type VARCHAR(50),
    ocr_result JSONB,
    error_message TEXT
);

-- Índices
CREATE INDEX IF NOT EXISTS idx_mobile_status ON mobile_invoice_queue(status);
CREATE INDEX IF NOT EXISTS idx_mobile_user ON mobile_invoice_queue(user_id);
CREATE INDEX IF NOT EXISTS idx_mobile_date ON mobile_invoice_queue(uploaded_at DESC);

-- Tabla de imágenes
CREATE TABLE IF NOT EXISTS mobile_invoice_images (
    invoice_id VARCHAR(50) PRIMARY KEY REFERENCES mobile_invoice_queue(id),
    image_data BYTEA NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""
