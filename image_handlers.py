# image_handlers.py - Módulo de manejo de imágenes para FastAPI

from fastapi import APIRouter, Response, HTTPException, UploadFile, File
from fastapi.responses import Response, FileResponse
from io import BytesIO
import base64
import os
from database import get_db_connection
from storage import save_image_to_db, get_image_from_db
from typing import Optional
import traceback

# Crear un router para los endpoints de imágenes
router = APIRouter()

@router.get("/admin/facturas/{factura_id}/imagen")
async def get_factura_imagen(factura_id: int):
    """Devuelve la imagen de una factura"""
    try:
        print(f"📸 GET /admin/facturas/{factura_id}/imagen - Solicitando imagen")
        
        # MÉTODO 1: Usar la función optimizada get_image_from_db
        try:
            image_data, mime_type = get_image_from_db(factura_id)
            
            if image_data:
                print(f"✅ Imagen encontrada: {len(image_data)} bytes, tipo {mime_type}")
                return Response(
                    content=image_data, 
                    media_type=mime_type or "image/jpeg",
                    headers={
                        "Cache-Control": "max-age=3600"
                    }
                )
            else:
                print(f"⚠️ get_image_from_db retornó None")
        except Exception as e:
            print(f"⚠️ get_image_from_db falló: {e}")
        
        # MÉTODO 2: Consulta directa a la base de datos (fallback)
        print("🔄 Intentando método alternativo...")
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT imagen_data, imagen_mime 
            FROM facturas 
            WHERE id = %s AND imagen_data IS NOT NULL
        """, (factura_id,))
        
        result = cursor.fetchone()
        
        if not result or not result[0]:
            cursor.close()
            conn.close()
            print(f"❌ No se encontró imagen para factura {factura_id}")
            raise HTTPException(status_code=404, detail="Imagen no encontrada")
        
        imagen_data, imagen_mime = result
        
        # Convertir a bytes si es necesario
        if not isinstance(imagen_data, bytes):
            imagen_data = bytes(imagen_data)
        
        cursor.close()
        conn.close()
        
        print(f"✅ Imagen obtenida: {len(imagen_data)} bytes")
        
        return Response(
            content=imagen_data, 
            media_type=imagen_mime or "image/jpeg",
            headers={
                "Cache-Control": "max-age=3600"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error obteniendo imagen: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/admin/facturas/{factura_id}/check-image")
async def check_factura_imagen(factura_id: int):
    """Verifica si una factura tiene imagen"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                CASE WHEN imagen_data IS NOT NULL THEN true ELSE false END AS tiene_imagen,
                LENGTH(imagen_data) as tamano
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        tiene_imagen = result[0]
        tamano = result[1]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "tieneImagen": tiene_imagen,
            "tamano": tamano
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error verificando imagen: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.post("/admin/facturas/{factura_id}/subir-imagen")
async def upload_factura_imagen(factura_id: int, file: UploadFile = File(...)):
    """Sube una imagen a una factura existente"""
    import tempfile
    temp_file = None
    
    try:
        print(f"📤 Subiendo imagen para factura {factura_id}")
        
        # Leer imagen
        imagen_data = await file.read()
        
        if not imagen_data:
            raise HTTPException(status_code=400, detail="Archivo vacío")
        
        print(f"✅ Archivo recibido: {len(imagen_data)} bytes")
        
        # Guardar temporalmente
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        temp_file.write(imagen_data)
        temp_file.close()
        
        # Determinar MIME type
        mime_type = file.content_type or "image/jpeg"
        if file.filename:
            if file.filename.lower().endswith(".png"):
                mime_type = "image/png"
            elif file.filename.lower().endswith(".webp"):
                mime_type = "image/webp"
        
        # Usar función save_image_to_db
        exito = save_image_to_db(factura_id, temp_file.name, mime_type)
        
        # Limpiar
        os.unlink(temp_file.name)
        
        if exito:
            return {
                "success": True,
                "message": "Imagen subida exitosamente",
                "size": len(imagen_data)
            }
        else:
            raise HTTPException(status_code=500, detail="Error guardando imagen")
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error subiendo imagen: {e}")
        traceback.print_exc()
        
        if temp_file and os.path.exists(temp_file.name):
            os.unlink(temp_file.name)
        
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


@router.get("/admin/facturas/{factura_id}/debug-imagen")
async def debug_imagen_factura(factura_id: int):
    """Diagnostica problemas con la imagen de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id,
                tiene_imagen,
                CASE 
                    WHEN imagen_data IS NULL THEN 'NULL' 
                    ELSE 'EXISTS'
                END AS estado_imagen,
                imagen_mime,
                LENGTH(imagen_data) AS tamano_bytes,
                fecha_cargue
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            cursor.close()
            conn.close()
            return {"error": "Factura no encontrada"}
        
        # Detectar inconsistencias
        inconsistencia = None
        if resultado[1] and resultado[2] == 'NULL':
            inconsistencia = "❌ Marcada con imagen pero no tiene datos"
        elif not resultado[1] and resultado[2] == 'EXISTS':
            inconsistencia = "⚠️ Tiene datos de imagen pero no está marcada (corrigiendo...)"
            
            # AUTO-CORRECCIÓN
            cursor.execute("""
                UPDATE facturas SET tiene_imagen = TRUE WHERE id = %s
            """, (factura_id,))
            conn.commit()
            print(f"✅ Flag corregido para factura {factura_id}")
        
        cursor.close()
        conn.close()
        
        return {
            "factura_id": resultado[0],
            "tiene_imagen_flag": resultado[1],
            "estado_datos_imagen": resultado[2],
            "mime_type": resultado[3],
            "tamano_bytes": resultado[4],
            "fecha_cargue": resultado[5].isoformat() if resultado[5] else None,
            "inconsistencia": inconsistencia or "✅ Todo correcto",
            "solucion": "Ejecuta /admin/facturas/{id}/fix-imagen si hay problemas" if inconsistencia else None
        }
        
    except Exception as e:
        print(f"❌ Error en debug-imagen: {e}")
        traceback.print_exc()
        return {"error": str(e)}


@router.post("/admin/facturas/{factura_id}/fix-imagen")
async def fix_imagen_factura(factura_id: int):
    """Corrige el flag tiene_imagen basado en datos reales"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar estado
        cursor.execute("""
            SELECT 
                tiene_imagen,
                CASE WHEN imagen_data IS NULL THEN FALSE ELSE TRUE END as tiene_datos_real
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            cursor.close()
            conn.close()
            return {
                "success": False,
                "message": "Factura no encontrada"
            }
        
        flag_actual = resultado[0]
        tiene_datos_real = resultado[1]
        
        # Corregir si hay diferencia
        if flag_actual != tiene_datos_real:
            cursor.execute("""
                UPDATE facturas SET tiene_imagen = %s WHERE id = %s
            """, (tiene_datos_real, factura_id))
            
            conn.commit()
            mensaje = f"✅ Flag corregido: {flag_actual} → {tiene_datos_real}"
        else:
            mensaje = f"✅ Flag ya está correcto: {flag_actual}"
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": mensaje,
            "flag_anterior": flag_actual,
            "flag_nuevo": tiene_datos_real
        }
        
    except Exception as e:
        print(f"❌ Error corrigiendo imagen: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}
