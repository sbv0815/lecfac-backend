# image_handlers.py - Módulo de manejo de imágenes para FastAPI

from fastapi import APIRouter, Response, HTTPException, UploadFile, File
from fastapi.responses import Response, FileResponse
from io import BytesIO
import base64
import os
from database import get_db_connection
from storage import save_image_to_db, get_image_from_db
from typing import Optional

# Crear un router para los endpoints de imágenes
router = APIRouter()

@router.get("/admin/facturas/{factura_id}/imagen")
async def get_factura_imagen(factura_id: int):
    """Devuelve la imagen de una factura"""
    try:
        # Primero intentamos usar la función get_image_from_db si está disponible
        try:
            image_data, mime_type = get_image_from_db(factura_id)
            
            if image_data:
                return Response(content=image_data, media_type=mime_type or "image/jpeg")
        except:
            # Si la función no está disponible o falla, usamos el método directo
            pass
        
        # Método alternativo: consulta directa a la base de datos
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT imagen_data, imagen_mime FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT imagen_data, imagen_mime FROM facturas WHERE id = ?", (factura_id,))
        
        result = cursor.fetchone()
        
        if not result or not result[0]:  # No hay imagen
            conn.close()
            raise HTTPException(status_code=404, detail="Imagen no encontrada")
        
        imagen_data, imagen_mime = result
        
        # Si la imagen es un string base64 (puede ocurrir en algunos sistemas)
        if isinstance(imagen_data, str) and imagen_data.startswith('data:'):
            try:
                # Extraer la parte base64 sin el prefijo
                _, base64_data = imagen_data.split(',', 1)
                imagen_data = base64.b64decode(base64_data)
            except:
                conn.close()
                raise HTTPException(status_code=500, detail="Error decodificando imagen")
        
        conn.close()
        
        # Enviar la imagen con el tipo MIME correcto
        return Response(content=imagen_data, media_type=imagen_mime or "image/jpeg")
        
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error obteniendo imagen: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/admin/facturas/{factura_id}/check-image")
async def check_factura_imagen(factura_id: int):
    """Verifica si una factura tiene imagen"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT CASE 
                    WHEN imagen_data IS NOT NULL THEN true 
                    ELSE false 
                END AS tiene_imagen 
                FROM facturas WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT CASE 
                    WHEN imagen_data IS NOT NULL THEN 1 
                    ELSE 0 
                END AS tiene_imagen 
                FROM facturas WHERE id = ?
            """, (factura_id,))
        
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        tiene_imagen = result[0]
        
        if isinstance(tiene_imagen, int):  # SQLite devuelve 0/1
            tiene_imagen = tiene_imagen == 1
        
        conn.close()
        
        return {
            "success": True,
            "tieneImagen": tiene_imagen
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error verificando imagen: {e}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.post("/admin/facturas/{factura_id}/subir-imagen")
async def upload_factura_imagen(factura_id: int, imagen: UploadFile = File(...)):
    """Sube una imagen a una factura existente"""
    try:
        # Leer la imagen
        imagen_data = await imagen.read()
        imagen_mime = imagen.content_type
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar si la factura existe
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT id FROM facturas WHERE id = ?", (factura_id,))
            
        if not cursor.fetchone():
            conn.close()
            raise HTTPException(status_code=404, detail="Factura no encontrada")
        
        # Actualizar la factura con la nueva imagen
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            try:
                # Intentar usar psycopg3 Binary si está disponible
                try:
                    from psycopg import Binary
                    binary_data = Binary(imagen_data)
                except ImportError:
                    # Si no está disponible, usar el dato directamente
                    binary_data = imagen_data
                
                cursor.execute("""
                    UPDATE facturas 
                    SET imagen_data = %s, imagen_mime = %s, tiene_imagen = TRUE
                    WHERE id = %s
                """, (binary_data, imagen_mime, factura_id))
            except Exception as e:
                print(f"Error al guardar imagen en PostgreSQL: {e}")
                # Método alternativo
                cursor.execute("""
                    UPDATE facturas 
                    SET imagen_data = %s, imagen_mime = %s, tiene_imagen = TRUE
                    WHERE id = %s
                """, (imagen_data, imagen_mime, factura_id))
        else:
            cursor.execute("""
                UPDATE facturas 
                SET imagen_data = ?, imagen_mime = ?, tiene_imagen = 1
                WHERE id = ?
            """, (imagen_data, imagen_mime, factura_id))
            
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "message": "Imagen subida exitosamente"
        }
        
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error subiendo imagen: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

# Añade estos endpoints a tu archivo image_handlers.py

@router.get("/admin/facturas/{factura_id}/debug-imagen")
async def debug_imagen_factura(factura_id: int):
    """Diagnostica problemas con la imagen de una factura"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Obtener información detallada sobre la imagen
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT 
                    id,
                    tiene_imagen,
                    CASE 
                        WHEN imagen_data IS NULL THEN 'NULL' 
                        ELSE 'PRESENTE'
                    END AS estado_imagen,
                    imagen_mime,
                    pg_column_size(imagen_data) AS tamano_bytes,
                    fecha_cargue
                FROM facturas 
                WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT 
                    id,
                    tiene_imagen,
                    CASE 
                        WHEN imagen_data IS NULL THEN 'NULL' 
                        ELSE 'PRESENTE'
                    END AS estado_imagen,
                    imagen_mime,
                    length(imagen_data) AS tamano_bytes,
                    fecha_cargue
                FROM facturas 
                WHERE id = %s
            """, (factura_id,))
        
        resultado = cursor.fetchone()
        
        if not resultado:
            conn.close()
            return {"error": "Factura no encontrada"}
        
        # Verificar si hay inconsistencia entre tiene_imagen y el dato real
        inconsistencia = False
        if resultado[1] and resultado[2] == 'NULL':
            inconsistencia = "Marcada con imagen pero no tiene datos"
        elif not resultado[1] and resultado[2] == 'PRESENTE':
            inconsistencia = "Tiene datos de imagen pero no está marcada"
            
            # Corregir automáticamente
            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute("""
                    UPDATE facturas SET tiene_imagen = TRUE WHERE id = %s
                """, (factura_id,))
            else:
                cursor.execute("""
                    UPDATE facturas SET tiene_imagen = 1 WHERE id = %s
                """, (factura_id,))
            conn.commit()
            
        conn.close()
        
        return {
            "factura_id": resultado[0],
            "tiene_imagen_flag": resultado[1],
            "estado_datos_imagen": resultado[2],
            "mime_type": resultado[3],
            "tamano_bytes": resultado[4],
            "fecha_cargue": resultado[5].isoformat() if resultado[5] else None,
            "inconsistencia": inconsistencia,
            "corregido": inconsistencia == "Tiene datos de imagen pero no está marcada"
        }
        
    except Exception as e:
        print(f"Error en debug-imagen: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        return {"error": str(e)}

@router.get("/admin/facturas/{factura_id}/fix-imagen")
async def fix_imagen_factura(factura_id: int):
    """Corrige problemas comunes con las imágenes de facturas"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar primero si tiene imagen
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT imagen_data, imagen_mime FROM facturas WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT imagen_data, imagen_mime FROM facturas WHERE id = %s
            """, (factura_id,))
        
        resultado = cursor.fetchone()
        
        if not resultado or resultado[0] is None:
            conn.close()
            return {
                "success": False,
                "message": "La factura no tiene datos de imagen para corregir"
            }
            
        # Actualizar la bandera tiene_imagen
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE facturas SET tiene_imagen = TRUE WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                UPDATE facturas SET tiene_imagen = 1 WHERE id = %s
            """, (factura_id,))
            
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "message": "Bandera de imagen corregida. La imagen debería ser visible ahora."
        }
        
    except Exception as e:
        print(f"Error corrigiendo imagen: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        return {"success": False, "error": str(e)}
