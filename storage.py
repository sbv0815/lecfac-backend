import os
from database import get_db_connection

def save_image_to_db(factura_id: int, image_path: str, mime_type: str) -> bool:
    """
    Guarda imagen en PostgreSQL como BYTEA
    Compatible con psycopg3 (psycopg)
    """
    try:
        # 1. Leer archivo de imagen
        if not os.path.exists(image_path):
            print(f"❌ Archivo no existe: {image_path}")
            return False
        
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        if not image_data:
            print(f"❌ Archivo vacío: {image_path}")
            return False
        
        print(f"📸 Leyendo imagen: {len(image_data)} bytes para factura {factura_id}")
        
        # 2. Obtener conexión NUEVA
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 3. Verificar que la factura existe
        cursor.execute("SELECT id, tiene_imagen FROM facturas WHERE id = %s", (factura_id,))
        result = cursor.fetchone()
        
        if not result:
            print(f"❌ Factura {factura_id} no existe en la BD")
            cursor.close()
            conn.close()
            return False
        
        print(f"✅ Factura {factura_id} existe, procediendo a guardar imagen...")
        
        # 4. IMPORTANTE: En psycopg3, los bytes se pasan directamente
        # No necesitas psycopg2.Binary()
        cursor.execute("""
            UPDATE facturas 
            SET imagen_data = %s, 
                imagen_mime = %s,
                tiene_imagen = TRUE
            WHERE id = %s
        """, (image_data, mime_type, factura_id))
        
        rows_affected = cursor.rowcount
        print(f"✅ UPDATE ejecutado, {rows_affected} fila(s) afectada(s)")
        
        # 5. COMMIT
        conn.commit()
        
        # 6. VERIFICAR que se guardó correctamente
        cursor.execute("""
            SELECT 
                LENGTH(imagen_data) as size_bytes,
                imagen_mime,
                tiene_imagen
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        verification = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        if verification and verification[0]:
            print(f"✅✅✅ IMAGEN GUARDADA EXITOSAMENTE ✅✅✅")
            print(f"  - Factura ID: {factura_id}")
            print(f"  - Tamaño: {verification[0]} bytes")
            print(f"  - MIME: {verification[1]}")
            print(f"  - Flag tiene_imagen: {verification[2]}")
            return True
        else:
            print(f"❌ VERIFICACIÓN FALLÓ - Imagen NO se guardó")
            return False
        
    except Exception as e:
        print(f"❌❌❌ ERROR GUARDANDO IMAGEN ❌❌❌")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_image_from_db(factura_id: int):
    """
    Recupera imagen de BD
    Compatible con psycopg3
    Retorna (bytes, mime_type) o (None, None)
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT imagen_data, imagen_mime 
            FROM facturas 
            WHERE id = %s AND imagen_data IS NOT NULL
        """, (factura_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0]:
            # En psycopg3, los datos ya vienen como bytes
            image_bytes = result[0]
            
            # Si por alguna razón viene como memoryview (puede pasar)
            if isinstance(image_bytes, memoryview):
                image_bytes = bytes(image_bytes)
            
            mime_type = result[1] or "image/jpeg"
            
            print(f"✅ Imagen recuperada: {len(image_bytes)} bytes, tipo {mime_type}")
            return image_bytes, mime_type
        
        print(f"❌ No hay imagen para factura {factura_id}")
        return None, None
        
    except Exception as e:
        print(f"❌ Error recuperando imagen: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def verify_image_exists(factura_id: int) -> dict:
    """
    Función de debug para verificar estado de imagen
    """
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
                END as imagen_status,
                LENGTH(imagen_data) as size_bytes,
                imagen_mime
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if not result:
            return {"error": "Factura no encontrada"}
        
        return {
            "factura_id": result[0],
            "tiene_imagen_flag": result[1],
            "imagen_data_status": result[2],
            "size_bytes": result[3],
            "mime_type": result[4],
            "inconsistencia": "OK" if (result[1] and result[2] == 'EXISTS') else "Marcada con imagen pero no tiene datos" if result[1] else "Consistente (sin imagen)"
        }
        
    except Exception as e:
        return {"error": str(e)}
