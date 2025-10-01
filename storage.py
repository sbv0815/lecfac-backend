import os
from database import get_db_connection

def save_image_to_db(factura_id: int, image_path: str, mime_type: str) -> bool:
    """Guarda imagen en PostgreSQL"""
    try:
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        print(f"Guardando imagen: {len(image_data)} bytes para factura {factura_id}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE facturas 
            SET imagen_data = %s, imagen_mime = %s 
            WHERE id = %s
        """, (image_data, mime_type, factura_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✓ Imagen guardada en BD para factura {factura_id}")
        return True
        
    except Exception as e:
        print(f"Error guardando imagen: {e}")
        return False


def get_image_from_db(factura_id: int):
    """Recupera imagen de BD"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT imagen_data, imagen_mime 
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return result[0], result[1]
        
        return None, None
        
    except Exception as e:
        print(f"Error recuperando imagen: {e}")
        return None, None

def get_image_from_db(factura_id: int):
    """Recupera imagen de BD"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT imagen_bytes, imagen_mime 
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result and result[0]:
            return result[0], result[1]
        
        return None, None
        
    except Exception as e:
        print(f"Error recuperando imagen: {e}")
        return None, None
        
    except Exception as e:
        print(f"Error obteniendo imagen: {e}")
        return None, None
