import os
from database import get_db_connection

def save_image_to_db(factura_id: int, image_path: str, mime_type: str) -> bool:
    """Guarda imagen en PostgreSQL"""
    try:
        # Leer archivo
        with open(image_path, 'rb') as f:
            image_data = f.read()
        
        print(f"Guardando imagen: {len(image_data)} bytes para factura {factura_id}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Verificar que la factura existe
        cursor.execute("SELECT id FROM facturas WHERE id = %s", (factura_id,))
        if not cursor.fetchone():
            print(f"❌ Factura {factura_id} no existe")
            conn.close()
            return False
        
        # Usar psycopg3 Binary adapter para BYTEA
        from psycopg import Binary
        
        # UPDATE con conversión explícita a Binary
        cursor.execute("""
            UPDATE facturas 
            SET imagen_data = %s, imagen_mime = %s 
            WHERE id = %s
        """, (Binary(image_data), mime_type, factura_id))
        
        rows_affected = cursor.rowcount
        conn.commit()
        
        # Verificar que se guardó
        cursor.execute("""
            SELECT LENGTH(imagen_data) as size 
            FROM facturas 
            WHERE id = %s
        """, (factura_id,))
        
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        
        if result and result[0]:
            print(f"✓ Imagen guardada en BD para factura {factura_id}: {result[0]} bytes")
            return True
        else:
            print(f"❌ Imagen NO se guardó para factura {factura_id}")
            return False
        
    except Exception as e:
        print(f"❌ Error guardando imagen: {e}")
        import traceback
        traceback.print_exc()
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
