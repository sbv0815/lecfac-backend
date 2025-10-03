# ============= update_usuarios_table.py =============
import os
from database import get_db_connection

def update_usuarios_table():
    """Agregar columnas nuevas a la tabla usuarios"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print("🔧 Actualizando tabla usuarios...")
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Agregar columnas si no existen
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS nombres VARCHAR(100)
            """)
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS apellidos VARCHAR(100)
            """)
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS celular VARCHAR(20)
            """)
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS ubicacion_lat DECIMAL(10,8)
            """)
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS ubicacion_lon DECIMAL(11,8)
            """)
            cursor.execute("""
                ALTER TABLE usuarios 
                ADD COLUMN IF NOT EXISTS ciudad VARCHAR(100)
            """)
        else:
            # Para SQLite, necesitamos verificar primero si las columnas existen
            cursor.execute("PRAGMA table_info(usuarios)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'nombres' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN nombres TEXT")
            if 'apellidos' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN apellidos TEXT")
            if 'celular' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN celular TEXT")
            if 'ubicacion_lat' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN ubicacion_lat REAL")
            if 'ubicacion_lon' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN ubicacion_lon REAL")
            if 'ciudad' not in columns:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN ciudad TEXT")
        
        conn.commit()
        print("✅ Tabla usuarios actualizada correctamente")
        
    except Exception as e:
        print(f"❌ Error actualizando tabla: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    update_usuarios_table()
