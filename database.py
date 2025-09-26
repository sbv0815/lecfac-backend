import os
import psycopg2
from psycopg2.extras import RealDictCursor
import bcrypt
from datetime import datetime

def get_db_connection():
    """Crea conexión a la base de datos PostgreSQL"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL no configurada")
        
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return None

def create_tables():
    """Crea las tablas si no existen"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Tabla usuarios
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                nombre VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabla facturas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS facturas (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
                establecimiento VARCHAR(255) NOT NULL,
                fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                archivo_url VARCHAR(500)
            )
        """)
        
        # Tabla productos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id SERIAL PRIMARY KEY,
                factura_id INTEGER REFERENCES facturas(id) ON DELETE CASCADE,
                codigo VARCHAR(50),
                nombre VARCHAR(255),
                valor DECIMAL(10,2)
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Tablas creadas exitosamente")
        return True
        
    except Exception as e:
        print(f"Error creando tablas: {e}")
        conn.rollback()
        conn.close()
        return False

def hash_password(password):
    """Hashea una contraseña"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password, hashed):
    """Verifica una contraseña contra su hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
