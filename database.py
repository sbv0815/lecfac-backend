import sqlite3
import bcrypt
from datetime import datetime
import os

DATABASE_PATH = "/tmp/lecfac.db"

def get_db_connection():
    """Crea conexión a SQLite"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row  # Para acceder por nombre de columna
        return conn
    except Exception as e:
        print(f"Error conectando a SQLite: {e}")
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                nombre TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Tabla facturas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS facturas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
                establecimiento TEXT NOT NULL,
                fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                archivo_url TEXT
            )
        """)
        
        # Tabla productos
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura_id INTEGER REFERENCES facturas(id) ON DELETE CASCADE,
                codigo TEXT,
                nombre TEXT,
                valor REAL
            )
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Tablas SQLite creadas exitosamente")
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
