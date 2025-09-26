import os
import asyncpg
import bcrypt
from datetime import datetime
import asyncio

DATABASE_URL = None
pool = None

async def init_db():
    """Inicializa el pool de conexiones"""
    global pool, DATABASE_URL
    
    DATABASE_URL = os.environ.get('DATABASE_URL')
    if not DATABASE_URL:
        raise Exception("DATABASE_URL no configurada")
    
    try:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
        print("Pool de conexiones creado")
        return pool
    except Exception as e:
        print(f"Error creando pool: {e}")
        return None

async def create_tables():
    """Crea las tablas si no existen"""
    if not pool:
        await init_db()
    
    try:
        async with pool.acquire() as conn:
            # Tabla usuarios
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    nombre VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Tabla facturas
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS facturas (
                    id SERIAL PRIMARY KEY,
                    usuario_id INTEGER REFERENCES usuarios(id) ON DELETE CASCADE,
                    establecimiento VARCHAR(255) NOT NULL,
                    fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    archivo_url VARCHAR(500)
                )
            """)
            
            # Tabla productos
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS productos (
                    id SERIAL PRIMARY KEY,
                    factura_id INTEGER REFERENCES facturas(id) ON DELETE CASCADE,
                    codigo VARCHAR(50),
                    nombre VARCHAR(255),
                    valor DECIMAL(10,2)
                )
            """)
            
        print("Tablas creadas exitosamente")
        return True
        
    except Exception as e:
        print(f"Error creando tablas: {e}")
        return False

def hash_password(password):
    """Hashea una contraseña"""
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password, hashed):
    """Verifica una contraseña contra su hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

async def get_pool():
    """Obtiene el pool de conexiones"""
    if not pool:
        await init_db()
    return pool
