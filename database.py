import os
import sqlite3
import bcrypt
from urllib.parse import urlparse

# Intentar importar psycopg2, pero continuar si falla
try:
    import psycopg2
    POSTGRESQL_AVAILABLE = True
    print("✅ psycopg2 disponible")
except ImportError as e:
    POSTGRESQL_AVAILABLE = False
    print(f"⚠️ psycopg2 no disponible: {e}")
    print("🔄 Usando SQLite como fallback")

def get_db_connection():
    """
    Obtiene conexión a la base de datos (PostgreSQL o SQLite según disponibilidad)
    """
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    if database_type == "postgresql" and POSTGRESQL_AVAILABLE:
        return get_postgresql_connection()
    else:
        if database_type == "postgresql" and not POSTGRESQL_AVAILABLE:
            print("⚠️ PostgreSQL solicitado pero no disponible, usando SQLite")
        return get_sqlite_connection()

def get_postgresql_connection():
    """Conexión a PostgreSQL"""
    if not POSTGRESQL_AVAILABLE:
        return None
        
    try:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            print("❌ DATABASE_URL no configurada")
            return None
            
        # Parsear URL de PostgreSQL
        url = urlparse(database_url)
        
        conn = psycopg2.connect(
            host=url.hostname,
            database=url.path[1:],  # Remover el '/' inicial
            user=url.username,
            password=url.password,
            port=url.port or 5432
        )
        
        print("✅ Conexión PostgreSQL exitosa")
        return conn
        
    except Exception as e:
        print(f"❌ Error conectando a PostgreSQL: {e}")
        print("🔄 Fallback a SQLite")
        return get_sqlite_connection()

def get_sqlite_connection():
    """Conexión a SQLite (fallback)"""
    try:
        conn = sqlite3.connect('lecfac.db')
        conn.row_factory = sqlite3.Row  # Para acceso por nombre de columna
        print("✅ Conexión SQLite exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a SQLite: {e}")
        return None

def create_tables():
    """Crear tablas según el tipo de base de datos"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    if database_type == "postgresql" and POSTGRESQL_AVAILABLE:
        create_postgresql_tables()
    else:
        create_sqlite_tables()

def create_postgresql_tables():
    """Crear tablas en PostgreSQL"""
    if not POSTGRESQL_AVAILABLE:
        print("❌ PostgreSQL no disponible, creando tablas SQLite")
        create_sqlite_tables()
        return
        
    conn = get_postgresql_connection()
    if not conn:
        print("❌ No se pudo crear conexión para crear tablas PostgreSQL")
        create_sqlite_tables()
        return
    
    try:
        cursor = conn.cursor()
        
        # Tabla usuarios
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            nombre VARCHAR(255),
            fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabla facturas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento TEXT,
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_productos INTEGER DEFAULT 0,
            valor_total DECIMAL(12,2) DEFAULT 0.00
        )
        ''')
        
        # Tabla productos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            codigo VARCHAR(100),
            nombre TEXT,
            valor DECIMAL(10,2) DEFAULT 0.00,
            cantidad INTEGER DEFAULT 1,
            categoria VARCHAR(100),
            fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Índices para mejorar rendimiento
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_factura ON productos(factura_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_email ON usuarios(email)')
        
        conn.commit()
        conn.close()
        print("✅ Tablas PostgreSQL creadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        print("🔄 Fallback a SQLite")
        create_sqlite_tables()
        if conn:
            conn.close()

def create_sqlite_tables():
    """Crear tablas en SQLite (fallback)"""
    conn = get_sqlite_connection()
    if not conn:
        print("❌ No se pudo crear conexión para crear tablas SQLite")
        return
    
    try:
        cursor = conn.cursor()
        
        # Tabla usuarios
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            nombre TEXT,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabla facturas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            establecimiento TEXT,
            fecha_cargue DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_productos INTEGER DEFAULT 0,
            valor_total REAL DEFAULT 0.00,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
        )
        ''')
        
        # Tabla productos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER NOT NULL,
            codigo TEXT,
            nombre TEXT,
            valor REAL DEFAULT 0.00,
            cantidad INTEGER DEFAULT 1,
            categoria TEXT,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (factura_id) REFERENCES facturas (id) ON DELETE CASCADE
        )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Tablas SQLite creadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas SQLite: {e}")
        if conn:
            conn.close()

def hash_password(password: str) -> str:
    """Hashea una contraseña usando bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Verifica una contraseña contra su hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def test_database_connection():
    """Función para probar la conexión a la base de datos"""
    print("🔧 Probando conexión a base de datos...")
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Detectar qué tipo de base estamos usando realmente
        try:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL conectado: {version}")
            actual_type = "postgresql"
        except:
            try:
                cursor.execute("SELECT sqlite_version()")
                version = cursor.fetchone()[0]
                print(f"✅ SQLite conectado: {version}")
                actual_type = "sqlite"
            except:
                print("❌ No se pudo identificar el tipo de base de datos")
                actual_type = "unknown"
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error probando conexión: {e}")
        if conn:
            conn.close()
        return False

def get_database_type():
    """Devuelve el tipo de base de datos que se está usando realmente"""
    conn = get_db_connection()
    if not conn:
        return "none"
    
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT version()")
            conn.close()
            return "postgresql"
        except:
            try:
                cursor.execute("SELECT sqlite_version()")
                conn.close()
                return "sqlite"
            except:
                conn.close()
                return "unknown"
    except:
        return "error"

if __name__ == "__main__":
    # Script para ejecutar migraciones manuales
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_database_connection()
        elif sys.argv[1] == "create":
            create_tables()
        elif sys.argv[1] == "type":
            print(f"Tipo de base de datos: {get_database_type()}")
    else:
        print("Comandos disponibles:")
        print("  python database.py test     - Probar conexión")
        print("  python database.py create   - Crear tablas")
        print("  python database.py type     - Ver tipo de BD")
