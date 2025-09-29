import os
import sqlite3
import bcrypt
from urllib.parse import urlparse

# Intentar importar psycopg3 primero, luego psycopg2
POSTGRESQL_AVAILABLE = False
PSYCOPG_VERSION = None

try:
    import psycopg
    POSTGRESQL_AVAILABLE = True
    PSYCOPG_VERSION = 3
    print("✅ psycopg3 disponible")
except ImportError:
    try:
        import psycopg2
        POSTGRESQL_AVAILABLE = True
        PSYCOPG_VERSION = 2
        print("✅ psycopg2 disponible")
    except ImportError as e:
        POSTGRESQL_AVAILABLE = False
        print(f"⚠️ PostgreSQL no disponible: {e}")
        print("🔄 Usando SQLite como fallback")

def get_db_connection():
    """Obtiene conexión a la base de datos"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    print(f"🔍 DATABASE_TYPE configurado: {database_type}")
    print(f"🔍 POSTGRESQL_AVAILABLE: {POSTGRESQL_AVAILABLE}")
    
    if database_type == "postgresql" and POSTGRESQL_AVAILABLE:
        conn = get_postgresql_connection()
        if conn:
            return conn
        else:
            print("⚠️ Conexión PostgreSQL falló, usando SQLite")
            return get_sqlite_connection()
    else:
        if database_type == "postgresql" and not POSTGRESQL_AVAILABLE:
            print("⚠️ PostgreSQL solicitado pero librerías no disponibles, usando SQLite")
        return get_sqlite_connection()

def get_postgresql_connection():
    """Conexión a PostgreSQL (compatible psycopg2 y psycopg3)"""
    if not POSTGRESQL_AVAILABLE:
        print("❌ PostgreSQL libraries no disponibles")
        return None
        
    try:
        database_url = os.environ.get("DATABASE_URL")
        
        print(f"🔍 DATABASE_URL configurada: {'Sí' if database_url else 'No'}")
        
        if not database_url:
            print("❌ DATABASE_URL no configurada en variables de entorno")
            return None
        
        print(f"🔗 Intentando conectar a PostgreSQL (psycopg{PSYCOPG_VERSION})...")
        
        # psycopg3 puede usar directamente la URL
        if PSYCOPG_VERSION == 3:
            import psycopg
            conn = psycopg.connect(database_url)
        else:
            # psycopg2 necesita parsear la URL
            import psycopg2
            url = urlparse(database_url)
            
            print(f"🔗 Host: {url.hostname}")
            print(f"🔗 Database: {url.path[1:]}")
            print(f"🔗 Port: {url.port or 5432}")
            
            conn = psycopg2.connect(
                host=url.hostname,
                database=url.path[1:],
                user=url.username,
                password=url.password,
                port=url.port or 5432
            )
        
        print(f"✅ Conexión PostgreSQL exitosa (psycopg{PSYCOPG_VERSION})")
        return conn
        
    except Exception as e:
        print(f"❌ ERROR CONECTANDO A POSTGRESQL: {e}")
        print(f"❌ Tipo de error: {type(e).__name__}")
        import traceback
        traceback.print_exc()
        print("🔄 Fallback a SQLite")
        return get_sqlite_connection()

def get_sqlite_connection():
    """Conexión a SQLite (fallback)"""
    try:
        conn = sqlite3.connect('lecfac.db')
        conn.row_factory = sqlite3.Row
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
        print("❌ No se pudo crear conexión PostgreSQL")
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
        
        # Tabla facturas - Esquema flexible
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento TEXT NOT NULL,
            datos_completos JSONB NOT NULL DEFAULT '{}',
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabla productos - Campos esenciales + JSON
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            codigo TEXT NOT NULL,
            nombre TEXT NOT NULL,
            valor DECIMAL(10,2) NOT NULL,
            datos_adicionales JSONB DEFAULT '{}',
            fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Índices
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_establecimiento ON facturas(establecimiento)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_datos_gin ON facturas USING GIN (datos_completos)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_factura ON productos(factura_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_codigo ON productos(codigo)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_datos_gin ON productos USING GIN (datos_adicionales)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_usuarios_email ON usuarios(email)')
        
        conn.commit()
        conn.close()
        print("✅ Tablas PostgreSQL creadas (esquema flexible con JSON)")
        
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
        print("❌ No se pudo crear conexión SQLite")
        return
    
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            nombre TEXT,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            establecimiento TEXT NOT NULL,
            fecha_cargue DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER NOT NULL,
            codigo TEXT NOT NULL,
            nombre TEXT NOT NULL,
            valor REAL NOT NULL,
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
        
        try:
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL conectado: {version}")
        except:
            try:
                cursor.execute("SELECT sqlite_version()")
                version = cursor.fetchone()[0]
                print(f"✅ SQLite conectado: {version}")
            except:
                print("❌ No se pudo identificar el tipo de base de datos")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error probando conexión: {e}")
        if conn:
            conn.close()
        return False
        print("  python database.py test     - Probar conexión")
        print("  python database.py create   - Crear tablas")
        print("  python database.py type     - Ver tipo de BD")
