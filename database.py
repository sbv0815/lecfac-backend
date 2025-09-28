import os
import psycopg2
import sqlite3
import bcrypt
from urllib.parse import urlparse

def get_db_connection():
    """
    Obtiene conexión a la base de datos (PostgreSQL o SQLite según configuración)
    """
    database_type = os.environ.get("DATABASE_TYPE", "sqlite")
    
    if database_type == "postgresql":
        return get_postgresql_connection()
    else:
        return get_sqlite_connection()

def get_postgresql_connection():
    """Conexión a PostgreSQL"""
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
        return None

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
    
    if database_type == "postgresql":
        create_postgresql_tables()
    else:
        create_sqlite_tables()

def create_postgresql_tables():
    """Crear tablas en PostgreSQL"""
    conn = get_postgresql_connection()
    if not conn:
        print("❌ No se pudo crear conexión para crear tablas PostgreSQL")
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

def migrate_sqlite_to_postgresql():
    """
    Función para migrar datos de SQLite a PostgreSQL
    ⚠️ Ejecutar solo una vez durante la migración
    """
    print("🔄 Iniciando migración SQLite → PostgreSQL")
    
    # Conexiones
    sqlite_conn = get_sqlite_connection()
    postgresql_conn = get_postgresql_connection()
    
    if not sqlite_conn or not postgresql_conn:
        print("❌ No se pudieron establecer las conexiones necesarias")
        return False
    
    try:
        sqlite_cursor = sqlite_conn.cursor()
        pg_cursor = postgresql_conn.cursor()
        
        # Migrar usuarios
        print("📤 Migrando usuarios...")
        sqlite_cursor.execute("SELECT email, password_hash, nombre FROM usuarios")
        usuarios = sqlite_cursor.fetchall()
        
        for usuario in usuarios:
            pg_cursor.execute(
                "INSERT INTO usuarios (email, password_hash, nombre) VALUES (%s, %s, %s) ON CONFLICT (email) DO NOTHING",
                usuario
            )
        
        # Obtener mapeo de IDs de usuarios
        user_id_map = {}
        sqlite_cursor.execute("SELECT id, email FROM usuarios")
        for old_id, email in sqlite_cursor.fetchall():
            pg_cursor.execute("SELECT id FROM usuarios WHERE email = %s", (email,))
            new_id = pg_cursor.fetchone()[0]
            user_id_map[old_id] = new_id
        
        # Migrar facturas
        print("📤 Migrando facturas...")
        sqlite_cursor.execute("SELECT id, usuario_id, establecimiento, fecha_cargue FROM facturas")
        facturas = sqlite_cursor.fetchall()
        
        factura_id_map = {}
        for old_id, usuario_id, establecimiento, fecha_cargue in facturas:
            new_usuario_id = user_id_map.get(usuario_id)
            if new_usuario_id:
                pg_cursor.execute(
                    "INSERT INTO facturas (usuario_id, establecimiento, fecha_cargue) VALUES (%s, %s, %s) RETURNING id",
                    (new_usuario_id, establecimiento, fecha_cargue)
                )
                new_id = pg_cursor.fetchone()[0]
                factura_id_map[old_id] = new_id
        
        # Migrar productos
        print("📤 Migrando productos...")
        sqlite_cursor.execute("SELECT factura_id, codigo, nombre, valor FROM productos")
        productos = sqlite_cursor.fetchall()
        
        for factura_id, codigo, nombre, valor in productos:
            new_factura_id = factura_id_map.get(factura_id)
            if new_factura_id:
                pg_cursor.execute(
                    "INSERT INTO productos (factura_id, codigo, nombre, valor) VALUES (%s, %s, %s, %s)",
                    (new_factura_id, codigo, nombre, valor)
                )
        
        postgresql_conn.commit()
        
        # Verificar migración
        pg_cursor.execute("SELECT COUNT(*) FROM usuarios")
        usuarios_count = pg_cursor.fetchone()[0]
        pg_cursor.execute("SELECT COUNT(*) FROM facturas")
        facturas_count = pg_cursor.fetchone()[0]
        pg_cursor.execute("SELECT COUNT(*) FROM productos")
        productos_count = pg_cursor.fetchone()[0]
        
        print(f"✅ Migración completada:")
        print(f"   - Usuarios: {usuarios_count}")
        print(f"   - Facturas: {facturas_count}")
        print(f"   - Productos: {productos_count}")
        
        sqlite_conn.close()
        postgresql_conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error en migración: {e}")
        if sqlite_conn:
            sqlite_conn.close()
        if postgresql_conn:
            postgresql_conn.close()
        return False

def test_database_connection():
    """Función para probar la conexión a la base de datos"""
    print("🔧 Probando conexión a base de datos...")
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        database_type = os.environ.get("DATABASE_TYPE", "sqlite")
        
        if database_type == "postgresql":
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0]
            print(f"✅ PostgreSQL conectado: {version}")
        else:
            cursor.execute("SELECT sqlite_version()")
            version = cursor.fetchone()[0]
            print(f"✅ SQLite conectado: {version}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Error probando conexión: {e}")
        if conn:
            conn.close()
        return False

if __name__ == "__main__":
    # Script para ejecutar migraciones manuales
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "migrate":
            migrate_sqlite_to_postgresql()
        elif sys.argv[1] == "test":
            test_database_connection()
        elif sys.argv[1] == "create":
            create_tables()
    else:
        print("Comandos disponibles:")
        print("  python database.py migrate  - Migrar SQLite a PostgreSQL")
        print("  python database.py test     - Probar conexión")
        print("  python database.py create   - Crear tablas")
