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
    """Crear tablas en PostgreSQL - Sistema colaborativo tipo Waze"""
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
        
        # 1. TABLA USUARIOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            nombre VARCHAR(255),
            facturas_aportadas INTEGER DEFAULT 0,
            productos_aportados INTEGER DEFAULT 0,
            puntos_contribucion INTEGER DEFAULT 0,
            fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 2. TABLA FACTURAS (metadata + validaciones)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento TEXT NOT NULL,
            cadena VARCHAR(50),
            total_factura INTEGER NOT NULL,
            fecha_factura DATE,
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            estado VARCHAR(20) DEFAULT 'procesando',
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura DECIMAL(5,2),
            CHECK (total_factura > 0)
        )
        ''')
        
        # 3. CATÁLOGO MAESTRO DE PRODUCTOS (unificado)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_maestro (
            id SERIAL PRIMARY KEY,
            codigo_ean VARCHAR(13) UNIQUE NOT NULL,
            nombre TEXT NOT NULL,
            marca VARCHAR(100),
            categoria VARCHAR(50),
            es_fresco BOOLEAN DEFAULT FALSE,
            precio_promedio INTEGER,
            veces_reportado INTEGER DEFAULT 1,
            primera_vez TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK (LENGTH(codigo_ean) >= 8)
        )
        ''')
        
        # 4. CÓDIGOS LOCALES (para productos frescos/PLU por cadena)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS codigos_locales (
            id SERIAL PRIMARY KEY,
            producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestro(id) ON DELETE CASCADE,
            cadena VARCHAR(50) NOT NULL,
            codigo_local VARCHAR(20) NOT NULL,
            descripcion_local TEXT,
            activo BOOLEAN DEFAULT TRUE,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cadena, codigo_local)
        )
        ''')
        
        # 5. PRECIOS HISTÓRICOS (crowdsourcing)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_historicos (
            id SERIAL PRIMARY KEY,
            producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestro(id),
            establecimiento TEXT NOT NULL,
            cadena VARCHAR(50),
            precio INTEGER NOT NULL,
            usuario_id INTEGER REFERENCES usuarios(id),
            factura_id INTEGER REFERENCES facturas(id),
            fecha_reporte TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            verificado BOOLEAN DEFAULT FALSE,
            outlier BOOLEAN DEFAULT FALSE,
            CHECK (precio > 0)
        )
        ''')
        
        # ÍNDICES PARA BÚSQUEDAS RÁPIDAS
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_cadena ON facturas(cadena)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_estado ON facturas(estado)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_ean ON productos_maestro(codigo_ean)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_nombre ON productos_maestro USING GIN (to_tsvector(\'spanish\', nombre))')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_categoria ON productos_maestro(categoria)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_fresco ON productos_maestro(es_fresco)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_codigos_producto ON codigos_locales(producto_maestro_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_codigos_cadena_codigo ON codigos_locales(cadena, codigo_local)')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_producto ON precios_historicos(producto_maestro_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_cadena ON precios_historicos(cadena)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_fecha ON precios_historicos(fecha_reporte DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_establecimiento ON precios_historicos(establecimiento)')
        
        conn.commit()
        conn.close()
        print("✅ Tablas PostgreSQL creadas (sistema colaborativo tipo Waze)")
        
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()

def create_sqlite_tables():
    """Crear tablas en SQLite (fallback simplificado)"""
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
            total_factura INTEGER NOT NULL,
            estado TEXT DEFAULT 'procesando',
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            fecha_cargue DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_maestro (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ean TEXT UNIQUE NOT NULL,
            nombre TEXT NOT NULL,
            precio_promedio INTEGER,
            veces_reportado INTEGER DEFAULT 1,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_historicos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_maestro_id INTEGER NOT NULL,
            establecimiento TEXT NOT NULL,
            precio INTEGER NOT NULL,
            usuario_id INTEGER,
            factura_id INTEGER,
            fecha_reporte DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestro (id) ON DELETE CASCADE,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id),
            FOREIGN KEY (factura_id) REFERENCES facturas (id)
        )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Tablas SQLite creadas exitosamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas SQLite: {e}")
        if conn:
            conn.close()

# ============================================
# FUNCIONES AUXILIARES PARA SISTEMA COLABORATIVO
# ============================================

def buscar_o_crear_producto(cursor, codigo_ean: str, nombre: str, precio: int, es_fresco: bool = False):
    """
    Busca producto en catálogo maestro o lo crea.
    Retorna el ID del producto maestro.
    """
    # Normalizar código
    codigo_ean = codigo_ean.strip()
    
    # Buscar producto existente
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute(
            "SELECT id, veces_reportado, precio_promedio FROM productos_maestro WHERE codigo_ean = %s",
            (codigo_ean,)
        )
    else:
        cursor.execute(
            "SELECT id, veces_reportado, precio_promedio FROM productos_maestro WHERE codigo_ean = ?",
            (codigo_ean,)
        )
    
    resultado = cursor.fetchone()
    
    if resultado:
        # Producto existe - actualizar estadísticas
        producto_id = resultado[0]
        veces = resultado[1] + 1
        precio_anterior = resultado[2] or 0
        nuevo_promedio = ((precio_anterior * (veces - 1)) + precio) / veces
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                UPDATE productos_maestro 
                SET precio_promedio = %s, 
                    veces_reportado = %s,
                    ultima_actualizacion = CURRENT_TIMESTAMP,
                    nombre = %s
                WHERE id = %s
            """, (int(nuevo_promedio), veces, nombre, producto_id))
        else:
            cursor.execute("""
                UPDATE productos_maestro 
                SET precio_promedio = ?, veces_reportado = ?, nombre = ?
                WHERE id = ?
            """, (int(nuevo_promedio), veces, nombre, producto_id))
        
        return producto_id
    else:
        # Producto nuevo - crear
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO productos_maestro 
                (codigo_ean, nombre, precio_promedio, veces_reportado, es_fresco)
                VALUES (%s, %s, %s, 1, %s)
                RETURNING id
            """, (codigo_ean, nombre, precio, es_fresco))
            return cursor.fetchone()[0]
        else:
            cursor.execute("""
                INSERT INTO productos_maestro 
                (codigo_ean, nombre, precio_promedio, veces_reportado)
                VALUES (?, ?, ?, 1)
            """, (codigo_ean, nombre, precio))
            return cursor.lastrowid

def registrar_precio_historico(cursor, producto_id: int, establecimiento: str, cadena: str, 
                               precio: int, usuario_id: int, factura_id: int):
    """Registra un precio en el histórico colaborativo"""
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            INSERT INTO precios_historicos 
            (producto_maestro_id, establecimiento, cadena, precio, usuario_id, factura_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (producto_id, establecimiento, cadena, precio, usuario_id, factura_id))
    else:
        cursor.execute("""
            INSERT INTO precios_historicos 
            (producto_maestro_id, establecimiento, precio, usuario_id, factura_id)
            VALUES (?, ?, ?, ?, ?)
        """, (producto_id, establecimiento, precio, usuario_id, factura_id))

def actualizar_estadisticas_usuario(cursor, usuario_id: int, productos_aportados: int):
    """Actualiza las estadísticas de contribución del usuario"""
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            UPDATE usuarios 
            SET facturas_aportadas = facturas_aportadas + 1,
                productos_aportados = productos_aportados + %s,
                puntos_contribucion = puntos_contribucion + %s
            WHERE id = %s
        """, (productos_aportados, productos_aportados * 10, usuario_id))
    else:
        cursor.execute("""
            UPDATE usuarios 
            SET facturas_aportadas = facturas_aportadas + 1
            WHERE id = ?
        """, (usuario_id,))

# ============================================
# FUNCIONES ORIGINALES
# ============================================

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
        print(f"⚠️ Error en migración: {e}")
        if conn:
            conn.close()
