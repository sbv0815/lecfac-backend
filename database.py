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
        
        if PSYCOPG_VERSION == 3:
            import psycopg
            conn = psycopg.connect(database_url)
        else:
            import psycopg2
            url = urlparse(database_url)
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
        import traceback
        traceback.print_exc()
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
    """Crear tablas en PostgreSQL con almacenamiento de imágenes"""
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
        
        # 2. TABLA FACTURAS (con todas las columnas necesarias)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento TEXT NOT NULL,
            cadena VARCHAR(50),
            total_factura INTEGER,
            fecha_factura DATE,
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fecha_procesamiento TIMESTAMP,
            fecha_validacion TIMESTAMP,
            estado VARCHAR(20) DEFAULT 'procesado',
            estado_validacion VARCHAR(20) DEFAULT 'pendiente',
            puntaje_calidad INTEGER DEFAULT 0,
            notas TEXT,
            procesado_por VARCHAR(50),
            tiene_imagen BOOLEAN DEFAULT FALSE,
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura DECIMAL(5,2)
        )
        ''')
        
        # AGREGAR COLUMNAS DE IMAGEN SI NO EXISTEN
        try:
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS imagen_data BYTEA
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS imagen_mime VARCHAR(20)
            """)
            print("✓ Columnas de imagen agregadas a facturas")
        except Exception as e:
            print(f"⚠️ Columnas de imagen ya existen o error: {e}")
        
        # AGREGAR COLUMNAS ADICIONALES SI NO EXISTEN
        try:
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS fecha_procesamiento TIMESTAMP
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS estado_validacion VARCHAR(20) DEFAULT 'pendiente'
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS puntaje_calidad INTEGER DEFAULT 0
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS notas TEXT
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS procesado_por VARCHAR(50)
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS tiene_imagen BOOLEAN DEFAULT FALSE
            """)
            cursor.execute("""
                ALTER TABLE facturas 
                ADD COLUMN IF NOT EXISTS fecha_validacion TIMESTAMP
            """)
            print("✓ Columnas adicionales agregadas a facturas")
        except Exception as e:
            print(f"⚠️ Error agregando columnas: {e}")
        
        # 3. TABLA PRODUCTOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            codigo VARCHAR(20),
            nombre VARCHAR(100),
            valor INTEGER
        )
        ''')
        
        # 4. TABLA PRODUCTOS MAESTRO
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
            CHECK (LENGTH(codigo_ean) >= 3)
        )
        ''')
        
        # 5. TABLA PRODUCTOS CATÁLOGO
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_catalogo (
            id SERIAL PRIMARY KEY,
            codigo_ean VARCHAR(13) UNIQUE,
            nombre_producto VARCHAR(100) NOT NULL,
            es_producto_fresco BOOLEAN DEFAULT FALSE,
            primera_fecha_reporte TIMESTAMP,
            total_reportes INTEGER DEFAULT 1,
            ultimo_reporte TIMESTAMP
        )
        ''')
        
        # 6. TABLA CÓDIGOS LOCALES
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS codigos_locales (
            id SERIAL PRIMARY KEY,
            producto_id INTEGER NOT NULL REFERENCES productos_catalogo(id) ON DELETE CASCADE,
            cadena VARCHAR(50) NOT NULL,
            codigo_local VARCHAR(20) NOT NULL,
            descripcion_local TEXT,
            activo BOOLEAN DEFAULT TRUE,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(cadena, codigo_local)
        )
        ''')
        
        # 7. TABLA PRECIOS HISTÓRICOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_historicos (
            id SERIAL PRIMARY KEY,
            producto_id INTEGER NOT NULL REFERENCES productos_maestro(id),
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
        
        # 8. TABLA PRECIOS PRODUCTOS
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_productos (
            id SERIAL PRIMARY KEY,
            producto_id INTEGER NOT NULL REFERENCES productos_catalogo(id) ON DELETE CASCADE,
            establecimiento VARCHAR(100) NOT NULL,
            cadena VARCHAR(50),
            precio INTEGER NOT NULL,
            usuario_id INTEGER REFERENCES usuarios(id),
            factura_id INTEGER REFERENCES facturas(id),
            fecha_reporte TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 9. TABLA HISTORIAL COMPRAS USUARIO
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS historial_compras_usuario (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            producto_id INTEGER NOT NULL REFERENCES productos_maestro(id),
            fecha_compra TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            precio_pagado INTEGER NOT NULL,
            establecimiento TEXT,
            cadena VARCHAR(50),
            factura_id INTEGER REFERENCES facturas(id)
        )
        ''')
        
        # 10. TABLA PATRONES DE COMPRA
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS patrones_compra (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            producto_id INTEGER NOT NULL REFERENCES productos_maestro(id),
            frecuencia_dias INTEGER,
            ultima_compra TIMESTAMP,
            proxima_compra_estimada TIMESTAMP,
            veces_comprado INTEGER DEFAULT 1,
            recordatorio_activo BOOLEAN DEFAULT TRUE,
            UNIQUE(usuario_id, producto_id)
        )
        ''')
        
        # 11. NUEVA TABLA: OCR LOGS (para el procesador automático)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ocr_logs (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER REFERENCES facturas(id) ON DELETE CASCADE,
            status VARCHAR(20),
            message TEXT,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # ÍNDICES
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_estado ON facturas(estado_validacion)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_ean ON productos_maestro(codigo_ean)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_fecha ON precios_historicos(fecha_reporte DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ocr_logs_factura ON ocr_logs(factura_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ocr_logs_created ON ocr_logs(created_at DESC)')
        
        # Índice para imágenes
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_imagen ON facturas(id) WHERE imagen_data IS NOT NULL')
        except:
            pass
        
        conn.commit()
        conn.close()
        print("✅ Tablas PostgreSQL creadas/actualizadas correctamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()

def create_sqlite_tables():
    """Crear tablas en SQLite con imágenes"""
    conn = get_sqlite_connection()
    if not conn:
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
            facturas_aportadas INTEGER DEFAULT 0,
            productos_aportados INTEGER DEFAULT 0,
            puntos_contribucion INTEGER DEFAULT 0,
            fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabla facturas completa
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            establecimiento TEXT NOT NULL,
            cadena TEXT,
            total_factura INTEGER,
            fecha_factura DATE,
            fecha_cargue DATETIME DEFAULT CURRENT_TIMESTAMP,
            fecha_procesamiento DATETIME,
            fecha_validacion DATETIME,
            estado TEXT DEFAULT 'pendiente',
            estado_validacion TEXT DEFAULT 'pendiente',
            puntaje_calidad INTEGER DEFAULT 0,
            notas TEXT,
            procesado_por TEXT,
            tiene_imagen INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura DECIMAL(5,2),
            imagen_data BLOB,
            imagen_mime TEXT,
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
            valor INTEGER,
            FOREIGN KEY (factura_id) REFERENCES facturas (id) ON DELETE CASCADE
        )
        ''')
        
        # Tabla productos maestro
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_maestro (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ean TEXT UNIQUE NOT NULL,
            nombre TEXT NOT NULL,
            marca TEXT,
            categoria TEXT,
            es_fresco INTEGER DEFAULT 0,
            precio_promedio INTEGER,
            veces_reportado INTEGER DEFAULT 1,
            primera_vez DATETIME DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Tabla productos catálogo
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_catalogo (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ean TEXT UNIQUE,
            nombre_producto TEXT NOT NULL,
            es_producto_fresco INTEGER DEFAULT 0,
            primera_fecha_reporte DATETIME,
            total_reportes INTEGER DEFAULT 1,
            ultimo_reporte DATETIME
        )
        ''')
        
        # Tabla OCR logs
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ocr_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER REFERENCES facturas(id),
            status TEXT,
            message TEXT,
            details TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Tablas SQLite creadas/actualizadas correctamente")
        
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
    """Prueba la conexión a la base de datos"""
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

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena comercial basándose en el nombre del establecimiento"""
    if not establecimiento:
        return "otro"
    
    establecimiento_lower = establecimiento.lower()
    
    cadenas = {
        'exito': ['exito', 'éxito', 'almacenes exito', 'almacenes éxito'],
        'carulla': ['carulla', 'carulla fresh', 'carulla express'],
        'jumbo': ['jumbo'],
        'olimpica': ['olimpica', 'olímpica', 'supertiendas olimpica', 'super tiendas olimpica'],
        'ara': ['ara', 'tiendas ara'],
        'd1': ['d1', 'tiendas d1', 'tienda d1'],
        'justo_bueno': ['justo & bueno', 'justo y bueno', 'justo&bueno'],
        'alkosto': ['alkosto', 'alkomprar'],
        'makro': ['makro'],
        'pricesmart': ['pricesmart', 'price smart'],
        'home_center': ['homecenter', 'home center'],
        'falabella': ['falabella'],
        'cruz_verde': ['cruz verde', 'cruzverde'],
        'farmatodo': ['farmatodo'],
        'la_rebaja': ['la rebaja', 'drogas la rebaja', 'droguerias la rebaja'],
        'cafam': ['cafam', 'supermercados cafam'],
        'colsubsidio': ['colsubsidio'],
        'euro': ['euro', 'eurosupermercados'],
        'metro': ['metro'],
        'consumo': ['consumo', 'almacenes consumo'],
        'zapatoca': ['zapatoca']
    }
    
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    
    return 'otro'

def obtener_productos_frecuentes_faltantes(usuario_id: int, codigos_detectados: set, limite: int = 3):
    """
    Identifica productos que el usuario compra frecuentemente pero no están en la factura actual
    """
    from datetime import datetime, timedelta
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    if os.environ.get("DATABASE_TYPE") == "postgresql":
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.veces_comprado,
                pm.precio_promedio,
                EXTRACT(DAY FROM (CURRENT_TIMESTAMP - pc.ultima_compra)) as dias_sin_comprar
            FROM patrones_compra pc
            JOIN productos_maestro pm ON pc.producto_id = pm.id
            WHERE pc.usuario_id = %s 
              AND pc.veces_comprado >= 3
              AND pc.recordatorio_activo = TRUE
              AND EXTRACT(DAY FROM (CURRENT_TIMESTAMP - pc.ultima_compra)) >= (pc.frecuencia_dias * 0.7)
            ORDER BY pc.veces_comprado DESC, dias_sin_comprar DESC
            LIMIT 10
        """, (usuario_id,))
    else:
        cursor.execute("""
            SELECT 
                pm.codigo_ean,
                pm.nombre,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.veces_comprado,
                pm.precio_promedio,
                julianday('now') - julianday(pc.ultima_compra) as dias_sin_comprar
            FROM patrones_compra pc
            JOIN productos_maestro pm ON pc.producto_id = pm.id
            WHERE pc.usuario_id = ? 
              AND pc.veces_comprado >= 3
              AND julianday('now') - julianday(pc.ultima_compra) >= (pc.frecuencia_dias * 0.7)
            ORDER BY pc.veces_comprado DESC, dias_sin_comprar DESC
            LIMIT 10
        """, (usuario_id,))
    
    candidatos = cursor.fetchall()
    conn.close()
    
    productos_sugeridos = []
    
    for prod in candidatos:
        codigo = prod[0]
        nombre = prod[1]
        frecuencia = prod[2]
        veces_comprado = prod[4]
        precio_promedio = prod[5]
        dias_sin_comprar = int(prod[6])
        
        if codigo not in codigos_detectados:
            relevancia = min(100, int((dias_sin_comprar / frecuencia) * 100))
            
            productos_sugeridos.append({
                "codigo": codigo,
                "nombre": nombre,
                "precio_estimado": precio_promedio or 0,
                "compras_anteriores": veces_comprado,
                "relevancia": relevancia,
                "mensaje": f"Normalmente compras este producto cada {frecuencia} días"
            })
    
    productos_sugeridos.sort(key=lambda x: x['relevancia'], reverse=True)
    return productos_sugeridos[:limite]

def confirmar_producto_manual(factura_id: int, codigo_ean: str, precio: int, usuario_id: int):
    """Agrega un producto confirmado manualmente por el usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT id FROM productos_maestro WHERE codigo_ean = %s", (codigo_ean,))
        else:
            cursor.execute("SELECT id FROM productos_maestro WHERE codigo_ean = ?", (codigo_ean,))
        
        resultado = cursor.fetchone()
        if not resultado:
            conn.close()
            return False
        
        producto_id = resultado[0]
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("SELECT establecimiento FROM facturas WHERE id = %s", (factura_id,))
        else:
            cursor.execute("SELECT establecimiento FROM facturas WHERE id = ?", (factura_id,))
        
        factura_info = cursor.fetchone()
        establecimiento = factura_info[0]
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO historial_compras_usuario 
                (usuario_id, producto_id, precio_pagado, establecimiento, factura_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (usuario_id, producto_id, precio, establecimiento, factura_id))
            
            cursor.execute("""
                INSERT INTO precios_historicos 
                (producto_id, establecimiento, precio, usuario_id, factura_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (producto_id, establecimiento, precio, usuario_id, factura_id))
        else:
            cursor.execute("""
                INSERT INTO historial_compras_usuario 
                (usuario_id, producto_id, precio_pagado, establecimiento, factura_id)
                VALUES (?, ?, ?, ?, ?)
            """, (usuario_id, producto_id, precio, establecimiento, factura_id))
        
        conn.commit()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error confirmando producto: {e}")
        conn.rollback()
        conn.close()
        return False
