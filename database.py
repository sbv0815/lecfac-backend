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
    """
    Crear tablas en PostgreSQL con NUEVA ARQUITECTURA
    Incluye tanto tablas nuevas como antiguas para migración gradual
    """
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
        
        print("🏗️ Creando tablas con nueva arquitectura...")
        
        # ============================================
        # NIVEL 0: USUARIOS (sin cambios)
        # ============================================
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
        print("✓ Tabla 'usuarios' creada")
        
        # ============================================
        # NIVEL 1: BASE UNIFICADA (GLOBAL)
        # ============================================
        
        # 1.1. ESTABLECIMIENTOS (NUEVA - Normalización de tiendas)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS establecimientos (
            id SERIAL PRIMARY KEY,
            nombre_normalizado VARCHAR(200) UNIQUE NOT NULL,
            cadena VARCHAR(50),
            tipo VARCHAR(50),
            ciudad VARCHAR(100),
            direccion TEXT,
            latitud DECIMAL(10, 8),
            longitud DECIMAL(11, 8),
            
            -- Estadísticas
            total_facturas_reportadas INTEGER DEFAULT 0,
            calificacion_promedio DECIMAL(3, 2),
            
            activo BOOLEAN DEFAULT TRUE,
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        print("✓ Tabla 'establecimientos' creada")
        
        # 1.2. PRODUCTOS_MAESTROS (NUEVA - Catálogo global unificado)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_maestros (
            id SERIAL PRIMARY KEY,
            codigo_ean VARCHAR(13) UNIQUE NOT NULL,
            nombre_normalizado VARCHAR(200) NOT NULL,
            nombre_comercial VARCHAR(200),
            marca VARCHAR(100),
            categoria VARCHAR(50),
            subcategoria VARCHAR(50),
            presentacion VARCHAR(50),
            es_producto_fresco BOOLEAN DEFAULT FALSE,
            imagen_url TEXT,
            
            -- Estadísticas globales
            total_reportes INTEGER DEFAULT 0,
            total_usuarios_reportaron INTEGER DEFAULT 0,
            precio_promedio_global INTEGER,
            precio_minimo_historico INTEGER,
            precio_maximo_historico INTEGER,
            
            -- Metadatos
            primera_vez_reportado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CHECK (LENGTH(codigo_ean) >= 8),
            CHECK (total_reportes >= 0)
        )
        ''')
        print("✓ Tabla 'productos_maestros' creada")
        
        # 1.3. PRECIOS_PRODUCTOS (NUEVA - Historial global)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_productos (
            id SERIAL PRIMARY KEY,
            producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),
            establecimiento_id INTEGER NOT NULL REFERENCES establecimientos(id),
            
            precio INTEGER NOT NULL,
            fecha_registro DATE NOT NULL,
            
            -- Origen del dato
            usuario_id INTEGER REFERENCES usuarios(id),
            factura_id INTEGER,
            
            -- Validación colaborativa
            verificado BOOLEAN DEFAULT FALSE,
            es_outlier BOOLEAN DEFAULT FALSE,
            votos_confianza INTEGER DEFAULT 0,
            
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CHECK (precio > 0)
        )
        ''')
        print("✓ Tabla 'precios_productos' creada")
        
        # ============================================
        # NIVEL 2: BASE LOCAL (POR USUARIO)
        # ============================================
        
        # 2.1. FACTURAS (ACTUALIZADA - ahora con establecimiento_id)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            
            -- Datos de la factura
            numero_factura VARCHAR(50),
            total_factura INTEGER,
            fecha_factura DATE,
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            -- Metadatos de procesamiento
            estado VARCHAR(20) DEFAULT 'procesado',
            estado_validacion VARCHAR(20) DEFAULT 'pendiente',
            puntaje_calidad INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura DECIMAL(5,2),
            
            -- Imagen
            tiene_imagen BOOLEAN DEFAULT FALSE,
            imagen_data BYTEA,
            imagen_mime VARCHAR(20),
            
            -- Auditoría
            fecha_procesamiento TIMESTAMP,
            fecha_validacion TIMESTAMP,
            procesado_por VARCHAR(50),
            notas TEXT,
            
            -- LEGACY: mantener temporalmente para migración
            establecimiento TEXT,
            cadena VARCHAR(50)
        )
        ''')
        print("✓ Tabla 'facturas' actualizada")
        
        # 2.2. ITEMS_FACTURA (NUEVA - Reemplaza tabla 'productos')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items_factura (
            id SERIAL PRIMARY KEY,
            
            -- Relaciones
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            producto_maestro_id INTEGER REFERENCES productos_maestros(id),
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
            
            -- Datos del item en la factura
            codigo_leido VARCHAR(20),
            nombre_leido VARCHAR(200),
            precio_pagado INTEGER NOT NULL,
            cantidad INTEGER DEFAULT 1,
            
            -- Matching con catálogo
            matching_confianza INTEGER,
            matching_manual BOOLEAN DEFAULT FALSE,
            
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            CHECK (precio_pagado >= 0),
            CHECK (cantidad > 0)
        )
        ''')
        print("✓ Tabla 'items_factura' creada")
        
        # 2.3. GASTOS_MENSUALES (NUEVA - Analytics personales)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS gastos_mensuales (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            
            total_gastado INTEGER NOT NULL,
            total_facturas INTEGER DEFAULT 0,
            total_productos INTEGER DEFAULT 0,
            promedio_por_factura INTEGER,
            
            fecha_calculo TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(usuario_id, anio, mes, establecimiento_id),
            CHECK (mes >= 1 AND mes <= 12)
        )
        ''')
        print("✓ Tabla 'gastos_mensuales' creada")
        
        # 2.4. PATRONES_COMPRA (ACTUALIZADA)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS patrones_compra (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),
            
            -- Análisis de frecuencia
            frecuencia_dias INTEGER,
            ultima_compra DATE,
            proxima_compra_estimada DATE,
            veces_comprado INTEGER DEFAULT 1,
            
            -- Preferencias
            establecimiento_preferido_id INTEGER REFERENCES establecimientos(id),
            precio_promedio_pagado INTEGER,
            
            recordatorio_activo BOOLEAN DEFAULT TRUE,
            
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(usuario_id, producto_maestro_id)
        )
        ''')
        print("✓ Tabla 'patrones_compra' actualizada")
        
        # ============================================
        # TABLAS AUXILIARES
        # ============================================
        
        # 3.1. CODIGOS_LOCALES (NUEVA - Productos sin EAN)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS codigos_locales (
            id SERIAL PRIMARY KEY,
            producto_maestro_id INTEGER REFERENCES productos_maestros(id),
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            
            codigo_local VARCHAR(20) NOT NULL,
            descripcion_local TEXT,
            activo BOOLEAN DEFAULT TRUE,
            
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(establecimiento_id, codigo_local)
        )
        ''')
        print("✓ Tabla 'codigos_locales' creada")
        
        # 3.2. MATCHING_LOGS (NUEVA - Auditoría de matching)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS matching_logs (
            id SERIAL PRIMARY KEY,
            item_factura_id INTEGER REFERENCES items_factura(id),
            
            codigo_leido VARCHAR(20),
            nombre_leido VARCHAR(200),
            producto_maestro_sugerido_id INTEGER REFERENCES productos_maestros(id),
            
            confianza INTEGER,
            metodo_matching VARCHAR(50),
            fue_aceptado BOOLEAN,
            
            fecha_matching TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        print("✓ Tabla 'matching_logs' creada")
        
        # 3.3. OCR_LOGS (mantener)
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
        print("✓ Tabla 'ocr_logs' creada")
        
        # ============================================
        # TABLAS LEGACY (mantener para migración)
        # ============================================
        print("📦 Manteniendo tablas legacy para migración...")
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            codigo VARCHAR(20),
            nombre VARCHAR(100),
            valor INTEGER
        )
        ''')
        
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
        
        # ============================================
        # ÍNDICES OPTIMIZADOS
        # ============================================
        print("📊 Creando índices optimizados...")
        
        # Índices para establecimientos
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_establecimientos_cadena ON establecimientos(cadena)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_establecimientos_ciudad ON establecimientos(ciudad)')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_establecimientos_nombre ON establecimientos(nombre_normalizado)')
        
        # Índices para productos_maestros
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_maestros_ean ON productos_maestros(codigo_ean)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_maestros_nombre ON productos_maestros(nombre_normalizado)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_maestros_categoria ON productos_maestros(categoria)')
        
        # Índices para precios_productos
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_producto_fecha ON precios_productos(producto_maestro_id, fecha_registro DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_establecimiento ON precios_productos(establecimiento_id, fecha_registro DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_usuario ON precios_productos(usuario_id)')
        cursor.execute('''CREATE UNIQUE INDEX IF NOT EXISTS idx_precios_unico_dia 
            ON precios_productos(producto_maestro_id, establecimiento_id, fecha_registro, usuario_id)''')
        
        # Índices para facturas
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_fecha ON facturas(fecha_factura DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_establecimiento ON facturas(establecimiento_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_usuario_fecha ON facturas(usuario_id, fecha_factura DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_facturas_estado ON facturas(estado_validacion)')
        
        # Índices para items_factura
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_factura ON items_factura(factura_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_producto_maestro ON items_factura(producto_maestro_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_usuario ON items_factura(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_items_usuario_fecha ON items_factura(usuario_id, fecha_creacion DESC)')
        
        # Índices para gastos_mensuales
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_gastos_usuario ON gastos_mensuales(usuario_id, anio DESC, mes DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_gastos_establecimiento ON gastos_mensuales(establecimiento_id)')
        
        # Índices para patrones_compra
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_patrones_usuario ON patrones_compra(usuario_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_patrones_recordatorios ON patrones_compra(usuario_id, recordatorio_activo, proxima_compra_estimada)')
        
        # Índices legacy
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_productos_ean ON productos_maestro(codigo_ean)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_precios_fecha ON precios_historicos(fecha_reporte DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ocr_logs_factura ON ocr_logs(factura_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ocr_logs_created ON ocr_logs(created_at DESC)')
        
        conn.commit()
        conn.close()
        print("✅ Tablas PostgreSQL creadas/actualizadas con NUEVA ARQUITECTURA")
        
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.close()

def create_sqlite_tables():
    """Crear tablas en SQLite con nueva arquitectura"""
    conn = get_sqlite_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        print("🏗️ Creando tablas SQLite con nueva arquitectura...")
        
        # Usuarios
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
        
        # Establecimientos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS establecimientos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_normalizado TEXT UNIQUE NOT NULL,
            cadena TEXT,
            tipo TEXT,
            ciudad TEXT,
            direccion TEXT,
            latitud REAL,
            longitud REAL,
            total_facturas_reportadas INTEGER DEFAULT 0,
            calificacion_promedio REAL,
            activo INTEGER DEFAULT 1,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Productos maestros
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS productos_maestros (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_ean TEXT UNIQUE NOT NULL,
            nombre_normalizado TEXT NOT NULL,
            nombre_comercial TEXT,
            marca TEXT,
            categoria TEXT,
            subcategoria TEXT,
            presentacion TEXT,
            es_producto_fresco INTEGER DEFAULT 0,
            imagen_url TEXT,
            total_reportes INTEGER DEFAULT 0,
            total_usuarios_reportaron INTEGER DEFAULT 0,
            precio_promedio_global INTEGER,
            precio_minimo_historico INTEGER,
            precio_maximo_historico INTEGER,
            primera_vez_reportado DATETIME DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # Facturas
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            numero_factura TEXT,
            total_factura INTEGER,
            fecha_factura DATE,
            fecha_cargue DATETIME DEFAULT CURRENT_TIMESTAMP,
            estado TEXT DEFAULT 'procesado',
            estado_validacion TEXT DEFAULT 'pendiente',
            puntaje_calidad INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura REAL,
            tiene_imagen INTEGER DEFAULT 0,
            imagen_data BLOB,
            imagen_mime TEXT,
            fecha_procesamiento DATETIME,
            fecha_validacion DATETIME,
            procesado_por TEXT,
            notas TEXT,
            establecimiento TEXT,
            cadena TEXT,
            FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
        )
        ''')
        
        # Items factura
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items_factura (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_id INTEGER NOT NULL,
            producto_maestro_id INTEGER REFERENCES productos_maestros(id),
            usuario_id INTEGER NOT NULL,
            codigo_leido TEXT,
            nombre_leido TEXT,
            precio_pagado INTEGER NOT NULL,
            cantidad INTEGER DEFAULT 1,
            matching_confianza INTEGER,
            matching_manual INTEGER DEFAULT 0,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (factura_id) REFERENCES facturas(id) ON DELETE CASCADE,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
        )
        ''')
        
        # Precios productos
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS precios_productos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_maestro_id INTEGER NOT NULL,
            establecimiento_id INTEGER NOT NULL,
            precio INTEGER NOT NULL,
            fecha_registro DATE NOT NULL,
            usuario_id INTEGER REFERENCES usuarios(id),
            factura_id INTEGER,
            verificado INTEGER DEFAULT 0,
            es_outlier INTEGER DEFAULT 0,
            votos_confianza INTEGER DEFAULT 0,
            fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id),
            FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id)
        )
        ''')
        
        # Gastos mensuales
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS gastos_mensuales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            anio INTEGER NOT NULL,
            mes INTEGER NOT NULL,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            total_gastado INTEGER NOT NULL,
            total_facturas INTEGER DEFAULT 0,
            total_productos INTEGER DEFAULT 0,
            promedio_por_factura INTEGER,
            fecha_calculo DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
            UNIQUE(usuario_id, anio, mes, establecimiento_id)
        )
        ''')
        
        # Tablas legacy
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
        print("✅ Tablas SQLite creadas/actualizadas con NUEVA ARQUITECTURA")
        
    except Exception as e:
        print(f"❌ Error creando tablas SQLite: {e}")
        if conn:
            conn.close()

# ============================================
# FUNCIONES HELPER PARA NUEVA ARQUITECTURA
# ============================================

def normalizar_nombre_establecimiento(nombre_raw: str) -> str:
    """Normaliza el nombre de un establecimiento"""
    if not nombre_raw:
        return ""
    
    # Convertir a minúsculas y quitar espacios extra
    nombre = nombre_raw.strip().lower()
    
    # Normalizar cadenas conocidas
    normalizaciones = {
        'éxito': 'exito',
        'olímpica': 'olimpica',
        'almacenes exito': 'exito',
        'almacenes éxito': 'exito',
        'supertiendas olimpica': 'olimpica',
        'justo & bueno': 'justo y bueno',
        'justo&bueno': 'justo y bueno',
    }
    
    for clave, valor in normalizaciones.items():
        if clave in nombre:
            nombre = nombre.replace(clave, valor)
    
    # Capitalizar primera letra de cada palabra
    return ' '.join(word.capitalize() for word in nombre.split())

def obtener_o_crear_establecimiento(nombre_raw: str, cadena: str = None) -> int:
    """
    Obtiene el ID de un establecimiento o lo crea si no existe
    Retorna: establecimiento_id
    """
    nombre_normalizado = normalizar_nombre_establecimiento(nombre_raw)
    if not cadena:
        cadena = detectar_cadena(nombre_raw)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Buscar existente
            cursor.execute(
                "SELECT id FROM establecimientos WHERE nombre_normalizado = %s",
                (nombre_normalizado,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                conn.close()
                return resultado[0]
            
            # Crear nuevo
            cursor.execute("""
                INSERT INTO establecimientos (nombre_normalizado, cadena)
                VALUES (%s, %s)
                RETURNING id
            """, (nombre_normalizado, cadena))
            
            establecimiento_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return establecimiento_id
            
        else:  # SQLite
            cursor.execute(
                "SELECT id FROM establecimientos WHERE nombre_normalizado = ?",
                (nombre_normalizado,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                conn.close()
                return resultado[0]
            
            cursor.execute("""
                INSERT INTO establecimientos (nombre_normalizado, cadena)
                VALUES (?, ?)
            """, (nombre_normalizado, cadena))
            
            establecimiento_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return establecimiento_id
            
    except Exception as e:
        print(f"Error obteniendo/creando establecimiento: {e}")
        conn.close()
        return None

def obtener_o_crear_producto_maestro(codigo_ean: str, nombre: str, precio: int = None) -> int:
    """
    Obtiene el ID de un producto maestro o lo crea si no existe
    Retorna: producto_maestro_id
    """
    if not codigo_ean or len(codigo_ean) < 8:
        return None
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            # Buscar existente
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
                (codigo_ean,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                # Actualizar estadísticas
                cursor.execute("""
                    UPDATE productos_maestros 
                    SET total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (resultado[0],))
                conn.commit()
                conn.close()
                return resultado[0]
            
            # Crear nuevo
            cursor.execute("""
                INSERT INTO productos_maestros 
                (codigo_ean, nombre_normalizado, precio_promedio_global, total_reportes)
                VALUES (%s, %s, %s, 1)
                RETURNING id
            """, (codigo_ean, nombre, precio))
            
            producto_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return producto_id
            
        else:  # SQLite
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE codigo_ean = ?",
                (codigo_ean,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                cursor.execute("""
                    UPDATE productos_maestros 
                    SET total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (resultado[0],))
                conn.commit()
                conn.close()
                return resultado[0]
            
            cursor.execute("""
                INSERT INTO productos_maestros 
                (codigo_ean, nombre_normalizado, precio_promedio_global, total_reportes)
                VALUES (?, ?, ?, 1)
            """, (codigo_ean, nombre, precio))
            
            producto_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return producto_id
            
    except Exception as e:
        print(f"Error obteniendo/creando producto maestro: {e}")
        conn.close()
        return None

# ============================================
# FUNCIONES LEGACY (mantener compatibilidad)
# ============================================

def hash_password(password: str) -> str:
    """Hashea una contraseña usando bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Verifica una contraseña contra su hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena comercial basándose en el nombre del establecimiento"""
    if not establecimiento:
        return "otro"
    
    establecimiento_lower = establecimiento.lower()
    
    cadenas = {
        'exito': ['exito', 'éxito', 'almacenes exito', 'almacenes éxito'],
        'carulla': ['carulla', 'carulla fresh', 'carulla express'],
        'jumbo': ['jumbo'],
        'olimpica': ['olimpica', 'olímpica', 'supertiendas olimpica'],
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
        'la_rebaja': ['la rebaja', 'drogas la rebaja'],
        'cafam': ['cafam'],
        'colsubsidio': ['colsubsidio'],
        'euro': ['euro'],
        'metro': ['metro'],
        'consumo': ['consumo', 'almacenes consumo'],
    }
    
    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena
    
    return 'otro'

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
