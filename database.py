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
            
            CHECK (LENGTH(codigo_ean) >= 3 AND LENGTH(codigo_ean) <= 14),
            CHECK (total_reportes >= 0)
        )
        ''')
        print("✓ Tabla 'productos_maestros' creada")
        
        # Corregir constraint de productos_maestros si existe
        print("🔧 Corrigiendo constraints de productos_maestros...")
        try:
            cursor.execute("""
                ALTER TABLE productos_maestros 
                DROP CONSTRAINT IF EXISTS productos_maestros_codigo_ean_check
            """)
            conn.commit()
            
            cursor.execute("""
                ALTER TABLE productos_maestros 
                ADD CONSTRAINT productos_maestros_codigo_ean_check 
                CHECK (LENGTH(codigo_ean) >= 3 AND LENGTH(codigo_ean) <= 14)
            """)
            conn.commit()
            print("✅ Constraint actualizado: códigos PLU (3+ dígitos) permitidos")
        except Exception as e:
            print(f"⚠️ Constraint ya correcto o error menor: {e}")
            conn.rollback()
        
        # 1.3. PRECIOS_PRODUCTOS (MIGRACIÓN INTELIGENTE)
        print("🔧 Configurando tabla precios_productos...")
        
        # Verificar si la tabla existe
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'precios_productos'
            )
        """)
        tabla_precios_existe = cursor.fetchone()[0]
        
        if tabla_precios_existe:
            print("   📋 Tabla precios_productos existe, verificando estructura...")
            
            # Obtener columnas existentes
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns 
                WHERE table_name = 'precios_productos'
            """)
            columnas_existentes = [row[0] for row in cursor.fetchall()]
            print(f"   📊 Columnas actuales: {', '.join(columnas_existentes)}")
            
            # Agregar establecimiento_id si no existe
            if 'establecimiento_id' not in columnas_existentes:
                print("   ➕ Agregando columna establecimiento_id...")
                try:
                    cursor.execute("""
                        ALTER TABLE precios_productos 
                        ADD COLUMN establecimiento_id INTEGER
                    """)
                    conn.commit()
                    print("   ✅ Columna establecimiento_id agregada")
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()
            
            # Manejar producto_id vs producto_maestro_id
            if 'producto_id' in columnas_existentes and 'producto_maestro_id' not in columnas_existentes:
                print("   🔄 Renombrando producto_id → producto_maestro_id...")
                try:
                    cursor.execute("""
                        ALTER TABLE precios_productos 
                        RENAME COLUMN producto_id TO producto_maestro_id
                    """)
                    conn.commit()
                    print("   ✅ Columna renombrada")
                    columnas_existentes.remove('producto_id')
                    columnas_existentes.append('producto_maestro_id')
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()
            elif 'producto_maestro_id' not in columnas_existentes and 'producto_id' not in columnas_existentes:
                print("   ➕ Agregando columna producto_maestro_id...")
                try:
                    cursor.execute("""
                        ALTER TABLE precios_productos 
                        ADD COLUMN producto_maestro_id INTEGER
                    """)
                    conn.commit()
                    print("   ✅ Columna producto_maestro_id agregada")
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()
            
            # Agregar otras columnas faltantes
            columnas_requeridas = {
                'precio': 'INTEGER',
                'fecha_registro': 'DATE',
                'usuario_id': 'INTEGER',
                'factura_id': 'INTEGER',
                'verificado': 'BOOLEAN DEFAULT FALSE',
                'es_outlier': 'BOOLEAN DEFAULT FALSE',
                'votos_confianza': 'INTEGER DEFAULT 0',
                'fecha_actualizacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                'fecha_creacion': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
            }
            
            for columna, tipo in columnas_requeridas.items():
                if columna not in columnas_existentes:
                    print(f"   ➕ Agregando columna {columna}...")
                    try:
                        cursor.execute(f"""
                            ALTER TABLE precios_productos 
                            ADD COLUMN {columna} {tipo}
                        """)
                        conn.commit()
                    except Exception as e:
                        print(f"   ⚠️ {columna}: {e}")
                        conn.rollback()
            
            # Limpiar y recrear constraints
            print("   🔧 Limpiando constraints viejos...")
            cursor.execute("""
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                WHERE rel.relname = 'precios_productos'
                AND con.contype = 'f'
            """)
            constraints = cursor.fetchall()
            
            for constraint in constraints:
                constraint_name = constraint[0]
                try:
                    cursor.execute(f"ALTER TABLE precios_productos DROP CONSTRAINT IF EXISTS {constraint_name}")
                    conn.commit()
                except:
                    conn.rollback()
            
            # Agregar constraints correctos
            print("   ✅ Agregando constraints correctos...")
            
            try:
                cursor.execute("""
                    ALTER TABLE precios_productos 
                    ADD CONSTRAINT precios_productos_producto_maestro_fkey 
                    FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id)
                """)
                conn.commit()
                print("   ✅ FK producto_maestro_id → productos_maestros")
            except Exception as e:
                print(f"   ⚠️ FK producto_maestro: {e}")
                conn.rollback()
            
            try:
                cursor.execute("""
                    ALTER TABLE precios_productos 
                    ADD CONSTRAINT precios_productos_establecimiento_fkey 
                    FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id)
                """)
                conn.commit()
                print("   ✅ FK establecimiento_id → establecimientos")
            except Exception as e:
                print(f"   ⚠️ FK establecimiento: {e}")
                conn.rollback()
            
            try:
                cursor.execute("""
                    ALTER TABLE precios_productos 
                    ADD CONSTRAINT precios_productos_usuario_fkey 
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
                """)
                conn.commit()
                print("   ✅ FK usuario_id → usuarios")
            except Exception as e:
                print(f"   ⚠️ FK usuario: {e}")
                conn.rollback()

        else:
            # Crear tabla desde cero con estructura correcta
            print("   ✨ Creando tabla precios_productos desde cero...")
            cursor.execute('''
            CREATE TABLE precios_productos (
                id SERIAL PRIMARY KEY,
                producto_maestro_id INTEGER NOT NULL,
                establecimiento_id INTEGER NOT NULL,
                precio INTEGER NOT NULL,
                fecha_registro DATE NOT NULL,
                usuario_id INTEGER,
                factura_id INTEGER,
                verificado BOOLEAN DEFAULT FALSE,
                es_outlier BOOLEAN DEFAULT FALSE,
                votos_confianza INTEGER DEFAULT 0,
                fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                CONSTRAINT precios_productos_producto_maestro_fkey 
                    FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id),
                CONSTRAINT precios_productos_establecimiento_fkey 
                    FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id),
                CONSTRAINT precios_productos_usuario_fkey
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
                
                CHECK (precio > 0)
            )
            ''')
            conn.commit()
            print("   ✅ Tabla creada con estructura correcta")

        print("✅ Tabla 'precios_productos' configurada correctamente")
        
        # ============================================
        # NIVEL 2: BASE LOCAL (POR USUARIO)
        # ============================================
        
        # 2.1. FACTURAS (ACTUALIZADA - ahora con establecimiento_id)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS facturas (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            
            numero_factura VARCHAR(50),
            total_factura INTEGER,
            fecha_factura DATE,
            fecha_cargue TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            estado VARCHAR(20) DEFAULT 'procesado',
            estado_validacion VARCHAR(20) DEFAULT 'pendiente',
            puntaje_calidad INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            productos_guardados INTEGER DEFAULT 0,
            porcentaje_lectura DECIMAL(5,2),
            
            tiene_imagen BOOLEAN DEFAULT FALSE,
            imagen_data BYTEA,
            imagen_mime VARCHAR(20),
            
            fecha_procesamiento TIMESTAMP,
            fecha_validacion TIMESTAMP,
            procesado_por VARCHAR(50),
            notas TEXT,
            
            establecimiento TEXT,
            cadena VARCHAR(50)
        )
        ''')
        
        # Agregar columnas faltantes si no existen
        def agregar_columna_segura(tabla, columna, tipo):
            try:
                cursor.execute(f"""
                    ALTER TABLE {tabla} 
                    ADD COLUMN IF NOT EXISTS {columna} {tipo}
                """)
                conn.commit()
                print(f"✓ Columna '{columna}' verificada en {tabla}")
            except Exception as e:
                print(f"⚠️ Columna {columna}: {e}")
                conn.rollback()
        
        agregar_columna_segura('facturas', 'establecimiento_id', 'INTEGER REFERENCES establecimientos(id)')
        agregar_columna_segura('facturas', 'productos_guardados', 'INTEGER DEFAULT 0')
        agregar_columna_segura('facturas', 'productos_detectados', 'INTEGER DEFAULT 0')
        agregar_columna_segura('facturas', 'estado_validacion', "VARCHAR(20) DEFAULT 'pendiente'")
        agregar_columna_segura('facturas', 'puntaje_calidad', 'INTEGER DEFAULT 0')
        agregar_columna_segura('facturas', 'porcentaje_lectura', 'DECIMAL(5,2)')
        
        print("✓ Tabla 'facturas' actualizada")
        
        # 2.2. ITEMS_FACTURA (NUEVA - Reemplaza tabla 'productos')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS items_factura (
            id SERIAL PRIMARY KEY,
            
            factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
            producto_maestro_id INTEGER REFERENCES productos_maestros(id),
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
            
            codigo_leido VARCHAR(20),
            nombre_leido VARCHAR(200),
            precio_pagado INTEGER NOT NULL,
            cantidad INTEGER DEFAULT 1,
            
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
            producto_maestro_id INTEGER REFERENCES productos_maestros(id),
            
            frecuencia_dias INTEGER,
            ultima_compra DATE,
            proxima_compra_estimada DATE,
            veces_comprado INTEGER DEFAULT 1,
            
            establecimiento_preferido_id INTEGER REFERENCES establecimientos(id),
            precio_promedio_pagado INTEGER,
            
            recordatorio_activo BOOLEAN DEFAULT TRUE,
            
            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(usuario_id, producto_maestro_id)
        )
        ''')
        
        agregar_columna_segura('patrones_compra', 'producto_maestro_id', 'INTEGER REFERENCES productos_maestros(id)')
        agregar_columna_segura('patrones_compra', 'establecimiento_preferido_id', 'INTEGER REFERENCES establecimientos(id)')
        agregar_columna_segura('patrones_compra', 'precio_promedio_pagado', 'INTEGER')
        
        print("✓ Tabla 'patrones_compra' actualizada")
        
        # ============================================
        # TABLAS AUXILIARES
        # ============================================
        
        # 3.1. CODIGOS_LOCALES (NUEVA - Productos sin EAN)
        # 3.3. CORRECCIONES_PRODUCTOS (Sistema de aprendizaje)
        print("🧠 Verificando tabla correcciones_productos...")
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'correcciones_productos'
            )
        """)
        tabla_correcciones_existe = cursor.fetchone()[0]
        
        if not tabla_correcciones_existe:
            print("   ✨ Creando tabla correcciones_productos...")
            cursor.execute('''
            CREATE TABLE correcciones_productos (
                id SERIAL PRIMARY KEY,
                
                nombre_ocr TEXT NOT NULL,
                codigo_ocr TEXT,
                
                codigo_correcto TEXT NOT NULL,
                nombre_correcto TEXT,
                
                nombre_normalizado TEXT NOT NULL,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                
                factura_id INTEGER REFERENCES facturas(id),
                usuario_id INTEGER,
                fecha_correccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                veces_aplicado INTEGER DEFAULT 0,
                
                UNIQUE(nombre_normalizado, establecimiento_id)
            )
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_nombre 
                ON correcciones_productos(nombre_normalizado)
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_establecimiento 
                ON correcciones_productos(establecimiento_id)
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_veces 
                ON correcciones_productos(veces_aplicado DESC)
            ''')
            
            conn.commit()
            print("   ✅ Tabla 'correcciones_productos' creada")
        else:
            print("   ✅ Tabla 'correcciones_productos' ya existe")
        
        # 3.4. PROCESSING_JOBS (Sistema de procesamiento asíncrono)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_jobs (
            id VARCHAR(50) PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id),
            video_path VARCHAR(255),
            status VARCHAR(20) DEFAULT 'pending',
            factura_id INTEGER REFERENCES facturas(id),
            frames_procesados INTEGER DEFAULT 0,
            frames_exitosos INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
        )
        ''')
        print("✓ Tabla 'processing_jobs' creada")

# Índices para processing_jobs
    crear_indice_seguro(
        'CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs(status, created_at DESC)',
        'processing_jobs.status'
    )
    crear_indice_seguro(
        'CREATE INDEX IF NOT EXISTS idx_processing_jobs_usuario ON processing_jobs(usuario_id, created_at DESC)',
        'processing_jobs.usuario'
    )
        
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'correcciones_productos'
            )
        """)
        tabla_correcciones_existe = cursor.fetchone()[0]
        
        if not tabla_correcciones_existe:
            print("   ✨ Creando tabla correcciones_productos...")
            cursor.execute('''
            CREATE TABLE correcciones_productos (
                id SERIAL PRIMARY KEY,
                
                nombre_ocr TEXT NOT NULL,
                codigo_ocr TEXT,
                
                codigo_correcto TEXT NOT NULL,
                nombre_correcto TEXT,
                
                nombre_normalizado TEXT NOT NULL,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                
                factura_id INTEGER REFERENCES facturas(id),
                usuario_id INTEGER,
                fecha_correccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                veces_aplicado INTEGER DEFAULT 0,
                
                UNIQUE(nombre_normalizado, establecimiento_id)
            )
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_nombre 
                ON correcciones_productos(nombre_normalizado)
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_establecimiento 
                ON correcciones_productos(establecimiento_id)
            ''')
            
            cursor.execute('''
            CREATE INDEX idx_correcciones_veces 
                ON correcciones_productos(veces_aplicado DESC)
            ''')
            
            conn.commit()
            print("   ✅ Tabla 'correcciones_productos' creada")
        else:
            print("   ✅ Tabla 'correcciones_productos' ya existe")
        
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
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS ocr_logs (
            id SERIAL PRIMARY KEY,
            factura_id INTEGER REFERENCES facturas(id),
            status TEXT,
            message TEXT,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        
        # ============================================
        # ÍNDICES OPTIMIZADOS
        # ============================================
        print("📊 Creando índices optimizados...")
        
        def crear_indice_seguro(sql_statement, descripcion):
            try:
                cursor.execute(sql_statement)
                conn.commit()
                return True
            except Exception as e:
                conn.rollback()
                return False
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_establecimientos_cadena ON establecimientos(cadena)', 'establecimientos.cadena')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_establecimientos_ciudad ON establecimientos(ciudad)', 'establecimientos.ciudad')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_establecimientos_nombre ON establecimientos(nombre_normalizado)', 'establecimientos.nombre')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_productos_maestros_ean ON productos_maestros(codigo_ean)', 'productos_maestros.codigo_ean')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_productos_maestros_nombre ON productos_maestros(nombre_normalizado)', 'productos_maestros.nombre')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_productos_maestros_categoria ON productos_maestros(categoria)', 'productos_maestros.categoria')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_precios_producto_maestro_fecha ON precios_productos(producto_maestro_id, fecha_registro DESC)', 'precios_productos.producto_fecha')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_precios_establecimiento ON precios_productos(establecimiento_id, fecha_registro DESC)', 'precios_productos.establecimiento')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_precios_usuario ON precios_productos(usuario_id)', 'precios_productos.usuario')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)', 'facturas.usuario')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_facturas_fecha ON facturas(fecha_factura DESC)', 'facturas.fecha')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_facturas_establecimiento ON facturas(establecimiento_id)', 'facturas.establecimiento_id')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_items_factura ON items_factura(factura_id)', 'items_factura.factura_id')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_items_producto_maestro ON items_factura(producto_maestro_id)', 'items_factura.producto')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_items_usuario ON items_factura(usuario_id)', 'items_factura.usuario')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_gastos_usuario ON gastos_mensuales(usuario_id, anio DESC, mes DESC)', 'gastos_mensuales.usuario')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_patrones_usuario_maestro ON patrones_compra(usuario_id, producto_maestro_id)', 'patrones_compra.usuario_producto')
        
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_correcciones_nombre ON correcciones_productos(nombre_normalizado)', 'correcciones_productos.nombre')
        crear_indice_seguro('CREATE INDEX IF NOT EXISTS idx_correcciones_establecimiento ON correcciones_productos(establecimiento_id)', 'correcciones_productos.establecimiento')
        
        print("✓ Índices creados")
        
        conn.commit()
        conn.close()
        print("✅ Base de datos PostgreSQL configurada correctamente")
        
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            conn.close()

def create_sqlite_tables():
    """Crear tablas en SQLite con nueva arquitectura"""
    conn = get_sqlite_connection()
    if not conn:
        return
    
    try:
        cursor = conn.cursor()
        
        print("🏗️ Creando tablas SQLite con nueva arquitectura...")
        
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
            cursor.execute('''
        CREATE TABLE IF NOT EXISTS processing_jobs (
            id TEXT PRIMARY KEY,
            usuario_id INTEGER REFERENCES usuarios(id),
            video_path TEXT,
            status TEXT DEFAULT 'pending',
            factura_id INTEGER REFERENCES facturas(id),
            frames_procesados INTEGER DEFAULT 0,
            frames_exitosos INTEGER DEFAULT 0,
            productos_detectados INTEGER DEFAULT 0,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            started_at DATETIME,
            completed_at DATETIME,
            CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
        )
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_processing_jobs_status 
         ON processing_jobs(status, created_at DESC)
        ''')

        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_processing_jobs_usuario 
         ON processing_jobs(usuario_id, created_at DESC)
        ''')
        
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
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS correcciones_productos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            nombre_ocr TEXT NOT NULL,
            codigo_ocr TEXT,
            
            codigo_correcto TEXT NOT NULL,
            nombre_correcto TEXT,
            
            nombre_normalizado TEXT NOT NULL,
            establecimiento_id INTEGER REFERENCES establecimientos(id),
            
            factura_id INTEGER REFERENCES facturas(id),
            usuario_id INTEGER,
            fecha_correccion DATETIME DEFAULT CURRENT_TIMESTAMP,
            veces_aplicado INTEGER DEFAULT 0,
            
            UNIQUE(nombre_normalizado, establecimiento_id)
        )
        ''')
        
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

def normalizar_nombre_establecimiento(nombre_raw: str) -> str:
    """Normaliza el nombre de un establecimiento"""
    if not nombre_raw:
        return ""
    
    nombre = nombre_raw.strip().lower()
    
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
            cursor.execute(
                "SELECT id FROM establecimientos WHERE nombre_normalizado = %s",
                (nombre_normalizado,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                conn.close()
                return resultado[0]
            
            cursor.execute("""
                INSERT INTO establecimientos (nombre_normalizado, cadena)
                VALUES (%s, %s)
                RETURNING id
            """, (nombre_normalizado, cadena))
            
            establecimiento_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return establecimiento_id
            
        else:
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
    if not codigo_ean or len(codigo_ean) < 3:
        return None
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE codigo_ean = %s",
                (codigo_ean,)
            )
            resultado = cursor.fetchone()
            
            if resultado:
                cursor.execute("""
                    UPDATE productos_maestros 
                    SET total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (resultado[0],))
                conn.commit()
                conn.close()
                return resultado[0]
            
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
            
        else:
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
                pm.nombre_normalizado,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.veces_comprado,
                pm.precio_promedio_global,
                EXTRACT(DAY FROM (CURRENT_TIMESTAMP - pc.ultima_compra)) as dias_sin_comprar
            FROM patrones_compra pc
            JOIN productos_maestros pm ON pc.producto_maestro_id = pm.id
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
                pm.nombre_normalizado,
                pc.frecuencia_dias,
                pc.ultima_compra,
                pc.veces_comprado,
                pm.precio_promedio_global,
                julianday('now') - julianday(pc.ultima_compra) as dias_sin_comprar
            FROM patrones_compra pc
            JOIN productos_maestros pm ON pc.producto_maestro_id = pm.id
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
            cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = %s", (codigo_ean,))
        else:
            cursor.execute("SELECT id FROM productos_maestros WHERE codigo_ean = ?", (codigo_ean,))
        
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



def crear_processing_job(usuario_id: int, video_path: str) -> str:
    """Crea un nuevo job de procesamiento"""
    import uuid
    
    job_id = str(uuid.uuid4())
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                INSERT INTO processing_jobs 
                (id, usuario_id, video_path, status)
                VALUES (%s, %s, %s, 'pending')
            """, (job_id, usuario_id, video_path))
        else:
            cursor.execute("""
                INSERT INTO processing_jobs 
                (id, usuario_id, video_path, status)
                VALUES (?, ?, ?, 'pending')
            """, (job_id, usuario_id, video_path))
        
        conn.commit()
        return job_id
        
    except Exception as e:
        print(f"❌ Error creando job: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()

def actualizar_job_status(job_id: str, status: str, **kwargs):
    """Actualiza el status de un job"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        campos = ["status = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else "status = ?"]
        valores = [status]
        
        if status == 'processing' and 'started_at' not in kwargs:
            kwargs['started_at'] = 'CURRENT_TIMESTAMP'
        elif status in ['completed', 'failed'] and 'completed_at' not in kwargs:
            kwargs['completed_at'] = 'CURRENT_TIMESTAMP'
        
        for key, value in kwargs.items():
            if value == 'CURRENT_TIMESTAMP':
                campos.append(f"{key} = CURRENT_TIMESTAMP")
            else:
                campos.append(f"{key} = %s" if os.environ.get("DATABASE_TYPE") == "postgresql" else f"{key} = ?")
                valores.append(value)
        
        valores.append(job_id)
        
        query = f"""
            UPDATE processing_jobs 
            SET {', '.join(campos)}
            WHERE id = {'%s' if os.environ.get("DATABASE_TYPE") == "postgresql" else '?'}
        """
        
        cursor.execute(query, valores)
        conn.commit()
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando job: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def obtener_job_info(job_id: str) -> dict:
    """Obtiene información completa de un job"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT 
                    id, usuario_id, status, factura_id, 
                    frames_procesados, frames_exitosos, productos_detectados,
                    error_message, created_at, started_at, completed_at
                FROM processing_jobs
                WHERE id = %s
            """, (job_id,))
        else:
            cursor.execute("""
                SELECT 
                    id, usuario_id, status, factura_id, 
                    frames_procesados, frames_exitosos, productos_detectados,
                    error_message, created_at, started_at, completed_at
                FROM processing_jobs
                WHERE id = ?
            """, (job_id,))
        
        row = cursor.fetchone()
        
        if not row:
            return None
        
        job_info = {
            "job_id": row[0],
            "usuario_id": row[1],
            "status": row[2],
            "factura_id": row[3],
            "frames_procesados": row[4],
            "frames_exitosos": row[5],
            "productos_detectados": row[6],
            "error_message": row[7],
            "created_at": str(row[8]),
            "started_at": str(row[9]) if row[9] else None,
            "completed_at": str(row[10]) if row[10] else None,
        }
        
        if job_info['status'] == 'completed' and job_info['factura_id']:
            job_info['factura'] = obtener_factura_completa(job_info['factura_id'])
        
        return job_info
        
    except Exception as e:
        print(f"❌ Error obteniendo job info: {e}")
        return None
    finally:
        conn.close()

def obtener_factura_completa(factura_id: int) -> dict:
    """Obtiene datos completos de una factura"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT 
                    f.id, f.establecimiento, f.total_factura, f.fecha_factura,
                    f.productos_guardados, e.nombre_normalizado
                FROM facturas f
                LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
                WHERE f.id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT 
                    f.id, f.establecimiento, f.total_factura, f.fecha_factura,
                    f.productos_guardados, e.nombre_normalizado
                FROM facturas f
                LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
                WHERE f.id = ?
            """, (factura_id,))
        
        factura_row = cursor.fetchone()
        
        if not factura_row:
            return None
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT codigo, nombre, valor
                FROM productos
                WHERE factura_id = %s
                ORDER BY id
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT codigo, nombre, valor
                FROM productos
                WHERE factura_id = ?
                ORDER BY id
            """, (factura_id,))
        
        productos = []
        for prod in cursor.fetchall():
            productos.append({
                "codigo": prod[0],
                "nombre": prod[1],
                "precio": prod[2]
            })
        
        return {
            "factura_id": factura_row[0],
            "establecimiento": factura_row[5] or factura_row[1],
            "total": factura_row[2],
            "fecha": str(factura_row[3]),
            "productos": productos,
            "total_productos": len(productos)
        }
        
    except Exception as e:
        print(f"❌ Error obteniendo factura completa: {e}")
        return None
    finally:
        conn.close()

def obtener_jobs_pendientes(usuario_id: int, limit: int = 10) -> list:
    """Obtiene los últimos jobs del usuario"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute("""
                SELECT id, status, created_at, completed_at
                FROM processing_jobs
                WHERE usuario_id = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (usuario_id, limit))
        else:
            cursor.execute("""
                SELECT id, status, created_at, completed_at
                FROM processing_jobs
                WHERE usuario_id = ?
                ORDER BY created_at DESC
                LIMIT ?
            """, (usuario_id, limit))
        
        jobs = []
        for row in cursor.fetchall():
            jobs.append({
                "job_id": row[0],
                "status": row[1],
                "created_at": str(row[2]),
                "completed_at": str(row[3]) if row[3] else None
            })
        
        return jobs
        
    except Exception as e:
        print(f"❌ Error obteniendo jobs pendientes: {e}")
        return []
    finally:
        conn.close()

if __name__ == "__main__":
    print("🔧 Inicializando sistema de base de datos...")
    test_database_connection()
    create_tables()  # Ya incluye processing_jobs ahora
    print("✅ Sistema inicializado")
