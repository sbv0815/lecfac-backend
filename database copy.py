import os
import sqlite3
import bcrypt
from urllib.parse import urlparse
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Any


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
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

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
            print(
                "⚠️ PostgreSQL solicitado pero librerías no disponibles, usando SQLite"
            )
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
            print("💡 Verifica que Railway tenga la variable DATABASE_URL configurada")
            return None

        print(f"🔗 Intentando conectar a PostgreSQL (psycopg{PSYCOPG_VERSION})...")

        if PSYCOPG_VERSION == 3:
            import psycopg
            conn = psycopg.connect(database_url)
        else:
            import psycopg2

            url = urlparse(database_url)

            # Debug: Ver qué estamos parseando
            print(f"🔍 Parseando DATABASE_URL:")
            print(f"   Host: {url.hostname}")
            print(f"   Port: {url.port or 5432}")
            print(f"   Database: {url.path[1:] if url.path else 'N/A'}")
            print(f"   User: {url.username}")

            # Validar que tenemos todos los componentes necesarios
            if not url.hostname:
                raise ValueError(f"DATABASE_URL inválida - hostname es None. URL: {database_url[:50]}...")

            conn = psycopg2.connect(
                host=url.hostname,
                database=url.path[1:],
                user=url.username,
                password=url.password,
                port=url.port or 5432,
                connect_timeout=10,
                sslmode='prefer',
                options='-c search_path=public'
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
        conn = sqlite3.connect("lecfac.db")
        conn.row_factory = sqlite3.Row
        print("✅ Conexión SQLite exitosa")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a SQLite: {e}")
        return None


def create_tables():
    """Crear tablas según el tipo de base de datos"""
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

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
        # NIVEL 0: USUARIOS
        # ============================================
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'usuarios' creada")

        # Agregar columnas adicionales
        print("🔧 Verificando columnas adicionales en usuarios...")

        columnas_usuarios_requeridas = {
            "ultimo_acceso": "TIMESTAMP",
            "activo": "BOOLEAN DEFAULT TRUE",
            "rol": "VARCHAR(50) DEFAULT 'usuario'",
        }

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'usuarios'
        """
        )
        columnas_existentes = [row[0] for row in cursor.fetchall()]

        for columna, tipo in columnas_usuarios_requeridas.items():
            if columna not in columnas_existentes:
                try:
                    cursor.execute(
                        f"""
                        ALTER TABLE usuarios
                        ADD COLUMN {columna} {tipo}
                    """
                    )
                    conn.commit()
                    print(f"   ✅ Columna '{columna}' agregada a usuarios")
                except Exception as e:
                    print(f"   ⚠️ {columna}: {e}")
                    conn.rollback()

        # ============================================
        # TABLA DE RECUPERACIÓN DE CONTRASEÑAS
        # ============================================
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS password_resets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                reset_code VARCHAR(6) NOT NULL,
                expire_at TIMESTAMP NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT unique_user_reset UNIQUE(user_id)
            )
        """
        )
        print("✓ Tabla 'password_resets' creada")

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reset_code
            ON password_resets(reset_code)
        """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_reset_user_id
            ON password_resets(user_id)
        """
        )
        print("✓ Índices de 'password_resets' creados")

        # ============================================
        # NIVEL 1: BASE UNIFICADA (GLOBAL)
        # ============================================

        # 1.1. ESTABLECIMIENTOS
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS establecimientos (
                id SERIAL PRIMARY KEY,
                nombre_normalizado VARCHAR(200) UNIQUE NOT NULL,
                cadena VARCHAR(50),
                tipo VARCHAR(50),
                ciudad VARCHAR(100),
                direccion TEXT,
                latitud DECIMAL(10, 8),
                longitud DECIMAL(11, 8),
                total_facturas_reportadas INTEGER DEFAULT 0,
                calificacion_promedio DECIMAL(3, 2),
                activo BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        print("✓ Tabla 'establecimientos' creada")

        # 1.2. PRODUCTOS_MAESTROS
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS productos_maestros (
                id SERIAL PRIMARY KEY,
                codigo_ean VARCHAR(13),
                nombre_normalizado VARCHAR(200) NOT NULL,
                nombre_comercial VARCHAR(200),
                marca VARCHAR(100),
                categoria VARCHAR(50),
                subcategoria VARCHAR(50),
                presentacion VARCHAR(50),
                es_producto_fresco BOOLEAN DEFAULT FALSE,
                imagen_url TEXT,
                total_reportes INTEGER DEFAULT 0,
                total_usuarios_reportaron INTEGER DEFAULT 0,
                precio_promedio_global INTEGER,
                precio_minimo_historico INTEGER,
                precio_maximo_historico INTEGER,
                primera_vez_reportado TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CHECK (codigo_ean IS NULL OR (LENGTH(codigo_ean) >= 3 AND LENGTH(codigo_ean) <= 14)),
                CHECK (total_reportes >= 0)
            )
        """
        )
        print("✓ Tabla 'productos_maestros' creada")

        # Corregir constraint de productos_maestros si existe
        print("🔧 Corrigiendo constraints de productos_maestros...")
        try:
            cursor.execute(
                """
                ALTER TABLE productos_maestros
                DROP CONSTRAINT IF EXISTS productos_maestros_codigo_ean_check
            """
            )
            conn.commit()

            cursor.execute(
                """
                ALTER TABLE productos_maestros
                ADD CONSTRAINT productos_maestros_codigo_ean_check
                CHECK (LENGTH(codigo_ean) >= 3 AND LENGTH(codigo_ean) <= 14)
            """
            )
            conn.commit()
            print("✅ Constraint actualizado: códigos PLU (3+ dígitos) permitidos")
        except Exception as e:
            print(f"⚠️ Constraint ya correcto o error menor: {e}")
            conn.rollback()

        # 1.3. PRECIOS_PRODUCTOS (MIGRACIÓN INTELIGENTE)
        print("🔧 Configurando tabla precios_productos...")

        cursor.execute(
            """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'precios_productos'
            )
        """
        )
        tabla_precios_existe = cursor.fetchone()[0]

        if tabla_precios_existe:
            print("   📋 Tabla precios_productos existe, verificando estructura...")

            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'precios_productos'
            """
            )
            columnas_existentes = [row[0] for row in cursor.fetchall()]
            print(f"   📊 Columnas actuales: {', '.join(columnas_existentes)}")

            if "establecimiento_id" not in columnas_existentes:
                print("   ➕ Agregando columna establecimiento_id...")
                try:
                    cursor.execute(
                        """
                        ALTER TABLE precios_productos
                        ADD COLUMN establecimiento_id INTEGER
                    """
                    )
                    conn.commit()
                    print("   ✅ Columna establecimiento_id agregada")
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()

            if (
                "producto_id" in columnas_existentes
                and "producto_maestro_id" not in columnas_existentes
            ):
                print("   🔄 Renombrando producto_id → producto_maestro_id...")
                try:
                    cursor.execute(
                        """
                        ALTER TABLE precios_productos
                        RENAME COLUMN producto_id TO producto_maestro_id
                    """
                    )
                    conn.commit()
                    print("   ✅ Columna renombrada")
                    columnas_existentes.remove("producto_id")
                    columnas_existentes.append("producto_maestro_id")
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()
            elif (
                "producto_maestro_id" not in columnas_existentes
                and "producto_id" not in columnas_existentes
            ):
                print("   ➕ Agregando columna producto_maestro_id...")
                try:
                    cursor.execute(
                        """
                        ALTER TABLE precios_productos
                        ADD COLUMN producto_maestro_id INTEGER
                    """
                    )
                    conn.commit()
                    print("   ✅ Columna producto_maestro_id agregada")
                except Exception as e:
                    print(f"   ⚠️ Error: {e}")
                    conn.rollback()

            columnas_requeridas = {
                "precio": "INTEGER",
                "fecha_registro": "DATE",
                "usuario_id": "INTEGER",
                "factura_id": "INTEGER",
                "verificado": "BOOLEAN DEFAULT FALSE",
                "es_outlier": "BOOLEAN DEFAULT FALSE",
                "votos_confianza": "INTEGER DEFAULT 0",
                "fecha_actualizacion": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
                "fecha_creacion": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            }

            for columna, tipo in columnas_requeridas.items():
                if columna not in columnas_existentes:
                    print(f"   ➕ Agregando columna {columna}...")
                    try:
                        cursor.execute(
                            f"""
                            ALTER TABLE precios_productos
                            ADD COLUMN {columna} {tipo}
                        """
                        )
                        conn.commit()
                    except Exception as e:
                        print(f"   ⚠️ {columna}: {e}")
                        conn.rollback()

            print("   🔧 Limpiando constraints viejos...")
            cursor.execute(
                """
                SELECT con.conname
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                WHERE rel.relname = 'precios_productos'
                AND con.contype = 'f'
            """
            )
            constraints = cursor.fetchall()

            for constraint in constraints:
                constraint_name = constraint[0]
                try:
                    cursor.execute(
                        f"ALTER TABLE precios_productos DROP CONSTRAINT IF EXISTS {constraint_name}"
                    )
                    conn.commit()
                except:
                    conn.rollback()

            print("   ✅ Agregando constraints correctos...")

            try:
                cursor.execute(
                    """
                    ALTER TABLE precios_productos
                    ADD CONSTRAINT precios_productos_producto_maestro_fkey
                    FOREIGN KEY (producto_maestro_id) REFERENCES productos_maestros(id)
                """
                )
                conn.commit()
                print("   ✅ FK producto_maestro_id → productos_maestros")
            except Exception as e:
                print(f"   ⚠️ FK producto_maestro: {e}")
                conn.rollback()

            try:
                cursor.execute(
                    """
                    ALTER TABLE precios_productos
                    ADD CONSTRAINT precios_productos_establecimiento_fkey
                    FOREIGN KEY (establecimiento_id) REFERENCES establecimientos(id)
                """
                )
                conn.commit()
                print("   ✅ FK establecimiento_id → establecimientos")
            except Exception as e:
                print(f"   ⚠️ FK establecimiento: {e}")
                conn.rollback()

            try:
                cursor.execute(
                    """
                    ALTER TABLE precios_productos
                    ADD CONSTRAINT precios_productos_usuario_fkey
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id)
                """
                )
                conn.commit()
                print("   ✅ FK usuario_id → usuarios")
            except Exception as e:
                print(f"   ⚠️ FK usuario: {e}")
                conn.rollback()

        else:
            print("   ✨ Creando tabla precios_productos desde cero...")
            cursor.execute(
                """
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
            """
            )
            conn.commit()
            print("   ✅ Tabla creada con estructura correcta")

        print("✅ Tabla 'precios_productos' configurada correctamente")

        # ============================================
        # NIVEL 2: BASE LOCAL (POR USUARIO)
        # ============================================

        # 2.1. FACTURAS
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'facturas' creada")

        # 2.2. ITEMS_FACTURA
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'items_factura' creada")

        # 2.3. GASTOS_MENSUALES
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'gastos_mensuales' creada")

        # 2.4. PATRONES_COMPRA
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'patrones_compra' creada")

        # ============================================
        # NUEVAS TABLAS PERSONALES
        # ============================================

        # 2.5. INVENTARIO_USUARIO (VERSIÓN COMPLETA)
        print("🏗️ Creando tabla inventario_usuario...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario_usuario (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),

                -- Cantidades y unidades
                cantidad_actual DECIMAL(10, 2) DEFAULT 0,
                unidad_medida VARCHAR(20) DEFAULT 'unidades',
                cantidad_por_unidad DECIMAL(10, 2),

                -- Precios
                precio_ultima_compra INTEGER,
                precio_promedio INTEGER,
                precio_minimo INTEGER,
                precio_maximo INTEGER,

                -- Establecimiento
                establecimiento TEXT,
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                ubicacion TEXT,

                -- Marca del producto
                marca TEXT,

                -- Fechas
                fecha_ultima_compra DATE,
                fecha_ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Frecuencia y alertas
                frecuencia_compra_dias INTEGER,
                fecha_estimada_agotamiento DATE,
                nivel_alerta DECIMAL(10, 2) DEFAULT 0,
                alerta_activa BOOLEAN DEFAULT TRUE,

                -- Estadísticas
                numero_compras INTEGER DEFAULT 0,
                cantidad_total_comprada DECIMAL(10, 2) DEFAULT 0,

                -- Relación con facturas
                ultima_factura_id INTEGER REFERENCES facturas(id),

                -- Notas del usuario
                notas TEXT,

                UNIQUE(usuario_id, producto_maestro_id),
                CHECK (cantidad_actual >= 0),
                CHECK (nivel_alerta >= 0),
                CHECK (numero_compras >= 0)
            )
        """
        )
        print("✓ Tabla 'inventario_usuario' creada")

        # Agregar columnas si la tabla ya existe (migración automática)
        print("🔧 Verificando columnas de inventario_usuario...")

        columnas_requeridas_inventario = {
            "precio_ultima_compra": "INTEGER",
            "precio_promedio": "INTEGER",
            "precio_minimo": "INTEGER",
            "precio_maximo": "INTEGER",
            "establecimiento": "TEXT",
            "establecimiento_id": "INTEGER",
            "ubicacion": "TEXT",
            "marca": "TEXT",
            "cantidad_por_unidad": "DECIMAL(10, 2)",
            "fecha_creacion": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "numero_compras": "INTEGER DEFAULT 0",
            "cantidad_total_comprada": "DECIMAL(10, 2) DEFAULT 0",
            "ultima_factura_id": "INTEGER",
        }

        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'inventario_usuario'
        """
        )
        columnas_existentes = [row[0] for row in cursor.fetchall()]

        for columna, tipo in columnas_requeridas_inventario.items():
            if columna not in columnas_existentes:
                try:
                    cursor.execute(
                        f"""
                        ALTER TABLE inventario_usuario
                        ADD COLUMN {columna} {tipo}
                    """
                    )
                    conn.commit()
                    print(f"   ✅ Columna '{columna}' agregada")
                except Exception as e:
                    print(f"   ⚠️ {columna}: {e}")
                    conn.rollback()

        # 2.6. PRESUPUESTO_USUARIO
        print("🏗️ Creando tabla presupuesto_usuario...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS presupuesto_usuario (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                monto_mensual INTEGER NOT NULL,
                monto_semanal INTEGER,
                anio INTEGER NOT NULL,
                mes INTEGER NOT NULL,
                gasto_actual INTEGER DEFAULT 0,
                gasto_semanal_actual INTEGER DEFAULT 0,
                fecha_inicio DATE NOT NULL,
                fecha_fin DATE NOT NULL,
                alerta_75_enviada BOOLEAN DEFAULT FALSE,
                alerta_90_enviada BOOLEAN DEFAULT FALSE,
                alerta_100_enviada BOOLEAN DEFAULT FALSE,
                activo BOOLEAN DEFAULT TRUE,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(usuario_id, anio, mes),
                CHECK (monto_mensual > 0),
                CHECK (mes >= 1 AND mes <= 12),
                CHECK (gasto_actual >= 0)
            )
        """
        )
        print("✓ Tabla 'presupuesto_usuario' creada")

        # 2.7. ALERTAS_USUARIO
        print("🏗️ Creando tabla alertas_usuario...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS alertas_usuario (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
                producto_maestro_id INTEGER REFERENCES productos_maestros(id),
                establecimiento_id INTEGER REFERENCES establecimientos(id),
                tipo_alerta VARCHAR(50) NOT NULL,
                umbral_valor INTEGER,
                umbral_porcentaje DECIMAL(5, 2),
                mensaje_personalizado TEXT,
                activa BOOLEAN DEFAULT TRUE,
                enviada BOOLEAN DEFAULT FALSE,
                fecha_envio TIMESTAMP,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                fecha_expiracion DATE,
                prioridad VARCHAR(20) DEFAULT 'media',
                canal_envio VARCHAR(20) DEFAULT 'app',
                CHECK (tipo_alerta IN ('stock_bajo', 'precio_bajo', 'presupuesto', 'producto_agotado', 'nuevo_precio', 'oferta_establecimiento')),
                CHECK (prioridad IN ('baja', 'media', 'alta', 'urgente')),
                CHECK (canal_envio IN ('app', 'email', 'push', 'sms'))
            )
        """
        )
        print("✓ Tabla 'alertas_usuario' creada")

        # ============================================
        # TABLAS AUXILIARES
        # ============================================

        # 3.1. CODIGOS_LOCALES
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'codigos_locales' creada")

        # 3.2. MATCHING_LOGS
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'matching_logs' creada")

        # 3.3. CORRECCIONES_PRODUCTOS
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS correcciones_productos (
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
        """
        )
        print("✓ Tabla 'correcciones_productos' creada")

        # 3.4. PROCESSING_JOBS
        cursor.execute(
            """
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
        """
        )
        print("✓ Tabla 'processing_jobs' creada")

        # ============================================
        # TABLAS LEGACY (mantener para migración)
        # ============================================
        print("📦 Manteniendo tablas legacy...")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS productos (
                id SERIAL PRIMARY KEY,
                factura_id INTEGER NOT NULL REFERENCES facturas(id) ON DELETE CASCADE,
                codigo VARCHAR(20),
                nombre VARCHAR(100),
                valor INTEGER
            )
        """
        )

        cursor.execute(
            """
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
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS productos_catalogo (
                id SERIAL PRIMARY KEY,
                codigo_ean VARCHAR(13) UNIQUE,
                nombre_producto VARCHAR(100) NOT NULL,
                es_producto_fresco BOOLEAN DEFAULT FALSE,
                primera_fecha_reporte TIMESTAMP,
                total_reportes INTEGER DEFAULT 1,
                ultimo_reporte TIMESTAMP
            )
        """
        )

        cursor.execute(
            """
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
        """
        )

        cursor.execute(
            """
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
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ocr_logs (
                id SERIAL PRIMARY KEY,
                factura_id INTEGER REFERENCES facturas(id),
                status TEXT,
                message TEXT,
                details TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )

        print("✓ Tablas legacy creadas")

        # ============================================
        # FUNCIÓN PARA CREAR ÍNDICES DE FORMA SEGURA
        # ============================================
        def crear_indice_seguro(sql_statement, descripcion):
            """Crea un índice de forma segura, manejando errores"""
            try:
                cursor.execute(sql_statement)
                conn.commit()
                print(f"   ✓ Índice {descripcion}")
                return True
            except Exception as e:
                print(f"   ⚠️ Índice {descripcion}: {e}")
                conn.rollback()
                return False

        # ============================================
        # ÍNDICES OPTIMIZADOS
        # ============================================
        print("📊 Creando índices optimizados...")

        # Índices de establecimientos
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_establecimientos_cadena ON establecimientos(cadena)",
            "establecimientos.cadena",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_establecimientos_ciudad ON establecimientos(ciudad)",
            "establecimientos.ciudad",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_establecimientos_nombre ON establecimientos(nombre_normalizado)",
            "establecimientos.nombre",
        )

        # Índices de productos_maestros
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_productos_maestros_ean ON productos_maestros(codigo_ean)",
            "productos_maestros.codigo_ean",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_productos_maestros_nombre ON productos_maestros(nombre_normalizado)",
            "productos_maestros.nombre",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_productos_maestros_categoria ON productos_maestros(categoria)",
            "productos_maestros.categoria",
        )

        # Índices de precios_productos
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_precios_producto_maestro_fecha ON precios_productos(producto_maestro_id, fecha_registro DESC)",
            "precios_productos.producto_fecha",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_precios_establecimiento ON precios_productos(establecimiento_id, fecha_registro DESC)",
            "precios_productos.establecimiento",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_precios_usuario ON precios_productos(usuario_id)",
            "precios_productos.usuario",
        )

        # Índices de facturas
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_facturas_usuario ON facturas(usuario_id)",
            "facturas.usuario",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_facturas_fecha ON facturas(fecha_factura DESC)",
            "facturas.fecha",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_facturas_establecimiento ON facturas(establecimiento_id)",
            "facturas.establecimiento_id",
        )

        # Índices de items_factura
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_items_factura ON items_factura(factura_id)",
            "items_factura.factura_id",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_items_producto_maestro ON items_factura(producto_maestro_id)",
            "items_factura.producto",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_items_usuario ON items_factura(usuario_id)",
            "items_factura.usuario",
        )

        # Índices de gastos_mensuales
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_gastos_usuario ON gastos_mensuales(usuario_id, anio DESC, mes DESC)",
            "gastos_mensuales.usuario",
        )

        # Índices de patrones_compra
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_patrones_usuario_maestro ON patrones_compra(usuario_id, producto_maestro_id)",
            "patrones_compra.usuario_producto",
        )

        # Índices de inventario_usuario
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_inventario_usuario ON inventario_usuario(usuario_id, producto_maestro_id)",
            "inventario_usuario.usuario_producto",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_inventario_establecimiento ON inventario_usuario(establecimiento_id)",
            "inventario_usuario.establecimiento",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_inventario_alerta ON inventario_usuario(usuario_id, alerta_activa, fecha_estimada_agotamiento)",
            "inventario_usuario.alertas",
        )

        # Índices de presupuesto_usuario
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_presupuesto_usuario_periodo ON presupuesto_usuario(usuario_id, anio DESC, mes DESC)",
            "presupuesto_usuario.periodo",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_presupuesto_activo ON presupuesto_usuario(usuario_id, activo, fecha_inicio, fecha_fin)",
            "presupuesto_usuario.activo",
        )

        # Índices de alertas_usuario
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_alertas_usuario_activas ON alertas_usuario(usuario_id, activa, tipo_alerta)",
            "alertas_usuario.activas",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_alertas_producto ON alertas_usuario(producto_maestro_id, activa)",
            "alertas_usuario.producto",
        )

        # Índices de processing_jobs
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_processing_jobs_status ON processing_jobs(status, created_at DESC)",
            "processing_jobs.status",
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_processing_jobs_usuario ON processing_jobs(usuario_id, created_at DESC)",
            "processing_jobs.usuario",
        )

        print("✅ Índices optimizados creados")

        # ⭐ MIGRACIÓN FORZADA: Agregar columnas faltantes de inventario_usuario
        print("🔄 Verificando columnas faltantes de inventario_usuario...")
        try:
            columnas_faltantes = [
                ("establecimiento_nombre", "VARCHAR(255)"),
                ("establecimiento_ubicacion", "VARCHAR(255)"),
                ("total_gastado", "DECIMAL(12,2) DEFAULT 0.0"),
                ("dias_desde_ultima_compra", "INTEGER DEFAULT 0"),
            ]

            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'inventario_usuario'
            """
            )
            columnas_existentes_inv = [row[0] for row in cursor.fetchall()]

            for nombre_col, tipo_col in columnas_faltantes:
                if nombre_col not in columnas_existentes_inv:
                    try:
                        cursor.execute(
                            f"""
                            ALTER TABLE inventario_usuario
                            ADD COLUMN IF NOT EXISTS {nombre_col} {tipo_col}
                        """
                        )
                        conn.commit()
                        print(
                            f"   ✅ Columna '{nombre_col}' agregada a inventario_usuario"
                        )
                    except Exception as e:
                        print(f"   ⚠️ '{nombre_col}': {e}")
                        conn.rollback()

            print("✅ Columnas de inventario_usuario verificadas")

        except Exception as e:
            print(f"⚠️ Error verificando columnas: {e}")
            conn.rollback()

        # ============================================
        # TABLA DE AUDITORÍA
        # ============================================
        print("🏗️ Creando tabla auditoria_productos...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_productos (
                id SERIAL PRIMARY KEY,
                usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
                producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),
                accion VARCHAR(20) NOT NULL CHECK (accion IN ('crear', 'actualizar', 'validar', 'eliminar')),
                datos_anteriores JSONB,
                datos_nuevos JSONB,
                razon TEXT,
                fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✓ Tabla 'auditoria_productos' creada")

        # Índices
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON auditoria_productos(usuario_id)",
            "auditoria_productos.usuario"
        )
        crear_indice_seguro(
            "CREATE INDEX IF NOT EXISTS idx_auditoria_producto ON auditoria_productos(producto_maestro_id)",
            "auditoria_productos.producto"
        )

        # Columnas adicionales en productos_maestros
        print("🔧 Agregando columnas de auditoría...")
        cursor.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
        """)
        columnas_pm = [row[0] for row in cursor.fetchall()]

        if 'auditado_manualmente' not in columnas_pm:
            try:
                cursor.execute("ALTER TABLE productos_maestros ADD COLUMN auditado_manualmente BOOLEAN DEFAULT FALSE")
                cursor.execute("ALTER TABLE productos_maestros ADD COLUMN validaciones_manuales INTEGER DEFAULT 0")
                cursor.execute("ALTER TABLE productos_maestros ADD COLUMN ultima_validacion TIMESTAMP")
                conn.commit()
                print("   ✅ Columnas de auditoría agregadas")
            except Exception as e:
                print(f"   ⚠️ {e}")
                conn.rollback()

        conn.commit()
        print("✅ Base de datos PostgreSQL configurada correctamente")
    except Exception as e:
        print(f"❌ Error creando tablas PostgreSQL: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
    finally:
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

        cursor.execute(
            """
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
        """
        )

        # Agregar columnas adicionales a usuarios en SQLite
        cursor.execute("PRAGMA table_info(usuarios)")
        columnas_existentes = [row[1] for row in cursor.fetchall()]

        if "ultimo_acceso" not in columnas_existentes:
            try:
                cursor.execute("ALTER TABLE usuarios ADD COLUMN ultimo_acceso DATETIME")
                print("   ✅ Columna 'ultimo_acceso' agregada")
            except:
                pass

        if "activo" not in columnas_existentes:
            try:
                cursor.execute(
                    "ALTER TABLE usuarios ADD COLUMN activo INTEGER DEFAULT 1"
                )
                print("   ✅ Columna 'activo' agregada")
            except:
                pass

        if "rol" not in columnas_existentes:
            try:
                cursor.execute(
                    "ALTER TABLE usuarios ADD COLUMN rol TEXT DEFAULT 'usuario'"
                )
                print("   ✅ Columna 'rol' agregada")
            except:
                pass

        # Más tablas SQLite...

        conn.commit()
        conn.close()
        print("✅ Tablas SQLite creadas/actualizadas")

    except Exception as e:
        print(f"❌ Error creando tablas SQLite: {e}")
        if conn:
            conn.close()


# ============================================
# FUNCIONES AUXILIARES
# ============================================


def normalizar_nombre_establecimiento(nombre_raw: str) -> str:
    """Normaliza el nombre de un establecimiento"""
    if not nombre_raw:
        return ""

    nombre = nombre_raw.strip().lower()

    normalizaciones = {
        "éxito": "exito",
        "olímpica": "olimpica",
        "almacenes exito": "exito",
        "almacenes éxito": "exito",
        "supertiendas olimpica": "olimpica",
        "justo & bueno": "justo y bueno",
        "justo&bueno": "justo y bueno",
    }

    for clave, valor in normalizaciones.items():
        if clave in nombre:
            nombre = nombre.replace(clave, valor)

    return " ".join(word.capitalize() for word in nombre.split())


def obtener_o_crear_establecimiento(nombre_raw: str, cadena: str = None) -> int:
    """Obtiene el ID de un establecimiento o lo crea si no existe"""
    nombre_normalizado = normalizar_nombre_establecimiento(nombre_raw)
    if not cadena:
        cadena = detectar_cadena(nombre_raw)

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM establecimientos WHERE nombre_normalizado = %s",
                (nombre_normalizado,),
            )
            resultado = cursor.fetchone()

            if resultado:
                conn.close()
                return resultado[0]

            cursor.execute(
                """
                INSERT INTO establecimientos (nombre_normalizado, cadena)
                VALUES (%s, %s)
                RETURNING id
            """,
                (nombre_normalizado, cadena),
            )

            establecimiento_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return establecimiento_id

        else:
            cursor.execute(
                "SELECT id FROM establecimientos WHERE nombre_normalizado = ?",
                (nombre_normalizado,),
            )
            resultado = cursor.fetchone()

            if resultado:
                conn.close()
                return resultado[0]

            cursor.execute(
                """
                INSERT INTO establecimientos (nombre_normalizado, cadena)
                VALUES (?, ?)
            """,
                (nombre_normalizado, cadena),
            )

            establecimiento_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return establecimiento_id

    except Exception as e:
        print(f"Error obteniendo/creando establecimiento: {e}")
        conn.close()
        return None


def obtener_o_crear_producto_maestro(
    codigo_ean: str, nombre: str, precio: int = None
) -> int:
    """Obtiene el ID de un producto maestro o lo crea si no existe"""
    if not codigo_ean or len(codigo_ean) < 3:
        return None

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE codigo_ean = %s", (codigo_ean,)
            )
            resultado = cursor.fetchone()

            if resultado:
                cursor.execute(
                    """
                    UPDATE productos_maestros
                    SET total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (resultado[0],),
                )
                conn.commit()
                conn.close()
                return resultado[0]

            cursor.execute(
                """
                INSERT INTO productos_maestros
                (codigo_ean, nombre_normalizado, precio_promedio_global, total_reportes)
                VALUES (%s, %s, %s, 1)
                RETURNING id
            """,
                (codigo_ean, nombre, precio),
            )

            producto_id = cursor.fetchone()[0]
            conn.commit()
            conn.close()
            return producto_id

        else:
            cursor.execute(
                "SELECT id FROM productos_maestros WHERE codigo_ean = ?", (codigo_ean,)
            )
            resultado = cursor.fetchone()

            if resultado:
                cursor.execute(
                    """
                    UPDATE productos_maestros
                    SET total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (resultado[0],),
                )
                conn.commit()
                conn.close()
                return resultado[0]

            cursor.execute(
                """
                INSERT INTO productos_maestros
                (codigo_ean, nombre_normalizado, precio_promedio_global, total_reportes)
                VALUES (?, ?, ?, 1)
            """,
                (codigo_ean, nombre, precio),
            )

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
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """Verifica una contraseña contra su hash"""
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena comercial basándose en el nombre del establecimiento"""
    if not establecimiento:
        return "otro"

    establecimiento_lower = establecimiento.lower()

    cadenas = {
        "exito": ["exito", "éxito", "almacenes exito", "almacenes éxito"],
        "carulla": ["carulla", "carulla fresh", "carulla express"],
        "jumbo": ["jumbo"],
        "olimpica": ["olimpica", "olímpica", "supertiendas olimpica"],
        "ara": ["ara", "tiendas ara"],
        "d1": ["d1", "tiendas d1", "tienda d1"],
        "justo_bueno": ["justo & bueno", "justo y bueno", "justo&bueno"],
        "alkosto": ["alkosto", "alkomprar"],
        "makro": ["makro"],
        "pricesmart": ["pricesmart", "price smart"],
        "home_center": ["homecenter", "home center"],
        "falabella": ["falabella"],
        "cruz_verde": ["cruz verde", "cruzverde"],
        "farmatodo": ["farmatodo"],
        "la_rebaja": ["la rebaja", "drogas la rebaja"],
        "cafam": ["cafam"],
        "colsubsidio": ["colsubsidio"],
        "euro": ["euro"],
        "metro": ["metro"],
        "consumo": ["consumo", "almacenes consumo"],
    }

    for cadena, palabras in cadenas.items():
        for palabra in palabras:
            if palabra in establecimiento_lower:
                return cadena

    return "otro"


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


def confirmar_producto_manual(producto_id: int, confirmado: bool):
    # Implementación pendiente
    pass


def actualizar_inventario_desde_factura(factura_id: int, usuario_id: int):
    """
    Actualiza el inventario del usuario basándose en los items de una factura

    Args:
        factura_id: ID de la factura procesada
        usuario_id: ID del usuario
    """
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo obtener conexión a la base de datos")
        return False

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        print(
            f"📦 Actualizando inventario para usuario {usuario_id} desde factura {factura_id}"
        )

        # 1. Obtener datos de la factura
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT establecimiento_id, establecimiento, fecha_factura
                FROM facturas
                WHERE id = %s
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT establecimiento_id, establecimiento, fecha_factura
                FROM facturas
                WHERE id = ?
            """,
                (factura_id,),
            )

        factura_data = cursor.fetchone()
        if not factura_data:
            print(f"⚠️ Factura {factura_id} no encontrada")
            cursor.close()
            conn.close()
            return False

        establecimiento_id = factura_data[0]
        establecimiento_nombre = factura_data[1]
        fecha_factura_raw = factura_data[2]

        # Convertir fecha_factura a date si es necesario
        if isinstance(fecha_factura_raw, str):
            fecha_compra = datetime.strptime(fecha_factura_raw, "%Y-%m-%d").date()
        elif hasattr(fecha_factura_raw, "date"):
            fecha_compra = (
                fecha_factura_raw.date()
                if callable(fecha_factura_raw.date)
                else fecha_factura_raw
            )
        else:
            fecha_compra = fecha_factura_raw or datetime.now().date()

        print(f"   🏪 Establecimiento: {establecimiento_nombre}")
        print(f"   📅 Fecha: {fecha_compra}")

        # 2. Obtener items de la factura
        if database_type == "postgresql":
            cursor.execute(
                """
                SELECT
                    producto_maestro_id,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    codigo_leido
                FROM items_factura
                WHERE factura_id = %s AND producto_maestro_id IS NOT NULL
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    producto_maestro_id,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    codigo_leido
                FROM items_factura
                WHERE factura_id = ? AND producto_maestro_id IS NOT NULL
            """,
                (factura_id,),
            )

        items = cursor.fetchall()

        if not items:
            print(f"⚠️ No hay items con producto_maestro_id en factura {factura_id}")
            cursor.close()
            conn.close()
            return False

        print(f"   📦 {len(items)} productos a actualizar")

        actualizados = 0
        creados = 0

        # 3. Actualizar o crear cada producto en el inventario
        for item in items:
            producto_maestro_id = item[0]
            nombre = item[1]
            precio = int(item[2]) if item[2] else 0
            cantidad = int(item[3]) if item[3] else 1
            codigo = item[4]

            try:
                # 3.1 Verificar si ya existe en inventario
                if database_type == "postgresql":
                    cursor.execute(
                        """
                        SELECT
                            id, cantidad_actual, precio_promedio, precio_minimo, precio_maximo,
                            numero_compras, cantidad_total_comprada, total_gastado, fecha_ultima_compra
                        FROM inventario_usuario
                        WHERE usuario_id = %s AND producto_maestro_id = %s
                    """,
                        (usuario_id, producto_maestro_id),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT
                            id, cantidad_actual, precio_promedio, precio_minimo, precio_maximo,
                            numero_compras, cantidad_total_comprada, total_gastado, fecha_ultima_compra
                        FROM inventario_usuario
                        WHERE usuario_id = ? AND producto_maestro_id = ?
                    """,
                        (usuario_id, producto_maestro_id),
                    )

                inventario_existente = cursor.fetchone()

                if inventario_existente:
                    # 3.2 ACTUALIZAR existente
                    inv_id = inventario_existente[0]
                    cantidad_actual = float(inventario_existente[1] or 0)
                    precio_promedio_actual = float(inventario_existente[2] or 0)
                    precio_min_actual = int(inventario_existente[3] or precio)
                    precio_max_actual = int(inventario_existente[4] or precio)
                    num_compras = int(inventario_existente[5] or 0)
                    cantidad_total = float(inventario_existente[6] or 0)
                    total_gastado = float(inventario_existente[7] or 0)
                    fecha_ultima_compra_anterior = inventario_existente[8]

                    # Calcular nuevos valores
                    nueva_cantidad = cantidad_actual + cantidad
                    nuevo_num_compras = num_compras + 1
                    nueva_cantidad_total = cantidad_total + cantidad
                    nuevo_total_gastado = total_gastado + (precio * cantidad)
                    nuevo_precio_promedio = int(
                        nuevo_total_gastado / nueva_cantidad_total
                        if nueva_cantidad_total > 0
                        else precio
                    )
                    nuevo_precio_min = min(precio_min_actual, precio)
                    nuevo_precio_max = max(precio_max_actual, precio)

                    # Calcular días desde última compra
                    dias_desde_ultima = 0
                    if fecha_ultima_compra_anterior:
                        try:
                            if isinstance(fecha_ultima_compra_anterior, str):
                                fecha_anterior = datetime.strptime(
                                    fecha_ultima_compra_anterior, "%Y-%m-%d"
                                ).date()
                            elif hasattr(fecha_ultima_compra_anterior, "date"):
                                fecha_anterior = (
                                    fecha_ultima_compra_anterior.date()
                                    if callable(fecha_ultima_compra_anterior.date)
                                    else fecha_ultima_compra_anterior
                                )
                            else:
                                fecha_anterior = fecha_ultima_compra_anterior

                            dias_desde_ultima = (fecha_compra - fecha_anterior).days
                        except:
                            dias_desde_ultima = 0

                    # UPDATE
                    if database_type == "postgresql":
                        cursor.execute(
                            """
                            UPDATE inventario_usuario
                            SET cantidad_actual = %s,
                                precio_ultima_compra = %s,
                                precio_promedio = %s,
                                precio_minimo = %s,
                                precio_maximo = %s,
                                establecimiento = %s,
                                establecimiento_id = %s,
                                fecha_ultima_compra = %s,
                                numero_compras = %s,
                                cantidad_total_comprada = %s,
                                total_gastado = %s,
                                ultima_factura_id = %s,
                                dias_desde_ultima_compra = %s,
                                fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """,
                            (
                                nueva_cantidad,
                                precio,
                                nuevo_precio_promedio,
                                nuevo_precio_min,
                                nuevo_precio_max,
                                establecimiento_nombre,
                                establecimiento_id,
                                fecha_compra,
                                nuevo_num_compras,
                                nueva_cantidad_total,
                                nuevo_total_gastado,
                                factura_id,
                                dias_desde_ultima,
                                inv_id,
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            UPDATE inventario_usuario
                            SET cantidad_actual = ?,
                                precio_ultima_compra = ?,
                                precio_promedio = ?,
                                precio_minimo = ?,
                                precio_maximo = ?,
                                establecimiento = ?,
                                establecimiento_id = ?,
                                fecha_ultima_compra = ?,
                                numero_compras = ?,
                                cantidad_total_comprada = ?,
                                total_gastado = ?,
                                ultima_factura_id = ?,
                                dias_desde_ultima_compra = ?,
                                fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                            WHERE id = ?
                        """,
                            (
                                nueva_cantidad,
                                precio,
                                nuevo_precio_promedio,
                                nuevo_precio_min,
                                nuevo_precio_max,
                                establecimiento_nombre,
                                establecimiento_id,
                                fecha_compra,
                                nuevo_num_compras,
                                nueva_cantidad_total,
                                nuevo_total_gastado,
                                factura_id,
                                dias_desde_ultima,
                                inv_id,
                            ),
                        )

                    actualizados += 1
                    print(f"      ✅ {nombre}: {cantidad_actual} → {nueva_cantidad}")

                else:
                    # 3.3 CREAR nuevo
                    if database_type == "postgresql":
                        cursor.execute(
                            """
                            INSERT INTO inventario_usuario (
                                usuario_id, producto_maestro_id,
                                cantidad_actual, precio_ultima_compra,
                                precio_promedio, precio_minimo, precio_maximo,
                                establecimiento, establecimiento_id,
                                fecha_ultima_compra, numero_compras,
                                cantidad_total_comprada, total_gastado,
                                ultima_factura_id, nivel_alerta,
                                dias_desde_ultima_compra, unidad_medida
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 'unidades'
                            )
                        """,
                            (
                                usuario_id,
                                producto_maestro_id,
                                cantidad,
                                precio,
                                precio,
                                precio,
                                precio,
                                establecimiento_nombre,
                                establecimiento_id,
                                fecha_compra,
                                1,
                                cantidad,
                                precio * cantidad,
                                factura_id,
                                cantidad * 0.3,  # Alerta cuando quede 30%
                            ),
                        )
                    else:
                        cursor.execute(
                            """
                            INSERT INTO inventario_usuario (
                                usuario_id, producto_maestro_id,
                                cantidad_actual, precio_ultima_compra,
                                precio_promedio, precio_minimo, precio_maximo,
                                establecimiento, establecimiento_id,
                                fecha_ultima_compra, numero_compras,
                                cantidad_total_comprada, total_gastado,
                                ultima_factura_id, nivel_alerta,
                                dias_desde_ultima_compra, unidad_medida
                            ) VALUES (
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'unidades'
                            )
                        """,
                            (
                                usuario_id,
                                producto_maestro_id,
                                cantidad,
                                precio,
                                precio,
                                precio,
                                precio,
                                establecimiento_nombre,
                                establecimiento_id,
                                fecha_compra,
                                1,
                                cantidad,
                                precio * cantidad,
                                factura_id,
                                cantidad * 0.3,
                            ),
                        )

                    creados += 1
                    print(
                        f"      ➕ {nombre}: nuevo producto agregado ({cantidad} unidades)"
                    )

            except Exception as e:
                print(f"      ❌ Error con {nombre}: {e}")
                import traceback

                traceback.print_exc()
                continue

        conn.commit()

        print(f"✅ Inventario actualizado:")
        print(f"   - {actualizados} productos actualizados")
        print(f"   - {creados} productos nuevos")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error actualizando inventario: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
        cursor.close()
        conn.close()
        return False

def agregar_tablas_auditoria_a_database_py():
    """
    Instrucciones: Copia este código y agrégalo en la función create_postgresql_tables()
    justo después de crear la tabla productos_maestros
    """

    # ============================================
    # TABLA: auditoria_productos
    # ============================================
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS auditoria_productos (
            id SERIAL PRIMARY KEY,
            usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
            producto_maestro_id INTEGER NOT NULL REFERENCES productos_maestros(id),
            accion VARCHAR(20) NOT NULL CHECK (accion IN ('crear', 'actualizar', 'validar', 'eliminar')),
            datos_anteriores JSONB,
            datos_nuevos JSONB,
            razon TEXT,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            ip_address VARCHAR(50),
            user_agent TEXT
        )
    """)
    print("✓ Tabla 'auditoria_productos' creada")

    # Índices de auditoria_productos
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_auditoria_usuario ON auditoria_productos(usuario_id)",
        "auditoria_productos.usuario"
    )
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_auditoria_producto ON auditoria_productos(producto_maestro_id)",
        "auditoria_productos.producto"
    )
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_auditoria_fecha ON auditoria_productos(fecha DESC)",
        "auditoria_productos.fecha"
    )
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_auditoria_accion ON auditoria_productos(accion)",
        "auditoria_productos.accion"
    )

    # ============================================
    # MODIFICAR productos_maestros - Agregar columnas de auditoría
    # ============================================
    print("🔧 Agregando columnas de auditoría a productos_maestros...")

    columnas_auditoria = {
        'auditado_manualmente': 'BOOLEAN DEFAULT FALSE',
        'validaciones_manuales': 'INTEGER DEFAULT 0',
        'ultima_validacion': 'TIMESTAMP',
        'imagen_url': 'VARCHAR(500)',
        'contenido': 'VARCHAR(100)'
    }

    cursor.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'productos_maestros'
    """)
    columnas_existentes_pm = [row[0] for row in cursor.fetchall()]

    for columna, tipo in columnas_auditoria.items():
        if columna not in columnas_existentes_pm:
            try:
                cursor.execute(f"""
                    ALTER TABLE productos_maestros
                    ADD COLUMN {columna} {tipo}
                """)
                conn.commit()
                print(f"   ✅ Columna '{columna}' agregada a productos_maestros")
            except Exception as e:
                print(f"   ⚠️ {columna}: {e}")
                conn.rollback()

    # Índices adicionales
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_productos_auditados ON productos_maestros(auditado_manualmente)",
        "productos_maestros.auditados"
    )
    crear_indice_seguro(
        "CREATE INDEX IF NOT EXISTS idx_productos_validaciones ON productos_maestros(validaciones_manuales DESC)",
        "productos_maestros.validaciones"
    )


# ==============================================================================
# FUNCIONES DE AUDITORÍA - AGREGAR AL FINAL DE database.py
# ==============================================================================

def obtener_productos_requieren_auditoria(limite=20, usuario_id=None):
    """
    Obtiene productos que requieren auditoría manual

    Args:
        limite: Número máximo de productos a retornar
        usuario_id: Si se especifica, excluye productos ya auditados por ese usuario

    Returns:
        Lista de diccionarios con datos de productos
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        if database_type == "postgresql":
            query = """
                SELECT
                    pm.id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pm.subcategoria,
                    pm.total_reportes,
                    pm.auditado_manualmente,
                    pm.validaciones_manuales,
                    CASE
                        WHEN pm.marca IS NULL AND pm.categoria IS NULL THEN 10
                        WHEN pm.marca IS NULL THEN 8
                        WHEN pm.categoria IS NULL THEN 7
                        WHEN LENGTH(pm.nombre_normalizado) < 5 THEN 6
                        ELSE 5
                    END as prioridad,
                    CASE
                        WHEN pm.marca IS NULL AND pm.categoria IS NULL THEN 'Sin marca ni categoría'
                        WHEN pm.marca IS NULL THEN 'Sin marca'
                        WHEN pm.categoria IS NULL THEN 'Sin categoría'
                        WHEN LENGTH(pm.nombre_normalizado) < 5 THEN 'Nombre muy corto'
                        ELSE 'Requiere validación'
                    END as razon
                FROM productos_maestros pm
            """

            if usuario_id:
                query += """
                    LEFT JOIN auditoria_productos a ON (
                        pm.id = a.producto_maestro_id
                        AND a.usuario_id = %s
                    )
                    WHERE pm.auditado_manualmente = FALSE
                    AND a.id IS NULL
                """
                query += """
                    AND (
                        pm.marca IS NULL
                        OR pm.categoria IS NULL
                        OR LENGTH(pm.nombre_normalizado) < 5
                    )
                    ORDER BY prioridad DESC, pm.total_reportes DESC
                    LIMIT %s
                """
                cursor.execute(query, (usuario_id, limite))
            else:
                query += """
                    WHERE pm.auditado_manualmente = FALSE
                    AND (
                        pm.marca IS NULL
                        OR pm.categoria IS NULL
                        OR LENGTH(pm.nombre_normalizado) < 5
                    )
                    ORDER BY prioridad DESC, pm.total_reportes DESC
                    LIMIT %s
                """
                cursor.execute(query, (limite,))

            productos = []
            for row in cursor.fetchall():
                productos.append({
                    'id': row[0],
                    'codigo_ean': row[1],
                    'nombre_normalizado': row[2],
                    'marca': row[3],
                    'categoria': row[4],
                    'subcategoria': row[5],
                    'total_reportes': row[6],
                    'auditado_manualmente': row[7],
                    'validaciones_manuales': row[8],
                    'prioridad': row[9],
                    'razon': row[10]
                })

            cursor.close()
            conn.close()
            return productos

        else:
            # SQLite version
            query = """
                SELECT
                    pm.id,
                    pm.codigo_ean,
                    pm.nombre_normalizado,
                    pm.marca,
                    pm.categoria,
                    pm.total_reportes
                FROM productos_maestros pm
                WHERE pm.auditado_manualmente = 0
                AND (
                    pm.marca IS NULL
                    OR pm.categoria IS NULL
                    OR LENGTH(pm.nombre_normalizado) < 5
                )
                ORDER BY pm.total_reportes DESC
                LIMIT ?
            """
            cursor.execute(query, (limite,))

            productos = []
            for row in cursor.fetchall():
                productos.append({
                    'id': row[0],
                    'codigo_ean': row[1],
                    'nombre_normalizado': row[2],
                    'marca': row[3],
                    'categoria': row[4],
                    'total_reportes': row[5]
                })

            cursor.close()
            conn.close()
            return productos

    except Exception as e:
        print(f"❌ Error obteniendo productos para auditar: {e}")
        if conn:
            cursor.close()
            conn.close()
        return []


def registrar_auditoria(usuario_id, producto_maestro_id, accion,
                        datos_anteriores=None, datos_nuevos=None, razon=None):
    """
    Registra una acción de auditoría en la base de datos

    Args:
        usuario_id: ID del usuario que realizó la auditoría
        producto_maestro_id: ID del producto auditado
        accion: Tipo de acción ('crear', 'actualizar', 'validar', 'eliminar')
        datos_anteriores: Dict con datos antes del cambio
        datos_nuevos: Dict con datos después del cambio
        razon: Texto explicando el motivo del cambio

    Returns:
        ID del registro de auditoría o None si falla
    """
    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        import json

        if database_type == "postgresql":
            cursor.execute("""
                INSERT INTO auditoria_productos (
                    usuario_id,
                    producto_maestro_id,
                    accion,
                    datos_anteriores,
                    datos_nuevos,
                    razon,
                    fecha
                ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                usuario_id,
                producto_maestro_id,
                accion,
                json.dumps(datos_anteriores) if datos_anteriores else None,
                json.dumps(datos_nuevos) if datos_nuevos else None,
                razon
            ))

            auditoria_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            return auditoria_id

        else:
            # SQLite version
            cursor.execute("""
                INSERT INTO auditoria_productos (
                    usuario_id,
                    producto_maestro_id,
                    accion,
                    datos_anteriores,
                    datos_nuevos,
                    razon
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                usuario_id,
                producto_maestro_id,
                accion,
                json.dumps(datos_anteriores) if datos_anteriores else None,
                json.dumps(datos_nuevos) if datos_nuevos else None,
                razon
            ))

            auditoria_id = cursor.lastrowid
            conn.commit()
            cursor.close()
            conn.close()
            return auditoria_id

    except Exception as e:
        print(f"❌ Error registrando auditoría: {e}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        return None


def obtener_estadisticas_auditoria(usuario_id):
    """
    Obtiene estadísticas de auditoría para un usuario

    Returns:
        Dict con estadísticas
    """
    conn = get_db_connection()
    if not conn:
        return {}

    # ... resto de la función ...

    return {
        'validados': stats.get('validar', 0),
        'creados': stats.get('crear', 0),
        'actualizados': stats.get('actualizar', 0),
        'total': sum(stats.values())
    }
def guardar_precio_producto(
    producto_maestro_id: int,
    establecimiento_id: int,
    precio: int,
    fecha_registro: date,
    usuario_id: int,
    factura_id: int,
    verificado: bool = False
) -> Optional[int]:
    """
    Guarda un precio de producto en la tabla precios_productos

    Esta función se debe llamar cada vez que se procesa un item de factura

    Args:
        producto_maestro_id: ID del producto en productos_maestros
        establecimiento_id: ID del establecimiento
        precio: Precio pagado (en pesos enteros)
        fecha_registro: Fecha de la compra
        usuario_id: ID del usuario que reporta
        factura_id: ID de la factura origen
        verificado: Si el precio fue verificado manualmente

    Returns:
        ID del registro de precio o None si falla

    Ejemplo:
        >>> guardar_precio_producto(
        ...     producto_maestro_id=123,
        ...     establecimiento_id=5,
        ...     precio=15000,
        ...     fecha_registro=date.today(),
        ...     usuario_id=1,
        ...     factura_id=456
        ... )
        789
    """
    from database import get_db_connection  # Importar desde tu database.py

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return None

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        # Validar que el precio sea razonable (> 0 y < 100 millones)
        if precio <= 0 or precio > 100_000_000:
            print(f"⚠️ Precio fuera de rango: {precio}")
            return None

        if database_type == "postgresql":
            # Verificar si ya existe un registro IDÉNTICO
            cursor.execute("""
                SELECT id FROM precios_productos
                WHERE producto_maestro_id = %s
                  AND establecimiento_id = %s
                  AND fecha_registro = %s
                  AND usuario_id = %s
                  AND factura_id = %s
            """, (producto_maestro_id, establecimiento_id, fecha_registro, usuario_id, factura_id))

            existe = cursor.fetchone()

            if existe:
                print(f"⚠️ Precio ya registrado (ID: {existe[0]})")
                return existe[0]

            # Insertar nuevo registro
            cursor.execute("""
                INSERT INTO precios_productos (
                    producto_maestro_id,
                    establecimiento_id,
                    precio,
                    fecha_registro,
                    usuario_id,
                    factura_id,
                    verificado,
                    es_outlier,
                    fecha_creacion,
                    fecha_actualizacion
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, FALSE,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                RETURNING id
            """, (
                producto_maestro_id,
                establecimiento_id,
                precio,
                fecha_registro,
                usuario_id,
                factura_id,
                verificado
            ))

            precio_id = cursor.fetchone()[0]
            conn.commit()

            # Actualizar estadísticas del producto maestro
            actualizar_estadisticas_producto(producto_maestro_id)

            print(f"✅ Precio guardado: Producto {producto_maestro_id} = ${precio:,} en establecimiento {establecimiento_id}")

            cursor.close()
            conn.close()
            return precio_id

        else:
            # SQLite version
            cursor.execute("""
                SELECT id FROM precios_productos
                WHERE producto_maestro_id = ?
                  AND establecimiento_id = ?
                  AND fecha_registro = ?
                  AND usuario_id = ?
                  AND factura_id = ?
            """, (producto_maestro_id, establecimiento_id, fecha_registro, usuario_id, factura_id))

            existe = cursor.fetchone()

            if existe:
                print(f"⚠️ Precio ya registrado (ID: {existe[0]})")
                return existe[0]

            cursor.execute("""
                INSERT INTO precios_productos (
                    producto_maestro_id,
                    establecimiento_id,
                    precio,
                    fecha_registro,
                    usuario_id,
                    factura_id,
                    verificado,
                    es_outlier
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0)
            """, (
                producto_maestro_id,
                establecimiento_id,
                precio,
                fecha_registro,
                usuario_id,
                factura_id,
                verificado
            ))

            precio_id = cursor.lastrowid
            conn.commit()

            cursor.close()
            conn.close()
            return precio_id

    except Exception as e:
        print(f"❌ Error guardando precio: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        return None


# ============================================================================
# FUNCIÓN 2: ACTUALIZAR ESTADÍSTICAS DE PRODUCTO
# ============================================================================

def actualizar_estadisticas_producto(producto_maestro_id: int) -> bool:
    """
    Actualiza las estadísticas globales de un producto basándose en precios_productos

    Calcula:
    - precio_promedio_global
    - precio_minimo_historico
    - precio_maximo_historico
    - total_reportes

    Args:
        producto_maestro_id: ID del producto a actualizar

    Returns:
        True si se actualizó correctamente
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        return False

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        if database_type == "postgresql":
            cursor.execute("""
                UPDATE productos_maestros
                SET precio_promedio_global = (
                        SELECT CAST(AVG(precio) AS INTEGER)
                        FROM precios_productos
                        WHERE producto_maestro_id = %s
                          AND es_outlier = FALSE
                    ),
                    precio_minimo_historico = (
                        SELECT MIN(precio)
                        FROM precios_productos
                        WHERE producto_maestro_id = %s
                          AND es_outlier = FALSE
                    ),
                    precio_maximo_historico = (
                        SELECT MAX(precio)
                        FROM precios_productos
                        WHERE producto_maestro_id = %s
                          AND es_outlier = FALSE
                    ),
                    total_reportes = (
                        SELECT COUNT(*)
                        FROM precios_productos
                        WHERE producto_maestro_id = %s
                    ),
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (producto_maestro_id, producto_maestro_id, producto_maestro_id,
                  producto_maestro_id, producto_maestro_id))

            conn.commit()
            cursor.close()
            conn.close()
            return True

        else:
            # SQLite version
            cursor.execute("""
                UPDATE productos_maestros
                SET precio_promedio_global = (
                        SELECT CAST(AVG(precio) AS INTEGER)
                        FROM precios_productos
                        WHERE producto_maestro_id = ?
                    ),
                    precio_minimo_historico = (
                        SELECT MIN(precio)
                        FROM precios_productos
                        WHERE producto_maestro_id = ?
                    ),
                    precio_maximo_historico = (
                        SELECT MAX(precio)
                        FROM precios_productos
                        WHERE producto_maestro_id = ?
                    ),
                    total_reportes = (
                        SELECT COUNT(*)
                        FROM precios_productos
                        WHERE producto_maestro_id = ?
                    ),
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (producto_maestro_id, producto_maestro_id, producto_maestro_id,
                  producto_maestro_id, producto_maestro_id))

            conn.commit()
            cursor.close()
            conn.close()
            return True

    except Exception as e:
        print(f"❌ Error actualizando estadísticas: {e}")
        if conn:
            conn.rollback()
            cursor.close()
            conn.close()
        return False


# ============================================================================
# FUNCIÓN 3: CONSULTAR DÓNDE ESTÁ MÁS BARATO UN PRODUCTO
# ============================================================================

def consultar_precios_producto(
    producto_maestro_id: int,
    limite: int = 10,
    dias_antiguedad_maxima: int = 30
) -> List[Dict[str, Any]]:
    """
    Consulta los precios más recientes de un producto en diferentes establecimientos

    Args:
        producto_maestro_id: ID del producto a consultar
        limite: Número máximo de resultados
        dias_antiguedad_maxima: Solo incluir precios de los últimos N días

    Returns:
        Lista de diccionarios con información de precios, ordenados de menor a mayor

    Ejemplo de resultado:
        [
            {
                'establecimiento_id': 5,
                'establecimiento_nombre': 'Éxito Unicentro',
                'precio': 15000,
                'fecha_registro': '2025-10-30',
                'dias_antiguedad': 1,
                'ahorro_vs_mas_caro': 3000
            },
            ...
        ]
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        if database_type == "postgresql":
            cursor.execute("""
                WITH ultimos_precios AS (
                    SELECT DISTINCT ON (establecimiento_id)
                        pp.establecimiento_id,
                        e.nombre_normalizado as establecimiento_nombre,
                        e.cadena,
                        e.ciudad,
                        pp.precio,
                        pp.fecha_registro,
                        CURRENT_DATE - pp.fecha_registro as dias_antiguedad
                    FROM precios_productos pp
                    INNER JOIN establecimientos e ON pp.establecimiento_id = e.id
                    WHERE pp.producto_maestro_id = %s
                      AND pp.es_outlier = FALSE
                      AND CURRENT_DATE - pp.fecha_registro <= %s
                    ORDER BY pp.establecimiento_id, pp.fecha_registro DESC
                )
                SELECT
                    establecimiento_id,
                    establecimiento_nombre,
                    cadena,
                    ciudad,
                    precio,
                    fecha_registro,
                    dias_antiguedad,
                    MAX(precio) OVER() - precio as ahorro_vs_mas_caro,
                    precio - MIN(precio) OVER() as diferencia_vs_mas_barato,
                    ROUND(((precio - MIN(precio) OVER())::NUMERIC / MIN(precio) OVER() * 100), 1) as porcentaje_mas_caro
                FROM ultimos_precios
                ORDER BY precio ASC, dias_antiguedad ASC
                LIMIT %s
            """, (producto_maestro_id, dias_antiguedad_maxima, limite))

            resultados = []
            for row in cursor.fetchall():
                resultados.append({
                    'establecimiento_id': row[0],
                    'establecimiento_nombre': row[1],
                    'cadena': row[2],
                    'ciudad': row[3],
                    'precio': row[4],
                    'fecha_registro': row[5],
                    'dias_antiguedad': row[6],
                    'ahorro_vs_mas_caro': row[7],
                    'diferencia_vs_mas_barato': row[8],
                    'porcentaje_mas_caro': float(row[9]) if row[9] else 0.0
                })

            cursor.close()
            conn.close()
            return resultados

        else:
            # SQLite version (simplificada)
            cursor.execute("""
                SELECT
                    pp.establecimiento_id,
                    e.nombre_normalizado,
                    e.cadena,
                    pp.precio,
                    pp.fecha_registro,
                    julianday('now') - julianday(pp.fecha_registro) as dias_antiguedad
                FROM precios_productos pp
                INNER JOIN establecimientos e ON pp.establecimiento_id = e.id
                WHERE pp.producto_maestro_id = ?
                  AND julianday('now') - julianday(pp.fecha_registro) <= ?
                ORDER BY pp.fecha_registro DESC
                LIMIT ?
            """, (producto_maestro_id, dias_antiguedad_maxima, limite))

            resultados = []
            for row in cursor.fetchall():
                resultados.append({
                    'establecimiento_id': row[0],
                    'establecimiento_nombre': row[1],
                    'cadena': row[2],
                    'precio': row[3],
                    'fecha_registro': row[4],
                    'dias_antiguedad': int(row[5])
                })

            # Ordenar por precio
            resultados.sort(key=lambda x: x['precio'])

            cursor.close()
            conn.close()
            return resultados

    except Exception as e:
        print(f"❌ Error consultando precios: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            cursor.close()
            conn.close()
        return []


# ============================================================================
# FUNCIÓN 4: BUSCAR PRODUCTO POR CÓDIGO EAN Y CONSULTAR PRECIOS
# ============================================================================

def buscar_producto_y_precios(codigo_ean: str) -> Optional[Dict[str, Any]]:
    """
    Busca un producto por código EAN y retorna sus datos + precios actuales

    Args:
        codigo_ean: Código de barras del producto

    Returns:
        Diccionario con información del producto y precios, o None si no existe

    Ejemplo:
        >>> resultado = buscar_producto_y_precios("7702189311234")
        >>> print(resultado['nombre'])
        'Coca Cola 2L'
        >>> print(resultado['precios'][0]['precio'])
        15000
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        return None

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    id,
                    codigo_ean,
                    nombre_normalizado,
                    marca,
                    categoria,
                    subcategoria,
                    precio_promedio_global,
                    precio_minimo_historico,
                    precio_maximo_historico,
                    total_reportes
                FROM productos_maestros
                WHERE codigo_ean = %s
            """, (codigo_ean,))
        else:
            cursor.execute("""
                SELECT
                    id,
                    codigo_ean,
                    nombre_normalizado,
                    marca,
                    categoria,
                    subcategoria,
                    precio_promedio_global,
                    precio_minimo_historico,
                    precio_maximo_historico,
                    total_reportes
                FROM productos_maestros
                WHERE codigo_ean = ?
            """, (codigo_ean,))

        producto = cursor.fetchone()

        if not producto:
            cursor.close()
            conn.close()
            return None

        # Obtener precios actuales
        precios = consultar_precios_producto(producto[0])

        resultado = {
            'id': producto[0],
            'codigo_ean': producto[1],
            'nombre': producto[2],
            'marca': producto[3],
            'categoria': producto[4],
            'subcategoria': producto[5],
            'precio_promedio': producto[6],
            'precio_minimo': producto[7],
            'precio_maximo': producto[8],
            'total_reportes': producto[9],
            'precios': precios,
            'donde_mas_barato': precios[0] if precios else None
        }

        cursor.close()
        conn.close()
        return resultado

    except Exception as e:
        print(f"❌ Error buscando producto: {e}")
        if conn:
            cursor.close()
            conn.close()
        return None


# ============================================================================
# FUNCIÓN 5: PROCESAR FACTURA Y GUARDAR PRECIOS AUTOMÁTICAMENTE
# ============================================================================

def procesar_items_factura_y_guardar_precios(factura_id: int, usuario_id: int) -> Dict[str, int]:
    """
    Procesa todos los items de una factura y guarda sus precios en precios_productos

    Esta función debe llamarse DESPUÉS de que se hayan guardado los items_factura

    Args:
        factura_id: ID de la factura procesada
        usuario_id: ID del usuario que subió la factura

    Returns:
        Diccionario con estadísticas del proceso

    Ejemplo:
        >>> stats = procesar_items_factura_y_guardar_precios(456, 1)
        >>> print(f"Guardados: {stats['precios_guardados']}")
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        return {'error': 'No se pudo conectar a la base de datos'}

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        # Obtener datos de la factura
        if database_type == "postgresql":
            cursor.execute("""
                SELECT establecimiento_id, fecha_factura
                FROM facturas
                WHERE id = %s
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT establecimiento_id, fecha_factura
                FROM facturas
                WHERE id = ?
            """, (factura_id,))

        factura = cursor.fetchone()

        if not factura:
            cursor.close()
            conn.close()
            return {'error': 'Factura no encontrada'}

        establecimiento_id = factura[0]
        fecha_factura = factura[1]

        # Convertir fecha_factura a date
        if isinstance(fecha_factura, str):
            fecha_registro = datetime.strptime(fecha_factura, '%Y-%m-%d').date()
        elif hasattr(fecha_factura, 'date'):
            fecha_registro = fecha_factura.date() if callable(fecha_factura.date) else fecha_factura
        else:
            fecha_registro = fecha_factura or date.today()

        # Obtener items de la factura que tienen producto_maestro_id
        if database_type == "postgresql":
            cursor.execute("""
                SELECT
                    producto_maestro_id,
                    precio_pagado,
                    cantidad
                FROM items_factura
                WHERE factura_id = %s
                  AND producto_maestro_id IS NOT NULL
            """, (factura_id,))
        else:
            cursor.execute("""
                SELECT
                    producto_maestro_id,
                    precio_pagado,
                    cantidad
                FROM items_factura
                WHERE factura_id = ?
                  AND producto_maestro_id IS NOT NULL
            """, (factura_id,))

        items = cursor.fetchall()

        cursor.close()
        conn.close()

        if not items:
            return {
                'precios_guardados': 0,
                'errores': 0,
                'mensaje': 'No hay items con producto_maestro_id'
            }

        # Guardar cada precio
        precios_guardados = 0
        errores = 0

        for item in items:
            producto_maestro_id = item[0]
            precio = int(item[1]) if item[1] else 0
            cantidad = int(item[2]) if item[2] else 1

            # Calcular precio unitario si es necesario
            if cantidad > 1:
                precio_unitario = precio // cantidad
            else:
                precio_unitario = precio

            # Guardar precio
            precio_id = guardar_precio_producto(
                producto_maestro_id=producto_maestro_id,
                establecimiento_id=establecimiento_id,
                precio=precio_unitario,
                fecha_registro=fecha_registro,
                usuario_id=usuario_id,
                factura_id=factura_id,
                verificado=False
            )

            if precio_id:
                precios_guardados += 1
            else:
                errores += 1

        return {
            'precios_guardados': precios_guardados,
            'errores': errores,
            'total_items': len(items)
        }

    except Exception as e:
        print(f"❌ Error procesando items de factura: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            cursor.close()
            conn.close()
        return {'error': str(e)}


# ============================================================================
# FUNCIÓN 6: COMPARAR PRECIOS ENTRE ESTABLECIMIENTOS (API READY)
# ============================================================================

def comparar_precios_establecimientos(
    producto_maestro_id: int,
    establecimiento_actual_id: int,
    radio_km: Optional[float] = None
) -> Dict[str, Any]:
    """
    Compara el precio de un producto en diferentes establecimientos
    y calcula cuánto se podría ahorrar

    Args:
        producto_maestro_id: ID del producto
        establecimiento_actual_id: Establecimiento de referencia
        radio_km: Si se especifica, solo incluye establecimientos cercanos

    Returns:
        Diccionario con comparación de precios
    """
    from database import get_db_connection

    conn = get_db_connection()
    if not conn:
        return {'error': 'No se pudo conectar a la base de datos'}

    cursor = conn.cursor()
    database_type = os.environ.get("DATABASE_TYPE", "sqlite").lower()

    try:
        # Obtener precio actual en el establecimiento de referencia
        if database_type == "postgresql":
            cursor.execute("""
                SELECT precio, fecha_registro
                FROM precios_productos
                WHERE producto_maestro_id = %s
                  AND establecimiento_id = %s
                  AND es_outlier = FALSE
                ORDER BY fecha_registro DESC
                LIMIT 1
            """, (producto_maestro_id, establecimiento_actual_id))
        else:
            cursor.execute("""
                SELECT precio, fecha_registro
                FROM precios_productos
                WHERE producto_maestro_id = ?
                  AND establecimiento_id = ?
                ORDER BY fecha_registro DESC
                LIMIT 1
            """, (producto_maestro_id, establecimiento_actual_id))

        precio_actual = cursor.fetchone()

        if not precio_actual:
            cursor.close()
            conn.close()
            return {'error': 'No hay precio registrado para este producto en el establecimiento'}

        # Obtener precios en otros establecimientos
        precios = consultar_precios_producto(producto_maestro_id, limite=20)

        if not precios:
            cursor.close()
            conn.close()
            return {
                'precio_actual': precio_actual[0],
                'establecimiento_actual_id': establecimiento_actual_id,
                'comparaciones': [],
                'ahorro_maximo': 0,
                'mensaje': 'No hay otros precios para comparar'
            }

        # Filtrar el establecimiento actual de las comparaciones
        otros_precios = [p for p in precios if p['establecimiento_id'] != establecimiento_actual_id]

        # Calcular ahorro máximo
        precio_mas_barato = min(p['precio'] for p in precios)
        ahorro_maximo = precio_actual[0] - precio_mas_barato

        cursor.close()
        conn.close()

        return {
            'precio_actual': precio_actual[0],
            'establecimiento_actual_id': establecimiento_actual_id,
            'fecha_precio_actual': str(precio_actual[1]),
            'precio_mas_barato': precio_mas_barato,
            'ahorro_maximo': ahorro_maximo if ahorro_maximo > 0 else 0,
            'porcentaje_ahorro': round((ahorro_maximo / precio_actual[0] * 100), 1) if precio_actual[0] > 0 else 0,
            'comparaciones': otros_precios[:10],
            'total_establecimientos_comparados': len(otros_precios)
        }

    except Exception as e:
        print(f"❌ Error comparando precios: {e}")
        if conn:
            cursor.close()
            conn.close()
        return {'error': str(e)}


if __name__ == "__main__":
    print("🔧 Inicializando sistema de base de datos...")
    test_database_connection()
    create_tables()
    print("✅ Sistema inicializado correctamente")
    print("✅ Tablas creadas/actualizadas incluyendo password_resets")
    print("=" * 80)
    print("EJEMPLO DE USO - SISTEMA DE COMPARACIÓN DE PRECIOS")
    print("=" * 80)

    # Ejemplo 1: Buscar producto y ver precios
    print("\n📋 EJEMPLO 1: Buscar Coca Cola 2L")
    resultado = buscar_producto_y_precios("7702189311234")

    if resultado:
        print(f"Producto: {resultado['nombre']}")
        print(f"Precio promedio: ${resultado['precio_promedio']:,}")
        print(f"\nDónde está más barato:")
        if resultado['donde_mas_barato']:
            mas_barato = resultado['donde_mas_barato']
            print(f"  {mas_barato['establecimiento_nombre']}: ${mas_barato['precio']:,}")

    # Ejemplo 2: Comparar precios
    print("\n💰 EJEMPLO 2: Comparar precios entre establecimientos")
    comparacion = comparar_precios_establecimientos(
        producto_maestro_id=123,
        establecimiento_actual_id=5
    )

    if 'ahorro_maximo' in comparacion:
        print(f"Precio actual: ${comparacion['precio_actual']:,}")
        print(f"Precio más barato: ${comparacion['precio_mas_barato']:,}")
        print(f"Ahorro posible: ${comparacion['ahorro_maximo']:,} ({comparacion['porcentaje_ahorro']}%)")
