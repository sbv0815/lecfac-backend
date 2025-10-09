# ============================================
# AGREGAR AL FINAL DE database.py (después de create_postgresql_tables)
# ============================================

def create_processing_jobs_table():
    """
    ✅ NUEVA TABLA: processing_jobs
    Para procesamiento asíncrono de videos
    """
    conn = get_db_connection()
    if not conn:
        return False
    
    cursor = conn.cursor()
    
    try:
        print("🎬 Creando tabla processing_jobs...")
        
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS processing_jobs (
                id VARCHAR(50) PRIMARY KEY,
                usuario_id INTEGER REFERENCES usuarios(id),
                video_path VARCHAR(255),
                
                -- Estados: pending, processing, completed, failed
                status VARCHAR(20) DEFAULT 'pending',
                
                -- Resultado
                factura_id INTEGER REFERENCES facturas(id),
                
                -- Metadatos
                frames_procesados INTEGER DEFAULT 0,
                frames_exitosos INTEGER DEFAULT 0,
                productos_detectados INTEGER DEFAULT 0,
                error_message TEXT,
                
                -- Timestamps
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                
                -- Índice para búsquedas rápidas
                CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
            )
            ''')
            
            # Crear índices
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_processing_jobs_status 
                ON processing_jobs(status, created_at DESC)
            ''')
            
            cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_processing_jobs_usuario 
                ON processing_jobs(usuario_id, created_at DESC)
            ''')
            
        else:  # SQLite
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
        
        conn.commit()
        print("✅ Tabla processing_jobs creada exitosamente")
        return True
        
    except Exception as e:
        print(f"❌ Error creando tabla processing_jobs: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


# ============================================
# FUNCIONES PARA MANEJAR JOBS ASÍNCRONOS
# ============================================

def crear_processing_job(usuario_id: int, video_path: str) -> str:
    """
    Crea un nuevo job de procesamiento
    Retorna: job_id
    """
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
        print(f"✅ Job creado: {job_id}")
        return job_id
        
    except Exception as e:
        print(f"❌ Error creando job: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def actualizar_job_status(job_id: str, status: str, **kwargs):
    """
    Actualiza el status de un job
    kwargs puede incluir: factura_id, error_message, frames_procesados, etc.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Construir UPDATE dinámico
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
        print(f"✅ Job {job_id} actualizado: {status}")
        return True
        
    except Exception as e:
        print(f"❌ Error actualizando job: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def obtener_job_info(job_id: str) -> dict:
    """
    Obtiene información completa de un job
    """
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
        
        # Si está completado, obtener datos de la factura
        if job_info['status'] == 'completed' and job_info['factura_id']:
            job_info['factura'] = obtener_factura_completa(job_info['factura_id'])
        
        return job_info
        
    except Exception as e:
        print(f"❌ Error obteniendo job info: {e}")
        return None
    finally:
        conn.close()


def obtener_factura_completa(factura_id: int) -> dict:
    """
    Obtiene datos completos de una factura para respuesta al cliente
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Obtener factura
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
        
        # Obtener productos
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
    """
    Obtiene los últimos jobs del usuario
    """
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


# ============================================
# INFORME DE ESTADO DEL SISTEMA
# ============================================

def generar_informe_sistema():
    """
    Genera un informe completo del estado de la base de datos
    y qué funcionalidades están implementadas
    """
    print("\n" + "="*70)
    print("📊 INFORME DEL SISTEMA LECFAC")
    print("="*70 + "\n")
    
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return
    
    cursor = conn.cursor()
    
    try:
        # 1. INFORMACIÓN DE CONEXIÓN
        print("🔌 CONEXIÓN A BASE DE DATOS")
        print("-" * 70)
        db_type = os.environ.get("DATABASE_TYPE", "sqlite")
        print(f"   Tipo: {db_type.upper()}")
        
        if db_type == "postgresql":
            cursor.execute("SELECT version()")
            version = cursor.fetchone()[0].split(',')[0]
            print(f"   Versión: {version}")
        else:
            cursor.execute("SELECT sqlite_version()")
            print(f"   Versión SQLite: {cursor.fetchone()[0]}")
        
        print("   ✅ Conexión exitosa\n")
        
        # 2. TABLAS EXISTENTES
        print("📋 TABLAS EN LA BASE DE DATOS")
        print("-" * 70)
        
        if db_type == "postgresql":
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                ORDER BY table_name
            """)
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' 
                ORDER BY name
            """)
        
        tablas = [row[0] for row in cursor.fetchall()]
        
        # Tablas requeridas con su propósito
        tablas_requeridas = {
            # ✅ Core del sistema
            "usuarios": "Gestión de usuarios",
            "facturas": "Facturas escaneadas",
            "productos": "Productos detectados en facturas",
            
            # ✅ Nueva arquitectura
            "establecimientos": "Catálogo de tiendas",
            "productos_maestros": "Catálogo global de productos",
            "precios_productos": "Histórico de precios",
            "items_factura": "Items en facturas (nueva estructura)",
            "gastos_mensuales": "Analytics de gastos",
            "patrones_compra": "Predicción de compras",
            "correcciones_productos": "Sistema de aprendizaje OCR",
            
            # ✅ NUEVO: Procesamiento asíncrono
            "processing_jobs": "⭐ Jobs de procesamiento asíncrono",
            
            # Auxiliares
            "codigos_locales": "Códigos sin EAN",
            "matching_logs": "Auditoría de matching",
            "ocr_logs": "Logs de OCR",
        }
        
        tablas_implementadas = []
        tablas_faltantes = []
        
        for tabla, descripcion in tablas_requeridas.items():
            if tabla in tablas:
                tablas_implementadas.append((tabla, descripcion))
            else:
                tablas_faltantes.append((tabla, descripcion))
        
        print("   ✅ IMPLEMENTADAS:")
        for tabla, desc in tablas_implementadas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]
            print(f"      • {tabla}: {desc} ({count} registros)")
        
        if tablas_faltantes:
            print("\n   ⚠️  FALTANTES:")
            for tabla, desc in tablas_faltantes:
                print(f"      • {tabla}: {desc}")
        
        print()
        
        # 3. ESTADÍSTICAS GENERALES
        print("📈 ESTADÍSTICAS DEL SISTEMA")
        print("-" * 70)
        
        # Usuarios
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        total_usuarios = cursor.fetchone()[0]
        print(f"   👥 Usuarios registrados: {total_usuarios}")
        
        # Facturas
        cursor.execute("SELECT COUNT(*) FROM facturas")
        total_facturas = cursor.fetchone()[0]
        print(f"   🧾 Facturas procesadas: {total_facturas}")
        
        # Productos
        if "productos_maestros" in tablas:
            cursor.execute("SELECT COUNT(*) FROM productos_maestros")
            total_productos_maestros = cursor.fetchone()[0]
            print(f"   📦 Productos en catálogo: {total_productos_maestros}")
        
        # Jobs (si existe la tabla)
        if "processing_jobs" in tablas:
            cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE status = 'pending'")
            jobs_pendientes = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE status = 'processing'")
            jobs_procesando = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE status = 'completed'")
            jobs_completados = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM processing_jobs WHERE status = 'failed'")
            jobs_fallidos = cursor.fetchone()[0]
            
            print(f"\n   🎬 JOBS DE PROCESAMIENTO:")
            print(f"      • Pendientes: {jobs_pendientes}")
            print(f"      • Procesando: {jobs_procesando}")
            print(f"      • Completados: {jobs_completados}")
            print(f"      • Fallidos: {jobs_fallidos}")
        
        print()
        
        # 4. FUNCIONALIDADES
        print("🎯 FUNCIONALIDADES IMPLEMENTADAS")
        print("-" * 70)
        
        funcionalidades = {
            "✅ OCR de facturas con Claude Vision": True,
            "✅ Detección de productos": True,
            "✅ Catálogo de productos maestros": "productos_maestros" in tablas,
            "✅ Sistema de correcciones OCR": "correcciones_productos" in tablas,
            "✅ Procesamiento asíncrono de videos": "processing_jobs" in tablas,
            "✅ Analytics de gastos mensuales": "gastos_mensuales" in tablas,
            "✅ Predicción de compras frecuentes": "patrones_compra" in tablas,
            "⚠️  Notificaciones push": False,
            "⚠️  Comparador de precios": "precios_productos" in tablas,
        }
        
        for func, estado in funcionalidades.items():
            simbolo = "✅" if estado else "❌"
            print(f"   {simbolo} {func.replace('✅ ', '').replace('⚠️  ', '')}")
        
        print()
        
        # 5. PENDIENTES
        print("📝 TAREAS PENDIENTES")
        print("-" * 70)
        
        pendientes = []
        
        if "processing_jobs" not in tablas:
            pendientes.append("❗ Crear tabla processing_jobs para procesamiento asíncrono")
        
        if total_facturas > 0 and "processing_jobs" in tablas:
            cursor.execute("SELECT COUNT(*) FROM processing_jobs")
            if cursor.fetchone()[0] == 0:
                pendientes.append("🔧 Modificar endpoint /invoices/parse-video para usar jobs asíncronos")
        
        if "correcciones_productos" not in tablas:
            pendientes.append("🧠 Implementar sistema de aprendizaje de correcciones OCR")
        
        pendientes.append("📱 Implementar notificaciones push para jobs completados")
        pendientes.append("🔔 Sistema de recordatorios de productos frecuentes")
        pendientes.append("📊 Dashboard de comparación de precios entre tiendas")
        
        if pendientes:
            for i, tarea in enumerate(pendientes, 1):
                print(f"   {i}. {tarea}")
        else:
            print("   🎉 ¡No hay tareas pendientes críticas!")
        
        print()
        
        # 6. RECOMENDACIONES
        print("💡 RECOMENDACIONES")
        print("-" * 70)
        
        if "processing_jobs" not in tablas:
            print("   🚨 CRÍTICO: Implementar procesamiento asíncrono")
            print("      → Mejor experiencia de usuario")
            print("      → Evita timeouts en conexión móvil")
            print("      → Permite cerrar app durante procesamiento")
        
        if db_type == "sqlite" and total_facturas > 100:
            print("   ⚠️  Considerar migrar a PostgreSQL para mejor rendimiento")
        
        if total_usuarios > 0:
            print("   ✅ Sistema en producción con usuarios activos")
        
        print()
        
        # 7. PRÓXIMOS PASOS
        print("🚀 PRÓXIMOS PASOS SUGERIDOS")
        print("-" * 70)
        print("   1. ✅ Ejecutar create_processing_jobs_table()")
        print("   2. 🔧 Modificar main.py con endpoints asíncronos")
        print("   3. 📱 Actualizar Flutter para polling de status")
        print("   4. 🧪 Probar con video de factura larga")
        print("   5. 📊 Monitorear rendimiento y tiempos")
        
        print("\n" + "="*70)
        print("✅ INFORME GENERADO EXITOSAMENTE")
        print("="*70 + "\n")
        
    except Exception as e:
        print(f"❌ Error generando informe: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


# ============================================
# EJECUTAR AL INICIAR
# ============================================

if __name__ == "__main__":
    print("🔧 Inicializando sistema de base de datos...")
    
    # 1. Verificar conexión
    test_database_connection()
    
    # 2. Crear tablas base
    create_tables()
    
    # 3. Crear tabla de jobs asíncronos
    create_processing_jobs_table()
    
    # 4. Generar informe
    generar_informe_sistema()
