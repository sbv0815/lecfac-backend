# Solo agregar al FINAL de tu database.py actual
# (Después de la función test_database_connection)

# ============================================
# PROCESAMIENTO ASÍNCRONO DE VIDEOS
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
        print(f"✅ Job creado: {job_id}")
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
        print(f"✅ Job {job_id} actualizado: {status}")
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


# ============================================
# EJECUTAR AL INICIAR (OPCIONAL)
# ============================================

if __name__ == "__main__":
    print("🔧 Inicializando sistema de base de datos...")
    
    test_database_connection()
    create_tables()
    create_processing_jobs_table()
    
    print("✅ Sistema inicializado")
