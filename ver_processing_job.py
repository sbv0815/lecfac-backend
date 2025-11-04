# ver_processing_job.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = conn.cursor()

print("🔍 ANÁLISIS COMPLETO PROCESSING JOB\n")

# Ver el job completo
cur.execute("""
    SELECT
        id,
        usuario_id,
        status,
        factura_id,
        frames_procesados,
        frames_exitosos,
        productos_detectados,
        error_message,
        created_at,
        started_at,
        completed_at
    FROM processing_jobs
    WHERE factura_id = 23
""")

job = cur.fetchone()

if job:
    print(f"Job ID: {job[0]}")
    print(f"Usuario: {job[1]}")
    print(f"Status: {job[2]}")
    print(f"Factura: {job[3]}")
    print(f"Frames procesados: {job[4]}")
    print(f"Frames exitosos: {job[5]}")
    print(f"Productos detectados: {job[6]}")
    print(f"Error: {job[7]}")
    print(f"Created: {job[8]}")
    print(f"Started: {job[9]}")
    print(f"Completed: {job[10]}")

    # Calcular tiempo de procesamiento
    if job[9] and job[10]:
        duracion = (job[10] - job[9]).total_seconds()
        print(f"\n⏱️ Duración: {duracion:.2f} segundos")

# Ver si hay otros jobs recientes
print("\n\n📋 ÚLTIMOS 5 PROCESSING JOBS:")
cur.execute("""
    SELECT id, factura_id, status, productos_detectados, error_message, completed_at
    FROM processing_jobs
    ORDER BY created_at DESC
    LIMIT 5
""")

for job in cur.fetchall():
    print(f"  - Job {job[0][:8]}... → Factura {job[1]} | {job[2]} | Productos: {job[3]}")

# CRÍTICO: Ver si hay columnas adicionales que no conocemos
print("\n\n📋 COLUMNAS EN PROCESSING_JOBS:")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'processing_jobs'
    ORDER BY ordinal_position
""")
for col in cur.fetchall():
    print(f"   {col[0]}: {col[1]}")

cur.close()
conn.close()
