# update_database.py
from database import get_db_connection

conn = get_db_connection()
cursor = conn.cursor()

try:
    # Agregar columnas nuevas si no existen
    cursor.execute(
        "ALTER TABLE facturas ADD COLUMN estado_validacion TEXT DEFAULT 'pendiente'"
    )
    print("✅ Columna estado_validacion agregada")
except Exception as e:
    print(f"⚠️ Columna estado_validacion ya existe: {e}")

try:
    cursor.execute("ALTER TABLE facturas ADD COLUMN motivo_rechazo TEXT")
    print("✅ Columna motivo_rechazo agregada")
except Exception as e:
    print(f"⚠️ Columna motivo_rechazo ya existe: {e}")

conn.commit()
conn.close()
print("✅ Base de datos actualizada")
