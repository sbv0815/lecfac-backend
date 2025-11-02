"""
Script para agregar columnas de estadísticas a codigos_locales
"""

import psycopg2

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

print("\n🔧 ACTUALIZANDO TABLA codigos_locales")
print("="*80)

# Verificar columnas existentes
cursor.execute("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'codigos_locales'
""")

columnas_existentes = [row[0] for row in cursor.fetchall()]
print(f"📋 Columnas actuales: {', '.join(columnas_existentes)}")

# Columnas necesarias
columnas_requeridas = {
    'veces_visto': 'INTEGER DEFAULT 1',
    'ultima_vez_visto': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
}

for columna, tipo in columnas_requeridas.items():
    if columna not in columnas_existentes:
        try:
            print(f"\n➕ Agregando columna '{columna}'...")
            cursor.execute(f"""
                ALTER TABLE codigos_locales
                ADD COLUMN {columna} {tipo}
            """)
            conn.commit()
            print(f"   ✅ Columna '{columna}' agregada")
        except Exception as e:
            print(f"   ⚠️ Error: {e}")
            conn.rollback()
    else:
        print(f"   ✓ Columna '{columna}' ya existe")

print("\n" + "="*80)
print("✅ Tabla actualizada")

cursor.close()
conn.close()
