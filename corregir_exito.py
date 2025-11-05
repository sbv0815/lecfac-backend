"""
Corrección rápida: Actualizar color de Éxito
"""
import sqlite3

conn = sqlite3.connect("lecfac.db")
cursor = conn.cursor()

# Actualizar Éxito
cursor.execute("""
    UPDATE establecimientos
    SET color_bg = '#e3f2fd', color_text = '#1565c0'
    WHERE nombre_normalizado = 'Éxito'
""")

print(f"✅ Éxito actualizado: {cursor.rowcount} registro(s)")

conn.commit()

# Verificar
cursor.execute("""
    SELECT nombre_normalizado, color_bg, color_text
    FROM establecimientos
    ORDER BY nombre_normalizado
""")

print("\n📊 Establecimientos actualizados:")
print("-" * 70)
for row in cursor.fetchall():
    nombre = row[0]
    bg = row[1] or 'N/A'
    text = row[2] or 'N/A'
    print(f"   {nombre:20} | BG: {bg:10} | TEXT: {text:10}")

conn.close()
print("\n✅ Corrección completada")
