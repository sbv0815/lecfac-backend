import psycopg2

conn = psycopg2.connect('postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway')
cur = conn.cursor()

# Ver todos los establecimientos
cur.execute("""
    SELECT id, nombre_normalizado, cadena, tipo
    FROM establecimientos
    ORDER BY id
""")

print("Establecimientos en la base de datos:")
print("-" * 60)
print(f"{'ID':<5} {'Nombre Normalizado':<30} {'Cadena':<15} {'Tipo':<10}")
print("-" * 60)
for row in cur.fetchall():
    print(f"{row[0]:<5} {row[1] or 'NULL':<30} {row[2] or 'NULL':<15} {row[3] or 'NULL':<10}")

# Ver específicamente Ara
print("\n" + "=" * 60)
print("Buscando 'Ara':")
cur.execute("""
    SELECT id, nombre_normalizado
    FROM establecimientos
    WHERE nombre_normalizado ILIKE '%ara%'
""")

for row in cur.fetchall():
    print(f"ID: {row[0]}, Nombre: {row[1]}")

conn.close()
