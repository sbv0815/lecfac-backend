"""
Agregar columnas de colores a tabla establecimientos en Railway
"""
import psycopg2
import sys

print("=" * 80)
print("🎨 AGREGAR COLORES A ESTABLECIMIENTOS")
print("=" * 80)

# Pedir DATABASE_URL
DATABASE_URL = input("\n📋 Pega tu DATABASE_URL de Railway: ").strip()

if not DATABASE_URL:
    print("❌ No se proporcionó DATABASE_URL")
    sys.exit(1)

try:
    print(f"\n🔗 Conectando a Railway...")
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    print("✅ Conectado\n")

    # Paso 1: Agregar columnas
    print("📦 PASO 1: Agregando columnas color_bg y color_text...")

    cur.execute("""
        ALTER TABLE establecimientos
        ADD COLUMN IF NOT EXISTS color_bg VARCHAR(20) DEFAULT '#e9ecef'
    """)

    cur.execute("""
        ALTER TABLE establecimientos
        ADD COLUMN IF NOT EXISTS color_text VARCHAR(20) DEFAULT '#495057'
    """)

    print("   ✅ Columnas agregadas\n")

    # Paso 2: Asignar colores
    print("🎨 PASO 2: Asignando colores a supermercados...\n")

    colores_por_supermercado = [
        ("ÉXITO", "#e3f2fd", "#1565c0", ["exito", "éxito"]),
        ("JUMBO", "#fff3e0", "#e65100", ["jumbo"]),
        ("CARULLA", "#f3e5f5", "#7b1fa2", ["carulla"]),
        ("OLÍMPICA", "#e8f5e9", "#2e7d32", ["olimpica", "olímpica"]),
        ("D1", "#fff9c4", "#f57f17", ["d1"]),
        ("ARA", "#ffe0b2", "#ef6c00", ["ara", "jeronimo"]),
    ]

    for nombre, bg, text, patrones in colores_por_supermercado:
        condiciones = " OR ".join([f"LOWER(nombre_normalizado) LIKE %s" for _ in patrones])
        query = f"""
            UPDATE establecimientos
            SET color_bg = %s, color_text = %s
            WHERE {condiciones}
        """

        params = [bg, text] + [f"%{p}%" for p in patrones]
        cur.execute(query, params)

        actualizados = cur.rowcount
        print(f"   🎨 {nombre:12} → {actualizados} establecimientos actualizados")

    # Paso 3: Verificar
    print("\n📊 PASO 3: Verificando resultados...\n")

    cur.execute("""
        SELECT
            nombre_normalizado,
            color_bg,
            color_text
        FROM establecimientos
        ORDER BY nombre_normalizado
    """)

    establecimientos = cur.fetchall()

    print(f"{'Establecimiento':30} | {'Color BG':10} | {'Color Text':10}")
    print("-" * 55)

    for est in establecimientos:
        nombre, bg, text = est
        print(f"{nombre[:30]:30} | {bg:10} | {text:10}")

    # Confirmar
    print("\n" + "=" * 80)
    respuesta = input("¿Guardar cambios? (si/no): ").strip().lower()

    if respuesta == 'si':
        conn.commit()
        print("✅ CAMBIOS GUARDADOS")
    else:
        conn.rollback()
        print("❌ CAMBIOS DESCARTADOS")

    print("=" * 80)

except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
    if conn:
        conn.rollback()

finally:
    if cur:
        cur.close()
    if conn:
        conn.close()
    print("\n✅ Desconectado")
