"""
Diagnosticar y Arreglar Establecimientos
=========================================
"""

import psycopg

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def diagnosticar_establecimientos():
    """Verifica qué establecimientos existen y crea los que faltan"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        print("=" * 70)
        print("📊 ESTABLECIMIENTOS EN LA BASE DE DATOS")
        print("=" * 70)

        # Ver todos los establecimientos
        cursor.execute("""
            SELECT id, nombre_normalizado, cadena, activo
            FROM establecimientos
            ORDER BY id
        """)

        establecimientos = cursor.fetchall()

        if establecimientos:
            print(f"\n✅ Total establecimientos: {len(establecimientos)}\n")
            for est in establecimientos:
                activo_icon = "✅" if est[3] else "❌"
                print(f"   {activo_icon} ID {est[0]}: {est[1]} (Cadena: {est[2]})")
        else:
            print("\n⚠️ No hay establecimientos en la base de datos")

        # Ver qué establecimientos usan las facturas
        print("\n" + "=" * 70)
        print("📄 ESTABLECIMIENTOS EN FACTURAS")
        print("=" * 70)

        cursor.execute("""
            SELECT DISTINCT establecimiento, COUNT(*) as total_facturas
            FROM facturas
            GROUP BY establecimiento
            ORDER BY total_facturas DESC
        """)

        facturas_est = cursor.fetchall()

        if facturas_est:
            print(f"\n✅ Establecimientos usados en facturas:\n")
            for est, count in facturas_est:
                print(f"   - {est}: {count} facturas")

                # Verificar si existe en tabla establecimientos
                cursor.execute("""
                    SELECT id FROM establecimientos
                    WHERE LOWER(TRIM(nombre_normalizado)) = LOWER(TRIM(%s))
                """, (est,))

                existe = cursor.fetchone()

                if not existe:
                    print(f"      ⚠️ NO EXISTE en tabla establecimientos")

                    # Crear el establecimiento
                    print(f"      ➕ Creando establecimiento...")

                    from database import detectar_cadena
                    cadena = detectar_cadena(est)

                    cursor.execute("""
                        INSERT INTO establecimientos (nombre_normalizado, cadena, activo)
                        VALUES (%s, %s, TRUE)
                        RETURNING id
                    """, (est, cadena))

                    nuevo_id = cursor.fetchone()[0]
                    conn.commit()

                    print(f"      ✅ Establecimiento creado: ID={nuevo_id}, Cadena={cadena}")
                else:
                    print(f"      ✅ Existe: ID={existe[0]}")

        print("\n" + "=" * 70)
        print("✅ DIAGNÓSTICO COMPLETADO")
        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    diagnosticar_establecimientos()
