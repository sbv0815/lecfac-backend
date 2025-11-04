"""
Corregir Inventarios Existentes con Datos Limpios
==================================================
Actualiza todos los inventarios para usar datos del catálogo maestro
"""

import psycopg

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def corregir_inventarios():
    """Corrige todos los inventarios con datos limpios del catálogo"""

    try:
        print("🔗 Conectando a Railway PostgreSQL...")
        conn = psycopg.connect(DATABASE_URL)
        cursor = conn.cursor()
        print("✅ Conexión exitosa\n")

        print("=" * 70)
        print("🧹 CORRECCIÓN DE INVENTARIOS CON DATOS LIMPIOS")
        print("=" * 70)

        # Obtener todos los inventarios que necesitan corrección
        cursor.execute("""
            SELECT
                i.id,
                i.usuario_id,
                i.producto_maestro_id,
                -- Datos actuales (pueden ser del OCR)
                i.cantidad_actual,
                i.precio_promedio,
                -- Datos LIMPIOS del catálogo
                pm.nombre_normalizado,
                pm.codigo_ean,
                pm.marca,
                pm.categoria,
                pm.subcategoria,
                pm.presentacion
            FROM inventario_usuario i
            INNER JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
            ORDER BY i.usuario_id, pm.nombre_normalizado
        """)

        inventarios = cursor.fetchall()

        if not inventarios:
            print("\n⚠️ No hay inventarios para corregir")
            conn.close()
            return

        print(f"\n📦 Corrigiendo {len(inventarios)} productos en inventarios...\n")

        # Agrupar por usuario
        usuarios = {}
        for inv in inventarios:
            usuario_id = inv[1]
            if usuario_id not in usuarios:
                usuarios[usuario_id] = []
            usuarios[usuario_id].append(inv)

        total_corregidos = 0

        for usuario_id, items in usuarios.items():
            print(f"👤 Usuario {usuario_id}: {len(items)} productos")

            for inv in items:
                inv_id = inv[0]
                producto_maestro_id = inv[2]
                cantidad_actual = inv[3]
                precio_promedio = inv[4]
                nombre_correcto = inv[5]
                codigo_ean = inv[6]
                marca = inv[7]
                categoria = inv[8]
                subcategoria = inv[9]
                presentacion = inv[10]

                try:
                    # ✅ Actualizar con datos limpios
                    # NO modificamos cantidades ni precios, solo metadatos
                    cursor.execute("""
                        UPDATE inventario_usuario
                        SET marca = %s
                        WHERE id = %s
                    """, (marca, inv_id))

                    total_corregidos += 1

                    # Mostrar solo algunos ejemplos
                    if total_corregidos <= 10:
                        print(f"   ✅ {nombre_correcto}")
                        if marca:
                            print(f"      Marca: {marca}")
                        if codigo_ean:
                            print(f"      EAN: {codigo_ean}")

                except Exception as e:
                    print(f"   ❌ Error con producto {producto_maestro_id}: {e}")
                    conn.rollback()
                    continue

            print()

        conn.commit()

        print("=" * 70)
        print(f"✅ CORRECCIÓN COMPLETADA")
        print(f"   Total productos corregidos: {total_corregidos}")
        print("=" * 70)

        # Mostrar resumen por usuario
        print("\n📊 RESUMEN POR USUARIO:\n")

        for usuario_id in usuarios.keys():
            cursor.execute("""
                SELECT
                    COUNT(*) as total_productos,
                    SUM(cantidad_actual) as total_unidades,
                    COUNT(DISTINCT pm.marca) as marcas_diferentes,
                    COUNT(DISTINCT pm.categoria) as categorias_diferentes
                FROM inventario_usuario i
                INNER JOIN productos_maestros pm ON i.producto_maestro_id = pm.id
                WHERE i.usuario_id = %s
            """, (usuario_id,))

            stats = cursor.fetchone()

            if stats:
                print(f"   👤 Usuario {usuario_id}:")
                print(f"      📦 {stats[0]} productos diferentes")
                print(f"      🔢 {int(stats[1])} unidades totales")
                print(f"      🏷️ {stats[2]} marcas")
                print(f"      📂 {stats[3]} categorías")
                print()

        print("=" * 70)
        print("✅ Los inventarios ahora usan datos limpios del catálogo maestro")
        print("=" * 70)

        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    corregir_inventarios()
