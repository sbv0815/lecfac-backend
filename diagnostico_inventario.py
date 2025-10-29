"""
🔬 DIAGNÓSTICO COMPLETO DEL PROBLEMA DE INVENTARIO
Archivo independiente para analizar sin tocar main.py
Compatible con PostgreSQL y SQLite, pero forzamos PostgreSQL.
"""

import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Cargar variables de entorno (.env)
load_dotenv()

# Forzar uso de PostgreSQL siempre para evitar caer en SQLite
if os.environ.get("DATABASE_TYPE", "").lower() != "postgresql":
    os.environ["DATABASE_TYPE"] = "postgresql"

from database import get_db_connection, create_tables

def get_placeholder():
    """Retorna el placeholder correcto según la BD"""
    db_type = os.environ.get("DATABASE_TYPE", "postgresql").lower()
    return "%s" if db_type == "postgresql" else "?"

def diagnostico_completo():
    """Ejecuta todos los diagnósticos necesarios"""

    print("=" * 80)
    print("🔬 DIAGNÓSTICO COMPLETO - PROBLEMA INVENTARIO")
    print("=" * 80)

    # ✅ Garantizar que el esquema exista en PostgreSQL (incluye items_factura.precio_pagado)
    print("🧱 Verificando/creando esquema en PostgreSQL...")
    create_tables()

    # Conexión
    conn = get_db_connection()
    if not conn:
        raise RuntimeError("No se pudo abrir conexión a la base de datos (PostgreSQL).")
    cursor = conn.cursor()
    ph = get_placeholder()

    resultados = {
        "timestamp": datetime.now().isoformat(),
        "database_type": os.environ.get("DATABASE_TYPE", "postgresql"),
        "diagnosticos": {}
    }

    # ========================================
    # 1. COMPARACIÓN USUARIO 1 VS USUARIO 2
    # ========================================
    print("\n1️⃣ COMPARANDO USUARIO 1 (✅) VS USUARIO 2 (❌)")
    print("-" * 80)

    for usuario_id in [1, 2]:
        print(f"\n👤 USUARIO {usuario_id}:")
        # Datos generales
        cursor.execute(f"""
            SELECT
                u.email,
                COUNT(DISTINCT f.id) as num_facturas,
                SUM(f.total_factura) as total_declarado,
                MIN(f.fecha_cargue) as primera_factura,
                MAX(f.fecha_cargue) as ultima_factura
            FROM usuarios u
            LEFT JOIN facturas f ON u.id = f.usuario_id
            WHERE u.id = {ph}
            GROUP BY u.email
        """, (usuario_id,))

        user_data = cursor.fetchone()
        if user_data:
            print(f"   Email: {user_data[0]}")
            print(f"   Facturas: {user_data[1]}")
            print(f"   Total declarado: ${float(user_data[2] or 0):,.0f}")
            print(f"   Primera factura: {user_data[3]}")
            print(f"   Última factura: {user_data[4]}")

            resultados["diagnosticos"][f"usuario_{usuario_id}"] = {
                "email": user_data[0],
                "num_facturas": user_data[1],
                "total_declarado": float(user_data[2] or 0),
                "primera_factura": str(user_data[3]),
                "ultima_factura": str(user_data[4])
            }
        else:
            print(f"   ⚠️ No se encontraron datos para usuario {usuario_id}")
            resultados["diagnosticos"][f"usuario_{usuario_id}"] = None
            continue

        # Items guardados
        cursor.execute(f"""
            SELECT
                COUNT(if_.id) as num_items,
                SUM(if_.precio_pagado) as suma_precios,
                AVG(if_.precio_pagado) as precio_promedio,
                MIN(if_.precio_pagado) as precio_min,
                MAX(if_.precio_pagado) as precio_max
            FROM items_factura if_
            JOIN facturas f ON if_.factura_id = f.id
            WHERE f.usuario_id = {ph}
        """, (usuario_id,))

        items_data = cursor.fetchone()
        if items_data and items_data[0]:
            suma_items = float(items_data[1] or 0)
            total_declarado = float(user_data[2] or 0)
            ratio = (suma_items / total_declarado) if total_declarado > 0 else 0.0

            print(f"\n   📦 ITEMS:")
            print(f"      Total items: {items_data[0]}")
            print(f"      Suma precios: ${suma_items:,.0f}")
            print(f"      Precio promedio: ${float(items_data[2] or 0):,.0f}")
            print(f"      Precio mínimo: ${float(items_data[3] or 0):,.0f}")
            print(f"      Precio máximo: ${float(items_data[4] or 0):,.0f}")
            print(f"      RATIO items/facturas: {ratio:.2f}x")

            resultados["diagnosticos"][f"usuario_{usuario_id}"]["items"] = {
                "num_items": int(items_data[0] or 0),
                "suma_precios": suma_items,
                "ratio": round(ratio, 2)
            }
        else:
            print(f"\n   📦 No hay items para este usuario")

    # ========================================
    # 2. ANÁLISIS POR FACTURA - USUARIO 2
    # ========================================
    print("\n\n2️⃣ ANÁLISIS DETALLADO - FACTURAS USUARIO 2")
    print("-" * 80)

    cursor.execute(f"""
        SELECT
            f.id,
            f.establecimiento,
            f.total_factura,
            f.fecha_cargue,
            COUNT(if_.id) as num_items,
            SUM(if_.precio_pagado) as suma_items
        FROM facturas f
        LEFT JOIN items_factura if_ ON f.id = if_.factura_id
        WHERE f.usuario_id = {ph}
        GROUP BY f.id, f.establecimiento, f.total_factura, f.fecha_cargue
        ORDER BY f.id
    """, (2,))

    facturas_u2 = []
    for row in cursor.fetchall():
        factura_id = row[0]
        establecimiento = row[1]
        total_declarado = float(row[2] or 0)
        fecha_cargue = row[3]
        num_items = int(row[4] or 0)
        suma_items = float(row[5] or 0)
        ratio = (suma_items / total_declarado) if total_declarado > 0 else 0.0

        print(f"\n📄 FACTURA #{factura_id} - {establecimiento}")
        print(f"   Fecha: {fecha_cargue}")
        print(f"   Total declarado: ${total_declarado:,.0f}")
        print(f"   Suma items: ${suma_items:,.0f}")
        print(f"   Items: {num_items}")
        print(f"   Ratio: {ratio:.2f}x {'⚠️ INFLADO' if ratio > 1.2 else '✅'}")

        # Ver items individuales de esta factura
        cursor.execute(f"""
            SELECT
                nombre_leido,
                cantidad,
                precio_pagado,
                (cantidad * precio_pagado) as subtotal
            FROM items_factura
            WHERE factura_id = {ph}
            ORDER BY precio_pagado DESC
            LIMIT 5
        """, (factura_id,))

        print(f"\n   🔍 Top 5 items más caros:")
        for item in cursor.fetchall():
            nombre = (item[0] or "Sin nombre")[:40]
            cantidad = int(item[1] or 0)
            precio = float(item[2] or 0)
            subtotal = float(item[3] or 0)
            print(f"      • {nombre:<40} | Cant: {cantidad} | Precio: ${precio:>8,.0f} | Subtotal: ${subtotal:>8,.0f}")

        facturas_u2.append({
            "factura_id": factura_id,
            "establecimiento": establecimiento,
            "total_declarado": total_declarado,
            "suma_items": suma_items,
            "ratio": round(ratio, 2),
            "fecha": str(fecha_cargue)
        })

    resultados["diagnosticos"]["facturas_usuario_2"] = facturas_u2

    # ========================================
    # 3. ANÁLISIS POR ESTABLECIMIENTO
    # ========================================
    print("\n\n3️⃣ ANÁLISIS POR ESTABLECIMIENTO")
    print("-" * 80)

    cursor.execute("""
        SELECT
            f.establecimiento,
            f.usuario_id,
            COUNT(f.id) as num_facturas,
            SUM(f.total_factura) as total_declarado,
            (SELECT SUM(precio_pagado)
             FROM items_factura if_inner
             WHERE if_inner.factura_id IN
                (SELECT id FROM facturas WHERE establecimiento = f.establecimiento AND usuario_id = f.usuario_id)
            ) as suma_items
        FROM facturas f
        GROUP BY f.establecimiento, f.usuario_id
        ORDER BY f.usuario_id, f.establecimiento
    """)

    print("\n📊 Ratio por Establecimiento:")
    for row in cursor.fetchall():
        establecimiento = row[0] or "Sin nombre"
        usuario = int(row[1] or 0)
        num_facturas = int(row[2] or 0)
        total_dec = float(row[3] or 0)
        suma_items = float(row[4] or 0)
        ratio = (suma_items / total_dec) if total_dec > 0 else 0.0
        estado = "✅" if 0.95 <= ratio <= 1.05 else "⚠️"
        print(f"   {estado} Usuario {usuario} | {establecimiento:<20} | Facturas: {num_facturas} | Ratio: {ratio:.2f}x")

    # ========================================
    # 4. PATRONES TEMPORALES
    # ========================================
    print("\n\n4️⃣ PATRONES TEMPORALES")
    print("-" * 80)

    cursor.execute("""
        SELECT
            DATE(f.fecha_cargue) as fecha,
            f.usuario_id,
            COUNT(f.id) as facturas
        FROM facturas f
        WHERE f.total_factura > 0
        GROUP BY DATE(f.fecha_cargue), f.usuario_id
        ORDER BY DATE(f.fecha_cargue)
    """)

    print("\n📅 Facturas por fecha:")
    for row in cursor.fetchall():
        print(f"   📆 {row[0]} | Usuario {row[1]} | {row[2]} facturas")

    # ========================================
    # 5. PRODUCTOS MÁS PROBLEMÁTICOS (Usuario 2)
    # ========================================
    print("\n\n5️⃣ PRODUCTOS MÁS PROBLEMÁTICOS (Usuario 2)")
    print("-" * 80)

    cursor.execute(f"""
        SELECT
            if_.nombre_leido,
            COUNT(*) as veces_comprado,
            AVG(if_.precio_pagado) as precio_promedio,
            MIN(if_.precio_pagado) as precio_min,
            MAX(if_.precio_pagado) as precio_max
        FROM items_factura if_
        JOIN facturas f ON if_.factura_id = f.id
        WHERE f.usuario_id = {ph}
        GROUP BY if_.nombre_leido
        ORDER BY MAX(if_.precio_pagado) DESC
        LIMIT 10
    """, (2,))

    print("\n🔍 Top 10 productos más caros:")
    for row in cursor.fetchall():
        nombre = (row[0] or "Sin nombre")[:50]
        promedio = float(row[2] or 0)
        maximo = float(row[4] or 0)
        print(f"   {nombre:<50} | Promedio: ${promedio:>8,.0f} | Max: ${maximo:>8,.0f}")

    # ========================================
    # 6. DIAGNÓSTICO FINAL
    # ========================================
    print("\n\n" + "=" * 80)
    print("🎯 DIAGNÓSTICO FINAL")
    print("=" * 80)

    u1 = resultados["diagnosticos"].get("usuario_1") or {}
    u2 = resultados["diagnosticos"].get("usuario_2") or {}
    u1r = (u1.get("items") or {}).get("ratio", 0)
    u2r = (u2.get("items") or {}).get("ratio", 0)

    if u1 and u2 and u1r and u2r:
        print(f"\n✅ Usuario 1: Ratio {u1r:.2f}x - {'CORRECTO' if 0.95 <= u1r <= 1.05 else 'VERIFICAR'}")
        print(f"❌ Usuario 2: Ratio {u2r:.2f}x - {'INFLADO' if u2r > 1.2 else 'CORRECTO'}")

        print("\n🔍 HIPÓTESIS:")
        if u2r > 1.5:
            print("1) Precios en items_factura podrían venir inflados por OCR/normalización.")
            print("2) Verifica normalizar_precio_unitario() y el parser para esa cadena.")
            diff = u2r - u1r
            print(f"   Diferencia U2 - U1: {diff:.2f}x")

    print("\n" + "=" * 80)
    print("📝 RECOMENDACIÓN")
    print("=" * 80)
    print("""\
Siguiente paso: validar datos RAW del OCR para Usuario 2
1) Subir una factura de CARULLA del Usuario 2
2) Revisar logs del parser y la función normalizar_precio_unitario()
3) Comparar ratio contra Usuario 1
""")

    # Guardar resultados en JSON
    with open("diagnostico_resultado.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, indent=2, ensure_ascii=False)

    print("\n✅ Resultados guardados en: diagnostico_resultado.json")
    print("=" * 80)

    cursor.close()
    conn.close()
    return resultados

if __name__ == "__main__":
    try:
        diagnostico_completo()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
