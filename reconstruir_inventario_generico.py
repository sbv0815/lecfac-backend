# reconstruir_inventario_generico.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def reconstruir_inventario_usuario(usuario_id: int, factura_id: int = None):
    """
    Reconstruye el inventario de un usuario desde cero

    Args:
        usuario_id: ID del usuario a reconstruir
        factura_id: Si se especifica, solo desde esa factura. Si es None, desde todas sus facturas
    """
    conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    cur = conn.cursor()

    # Obtener email del usuario
    cur.execute("SELECT email FROM usuarios WHERE id = %s", (usuario_id,))
    usuario = cur.fetchone()

    if not usuario:
        print(f"❌ Usuario {usuario_id} no existe")
        cur.close()
        conn.close()
        return False

    print(f"\n{'='*60}")
    print(f"🔧 RECONSTRUYENDO INVENTARIO")
    print(f"{'='*60}")
    print(f"Usuario: {usuario[0]} (ID: {usuario_id})")

    # 1. Ver estado actual
    cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = %s", (usuario_id,))
    antes = cur.fetchone()[0]
    print(f"📊 Productos actuales en inventario: {antes}")

    # 2. Ver facturas del usuario
    if factura_id:
        cur.execute("""
            SELECT id, establecimiento, total_factura, fecha_factura
            FROM facturas
            WHERE id = %s AND usuario_id = %s
        """, (factura_id, usuario_id))
        print(f"📦 Reconstruyendo SOLO desde factura #{factura_id}")
    else:
        cur.execute("""
            SELECT id, establecimiento, total_factura, fecha_factura
            FROM facturas
            WHERE usuario_id = %s
            ORDER BY fecha_factura DESC
        """, (usuario_id,))
        print(f"📦 Reconstruyendo desde TODAS las facturas del usuario")

    facturas = cur.fetchall()

    if not facturas:
        print(f"⚠️ No hay facturas para este usuario")
        cur.close()
        conn.close()
        return False

    print(f"\n📄 Facturas encontradas: {len(facturas)}")
    for f in facturas:
        print(f"   • Factura #{f[0]}: {f[1]} - ${f[2]:,} ({f[3]})")

    # 3. Limpiar inventario actual
    print(f"\n🗑️ Limpiando inventario actual...")
    cur.execute("DELETE FROM inventario_usuario WHERE usuario_id = %s", (usuario_id,))
    borrados = cur.rowcount
    print(f"   ✅ Borrados: {borrados} productos")

    conn.commit()

    # 4. Reconstruir desde items_factura
    print(f"\n📦 Reconstruyendo inventario desde items_factura...")

    if factura_id:
        where_clause = "AND f.id = %s"
        params = (usuario_id, factura_id)
    else:
        where_clause = ""
        params = (usuario_id,)

    query = f"""
        INSERT INTO inventario_usuario (
            usuario_id,
            producto_maestro_id,
            cantidad_actual,
            unidad_medida,
            fecha_ultima_actualizacion,
            fecha_ultima_compra,
            precio_ultima_compra,
            precio_promedio,
            precio_minimo,
            precio_maximo,
            numero_compras,
            cantidad_total_comprada,
            total_gastado,
            ultima_factura_id,
            establecimiento_id,
            establecimiento
        )
        SELECT
            %s as usuario_id,
            i.producto_maestro_id,
            SUM(i.cantidad) as cantidad_actual,
            'unidades' as unidad_medida,
            NOW() as fecha_ultima_actualizacion,
            MAX(f.fecha_factura) as fecha_ultima_compra,
            (SELECT precio_pagado FROM items_factura
             WHERE producto_maestro_id = i.producto_maestro_id
               AND factura_id = MAX(f.id)) as precio_ultima_compra,
            CAST(AVG(i.precio_pagado) AS INTEGER) as precio_promedio,
            MIN(i.precio_pagado) as precio_minimo,
            MAX(i.precio_pagado) as precio_maximo,
            COUNT(DISTINCT f.id) as numero_compras,
            SUM(i.cantidad) as cantidad_total_comprada,
            SUM(i.precio_pagado * i.cantidad) as total_gastado,
            MAX(f.id) as ultima_factura_id,
            MAX(f.establecimiento_id) as establecimiento_id,
            MAX(f.establecimiento) as establecimiento
        FROM items_factura i
        JOIN facturas f ON i.factura_id = f.id
        WHERE f.usuario_id = %s
          AND i.producto_maestro_id IS NOT NULL
          {where_clause}
        GROUP BY i.producto_maestro_id
    """

    cur.execute(query, params + params)
    insertados = cur.rowcount
    print(f"   ✅ Insertados: {insertados} productos")

    conn.commit()

    # 5. Verificar resultado
    print(f"\n{'='*60}")
    print(f"📊 RESULTADO FINAL")
    print(f"{'='*60}")

    cur.execute("SELECT COUNT(*) FROM inventario_usuario WHERE usuario_id = %s", (usuario_id,))
    despues = cur.fetchone()[0]
    print(f"Productos en inventario: {despues}")

    # 6. Mostrar productos
    print(f"\n📦 Productos en inventario (primeros 15):")
    cur.execute("""
        SELECT
            pm.nombre_normalizado,
            inv.cantidad_actual,
            inv.precio_promedio,
            inv.numero_compras,
            inv.ultima_factura_id
        FROM inventario_usuario inv
        JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
        WHERE inv.usuario_id = %s
        ORDER BY pm.nombre_normalizado
        LIMIT 15
    """, (usuario_id,))

    for p in cur.fetchall():
        print(f"   • {p[0][:40]:<40} | Cant: {p[1]:<4} | Precio: ${p[2]:>6,} | Compras: {p[3]} | Factura: #{p[4]}")

    # 7. Verificar mermeladas
    cur.execute("""
        SELECT COUNT(*)
        FROM inventario_usuario inv
        JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
        WHERE inv.usuario_id = %s
          AND LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
    """, (usuario_id,))

    mermeladas = cur.fetchone()[0]

    if mermeladas > 0:
        print(f"\n🍓 MERMELADAS encontradas: {mermeladas}")
        cur.execute("""
            SELECT pm.nombre_normalizado, inv.ultima_factura_id, inv.numero_compras
            FROM inventario_usuario inv
            JOIN productos_maestros pm ON inv.producto_maestro_id = pm.id
            WHERE inv.usuario_id = %s
              AND LOWER(pm.nombre_normalizado) LIKE '%mermelada%'
        """, (usuario_id,))

        for m in cur.fetchall():
            print(f"   • {m[0]} (Factura: {m[1]}, Compras: {m[2]})")
    else:
        print(f"\n✅ NO hay mermeladas en el inventario")

    cur.close()
    conn.close()

    print(f"\n{'='*60}")
    print(f"✅ INVENTARIO RECONSTRUIDO CORRECTAMENTE")
    print(f"{'='*60}\n")

    return True


# ============================================
# EJECUTAR PARA USUARIO 1 (Santiago)
# ============================================

if __name__ == "__main__":
    print("\n🚀 INICIANDO RECONSTRUCCIÓN DE INVENTARIO...\n")

    # Opción 1: Reconstruir solo desde factura 22
    reconstruir_inventario_usuario(usuario_id=1, factura_id=22)

    # Opción 2: Reconstruir desde TODAS las facturas del usuario 1
    # reconstruir_inventario_usuario(usuario_id=1)

    print("\n✅ Verifica el resultado en:")
    print("   • Backend: https://lecfac-backend-production.up.railway.app/api/inventario/usuario/1")
    print("   • Flutter: Módulo de Inventario\n")
