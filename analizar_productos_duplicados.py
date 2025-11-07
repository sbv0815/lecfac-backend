#!/usr/bin/env python3
"""
analizar_productos_duplicados.py
Analiza productos duplicados y similares
"""

import psycopg2
from difflib import SequenceMatcher

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"

def buscar_productos_similares():
    """Busca productos con nombres similares que podrían ser el mismo"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("🔍 BUSCANDO PRODUCTOS DUPLICADOS/SIMILARES:\n")

    # Obtener todos los productos
    cur.execute("""
        SELECT id, codigo_ean, nombre_normalizado
        FROM productos_maestros
        ORDER BY nombre_normalizado
    """)

    productos = cur.fetchall()

    # Buscar similares
    similares = []
    for i in range(len(productos)):
        for j in range(i+1, len(productos)):
            # Calcular similitud
            ratio = SequenceMatcher(None,
                                  productos[i][2].lower(),
                                  productos[j][2].lower()).ratio()

            if ratio > 0.7:  # 70% similar
                similares.append({
                    'prod1': productos[i],
                    'prod2': productos[j],
                    'similitud': ratio
                })

    # Mostrar similares
    if similares:
        print("⚠️ PRODUCTOS POSIBLEMENTE DUPLICADOS:")
        for s in sorted(similares, key=lambda x: x['similitud'], reverse=True)[:10]:
            print(f"\nSimilitud: {s['similitud']:.0%}")
            print(f"   1) ID: {s['prod1'][0]}, EAN: {s['prod1'][1]}")
            print(f"      Nombre: '{s['prod1'][2]}'")
            print(f"   2) ID: {s['prod2'][0]}, EAN: {s['prod2'][1]}")
            print(f"      Nombre: '{s['prod2'][2]}'")

    # Buscar específicamente "harina" y "marina"
    print("\n\n📦 PRODUCTOS CON 'HARINA' O 'MARINA':")
    cur.execute("""
        SELECT id, codigo_ean, nombre_normalizado
        FROM productos_maestros
        WHERE nombre_normalizado ILIKE '%harina%'
           OR nombre_normalizado ILIKE '%marina%'
        ORDER BY nombre_normalizado
    """)

    for prod in cur.fetchall():
        print(f"   ID: {prod[0]}, EAN: {prod[1]} → '{prod[2]}'")

    cur.close()
    conn.close()

def analizar_codigos_cortos():
    """Analiza códigos que deberían ser PLUs"""
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    print("\n\n🏷️ CÓDIGOS QUE DEBERÍAN SER PLUs:")

    # Códigos de menos de 8 dígitos
    cur.execute("""
        SELECT
            pm.id,
            pm.codigo_ean,
            pm.nombre_normalizado,
            pm.es_producto_fresco,
            COUNT(DISTINCT if.factura_id) as veces_usado
        FROM productos_maestros pm
        LEFT JOIN items_factura if ON pm.id = if.producto_maestro_id
        WHERE LENGTH(pm.codigo_ean) < 8
        GROUP BY pm.id, pm.codigo_ean, pm.nombre_normalizado, pm.es_producto_fresco
        ORDER BY LENGTH(pm.codigo_ean), pm.codigo_ean
    """)

    for prod in cur.fetchall():
        print(f"   Código: '{prod[1]}' ({len(prod[1])} díg)")
        print(f"      → {prod[2]}")
        print(f"      → Fresco: {prod[3]}, Usado: {prod[4]} veces")

    cur.close()
    conn.close()

def proponer_solucion():
    """Propone solución para el sistema"""
    print("\n\n" + "="*70)
    print("💡 SOLUCIÓN PROPUESTA:")
    print("="*70)

    print("""
1. REGLA PARA DIFERENCIAR CÓDIGOS:
   - Si el código tiene 8+ dígitos → ES UN EAN (universal)
   - Si el código tiene 7 o menos dígitos → ES UN PLU (específico del super)

2. DONDE GUARDAR:
   - EANs → productos_maestros.codigo_ean
   - PLUs → productos_por_establecimiento (con establecimiento_id)

3. EJEMPLO:
   Factura Jumbo:
   - "7702000112311" (13 díg) → EAN universal de Harina Haz de Oro
   - "1045" (4 díg) → PLU de zanahoria EN JUMBO

   Factura Éxito:
   - "7702000112311" → Mismo EAN, mismo producto
   - "2001" → PLU de zanahoria EN ÉXITO (diferente código!)

4. NORMALIZACIÓN DE NOMBRES:
   Implementar fuzzy matching para evitar duplicados como:
   - "harina haz de oro" vs "marina haz o"
   - "CHOCOLA" vs "chocolate"
""")

def main():
    buscar_productos_similares()
    analizar_codigos_cortos()
    proponer_solucion()

if __name__ == "__main__":
    main()
