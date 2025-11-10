#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
PLAN DE LIMPIEZA GRADUAL - LecFac
============================================================================
Plan específico basado en la auditoría realizada
============================================================================
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse

DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@66.33.22.229:52874/railway"

def conectar():
    url = urlparse(DATABASE_URL)
    return psycopg2.connect(
        host=url.hostname,
        database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port or 5432,
        cursor_factory=RealDictCursor,
        connect_timeout=30
    )

print("\n" + "="*80)
print("🧹 PLAN DE LIMPIEZA GRADUAL - LecFac")
print("="*80)
print("""
ESTRATEGIA:
1. productos_maestros_v2 = Tabla limpia (objetivo)
2. productos_maestros = Tabla legacy (mantener por compatibilidad)
3. Limpieza gradual sin romper nada

ACCIONES A REALIZAR:
""")

# ============================================================================
# FASE 1: IDENTIFICAR DUPLICADOS POR NOMBRE SIMILAR
# ============================================================================

print("\n" + "="*80)
print("📋 FASE 1: IDENTIFICAR DUPLICADOS POR NOMBRE SIMILAR")
print("="*80)

conn = conectar()
cursor = conn.cursor()

print("\n🔍 Buscando nombres muy similares en productos_maestros_v2...")

cursor.execute("""
    WITH productos_normalizados AS (
        SELECT
            id,
            nombre_consolidado,
            UPPER(TRIM(REGEXP_REPLACE(nombre_consolidado, '[^a-zA-Z0-9 ]', '', 'g'))) as nombre_limpio,
            marca,
            veces_visto
        FROM productos_maestros_v2
    ),
    grupos_similares AS (
        SELECT
            nombre_limpio,
            COUNT(*) as cantidad,
            STRING_AGG(id::text, ', ' ORDER BY veces_visto DESC) as ids,
            STRING_AGG(nombre_consolidado, ' | ' ORDER BY veces_visto DESC) as nombres_originales,
            MAX(veces_visto) as max_veces_visto
        FROM productos_normalizados
        GROUP BY nombre_limpio
        HAVING COUNT(*) > 1
    )
    SELECT * FROM grupos_similares
    ORDER BY cantidad DESC, max_veces_visto DESC
""")

duplicados_nombre = cursor.fetchall()

if duplicados_nombre:
    print(f"\n⚠️  Encontrados {len(duplicados_nombre)} grupos de nombres similares:\n")
    for i, grupo in enumerate(duplicados_nombre, 1):
        print(f"  {i}. '{grupo['nombre_limpio']}'")
        print(f"     Cantidad: {grupo['cantidad']}")
        print(f"     IDs: {grupo['ids']}")
        print(f"     Nombres originales: {grupo['nombres_originales']}")
        print(f"     Más usado: {grupo['max_veces_visto']} veces")
        print()
else:
    print("\n✅ No hay duplicados por nombre")

# ============================================================================
# FASE 2: PRODUCTOS QUE NECESITAN COMPLETAR MARCA
# ============================================================================

print("\n" + "="*80)
print("📋 FASE 2: PRODUCTOS SIN MARCA (Top 10 más usados)")
print("="*80)

cursor.execute("""
    SELECT id, nombre_consolidado, veces_visto
    FROM productos_maestros_v2
    WHERE marca IS NULL OR marca = ''
    ORDER BY veces_visto DESC
    LIMIT 10
""")

sin_marca = cursor.fetchall()

if sin_marca:
    print(f"\n⚠️  {len(sin_marca)} productos sin marca (mostrando top 10):\n")
    for p in sin_marca:
        print(f"  ID {p['id']:3d}: {p['nombre_consolidado'][:50]:50s} ({p['veces_visto']} usos)")
else:
    print("\n✅ Todos los productos tienen marca")

# ============================================================================
# FASE 3: PRODUCTOS EN productos_maestros QUE NO ESTÁN EN _v2
# ============================================================================

print("\n" + "="*80)
print("📋 FASE 3: PRODUCTOS FALTANTES EN productos_maestros_v2")
print("="*80)

cursor.execute("""
    SELECT
        pm.id,
        pm.nombre_normalizado,
        pm.marca
    FROM productos_maestros pm
    LEFT JOIN productos_maestros_v2 pmv2 ON pm.nombre_normalizado = pmv2.nombre_consolidado
    WHERE pmv2.id IS NULL
    ORDER BY pm.id
""")

faltantes = cursor.fetchall()

if faltantes:
    print(f"\n⚠️  {len(faltantes)} productos en productos_maestros que NO están en _v2:\n")
    for p in faltantes:
        print(f"  ID {p['id']:3d}: {p['nombre_normalizado'][:50]:50s} | Marca: {p['marca'] or 'N/A'}")
else:
    print("\n✅ Todos los productos de productos_maestros están en _v2")

cursor.close()
conn.close()

# ============================================================================
# RECOMENDACIONES FINALES
# ============================================================================

print("\n" + "="*80)
print("💡 RECOMENDACIONES PRIORITARIAS")
print("="*80)

print("""
ACCIONES INMEDIATAS:

1️⃣ FUSIONAR DUPLICADOS DE NOMBRE (si los hay)
   - Script: limpieza_productos.py → Opción 2 (Analizar duplicados)
   - Revisar manualmente cada caso
   - Fusionar los menos usados con los más usados

2️⃣ COMPLETAR MARCAS FALTANTES
   - Top prioridad: Los 10 productos más usados sin marca
   - Puedes hacerlo desde el dashboard web (botón editar)
   - O ejecutar SQL directo:

   UPDATE productos_maestros_v2
   SET marca = 'NOMBRE_MARCA'
   WHERE id = ID_PRODUCTO;

3️⃣ MIGRAR PRODUCTOS FALTANTES
   - Revisar si los productos en productos_maestros (legacy)
     realmente deben estar en productos_maestros_v2
   - Si sí, copiarlos manualmente

4️⃣ AGREGAR CÓDIGOS EAN
   - Los productos más usados deberían tener EAN
   - Buscar en Google/OpenFoodFacts
   - Agregar manualmente desde el dashboard

5️⃣ NORMALIZAR NOMBRES
   - "BIZCOCHO" vs "BIZCOCHOS" → elegir uno
   - "huevo rojo a 15" → "HUEVO ROJO"
   - Usar mayúsculas consistentes

NO HACER (POR AHORA):
❌ No eliminar productos_maestros (puede romper relaciones)
❌ No hacer DELETE masivos sin backup
❌ No normalizar automáticamente sin revisar
""")

print("\n" + "="*80)
print("✅ ANÁLISIS COMPLETO")
print("="*80)
print("""
PRÓXIMOS PASOS:
1. Revisar este reporte
2. Decidir qué productos fusionar
3. Ejecutar limpieza_productos.py para acciones específicas
4. Hacer cambios graduales (no todo de una vez)
""")
