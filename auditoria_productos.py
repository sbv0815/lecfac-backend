#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
============================================================================
AUDITORÍA DE PRODUCTOS - LecFac
============================================================================
Script para analizar el estado de productos_maestros y productos_maestros_v2
Ejecutar: python auditoria_productos.py
============================================================================
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlparse
import os
from datetime import datetime

# Función simple para mostrar tablas sin dependencias
def mostrar_tabla(datos, titulo=""):
    """Muestra datos en formato tabla simple"""
    if not datos:
        print("(Sin datos)")
        return

    if titulo:
        print(f"\n{titulo}")

    # Obtener headers
    headers = list(datos[0].keys())

    # Calcular anchos
    anchos = {}
    for h in headers:
        anchos[h] = max(len(str(h)), max(len(str(row[h])) for row in datos))

    # Imprimir header
    print("\n" + "-" * (sum(anchos.values()) + len(headers) * 3))
    print("| " + " | ".join(str(h).ljust(anchos[h]) for h in headers) + " |")
    print("-" * (sum(anchos.values()) + len(headers) * 3))

    # Imprimir filas
    for row in datos:
        print("| " + " | ".join(str(row[h]).ljust(anchos[h]) for h in headers) + " |")

    print("-" * (sum(anchos.values()) + len(headers) * 3))

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Pega aquí tu DATABASE_URL de Railway
# NOTA: Si tienes problemas de DNS, reemplaza el hostname por la IP
DATABASE_URL = os.environ.get('DATABASE_URL') or "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@66.33.22.229:52874/railway"

def conectar():
    """Conecta a PostgreSQL"""
    url = urlparse(DATABASE_URL)

    conn = psycopg2.connect(
        host=url.hostname,
        database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port or 5432,
        cursor_factory=RealDictCursor,
        connect_timeout=30
    )

    return conn

# ============================================================================
# FUNCIONES DE AUDITORÍA
# ============================================================================

def resumen_general():
    """Compara ambas tablas"""
    print("\n" + "="*80)
    print("📊 RESUMEN GENERAL")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            'productos_maestros' as tabla,
            COUNT(*) as total,
            COUNT(DISTINCT codigo_ean) as eans_unicos,
            COUNT(*) FILTER (WHERE codigo_ean IS NOT NULL) as con_ean,
            COUNT(*) FILTER (WHERE marca IS NOT NULL) as con_marca
        FROM productos_maestros

        UNION ALL

        SELECT
            'productos_maestros_v2',
            COUNT(*),
            COUNT(DISTINCT codigo_ean),
            COUNT(*) FILTER (WHERE codigo_ean IS NOT NULL),
            COUNT(*) FILTER (WHERE marca IS NOT NULL)
        FROM productos_maestros_v2
    """)

    resultados = cursor.fetchall()

    mostrar_tabla(resultados)

    cursor.close()
    conn.close()

    return resultados


def duplicados_v2():
    """Analiza duplicados en productos_maestros_v2"""
    print("\n" + "="*80)
    print("🔍 DUPLICADOS EN productos_maestros_v2")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            codigo_ean,
            COUNT(*) as cantidad,
            STRING_AGG(id::text, ', ') as ids,
            STRING_AGG(nombre_consolidado, ' | ') as nombres
        FROM productos_maestros_v2
        WHERE codigo_ean IS NOT NULL AND codigo_ean != ''
        GROUP BY codigo_ean
        HAVING COUNT(*) > 1
        ORDER BY cantidad DESC
        LIMIT 20
    """)

    resultados = cursor.fetchall()

    if resultados:
        print(f"\n⚠️  Encontrados {len(resultados)} grupos de duplicados (mostrando top 20):")
        mostrar_tabla(resultados)
    else:
        print("\n✅ No hay duplicados por EAN")

    cursor.close()
    conn.close()

    return resultados


def productos_en_ambas():
    """Productos que están en ambas tablas"""
    print("\n" + "="*80)
    print("🔄 PRODUCTOS EN AMBAS TABLAS")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            pm.id as id_legacy,
            pm.codigo_ean,
            pm.nombre_normalizado as nombre_legacy,
            pmv2.id as id_v2,
            pmv2.nombre_consolidado as nombre_v2,
            CASE
                WHEN pm.nombre_normalizado = pmv2.nombre_consolidado THEN '✅ Igual'
                ELSE '⚠️ Diferente'
            END as comparacion
        FROM productos_maestros pm
        INNER JOIN productos_maestros_v2 pmv2 ON pm.codigo_ean = pmv2.codigo_ean
        WHERE pm.codigo_ean IS NOT NULL AND pm.codigo_ean != ''
        ORDER BY pm.id DESC
        LIMIT 20
    """)

    resultados = cursor.fetchall()

    if not resultados:
        print("\n⚠️ No hay productos con EAN en común entre ambas tablas")
        print("   (Esto es normal si los EANs están vacíos o nulos)")
    else:
        print(f"\n{len(resultados)} productos están en ambas tablas (mostrando top 20):")
        mostrar_tabla(resultados)

    cursor.close()
    conn.close()

    return resultados


def caso_huevos():
    """Analiza específicamente el caso HUEVOS"""
    print("\n" + "="*80)
    print("🥚 CASO ESPECÍFICO: HUEVOS")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            'productos_maestros' as tabla,
            id,
            codigo_ean,
            nombre_normalizado as nombre
        FROM productos_maestros
        WHERE UPPER(nombre_normalizado) LIKE '%HUEVO%'

        UNION ALL

        SELECT
            'productos_maestros_v2',
            id,
            codigo_ean,
            nombre_consolidado
        FROM productos_maestros_v2
        WHERE UPPER(nombre_consolidado) LIKE '%HUEVO%'

        ORDER BY codigo_ean NULLS LAST, tabla
    """)

    resultados = cursor.fetchall()

    print(f"\n{len(resultados)} productos con 'HUEVO':")
    mostrar_tabla(resultados)

    cursor.close()
    conn.close()

    return resultados


def productos_sin_codigo():
    """Productos sin EAN en v2"""
    print("\n" + "="*80)
    print("⚠️  PRODUCTOS SIN CÓDIGO EN productos_maestros_v2")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id,
            nombre_consolidado,
            marca,
            veces_visto
        FROM productos_maestros_v2
        WHERE (codigo_ean IS NULL OR codigo_ean = '')
        ORDER BY veces_visto DESC NULLS LAST
        LIMIT 20
    """)

    resultados = cursor.fetchall()

    print(f"\n{len(resultados)} productos sin código (mostrando top 20 más usados):")
    mostrar_tabla(resultados)

    cursor.close()
    conn.close()

    return resultados


def uso_en_facturas():
    """Verifica qué tabla se usa más en items_factura"""
    print("\n" + "="*80)
    print("📊 USO EN FACTURAS (items_factura)")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            'Total items en facturas' as relacion,
            COUNT(*) as cantidad
        FROM items_factura

        UNION ALL

        SELECT
            'Items con producto_maestro_id',
            COUNT(*)
        FROM items_factura
        WHERE producto_maestro_id IS NOT NULL
    """)

    resultados = cursor.fetchall()

    mostrar_tabla(resultados)

    cursor.close()
    conn.close()

    return resultados


def plus_faltantes():
    """Detecta PLUs que deberían estar en productos_por_establecimiento"""
    print("\n" + "="*80)
    print("📦 PLUs FALTANTES EN productos_por_establecimiento")
    print("="*80)

    conn = conectar()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            pm.id,
            pm.nombre_consolidado,
            COUNT(i.id) as veces_comprado,
            COUNT(DISTINCT pe.id) as plus_registrados,
            STRING_AGG(DISTINCT i.codigo_leido, ', ') as codigos_detectados
        FROM productos_maestros_v2 pm
        JOIN items_factura i ON pm.id = i.producto_maestro_id
        LEFT JOIN productos_por_establecimiento pe ON pm.id = pe.producto_maestro_id
        WHERE i.codigo_leido IS NOT NULL
          AND LENGTH(i.codigo_leido) BETWEEN 3 AND 7
        GROUP BY pm.id, pm.nombre_consolidado
        HAVING COUNT(DISTINCT pe.id) = 0
        ORDER BY veces_comprado DESC
        LIMIT 20
    """)

    resultados = cursor.fetchall()

    if resultados:
        print(f"\n⚠️  {len(resultados)} productos con PLUs detectados pero no guardados:")
        mostrar_tabla(resultados)
    else:
        print("\n✅ Todos los PLUs están correctamente guardados")

    cursor.close()
    conn.close()

    return resultados


# ============================================================================
# FUNCIÓN PRINCIPAL
# ============================================================================

def main():
    """Ejecuta auditoría completa"""
    print("\n" + "="*80)
    print("🔍 AUDITORÍA DE PRODUCTOS - LecFac")
    print("="*80)
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # Probar conexión
        print("\n🔗 Conectando a Railway...")
        conn = conectar()
        print("✅ Conexión exitosa")
        conn.close()

        # Ejecutar auditorías
        resumen = resumen_general()
        duplicados = duplicados_v2()
        ambas = productos_en_ambas()
        huevos = caso_huevos()
        sin_codigo = productos_sin_codigo()
        facturas = uso_en_facturas()
        plus = plus_faltantes()

        # Resumen final
        print("\n" + "="*80)
        print("📋 RESUMEN DE HALLAZGOS")
        print("="*80)

        total_v2 = resumen[1]['total']
        duplicados_count = len(duplicados) if duplicados else 0
        sin_codigo_count = len(sin_codigo) if sin_codigo else 0
        plus_faltantes_count = len(plus) if plus else 0

        print(f"""
✅ Total productos en v2: {total_v2}
⚠️  Grupos duplicados: {duplicados_count}
⚠️  Sin código: {sin_codigo_count}
⚠️  PLUs faltantes: {plus_faltantes_count}

💡 RECOMENDACIONES:
   1. Limpiar {duplicados_count} grupos de duplicados
   2. Completar códigos para {sin_codigo_count} productos
   3. Migrar {plus_faltantes_count} PLUs faltantes
        """)

        # Guardar reporte
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'auditoria_productos_{timestamp}.txt'

        print(f"\n💾 ¿Guardar reporte completo en '{filename}'? (s/n): ", end='')
        if input().lower() == 's':
            # Aquí podrías guardar el reporte
            print(f"✅ Reporte guardado (funcionalidad pendiente)")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
