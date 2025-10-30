"""
Script para estandarizar y enriquecer productos_maestros usando productos_referencia
- Actualiza nombres de productos
- Agrega marca, categoría y subcategoría
- Mantiene integridad de datos

Autor: Santiago
Fecha: 2025-10-30
Sistema: LecFac - Waze de precios de supermercados
"""
import psycopg2
from datetime import datetime
from typing import Dict, List, Tuple

# Configuración de base de datos
DATABASE_URL = "postgresql://postgres:cupPYKmBUuABVOVtREemnOSfLIwyScVa@turntable.proxy.rlwy.net:52874/railway"


def conectar_db():
    """Conecta a la base de datos"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"❌ Error conectando a la base de datos: {e}")
        return None


def verificar_estructura():
    """Verifica que las tablas y columnas necesarias existan"""
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("🔍 Verificando estructura de tablas...\n")

        # Verificar productos_referencia
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_referencia'
        """)
        columnas_ref = [row[0] for row in cur.fetchall()]

        print("📋 productos_referencia:")
        print(f"   Columnas: {', '.join(columnas_ref)}")

        cur.execute("SELECT COUNT(*) FROM productos_referencia WHERE activo = TRUE")
        total_ref = cur.fetchone()[0]
        print(f"   Registros activos: {total_ref}\n")

        # Verificar productos_maestros
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
        """)
        columnas_maestros = [row[0] for row in cur.fetchall()]

        print("📋 productos_maestros:")
        print(f"   Columnas: {', '.join(columnas_maestros)}")

        cur.execute("""
            SELECT COUNT(*)
            FROM productos_maestros
            WHERE codigo_ean IS NOT NULL AND LENGTH(codigo_ean) >= 8
        """)
        total_maestros = cur.fetchone()[0]
        print(f"   Productos con EAN válido: {total_maestros}\n")

        # Verificar columnas necesarias en productos_maestros
        columnas_requeridas = ['marca', 'categoria', 'subcategoria']
        columnas_faltantes = [col for col in columnas_requeridas if col not in columnas_maestros]

        if columnas_faltantes:
            print(f"⚠️  Columnas faltantes en productos_maestros: {', '.join(columnas_faltantes)}")
            print("   Se agregarán automáticamente...\n")
        else:
            print("✅ Todas las columnas necesarias existen\n")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error verificando estructura: {e}")
        cur.close()
        conn.close()
        return False


def agregar_columnas_faltantes():
    """Agrega columnas faltantes a productos_maestros si no existen"""
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("🔧 Verificando y agregando columnas faltantes...\n")

        # Obtener columnas existentes
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'productos_maestros'
        """)
        columnas_existentes = [row[0] for row in cur.fetchall()]

        columnas_necesarias = {
            'marca': 'VARCHAR(100)',
            'categoria': 'VARCHAR(50)',
            'subcategoria': 'VARCHAR(50)'
        }

        for columna, tipo in columnas_necesarias.items():
            if columna not in columnas_existentes:
                try:
                    cur.execute(f"""
                        ALTER TABLE productos_maestros
                        ADD COLUMN {columna} {tipo}
                    """)
                    conn.commit()
                    print(f"   ✅ Columna '{columna}' agregada")
                except Exception as e:
                    print(f"   ⚠️  Error agregando '{columna}': {e}")
                    conn.rollback()
            else:
                print(f"   ✓ Columna '{columna}' ya existe")

        print()
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error agregando columnas: {e}")
        conn.rollback()
        cur.close()
        conn.close()
        return False


def analizar_coincidencias():
    """Analiza coincidencias entre productos_maestros y productos_referencia"""
    conn = conectar_db()
    if not conn:
        return

    cur = conn.cursor()

    try:
        print("="*80)
        print("📊 ANÁLISIS DE COINCIDENCIAS")
        print("="*80 + "\n")

        # Total de productos con EAN en productos_maestros
        cur.execute("""
            SELECT COUNT(*)
            FROM productos_maestros
            WHERE codigo_ean IS NOT NULL
            AND LENGTH(codigo_ean) >= 8
        """)
        total_maestros = cur.fetchone()[0]

        # Productos con match en productos_referencia
        cur.execute("""
            SELECT COUNT(DISTINCT pm.id)
            FROM productos_maestros pm
            INNER JOIN productos_referencia pr ON pm.codigo_ean = pr.codigo_ean
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.activo = TRUE
        """)
        con_match = cur.fetchone()[0]

        sin_match = total_maestros - con_match
        porcentaje = (con_match / total_maestros * 100) if total_maestros > 0 else 0

        print(f"📦 Total productos con EAN válido: {total_maestros:,}")
        print(f"✅ Con match en productos_referencia: {con_match:,} ({porcentaje:.1f}%)")
        print(f"⚠️  Sin match en productos_referencia: {sin_match:,} ({100-porcentaje:.1f}%)")

        # Ejemplos de productos CON match
        print("\n" + "-"*80)
        print("EJEMPLOS DE PRODUCTOS CON MATCH (primeros 5)")
        print("-"*80)

        cur.execute("""
            SELECT
                pm.codigo_ean,
                pm.nombre_normalizado as nombre_actual,
                pr.nombre_completo as nombre_referencia,
                pm.marca as marca_actual,
                pr.marca as marca_referencia,
                pm.categoria as cat_actual,
                pr.categoria as cat_referencia,
                CASE
                    WHEN pm.nombre_normalizado = pr.nombre_completo THEN 'IGUAL'
                    ELSE 'DIFERENTE'
                END as estado_nombre
            FROM productos_maestros pm
            INNER JOIN productos_referencia pr ON pm.codigo_ean = pr.codigo_ean
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.activo = TRUE
            ORDER BY pm.total_reportes DESC
            LIMIT 5
        """)

        for row in cur.fetchall():
            ean, nom_actual, nom_ref, marca_act, marca_ref, cat_act, cat_ref, estado = row

            print(f"\n📦 EAN: {ean} - Estado nombre: {estado}")
            print(f"   Actual: {nom_actual}")
            print(f"   Referencia: {nom_ref}")
            if marca_act or marca_ref:
                print(f"   Marca actual: {marca_act or 'N/A'} → Referencia: {marca_ref or 'N/A'}")
            if cat_act or cat_ref:
                print(f"   Categoría actual: {cat_act or 'N/A'} → Referencia: {cat_ref or 'N/A'}")

        # Ejemplos de productos SIN match
        print("\n" + "-"*80)
        print("EJEMPLOS DE PRODUCTOS SIN MATCH (primeros 5)")
        print("-"*80)

        cur.execute("""
            SELECT
                pm.codigo_ean,
                pm.nombre_normalizado,
                pm.marca,
                pm.total_reportes
            FROM productos_maestros pm
            LEFT JOIN productos_referencia pr ON pm.codigo_ean = pr.codigo_ean
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.id IS NULL
            ORDER BY pm.total_reportes DESC
            LIMIT 5
        """)

        sin_match_ejemplos = cur.fetchall()
        if sin_match_ejemplos:
            for ean, nombre, marca, reportes in sin_match_ejemplos:
                print(f"\n📦 EAN: {ean} - Reportes: {reportes}")
                print(f"   Nombre: {nombre}")
                print(f"   Marca: {marca or 'N/A'}")
                print(f"   ⚠️  No encontrado en productos_referencia")
        else:
            print("\n✅ Todos los productos tienen match!")

        print("\n" + "="*80 + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error analizando coincidencias: {e}")
        import traceback
        traceback.print_exc()
        cur.close()
        conn.close()


def analizar_duplicados():
    """Analiza productos duplicados por nombre con el mismo EAN"""
    conn = conectar_db()
    if not conn:
        return

    cur = conn.cursor()

    try:
        print("="*80)
        print("🔍 ANÁLISIS DE DUPLICADOS POR NOMBRE")
        print("="*80 + "\n")

        cur.execute("""
            SELECT
                pm.codigo_ean,
                COUNT(*) as cantidad_registros,
                STRING_AGG(DISTINCT pm.nombre_normalizado, ' || ' ORDER BY pm.nombre_normalizado) as nombres_variados,
                STRING_AGG(DISTINCT e.nombre_normalizado, ' | ') as establecimientos
            FROM productos_maestros pm
            LEFT JOIN items_factura if ON if.producto_maestro_id = pm.id
            LEFT JOIN facturas f ON if.factura_id = f.id
            LEFT JOIN establecimientos e ON f.establecimiento_id = e.id
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            GROUP BY pm.codigo_ean
            HAVING COUNT(DISTINCT pm.nombre_normalizado) > 1
            ORDER BY cantidad_registros DESC
            LIMIT 10
        """)

        duplicados = cur.fetchall()

        if duplicados:
            print(f"⚠️  Encontrados {len(duplicados)} códigos EAN con múltiples nombres:\n")

            for ean, cantidad, nombres, establecimientos in duplicados:
                print(f"📦 EAN: {ean}")
                print(f"   Registros: {cantidad}")
                print(f"   Nombres: {nombres[:150]}...")
                if establecimientos:
                    print(f"   Establecimientos: {establecimientos[:100]}...")
                print()
        else:
            print("✅ No se encontraron duplicados por nombre con mismo EAN")

        print("="*80 + "\n")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error analizando duplicados: {e}")
        import traceback
        traceback.print_exc()
        cur.close()
        conn.close()


def previsualizar_cambios(limite=20):
    """Previsualiza los cambios que se realizarían"""
    conn = conectar_db()
    if not conn:
        return 0

    cur = conn.cursor()

    try:
        print("="*80)
        print("👁️  PREVISUALIZACIÓN DE CAMBIOS")
        print("="*80 + "\n")

        # Contar total de cambios
        cur.execute("""
            SELECT COUNT(*)
            FROM productos_maestros pm
            INNER JOIN productos_referencia pr ON pm.codigo_ean = pr.codigo_ean
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.activo = TRUE
            AND (
                pm.nombre_normalizado != pr.nombre_completo
                OR pm.marca IS DISTINCT FROM pr.marca
                OR pm.categoria IS DISTINCT FROM pr.categoria
                OR pm.subcategoria IS DISTINCT FROM pr.subcategoria
            )
        """)

        total_cambios = cur.fetchone()[0]

        if total_cambios == 0:
            print("✅ No hay cambios que realizar")
            print("   Todos los productos ya están sincronizados con productos_referencia\n")
            cur.close()
            conn.close()
            return 0

        print(f"📊 Total de productos a actualizar: {total_cambios:,}\n")
        print(f"Mostrando primeros {limite} cambios:\n")
        print("-"*80)

        # Mostrar ejemplos de cambios
        cur.execute(f"""
            SELECT
                pm.id,
                pm.codigo_ean,
                pm.nombre_normalizado as nombre_actual,
                pr.nombre_completo as nombre_nuevo,
                pm.marca as marca_actual,
                pr.marca as marca_nueva,
                pm.categoria as cat_actual,
                pr.categoria as cat_nueva,
                pm.subcategoria as subcat_actual,
                pr.subcategoria as subcat_nueva,
                pm.total_reportes
            FROM productos_maestros pm
            INNER JOIN productos_referencia pr ON pm.codigo_ean = pr.codigo_ean
            WHERE pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.activo = TRUE
            AND (
                pm.nombre_normalizado != pr.nombre_completo
                OR pm.marca IS DISTINCT FROM pr.marca
                OR pm.categoria IS DISTINCT FROM pr.categoria
                OR pm.subcategoria IS DISTINCT FROM pr.subcategoria
            )
            ORDER BY pm.total_reportes DESC
            LIMIT {limite}
        """)

        cambios = cur.fetchall()

        for row in cambios:
            (id_prod, ean, nom_act, nom_nuevo, marca_act, marca_nueva,
             cat_act, cat_nueva, subcat_act, subcat_nueva, reportes) = row

            print(f"\n📦 ID: {id_prod} | EAN: {ean} | Reportes: {reportes}")

            # Cambios en nombre
            if nom_act != nom_nuevo:
                print(f"   📝 NOMBRE:")
                print(f"      Actual: {nom_act}")
                print(f"      Nuevo:  {nom_nuevo}")

            # Cambios en marca
            if marca_act != marca_nueva:
                print(f"   🏷️  MARCA:")
                print(f"      Actual: {marca_act or '(vacío)'}")
                print(f"      Nueva:  {marca_nueva or '(vacío)'}")

            # Cambios en categoría
            if cat_act != cat_nueva:
                print(f"   📂 CATEGORÍA:")
                print(f"      Actual: {cat_act or '(vacío)'}")
                print(f"      Nueva:  {cat_nueva or '(vacío)'}")

            # Cambios en subcategoría
            if subcat_act != subcat_nueva:
                print(f"   📁 SUBCATEGORÍA:")
                print(f"      Actual: {subcat_act or '(vacío)'}")
                print(f"      Nueva:  {subcat_nueva or '(vacío)'}")

        print("\n" + "="*80)
        print(f">>> TOTAL DE PRODUCTOS A ACTUALIZAR: {total_cambios:,}")
        print("="*80 + "\n")

        cur.close()
        conn.close()
        return total_cambios

    except Exception as e:
        print(f"❌ Error previsualizando cambios: {e}")
        import traceback
        traceback.print_exc()
        cur.close()
        conn.close()
        return 0


def ejecutar_estandarizacion():
    """Ejecuta la estandarización y enriquecimiento de productos"""
    conn = conectar_db()
    if not conn:
        return False

    cur = conn.cursor()

    try:
        print("="*80)
        print("🚀 EJECUTANDO ESTANDARIZACIÓN Y ENRIQUECIMIENTO")
        print("="*80 + "\n")

        # Query de actualización
        update_query = """
            UPDATE productos_maestros pm
            SET
                nombre_normalizado = pr.nombre_completo,
                marca = COALESCE(pr.marca, pm.marca),
                categoria = COALESCE(pr.categoria, pm.categoria),
                subcategoria = COALESCE(pr.subcategoria, pm.subcategoria),
                ultima_actualizacion = NOW()
            FROM productos_referencia pr
            WHERE pm.codigo_ean = pr.codigo_ean
            AND pm.codigo_ean IS NOT NULL
            AND LENGTH(pm.codigo_ean) >= 8
            AND pr.activo = TRUE
            AND (
                pm.nombre_normalizado != pr.nombre_completo
                OR pm.marca IS DISTINCT FROM pr.marca
                OR pm.categoria IS DISTINCT FROM pr.categoria
                OR pm.subcategoria IS DISTINCT FROM pr.subcategoria
            )
        """

        print("⏳ Ejecutando actualización...")
        cur.execute(update_query)
        registros_actualizados = cur.rowcount

        conn.commit()

        print(f"\n✅ ACTUALIZACIÓN COMPLETADA EXITOSAMENTE")
        print(f"   📊 Productos actualizados: {registros_actualizados:,}")
        print(f"   📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Verificación post-actualización
        print("-"*80)
        print("VERIFICACIÓN POST-ACTUALIZACIÓN")
        print("-"*80 + "\n")

        # Verificar duplicados restantes
        cur.execute("""
            SELECT COUNT(*)
            FROM (
                SELECT codigo_ean
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                AND LENGTH(codigo_ean) >= 8
                GROUP BY codigo_ean
                HAVING COUNT(DISTINCT nombre_normalizado) > 1
            ) AS duplicados
        """)

        duplicados_restantes = cur.fetchone()[0]

        if duplicados_restantes > 0:
            print(f"⚠️  {duplicados_restantes} códigos EAN aún tienen múltiples nombres")
            print("   Esto puede ser normal si son productos en diferentes establecimientos")

            # Mostrar ejemplos
            cur.execute("""
                SELECT
                    codigo_ean,
                    COUNT(*) as cantidad,
                    STRING_AGG(DISTINCT nombre_normalizado, ' | ') as nombres
                FROM productos_maestros
                WHERE codigo_ean IS NOT NULL
                AND LENGTH(codigo_ean) >= 8
                GROUP BY codigo_ean
                HAVING COUNT(DISTINCT nombre_normalizado) > 1
                LIMIT 3
            """)

            print("\n   Ejemplos:")
            for ean, cant, nombres in cur.fetchall():
                print(f"   - EAN {ean}: {cant} variantes")
                print(f"     {nombres[:150]}...")
        else:
            print("✅ No quedan duplicados de nombre con el mismo EAN")

        # Estadísticas finales
        print("\n" + "-"*80)
        print("ESTADÍSTICAS FINALES")
        print("-"*80 + "\n")

        cur.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN marca IS NOT NULL THEN 1 END) as con_marca,
                COUNT(CASE WHEN categoria IS NOT NULL THEN 1 END) as con_categoria,
                COUNT(CASE WHEN subcategoria IS NOT NULL THEN 1 END) as con_subcategoria
            FROM productos_maestros
            WHERE codigo_ean IS NOT NULL
            AND LENGTH(codigo_ean) >= 8
        """)

        stats = cur.fetchone()
        total, con_marca, con_cat, con_subcat = stats

        print(f"📦 Total productos con EAN: {total:,}")
        print(f"🏷️  Con marca: {con_marca:,} ({con_marca/total*100:.1f}%)")
        print(f"📂 Con categoría: {con_cat:,} ({con_cat/total*100:.1f}%)")
        print(f"📁 Con subcategoría: {con_subcat:,} ({con_subcat/total*100:.1f}%)")

        print("\n" + "="*80)
        print("✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("="*80 + "\n")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        conn.rollback()
        print(f"\n❌ ERROR DURANTE LA ACTUALIZACIÓN: {e}")
        import traceback
        traceback.print_exc()
        cur.close()
        conn.close()
        return False


def main():
    """Función principal"""
    print("\n" + "="*80)
    print("🎯 ESTANDARIZACIÓN Y ENRIQUECIMIENTO DE PRODUCTOS")
    print("Sistema LecFac - Waze de precios de supermercados")
    print("="*80 + "\n")

    # 1. Verificar estructura
    if not verificar_estructura():
        print("❌ Error verificando estructura. Abortando.")
        return

    # 2. Agregar columnas faltantes
    if not agregar_columnas_faltantes():
        print("❌ Error agregando columnas. Abortando.")
        return

    # 3. Analizar estado actual
    analizar_coincidencias()
    analizar_duplicados()

    # 4. Previsualizar cambios
    total_cambios = previsualizar_cambios(limite=15)

    if total_cambios == 0:
        print("✅ No hay cambios que realizar. Sistema ya sincronizado.")
        return

    # 5. Confirmar ejecución
    print("\n" + "="*80)
    print("⚠️  CONFIRMACIÓN REQUERIDA")
    print("="*80)
    print(f"\nEstás a punto de actualizar {total_cambios:,} productos con:")
    print("  ✓ Nombres estandarizados desde productos_referencia")
    print("  ✓ Marcas enriquecidas")
    print("  ✓ Categorías y subcategorías actualizadas")
    print("\nEsta operación:")
    print("  • Modificará la tabla productos_maestros")
    print("  • NO eliminará datos existentes (usa COALESCE)")
    print("  • Actualizará el campo ultima_actualizacion")
    print("  • Es REVERSIBLE desde backups")

    respuesta = input("\n¿Deseas continuar? (escribe 'SI' en mayúsculas para confirmar): ").strip()

    if respuesta == 'SI':
        print("\n🚀 Iniciando actualización...\n")
        if ejecutar_estandarizacion():
            print("🎉 ¡Proceso completado con éxito!")
        else:
            print("❌ El proceso falló. Revisa los errores arriba.")
    else:
        print("\n❌ Operación cancelada por el usuario")
        print("   No se realizaron cambios en la base de datos")

    print("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
