"""
Script de Importación: Base de productos de referencia desde Excel
Limpia, deduplica e importa productos a la tabla productos_referencia
"""

import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import re

# Cargar variables de entorno
load_dotenv()

def conectar_db():
    """Conectar a la base de datos de Railway usando DATABASE_URL"""
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL no encontrada en las variables de entorno")

    url = urlparse(database_url)

    return psycopg2.connect(
        host=url.hostname,
        database=url.path[1:],
        user=url.username,
        password=url.password,
        port=url.port or 5432,
        connect_timeout=10,
        sslmode='prefer'
    )

def limpiar_texto(texto):
    """Limpia y normaliza texto"""
    if pd.isna(texto):
        return None

    texto = str(texto).strip()
    # Remover espacios múltiples
    texto = re.sub(r'\s+', ' ', texto)
    # Normalizar mayúsculas (solo primera letra de cada palabra)
    # texto = texto.title()  # Comentado porque puede romper siglas

    return texto if texto else None

def normalizar_ean(ean):
    """Normaliza el código EAN"""
    if pd.isna(ean):
        return None

    ean = str(ean).strip()

    # Remover puntos decimales si es float
    if '.' in ean:
        ean = ean.split('.')[0]

    # Solo dígitos
    if not ean.isdigit():
        return None

    # Validar longitud (3-14 dígitos)
    if len(ean) < 3 or len(ean) > 14:
        return None

    return ean

def seleccionar_mejor_registro(grupo):
    """
    De un grupo de duplicados, selecciona el mejor registro
    Criterios:
    1. Nombre más largo y completo
    2. Menos valores nulos
    3. Primer registro si son iguales
    """
    # Calcular score para cada registro
    grupo = grupo.copy()

    # Score por longitud de nombre
    grupo['score_nombre'] = grupo['prod_name_long'].fillna('').str.len()

    # Score por completitud (menos nulos)
    grupo['score_completitud'] = grupo.notna().sum(axis=1)

    # Score total
    grupo['score_total'] = grupo['score_nombre'] + (grupo['score_completitud'] * 10)

    # Retornar el mejor
    mejor = grupo.loc[grupo['score_total'].idxmax()]

    return mejor

def limpiar_excel(ruta_excel):
    """
    Limpia el Excel y retorna DataFrame limpio y deduplicado
    """
    print("=" * 80)
    print("📊 CARGANDO Y LIMPIANDO EXCEL")
    print("=" * 80)
    print()

    # Leer Excel
    print("📁 Leyendo archivo Excel...")
    df = pd.read_excel(ruta_excel)
    print(f"✅ Cargado: {len(df)} registros")
    print()

    # Limpiar textos
    print("🧹 Limpiando textos...")
    df['prod_name'] = df['prod_name'].apply(limpiar_texto)
    df['prod_name_long'] = df['prod_name_long'].apply(limpiar_texto)
    df['prod_brand'] = df['prod_brand'].apply(limpiar_texto)
    df['category'] = df['category'].apply(limpiar_texto)
    df['subcategory'] = df['subcategory'].apply(limpiar_texto)
    df['tags'] = df['tags'].apply(limpiar_texto)
    print("✅ Textos limpios")
    print()

    # Normalizar EANs
    print("🔢 Normalizando códigos EAN...")
    df['prod_id'] = df['prod_id'].apply(normalizar_ean)

    # Eliminar registros sin EAN válido
    antes = len(df)
    df = df[df['prod_id'].notna()]
    despues = len(df)
    print(f"✅ EANs normalizados ({antes - despues} registros sin EAN válido eliminados)")
    print()

    # Deduplicar
    print("🔍 Deduplicando por EAN...")
    print(f"   Antes: {len(df)} registros")
    print(f"   EANs únicos: {df['prod_id'].nunique()}")

    # Agrupar por EAN y seleccionar el mejor
    df_limpio = df.groupby('prod_id', group_keys=False).apply(seleccionar_mejor_registro)
    df_limpio = df_limpio.reset_index(drop=True)

    # Limpiar columnas de score
    if 'score_nombre' in df_limpio.columns:
        df_limpio = df_limpio.drop(columns=['score_nombre', 'score_completitud', 'score_total'])

    print(f"   Después: {len(df_limpio)} registros únicos")
    print(f"   Duplicados eliminados: {len(df) - len(df_limpio)}")
    print()

    return df_limpio

def crear_tabla_referencia(cursor):
    """Crea la tabla productos_referencia si no existe"""
    print("=" * 80)
    print("🏗️ CREANDO TABLA productos_referencia")
    print("=" * 80)
    print()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS productos_referencia (
            id SERIAL PRIMARY KEY,
            codigo_ean VARCHAR(14) UNIQUE NOT NULL,
            nombre_completo VARCHAR(300) NOT NULL,
            nombre_corto VARCHAR(200),
            marca VARCHAR(100),
            categoria VARCHAR(100),
            subcategoria VARCHAR(100),
            tags TEXT,
            fecha_importacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            activo BOOLEAN DEFAULT TRUE
        )
    """)

    # Crear índices para búsquedas rápidas
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_productos_ref_ean
        ON productos_referencia(codigo_ean)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_productos_ref_marca
        ON productos_referencia(marca)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_productos_ref_subcategoria
        ON productos_referencia(subcategoria)
    """)

    print("✅ Tabla productos_referencia creada/verificada")
    print("✅ Índices creados")
    print()

def importar_a_db(df, cursor, conn):
    """Importa los datos limpios a la base de datos"""
    print("=" * 80)
    print("📥 IMPORTANDO A BASE DE DATOS")
    print("=" * 80)
    print()

    # Verificar si ya hay datos
    cursor.execute("SELECT COUNT(*) FROM productos_referencia")
    registros_existentes = cursor.fetchone()[0]

    if registros_existentes > 0:
        print(f"⚠️  La tabla ya tiene {registros_existentes} registros")
        respuesta = input("¿Deseas LIMPIAR la tabla antes de importar? (SI/NO): ").strip().upper()

        if respuesta == "SI":
            print("🗑️  Limpiando tabla existente...")
            cursor.execute("DELETE FROM productos_referencia")
            conn.commit()
            print("✅ Tabla limpiada")
        else:
            print("⚠️  Importando sobre datos existentes (puede haber conflictos)")
        print()

    # Importar registros
    print(f"📦 Importando {len(df)} productos...")

    exitosos = 0
    duplicados = 0
    errores = 0

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO productos_referencia
                (codigo_ean, nombre_completo, nombre_corto, marca, categoria, subcategoria, tags)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (codigo_ean) DO UPDATE SET
                    nombre_completo = EXCLUDED.nombre_completo,
                    nombre_corto = EXCLUDED.nombre_corto,
                    marca = EXCLUDED.marca,
                    categoria = EXCLUDED.categoria,
                    subcategoria = EXCLUDED.subcategoria,
                    tags = EXCLUDED.tags,
                    fecha_importacion = CURRENT_TIMESTAMP
            """, (
                row['prod_id'],
                row['prod_name_long'],
                row['prod_name'],
                row['prod_brand'],
                row['category'],
                row['subcategory'],  # ← Corregido: subcategory (en inglés)
                row['tags']
            ))
            exitosos += 1

            if (index + 1) % 1000 == 0:
                print(f"   Procesados: {index + 1}/{len(df)}")
                conn.commit()

        except Exception as e:
            if 'duplicate key' in str(e).lower():
                duplicados += 1
            else:
                errores += 1
                print(f"   ❌ Error en fila {index}: {e}")

    # Commit final
    conn.commit()

    print()
    print("=" * 80)
    print("📊 RESULTADO DE IMPORTACIÓN")
    print("=" * 80)
    print(f"✅ Registros importados exitosamente: {exitosos}")
    if duplicados > 0:
        print(f"⚠️  Duplicados encontrados (actualizados): {duplicados}")
    if errores > 0:
        print(f"❌ Errores: {errores}")
    print()

def verificar_importacion(cursor):
    """Verifica la importación con estadísticas"""
    print("=" * 80)
    print("🔍 VERIFICACIÓN FINAL")
    print("=" * 80)
    print()

    # Total de registros
    cursor.execute("SELECT COUNT(*) FROM productos_referencia")
    total = cursor.fetchone()[0]
    print(f"Total de productos en referencia: {total}")

    # Productos con marca
    cursor.execute("SELECT COUNT(*) FROM productos_referencia WHERE marca IS NOT NULL")
    con_marca = cursor.fetchone()[0]
    print(f"Productos con marca: {con_marca} ({con_marca*100/total:.1f}%)")

    # Top 5 marcas
    cursor.execute("""
        SELECT marca, COUNT(*) as cantidad
        FROM productos_referencia
        WHERE marca IS NOT NULL
        GROUP BY marca
        ORDER BY cantidad DESC
        LIMIT 5
    """)
    print("\nTop 5 marcas:")
    for marca, cantidad in cursor.fetchall():
        print(f"  {marca}: {cantidad} productos")

    # Top 5 subcategorías
    cursor.execute("""
        SELECT subcategoria, COUNT(*) as cantidad
        FROM productos_referencia
        WHERE subcategoria IS NOT NULL
        GROUP BY subcategoria
        ORDER BY cantidad DESC
        LIMIT 5
    """)
    print("\nTop 5 subcategorías:")
    for subcat, cantidad in cursor.fetchall():
        print(f"  {subcat}: {cantidad} productos")

    # Ejemplos de productos importados
    cursor.execute("""
        SELECT codigo_ean, nombre_completo, marca, subcategoria
        FROM productos_referencia
        ORDER BY RANDOM()
        LIMIT 5
    """)
    print("\nEjemplos de productos importados:")
    for ean, nombre, marca, subcat in cursor.fetchall():
        print(f"\n  EAN: {ean}")
        print(f"  Nombre: {nombre}")
        print(f"  Marca: {marca}")
        print(f"  Subcategoría: {subcat}")

    print()

def main():
    """Función principal"""
    print("=" * 80)
    print("📦 IMPORTACIÓN DE BASE DE PRODUCTOS DE REFERENCIA")
    print("=" * 80)
    print()

    try:
        # Ruta del archivo Excel
        ruta_excel = r'C:\Programas\lecfac1\base_codigos_productos.xlsx'

        # Limpiar Excel
        df_limpio = limpiar_excel(ruta_excel)

        # Preview de los datos limpios
        print("=" * 80)
        print("👀 PREVIEW DE DATOS LIMPIOS")
        print("=" * 80)
        print()
        print(df_limpio.head(10).to_string())
        print()
        print(f"Total de productos únicos listos para importar: {len(df_limpio)}")
        print()

        # Confirmar importación
        print("=" * 80)
        print("⚠️  CONFIRMACIÓN")
        print("=" * 80)
        print(f"Se importarán {len(df_limpio)} productos únicos a la base de datos.")
        print("Esta operación puede tomar algunos minutos.")
        print()

        respuesta = input("¿Deseas continuar con la importación? (SI/NO): ").strip().upper()

        if respuesta != "SI":
            print()
            print("❌ Importación cancelada")
            return

        print()

        # Conectar a la base de datos
        print("📡 Conectando a la base de datos...")
        conn = conectar_db()
        cursor = conn.cursor()
        print("✅ Conexión establecida")
        print()

        # Crear tabla
        crear_tabla_referencia(cursor)
        conn.commit()

        # Importar datos
        importar_a_db(df_limpio, cursor, conn)

        # Verificar
        verificar_importacion(cursor)

        print("=" * 80)
        print("✅ IMPORTACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 80)
        print()
        print("📝 Próximos pasos:")
        print("  1. La tabla productos_referencia está lista")
        print("  2. Ahora hay que modificar el procesamiento de facturas")
        print("  3. Para usar esta referencia en matching automático")
        print()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
