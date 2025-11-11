"""
============================================================================
NORMALIZACIÓN MASIVA DE PRODUCTOS V2.0 - CON SISTEMA DE APRENDIZAJE
============================================================================

PROPÓSITO:
Script de una sola ejecución para normalizar TODOS los nombres de productos
existentes en productos_maestros Y productos_maestros_v2 usando Perplexity
e integrando con el sistema de aprendizaje automático.

MEJORAS V2.0:
✅ Normaliza AMBAS tablas (productos_maestros + productos_maestros_v2)
✅ Obtiene establecimiento REAL desde items_facturas
✅ Compatible SQLite Y PostgreSQL
✅ Integra con aprendizaje_manager (guarda correcciones aprendidas)
✅ Evita reprocesar productos ya normalizados
✅ Genera reporte detallado antes/después
✅ Usa aprendizaje previo antes de llamar Perplexity (ahorra $$$)

PROCESO:
1. Identificar productos que necesitan normalización
2. Buscar en aprendizaje si ya conocemos la corrección
3. Si no existe, validar con Perplexity
4. Actualizar nombre_normalizado en ambas tablas
5. Guardar en correcciones_aprendidas para futuro
6. Generar reporte completo

AUTOR: LecFac Team
FECHA: 2025-11-11
VERSION: 2.0
============================================================================
"""

import time
from datetime import datetime
from database import get_db_connection
from perplexity_validator import validar_nombre_producto
from aprendizaje_manager import AprendizajeManager
import os
import sys

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

BATCH_SIZE = 50  # Procesar de 50 en 50
DELAY_BETWEEN_BATCHES = 30  # Esperar 30 segundos entre lotes
DELAY_BETWEEN_REQUESTS = 2  # Esperar 2 segundos entre cada request a Perplexity
MAX_PRODUCTOS = None  # None = todos, o número para limitar (ej: 10 para prueba)
DRY_RUN = False  # True = solo mostrar, False = guardar cambios

# Tablas a normalizar
NORMALIZAR_PRODUCTOS_MAESTROS = True
NORMALIZAR_PRODUCTOS_MAESTROS_V2 = True

# Criterios de normalización
FORZAR_RENORMALIZACION = False  # True = normalizar TODO, False = solo sin normalizar
MIN_CONFIANZA_APRENDIZAJE = 0.85  # Mínima confianza para usar aprendizaje sin Perplexity


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def limpiar_nombre(nombre: str) -> str:
    """Limpia y normaliza nombres de productos"""
    import unicodedata

    if not nombre:
        return nombre

    # Convertir a mayúsculas
    nombre = nombre.upper()

    # Quitar tildes
    nombre = ''.join(
        c for c in unicodedata.normalize('NFD', nombre)
        if unicodedata.category(c) != 'Mn'
    )

    # Limpiar espacios múltiples
    nombre = ' '.join(nombre.split())

    return nombre


def obtener_establecimiento_real(cursor, producto_id: int, tabla: str) -> str:
    """
    Obtiene el establecimiento más común donde se compró el producto
    desde items_facturas (datos reales)
    """
    try:
        # Determinar columna según tabla
        columna_producto = "producto_maestro_id" if tabla == "productos_maestros" else "producto_maestro_v2_id"

        query = f"""
            SELECT e.nombre_normalizado, COUNT(*) as veces
            FROM items_facturas i
            JOIN facturas f ON i.factura_id = f.id
            JOIN establecimientos e ON f.establecimiento_id = e.id
            WHERE i.{columna_producto} = ?
            GROUP BY e.nombre_normalizado
            ORDER BY veces DESC
            LIMIT 1
        """

        cursor.execute(query, (producto_id,))
        resultado = cursor.fetchone()

        if resultado:
            return resultado[0]

        # Fallback: establecimiento por defecto
        return "EXITO"

    except Exception as e:
        print(f"      ⚠️ Error obteniendo establecimiento: {e}")
        return "EXITO"


def necesita_normalizacion(nombre: str) -> bool:
    """
    Determina si un nombre necesita normalización

    Criterios:
    - Está vacío
    - Tiene minúsculas
    - Tiene caracteres especiales raros
    - Es muy corto (< 3 caracteres)
    - Tiene números sospechosos al inicio
    """
    if not nombre or len(nombre.strip()) < 3:
        return True

    # Si tiene minúsculas, necesita normalización
    if nombre != nombre.upper():
        return True

    # Si empieza con número (probable error OCR)
    if nombre[0].isdigit():
        return True

    # Si tiene más de 3 espacios seguidos
    if '   ' in nombre:
        return True

    return False


def obtener_productos_tabla(cursor, tabla: str, limit=None):
    """
    Obtiene productos de una tabla específica que necesitan normalización
    """

    if FORZAR_RENORMALIZACION:
        # Normalizar TODOS los productos
        condicion = "nombre_normalizado IS NOT NULL AND nombre_normalizado != ''"
    else:
        # Solo productos sin normalizar o con nombres sospechosos
        condicion = """
            (nombre_normalizado IS NULL
             OR nombre_normalizado = ''
             OR nombre_normalizado != UPPER(nombre_normalizado)
             OR LENGTH(nombre_normalizado) < 3)
        """

    query = f"""
        SELECT
            id,
            codigo_ean,
            nombre_normalizado,
            precio_promedio_global
        FROM {tabla}
        WHERE {condicion}
        ORDER BY id ASC
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    return cursor.fetchall()


def es_compatible_postgres(conn) -> bool:
    """Detecta si la BD es PostgreSQL o SQLite"""
    try:
        # PostgreSQL tiene pg_catalog
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM pg_catalog.pg_tables LIMIT 1")
        return True
    except:
        return False


# ==============================================================================
# PROCESO DE NORMALIZACIÓN
# ==============================================================================

def normalizar_producto(cursor, conn, aprendizaje_mgr, producto, tabla: str, stats: dict):
    """
    Normaliza UN producto siguiendo el flujo de 4 capas

    Returns:
        dict con resultado de la normalización
    """

    producto_id, codigo_ean, nombre_actual, precio = producto

    print(f"\n   📦 Producto ID {producto_id} ({tabla})")
    print(f"      📝 Nombre actual: {nombre_actual or '(vacío)'}")
    print(f"      💰 Precio: ${precio:,}" if precio else "      💰 Precio: N/A")

    resultado = {
        'id': producto_id,
        'tabla': tabla,
        'nombre_viejo': nombre_actual,
        'nombre_nuevo': None,
        'fuente': None,
        'precio': precio,
        'establecimiento': None,
        'error': None
    }

    try:
        # 1️⃣ Limpiar nombre actual (Capa Python)
        nombre_limpio = limpiar_nombre(nombre_actual) if nombre_actual else ""

        if nombre_limpio:
            print(f"      🧹 Limpiado Python: {nombre_limpio}")

        # 2️⃣ Obtener establecimiento real
        establecimiento = obtener_establecimiento_real(cursor, producto_id, tabla)
        resultado['establecimiento'] = establecimiento
        print(f"      🏪 Establecimiento: {establecimiento}")

        # 3️⃣ Buscar en aprendizaje previo
        if nombre_limpio:
            correccion_aprendida = aprendizaje_mgr.buscar_correccion_aprendida(
                ocr_normalizado=nombre_limpio,
                establecimiento=establecimiento
            )

            if correccion_aprendida and correccion_aprendida['confianza'] >= MIN_CONFIANZA_APRENDIZAJE:
                # ✅ ENCONTRADO EN APRENDIZAJE - NO llamar Perplexity
                nombre_final = correccion_aprendida['nombre_validado']
                fuente = 'aprendizaje'

                print(f"      ✅ APRENDIZAJE (confianza {correccion_aprendida['confianza']:.2f}): {nombre_final}")
                print(f"      💰 AHORRO: $0.005 + 2 segundos")

                stats['ahorrados_aprendizaje'] += 1

                resultado['nombre_nuevo'] = nombre_final
                resultado['fuente'] = fuente

                return resultado

        # 4️⃣ Validar con Perplexity (no encontrado en aprendizaje o confianza baja)
        print(f"      🔍 Validando con Perplexity...")

        validacion = validar_nombre_producto(
            nombre_ocr=nombre_actual or "PRODUCTO",
            precio=int(precio) if precio else 0,
            supermercado=establecimiento,
            codigo=codigo_ean if codigo_ean else ""
        )

        nombre_final = validacion['nombre_validado']
        fuente = validacion['fuente']

        print(f"      ✅ Perplexity: {nombre_final}")
        print(f"      📊 Fuente: {fuente}")

        stats['validados_perplexity'] += 1

        # 5️⃣ Guardar en aprendizaje para futuras consultas
        if fuente == 'perplexity' and nombre_limpio:
            aprendizaje_mgr.guardar_correccion_aprendida(
                ocr_original=nombre_actual or "",
                ocr_normalizado=nombre_limpio,
                nombre_validado=nombre_final,
                establecimiento=establecimiento,
                confianza_inicial=0.90  # Alta confianza inicial de Perplexity
            )
            print(f"      💾 Guardado en aprendizaje")

        resultado['nombre_nuevo'] = nombre_final
        resultado['fuente'] = fuente

        # Pequeño delay para no saturar API
        time.sleep(DELAY_BETWEEN_REQUESTS)

        return resultado

    except Exception as e:
        print(f"      ❌ Error: {e}")
        resultado['error'] = str(e)
        stats['errores'] += 1
        return resultado


def actualizar_producto(cursor, conn, producto_id: int, nombre_nuevo: str, tabla: str, es_postgres: bool):
    """
    Actualiza el nombre normalizado en la BD
    Compatible con SQLite y PostgreSQL
    """

    if es_postgres:
        query = f"UPDATE {tabla} SET nombre_normalizado = %s WHERE id = %s"
    else:
        query = f"UPDATE {tabla} SET nombre_normalizado = ? WHERE id = ?"

    cursor.execute(query, (nombre_nuevo, producto_id))
    conn.commit()


# ==============================================================================
# PROCESO PRINCIPAL
# ==============================================================================

def normalizar_productos():
    """
    Proceso principal de normalización masiva V2.0
    """

    print("=" * 80)
    print("🔄 NORMALIZACIÓN MASIVA DE PRODUCTOS V2.0 - CON APRENDIZAJE")
    print("=" * 80)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 Modo: {'DRY RUN (solo mostrar)' if DRY_RUN else 'PRODUCCIÓN (guardar cambios)'}")
    print(f"📦 Batch size: {BATCH_SIZE} productos por lote")
    print(f"⏱️  Delay entre lotes: {DELAY_BETWEEN_BATCHES}s")
    print(f"⏱️  Delay entre requests: {DELAY_BETWEEN_REQUESTS}s")
    print(f"🎯 Confianza mínima aprendizaje: {MIN_CONFIANZA_APRENDIZAJE}")

    if MAX_PRODUCTOS:
        print(f"⚠️  LIMITADO A: {MAX_PRODUCTOS} productos")

    if FORZAR_RENORMALIZACION:
        print(f"⚠️  FORZAR RENORMALIZACIÓN: Todos los productos")

    print("\n📋 Tablas a normalizar:")
    if NORMALIZAR_PRODUCTOS_MAESTROS:
        print("   ✅ productos_maestros")
    if NORMALIZAR_PRODUCTOS_MAESTROS_V2:
        print("   ✅ productos_maestros_v2")

    print("=" * 80)

    # Confirmar si no es dry run
    if not DRY_RUN:
        respuesta = input("\n⚠️  ¿CONFIRMAS que quieres ACTUALIZAR la base de datos? (escribe 'SI'): ")
        if respuesta != "SI":
            print("❌ Cancelado por el usuario")
            return

    # Conectar a BD
    print("\n📡 Conectando a base de datos...")
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    es_postgres = es_compatible_postgres(conn)
    print(f"✅ Conectado a: {'PostgreSQL' if es_postgres else 'SQLite'}")

    # Inicializar AprendizajeManager
    print("🧠 Inicializando sistema de aprendizaje...")
    aprendizaje_mgr = AprendizajeManager(cursor, conn)
    print("✅ Sistema de aprendizaje listo")

    # Estadísticas globales
    stats = {
        'total_procesados': 0,
        'total_actualizados': 0,
        'validados_perplexity': 0,
        'ahorrados_aprendizaje': 0,
        'sin_cambios': 0,
        'errores': 0
    }

    # Reporte de cambios
    reporte = []

    # Procesar cada tabla
    tablas_a_procesar = []
    if NORMALIZAR_PRODUCTOS_MAESTROS:
        tablas_a_procesar.append('productos_maestros')
    if NORMALIZAR_PRODUCTOS_MAESTROS_V2:
        tablas_a_procesar.append('productos_maestros_v2')

    for tabla in tablas_a_procesar:
        print(f"\n{'=' * 80}")
        print(f"📋 PROCESANDO TABLA: {tabla}")
        print(f"{'=' * 80}")

        # Obtener productos de esta tabla
        productos = obtener_productos_tabla(cursor, tabla, MAX_PRODUCTOS)
        total_tabla = len(productos)

        print(f"✅ {total_tabla} productos encontrados en {tabla}")

        if total_tabla == 0:
            print(f"ℹ️  No hay productos que normalizar en {tabla}")
            continue

        # Procesar en lotes
        for i in range(0, total_tabla, BATCH_SIZE):
            lote = productos[i:i + BATCH_SIZE]
            lote_num = (i // BATCH_SIZE) + 1
            total_lotes = (total_tabla + BATCH_SIZE - 1) // BATCH_SIZE

            print(f"\n{'─' * 80}")
            print(f"📦 LOTE {lote_num}/{total_lotes} de {tabla} ({len(lote)} productos)")
            print(f"{'─' * 80}")

            for producto in lote:
                # Normalizar producto
                resultado = normalizar_producto(cursor, conn, aprendizaje_mgr, producto, tabla, stats)

                stats['total_procesados'] += 1

                # Si hubo cambio
                if resultado['nombre_nuevo'] and resultado['nombre_nuevo'] != resultado['nombre_viejo']:

                    # Actualizar en BD si no es dry run
                    if not DRY_RUN:
                        try:
                            actualizar_producto(
                                cursor, conn,
                                resultado['id'],
                                resultado['nombre_nuevo'],
                                tabla,
                                es_postgres
                            )
                            print(f"      ✅ Actualizado en BD")
                            stats['total_actualizados'] += 1
                        except Exception as e:
                            print(f"      ❌ Error actualizando: {e}")
                            stats['errores'] += 1
                    else:
                        print(f"      ⚠️  DRY RUN - NO se guardó")

                    # Agregar a reporte
                    reporte.append(resultado)

                else:
                    stats['sin_cambios'] += 1

            # Delay entre lotes (excepto el último)
            if i + BATCH_SIZE < total_tabla:
                print(f"\n⏱️  Esperando {DELAY_BETWEEN_BATCHES}s antes del siguiente lote...")
                time.sleep(DELAY_BETWEEN_BATCHES)

    # Cerrar conexión
    cursor.close()
    conn.close()

    # Reporte final
    print("\n" + "=" * 80)
    print("📊 REPORTE FINAL DE NORMALIZACIÓN V2.0")
    print("=" * 80)
    print(f"Total procesados:          {stats['total_procesados']}")
    print(f"✅ Actualizados:            {stats['total_actualizados']}")
    print(f"🔍 Validados con Perplexity: {stats['validados_perplexity']}")
    print(f"🧠 Ahorrados (aprendizaje):  {stats['ahorrados_aprendizaje']}")
    print(f"ℹ️  Sin cambios:             {stats['sin_cambios']}")
    print(f"❌ Errores:                 {stats['errores']}")

    # Calcular ahorros
    costo_perplexity = 0.005
    ahorro_dinero = stats['ahorrados_aprendizaje'] * costo_perplexity
    ahorro_tiempo = stats['ahorrados_aprendizaje'] * 2  # 2 segundos por request

    print(f"\n💰 AHORROS CON APRENDIZAJE:")
    print(f"   Dinero:  ${ahorro_dinero:.3f}")
    print(f"   Tiempo:  {ahorro_tiempo}s ({ahorro_tiempo/60:.1f} minutos)")

    tasa_exito = (stats['total_procesados'] - stats['errores']) / stats['total_procesados'] * 100 if stats['total_procesados'] > 0 else 0
    print(f"\n📈 Tasa de éxito:          {tasa_exito:.1f}%")

    # Guardar reporte detallado
    if reporte and not DRY_RUN:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archivo_reporte = f"reporte_normalizacion_v2_{timestamp}.txt"

        with open(archivo_reporte, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("REPORTE DE NORMALIZACIÓN DE PRODUCTOS V2.0\n")
            f.write("=" * 80 + "\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total cambios: {len(reporte)}\n")
            f.write(f"Validados con Perplexity: {stats['validados_perplexity']}\n")
            f.write(f"Ahorrados con aprendizaje: {stats['ahorrados_aprendizaje']}\n")
            f.write(f"Ahorro total: ${ahorro_dinero:.3f}\n")
            f.write("=" * 80 + "\n\n")

            for cambio in reporte:
                f.write(f"{'─' * 80}\n")
                f.write(f"Tabla: {cambio['tabla']}\n")
                f.write(f"ID: {cambio['id']}\n")
                f.write(f"ANTES:  {cambio['nombre_viejo'] or '(vacío)'}\n")
                f.write(f"DESPUÉS: {cambio['nombre_nuevo']}\n")
                f.write(f"Precio: ${cambio['precio']:,}\n" if cambio['precio'] else "Precio: N/A\n")
                f.write(f"Establecimiento: {cambio['establecimiento']}\n")
                f.write(f"Fuente: {cambio['fuente']}\n")
                if cambio['error']:
                    f.write(f"Error: {cambio['error']}\n")
                f.write("\n")

        print(f"\n📄 Reporte guardado en: {archivo_reporte}")

    print("=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)


# ==============================================================================
# ANÁLISIS PREVIO
# ==============================================================================

def analizar_productos():
    """
    Analiza cuántos productos necesitan normalización sin procesarlos
    """

    print("=" * 80)
    print("📊 ANÁLISIS DE PRODUCTOS A NORMALIZAR")
    print("=" * 80)

    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()

    tablas = ['productos_maestros', 'productos_maestros_v2']

    for tabla in tablas:
        print(f"\n📋 Tabla: {tabla}")
        print("─" * 80)

        # Total productos
        cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
        total = cursor.fetchone()[0]
        print(f"Total productos: {total}")

        # Productos con nombre
        cursor.execute(f"SELECT COUNT(*) FROM {tabla} WHERE nombre_normalizado IS NOT NULL AND nombre_normalizado != ''")
        con_nombre = cursor.fetchone()[0]
        print(f"Con nombre: {con_nombre}")

        # Productos sin nombre o vacío
        sin_nombre = total - con_nombre
        print(f"Sin nombre: {sin_nombre}")

        # Productos con minúsculas (necesitan normalización)
        cursor.execute(f"""
            SELECT COUNT(*) FROM {tabla}
            WHERE nombre_normalizado IS NOT NULL
            AND nombre_normalizado != UPPER(nombre_normalizado)
        """)
        con_minusculas = cursor.fetchone()[0]
        print(f"Con minúsculas: {con_minusculas}")

        # Productos muy cortos
        cursor.execute(f"""
            SELECT COUNT(*) FROM {tabla}
            WHERE nombre_normalizado IS NOT NULL
            AND LENGTH(nombre_normalizado) < 3
        """)
        muy_cortos = cursor.fetchone()[0]
        print(f"Muy cortos (<3 chars): {muy_cortos}")

        # Total que necesitan normalización
        necesitan = sin_nombre + con_minusculas + muy_cortos
        print(f"\n⚠️  NECESITAN NORMALIZACIÓN: {necesitan}")

        # Estimación de costo
        costo_estimado = necesitan * 0.005
        tiempo_estimado = necesitan * 2 / 60  # minutos

        print(f"💰 Costo estimado: ${costo_estimado:.2f}")
        print(f"⏱️  Tiempo estimado: {tiempo_estimado:.1f} minutos")

    cursor.close()
    conn.close()

    print("\n" + "=" * 80)


# ==============================================================================
# PRUEBA PEQUEÑA
# ==============================================================================

def prueba_pequeña():
    """
    Ejecuta una prueba con pocos productos
    """
    global MAX_PRODUCTOS, DRY_RUN, NORMALIZAR_PRODUCTOS_MAESTROS, NORMALIZAR_PRODUCTOS_MAESTROS_V2

    MAX_PRODUCTOS = 5
    DRY_RUN = True
    NORMALIZAR_PRODUCTOS_MAESTROS = True
    NORMALIZAR_PRODUCTOS_MAESTROS_V2 = True

    print("🧪 MODO PRUEBA: 5 productos por tabla, DRY RUN")
    normalizar_productos()


# ==============================================================================
# EJECUCIÓN
# ==============================================================================

if __name__ == "__main__":
    if len(sys.argv) > 1:
        comando = sys.argv[1]

        if comando == "--analizar":
            analizar_productos()

        elif comando == "--prueba":
            prueba_pequeña()

        elif comando == "--produccion":
            DRY_RUN = False
            normalizar_productos()

        elif comando == "--solo-maestros":
            NORMALIZAR_PRODUCTOS_MAESTROS = True
            NORMALIZAR_PRODUCTOS_MAESTROS_V2 = False
            DRY_RUN = False
            normalizar_productos()

        elif comando == "--solo-v2":
            NORMALIZAR_PRODUCTOS_MAESTROS = False
            NORMALIZAR_PRODUCTOS_MAESTROS_V2 = True
            DRY_RUN = False
            normalizar_productos()

        else:
            print("Uso:")
            print("  python normalizar_productos_v2.py --analizar        # Ver cuántos productos necesitan normalización")
            print("  python normalizar_productos_v2.py --prueba          # Prueba con 5 productos por tabla (DRY RUN)")
            print("  python normalizar_productos_v2.py --produccion      # Ejecutar en AMBAS tablas (REAL)")
            print("  python normalizar_productos_v2.py --solo-maestros   # Solo productos_maestros")
            print("  python normalizar_productos_v2.py --solo-v2         # Solo productos_maestros_v2")
    else:
        # Por defecto, mostrar análisis
        analizar_productos()
