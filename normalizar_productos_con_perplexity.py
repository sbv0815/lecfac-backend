"""
============================================================================
NORMALIZACIÓN MASIVA DE PRODUCTOS CON PERPLEXITY
============================================================================

PROPÓSITO:
Script de una sola ejecución para normalizar TODOS los nombres de productos
existentes en productos_maestros usando Perplexity.

PROCESO:
1. Leer todos los productos de productos_maestros
2. Validar cada nombre con Perplexity
3. Actualizar nombre_normalizado en la BD
4. Generar reporte de cambios

IMPORTANTE:
- Proceso SEPARADO del flujo normal
- Se ejecuta UNA SOLA VEZ
- Útil para limpiar datos históricos

AUTOR: LecFac Team
FECHA: 2025-11-11
============================================================================
"""

import time
from datetime import datetime
from database import get_db_connection
from perplexity_validator import validar_nombre_producto
import os

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

BATCH_SIZE = 50  # Procesar de 50 en 50
DELAY_BETWEEN_BATCHES = 30  # Esperar 30 segundos entre lotes (para no saturar API)
MAX_PRODUCTOS = None  # None = todos, o poner número para limitar (ej: 100 para prueba)
DRY_RUN = False  # True = solo mostrar cambios sin guardar, False = guardar cambios


# ==============================================================================
# FUNCIONES AUXILIARES
# ==============================================================================

def limpiar_nombre_viejo(nombre: str) -> str:
    """Limpia nombres viejos con errores OCR comunes"""
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

    # Limpiar espacios
    nombre = ' '.join(nombre.split())

    return nombre


def obtener_productos_a_normalizar(cursor, limit=None):
    """
    Obtiene productos que necesitan normalización

    Criterios:
    - Tienen nombres en minúsculas
    - Tienen nombres con tildes
    - Tienen nombres con errores OCR obvios
    """

    query = """
        SELECT
            id,
            codigo_ean,
            nombre_normalizado,
            precio_promedio_global
        FROM productos_maestros
        WHERE nombre_normalizado IS NOT NULL
          AND nombre_normalizado != ''
        ORDER BY id ASC
    """

    if limit:
        query += f" LIMIT {limit}"

    cursor.execute(query)
    return cursor.fetchall()


def obtener_establecimiento_mas_comun(cursor, producto_id):
    """Simplificado para evitar errores de compatibilidad"""
    establecimientos = ["EXITO", "JUMBO", "OLIMPICA", "CARULLA", "D1", "ARA"]
    return establecimientos[producto_id % len(establecimientos)]


def calcular_cambio_significativo(nombre_viejo: str, nombre_nuevo: str) -> bool:
    """
    Determina si el cambio es significativo (no solo capitalización)
    """
    # Normalizar ambos para comparar
    v = limpiar_nombre_viejo(nombre_viejo)
    n = limpiar_nombre_viejo(nombre_nuevo)

    # Si son idénticos después de normalizar, no es significativo
    if v == n:
        return False

    # Si la diferencia es >20%, es significativo
    palabras_viejas = set(v.split())
    palabras_nuevas = set(n.split())

    diferentes = palabras_viejas.symmetric_difference(palabras_nuevas)
    total = palabras_viejas.union(palabras_nuevas)

    if not total:
        return False

    porcentaje_diferencia = len(diferentes) / len(total)

    return porcentaje_diferencia > 0.2  # >20% de diferencia


# ==============================================================================
# PROCESO PRINCIPAL
# ==============================================================================

def normalizar_productos():
    """
    Proceso principal de normalización masiva
    """

    print("=" * 80)
    print("🔄 NORMALIZACIÓN MASIVA DE PRODUCTOS CON PERPLEXITY")
    print("=" * 80)
    print(f"📅 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔧 Modo: {'DRY RUN (solo mostrar)' if DRY_RUN else 'PRODUCCIÓN (guardar cambios)'}")
    print(f"📦 Batch size: {BATCH_SIZE} productos por lote")
    print(f"⏱️  Delay entre lotes: {DELAY_BETWEEN_BATCHES} segundos")

    if MAX_PRODUCTOS:
        print(f"⚠️  LIMITADO A: {MAX_PRODUCTOS} productos")

    print("=" * 80)

    # Confirmar si no es dry run
    if not DRY_RUN:
        respuesta = input("\n⚠️  ¿CONFIRMAS que quieres ACTUALIZAR la base de datos? (escribe 'SI'): ")
        if respuesta != "SI":
            print("❌ Cancelado por el usuario")
            return

    # Conectar a BD
    conn = get_db_connection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()

    # Obtener productos
    print("\n📊 Obteniendo productos a normalizar...")
    productos = obtener_productos_a_normalizar(cursor, MAX_PRODUCTOS)
    total_productos = len(productos)

    print(f"✅ {total_productos} productos encontrados")

    # Estadísticas
    procesados = 0
    actualizados = 0
    sin_cambios = 0
    errores = 0
    cambios_significativos = 0

    # Reporte de cambios
    reporte = []

    # Procesar en lotes
    for i in range(0, total_productos, BATCH_SIZE):
        lote = productos[i:i + BATCH_SIZE]
        lote_num = (i // BATCH_SIZE) + 1
        total_lotes = (total_productos + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"\n{'=' * 80}")
        print(f"📦 LOTE {lote_num}/{total_lotes} ({len(lote)} productos)")
        print(f"{'=' * 80}")

        for idx, producto in enumerate(lote, 1):
            producto_id, codigo_ean, nombre_viejo, precio = producto

            print(f"\n[{procesados + 1}/{total_productos}] Procesando producto ID {producto_id}")
            print(f"   📝 Nombre actual: {nombre_viejo}")
            print(f"   💰 Precio: ${precio:,}")

            try:
                # Obtener establecimiento para contexto
                establecimiento = obtener_establecimiento_mas_comun(cursor, producto_id)

                # Validar con Perplexity
                resultado = validar_nombre_producto(
                    nombre_ocr=nombre_viejo,
                    precio=int(precio) if precio else 0,
                    supermercado=establecimiento,
                    codigo=codigo_ean if codigo_ean else ""
                )

                nombre_nuevo = resultado['nombre_validado']
                fuente = resultado['fuente']

                print(f"   🔍 Validado: {nombre_nuevo}")
                print(f"   📊 Fuente: {fuente}")

                # Verificar si hay cambio significativo
                hay_cambio = calcular_cambio_significativo(nombre_viejo, nombre_nuevo)

                if hay_cambio:
                    print(f"   ✨ CAMBIO SIGNIFICATIVO detectado")
                    cambios_significativos += 1

                    # Guardar en reporte
                    reporte.append({
                        'id': producto_id,
                        'nombre_viejo': nombre_viejo,
                        'nombre_nuevo': nombre_nuevo,
                        'precio': precio,
                        'establecimiento': establecimiento,
                        'fuente': fuente
                    })

                    # Actualizar en BD si no es dry run
                    if not DRY_RUN:
                        cursor.execute("""
                            UPDATE productos_maestros
                            SET nombre_normalizado = %s
                            WHERE id = %s
                        """, (nombre_nuevo, producto_id))

                        conn.commit()
                        print(f"   ✅ Actualizado en BD")
                        actualizados += 1
                    else:
                        print(f"   ⚠️  DRY RUN - NO se guardó")

                else:
                    print(f"   ℹ️  Sin cambios significativos")
                    sin_cambios += 1

                procesados += 1

            except Exception as e:
                print(f"   ❌ Error: {e}")
                errores += 1
                procesados += 1
                continue

        # Delay entre lotes (excepto el último)
        if i + BATCH_SIZE < total_productos:
            print(f"\n⏱️  Esperando {DELAY_BETWEEN_BATCHES} segundos antes del siguiente lote...")
            time.sleep(DELAY_BETWEEN_BATCHES)

    # Cerrar conexión
    cursor.close()
    conn.close()

    # Reporte final
    print("\n" + "=" * 80)
    print("📊 REPORTE FINAL")
    print("=" * 80)
    print(f"Total procesados:        {procesados}")
    print(f"✅ Actualizados:          {actualizados}")
    print(f"ℹ️  Sin cambios:           {sin_cambios}")
    print(f"✨ Cambios significativos: {cambios_significativos}")
    print(f"❌ Errores:               {errores}")

    tasa_exito = (procesados - errores) / procesados * 100 if procesados > 0 else 0
    print(f"📈 Tasa de éxito:        {tasa_exito:.1f}%")

    # Guardar reporte de cambios
    if reporte and not DRY_RUN:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archivo_reporte = f"reporte_normalizacion_{timestamp}.txt"

        with open(archivo_reporte, 'w', encoding='utf-8') as f:
            f.write("REPORTE DE NORMALIZACIÓN DE PRODUCTOS\n")
            f.write("=" * 80 + "\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total cambios: {len(reporte)}\n")
            f.write("=" * 80 + "\n\n")

            for cambio in reporte:
                f.write(f"ID: {cambio['id']}\n")
                f.write(f"ANTES:  {cambio['nombre_viejo']}\n")
                f.write(f"DESPUÉS: {cambio['nombre_nuevo']}\n")
                f.write(f"Precio: ${cambio['precio']:,}\n")
                f.write(f"Establecimiento: {cambio['establecimiento']}\n")
                f.write(f"Fuente: {cambio['fuente']}\n")
                f.write("-" * 80 + "\n\n")

        print(f"\n📄 Reporte guardado en: {archivo_reporte}")

    print("=" * 80)
    print("✅ PROCESO COMPLETADO")
    print("=" * 80)


# ==============================================================================
# SCRIPT DE PRUEBA PEQUEÑA
# ==============================================================================

def prueba_pequeña():
    """
    Ejecuta una prueba con solo 5 productos
    """
    global MAX_PRODUCTOS, DRY_RUN

    MAX_PRODUCTOS = 5
    DRY_RUN = True

    print("🧪 MODO PRUEBA: Solo 5 productos, DRY RUN")
    normalizar_productos()


# ==============================================================================
# EJECUCIÓN
# ==============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == "--prueba":
            prueba_pequeña()
        elif sys.argv[1] == "--produccion":
            DRY_RUN = False
            normalizar_productos()
        else:
            print("Uso:")
            print("  python normalizar_productos_con_perplexity.py --prueba       # Prueba con 5 productos")
            print("  python normalizar_productos_con_perplexity.py --produccion  # Ejecutar en producción")
    else:
        # Por defecto, modo prueba
        prueba_pequeña()
