"""
============================================================================
SISTEMA DE PROCESAMIENTO AUTOMÁTICO DE OCR PARA FACTURAS
VERSIÓN 3.0 - INTEGRACIÓN COMPLETA CON PRODUCTRESOLVER
============================================================================

MEJORAS EN ESTA VERSIÓN:
✅ Integración con ProductResolver (sistema de productos canónicos)
✅ Normalización inteligente de códigos por establecimiento
✅ Detección automática de duplicados
✅ Validación robusta de precios colombianos
✅ Manejo de múltiples tipos de códigos (EAN, PLU, internos)
✅ Actualización automática de inventario
✅ Sistema de auditoría completo

ARQUITECTURA:
- productos_canonicos: Verdad única del producto
- productos_variantes: Alias por establecimiento
- productos_maestros: Legacy (compatibilidad)
- items_factura: Items con referencias a todos los sistemas

AUTOR: LecFac Team
ÚLTIMA ACTUALIZACIÓN: 2025-11-04
============================================================================
"""

import threading
from queue import Queue
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional, Tuple
import traceback

# Importar funciones de base de datos
from database import (
    get_db_connection,
    detectar_cadena,
    actualizar_inventario_desde_factura,
    procesar_items_factura_y_guardar_precios
)
from claude_invoice import parse_invoice_with_claude

# ✅ Importar normalizador de códigos
from normalizador_codigos import normalizar_codigo_por_establecimiento

# ✅ Importar ProductResolver (sistema canónico)
try:
    from product_resolver import ProductResolver
    PRODUCT_RESOLVER_AVAILABLE = True
    print("✅ ProductResolver cargado correctamente")
except ImportError as e:
    PRODUCT_RESOLVER_AVAILABLE = False
    print(f"❌ ProductResolver no disponible: {e}")
    print("   El sistema NO funcionará correctamente sin ProductResolver")

# ✅ Importar detector de duplicados
try:
    from duplicate_detector import detectar_duplicados_automaticamente
    DUPLICATE_DETECTOR_AVAILABLE = True
    print("✅ Detector de duplicados cargado")
except ImportError:
    DUPLICATE_DETECTOR_AVAILABLE = False
    print("⚠️ Detector de duplicados no disponible")

# Colas y tracking globales
ocr_queue = Queue()
processing = {}
error_log = []


# ==============================================================================
# FUNCIÓN PARA LIMPIAR PRECIOS COLOMBIANOS
# ==============================================================================

def limpiar_precio_colombiano(precio_str) -> int:
    """
    Convierte precio colombiano a entero (sin decimales).

    En Colombia NO se usan decimales/centavos, solo pesos enteros.

    Casos manejados:
    - None, vacío → 0
    - Enteros → sin cambios
    - Floats → convertir a entero
    - Strings con separadores de miles (., ,) → limpiar y convertir
    - Strings con símbolos ($, COP) → limpiar y convertir

    Args:
        precio_str: Precio en cualquier formato

    Returns:
        int: Precio en pesos colombianos enteros

    Examples:
        >>> limpiar_precio_colombiano("$1.500")
        1500
        >>> limpiar_precio_colombiano("12,350")
        12350
        >>> limpiar_precio_colombiano(2500.0)
        2500
    """
    # Caso 1: None o vacío
    if precio_str is None or precio_str == "":
        return 0

    # Caso 2: Ya es un entero
    if isinstance(precio_str, int):
        return max(0, precio_str)  # No permitir negativos

    # Caso 3: Es un float
    if isinstance(precio_str, float):
        # Si es un float "limpio" como 2500.0
        if precio_str == int(precio_str):
            return max(0, int(precio_str))
        # Si tiene decimales significativos, podría ser error OCR
        # Ejemplo: 25.50 probablemente es $2,550
        return max(0, int(precio_str * 100))

    # Caso 4: Es string - procesar
    precio_str = str(precio_str).strip()

    # Eliminar espacios y símbolos de moneda
    precio_str = precio_str.replace(" ", "")
    precio_str = precio_str.replace("$", "")
    precio_str = precio_str.replace("COP", "")
    precio_str = precio_str.replace("cop", "")
    precio_str = precio_str.strip()

    # Caso 4A: Tiene múltiples puntos o comas (separador de miles)
    # Ejemplo: "1.500.000" o "1,500,000"
    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        precio_str = precio_str.replace(",", "").replace(".", "")

    # Caso 4B: Tiene un solo punto o coma
    elif '.' in precio_str or ',' in precio_str:
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        # Si hay 3 dígitos después, es separador de miles
        # Ejemplo: "1.500" = $1,500
        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        # Si hay 1-2 dígitos, en Colombia igual es separador de miles mal escrito
        # Ejemplo: "1.5" probablemente es $1,500 (OCR error)
        elif len(partes) == 2 and len(partes[1]) <= 2:
            precio_str = precio_str.replace(",", "").replace(".", "")
        else:
            precio_str = precio_str.replace(",", "").replace(".", "")

    # Convertir a entero
    try:
        precio = int(float(precio_str))

        if precio < 0:
            print(f"   ⚠️ Precio negativo detectado: {precio}, retornando 0")
            return 0

        return precio

    except (ValueError, TypeError) as e:
        print(f"   ⚠️ No se pudo convertir precio '{precio_str}': {e}")
        return 0


# ==============================================================================
# VALIDACIONES DE PRODUCTOS
# ==============================================================================

def validar_producto(nombre: str, precio: int, codigo: str = "") -> Tuple[bool, Optional[str]]:
    """
    Valida que un producto cumpla con los requisitos mínimos

    Args:
        nombre: Nombre del producto
        precio: Precio en pesos
        codigo: Código del producto (opcional)

    Returns:
        Tuple[es_valido, razon_rechazo]

    Examples:
        >>> validar_producto("Leche Colanta", 4500, "7702129001234")
        (True, None)
        >>> validar_producto("", 1000, "123")
        (False, "Producto sin nombre")
    """
    # Validar nombre
    if not nombre or nombre.strip() == "":
        return False, "Producto sin nombre"

    if len(nombre.strip()) < 2:
        return False, f"Nombre muy corto: '{nombre}'"

    # Validar precio
    if precio <= 0:
        return False, f"Precio inválido: ${precio:,}"

    if precio < 10:
        return False, f"Precio muy bajo: ${precio:,} (posible error OCR)"

    if precio > 10_000_000:
        return False, f"Precio sospechosamente alto: ${precio:,} (verificar)"

    # Validaciones adicionales
    nombre_lower = nombre.lower()

    # Detectar textos basura comunes del OCR
    palabras_basura = ['total', 'subtotal', 'iva', 'descuento', 'cambio', 'efectivo']
    if any(palabra in nombre_lower for palabra in palabras_basura):
        return False, f"Nombre parece ser campo de totales: '{nombre}'"

    return True, None


# ==============================================================================
# CLASE OCPROCESSOR - VERSIÓN 3.0 CON PRODUCTRESOLVER
# ==============================================================================

class OCRProcessor:
    """
    Procesador automático de facturas con OCR - Versión 3.0

    Características:
    - Procesamiento asíncrono con cola
    - Integración con ProductResolver
    - Detección de duplicados
    - Validación robusta
    - Sistema de auditoría
    - Actualización automática de inventario
    """

    def __init__(self):
        """Inicializa el procesador"""
        self.is_running = False
        self.processed_count = 0
        self.error_count = 0
        self.success_rate = 100.0
        self.last_processed = None
        self.worker_thread = None

        # Validar que ProductResolver esté disponible
        if not PRODUCT_RESOLVER_AVAILABLE:
            print("❌ ADVERTENCIA: ProductResolver no está disponible")
            print("   El sistema NO funcionará correctamente")

    def start(self):
        """Inicia el procesador en background"""
        if self.is_running:
            print("⚠️ Procesador ya está en ejecución")
            return

        if not PRODUCT_RESOLVER_AVAILABLE:
            print("❌ No se puede iniciar: ProductResolver no disponible")
            return

        self.is_running = True
        self.worker_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.worker_thread.start()

        print("=" * 80)
        print("🤖 PROCESADOR OCR AUTOMÁTICO INICIADO")
        print("=" * 80)
        print("✅ VERSIÓN 3.0 - ProductResolver integrado")
        print("✅ Normalización inteligente de códigos")
        print("✅ Detección automática de duplicados")
        print("✅ Validación robusta de productos")
        print("✅ Actualización automática de inventario")
        print("🏪 Soporta: ARA, D1, Éxito, Jumbo, Olímpica y más")
        print("=" * 80)

    def stop(self):
        """Detiene el procesador"""
        self.is_running = False
        print("🛑 Deteniendo procesador OCR...")

    def process_queue(self):
        """Procesa facturas continuamente de la cola"""
        while self.is_running:
            try:
                if not ocr_queue.empty():
                    task = ocr_queue.get(timeout=1)
                    self.process_invoice(task)

                    # Actualizar tasa de éxito
                    if self.processed_count + self.error_count > 0:
                        self.success_rate = (self.processed_count /
                                           (self.processed_count + self.error_count)) * 100

                    time.sleep(1)
                else:
                    time.sleep(5)

            except Exception as e:
                print(f"❌ Error en procesador: {e}")
                error_log.append({
                    'timestamp': datetime.now(),
                    'error': str(e),
                    'traceback': traceback.format_exc()
                })
                time.sleep(5)

    def process_invoice(self, task: Dict[str, Any]):
        """
        Procesa una factura individual

        Args:
            task: Diccionario con:
                - factura_id: ID de la factura
                - image_path: Ruta a la imagen
                - user_id: ID del usuario (default: 1)
        """
        factura_id = task.get('factura_id')
        image_path = task.get('image_path')
        user_id = task.get('user_id', 1)

        if not factura_id or not image_path:
            print(f"❌ Task inválido: {task}")
            return

        try:
            print(f"\n{'='*80}")
            print(f"🔄 PROCESANDO FACTURA #{factura_id}")
            print(f"{'='*80}")

            processing[factura_id] = {
                'status': 'processing',
                'started_at': datetime.now()
            }

            # Verificar que existe la imagen
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Imagen no encontrada: {image_path}")

            # Procesar con Claude OCR
            print("📸 Extrayendo datos con Claude Vision...")
            result = parse_invoice_with_claude(image_path)

            # Conectar a base de datos
            conn = get_db_connection()
            if not conn:
                raise Exception("No se pudo conectar a la base de datos")

            cursor = conn.cursor()

            if result.get("success"):
                # Procesar resultado exitoso
                self._process_successful_ocr(
                    cursor, conn, factura_id, result["data"], user_id
                )
                conn.commit()

                # Actualizar inventario del usuario
                print(f"\n📦 Actualizando inventario del usuario {user_id}...")
                try:
                    actualizar_inventario_desde_factura(factura_id, user_id)
                    print(f"   ✅ Inventario actualizado")
                except Exception as e:
                    print(f"   ⚠️ Error actualizando inventario: {e}")

                # Guardar precios en tabla de precios
                print(f"\n💰 Guardando precios históricos...")
                try:
                    resultado_precios = procesar_items_factura_y_guardar_precios(factura_id, user_id)
                    if resultado_precios.get('precios_guardados', 0) > 0:
                        print(f"   ✅ {resultado_precios['precios_guardados']} precios guardados")
                except Exception as e:
                    print(f"   ⚠️ Error guardando precios: {e}")

                self.processed_count += 1
                self.last_processed = datetime.now()

                processing[factura_id] = {
                    'status': 'completed',
                    'completed_at': datetime.now()
                }

                print(f"\n✅ FACTURA #{factura_id} PROCESADA EXITOSAMENTE")
                print(f"{'='*80}\n")

            else:
                # Procesar fallo de OCR
                self._process_failed_ocr(
                    cursor, factura_id, result.get("error", "Error desconocido")
                )
                conn.commit()

                self.error_count += 1

                processing[factura_id] = {
                    'status': 'error',
                    'error': result.get("error"),
                    'failed_at': datetime.now()
                }

                print(f"⚠️ Error OCR en factura #{factura_id}: {result.get('error')}")

            conn.close()

        except Exception as e:
            print(f"❌ Error procesando factura {factura_id}: {e}")
            traceback.print_exc()

            self.error_count += 1
            processing[factura_id] = {
                'status': 'error',
                'error': str(e),
                'failed_at': datetime.now()
            }

    def _process_successful_ocr(self, cursor, conn, factura_id: int, data: Dict, user_id: int):
        """
        Procesa un resultado exitoso de OCR

        Args:
            cursor: Cursor de base de datos
            conn: Conexión a base de datos
            factura_id: ID de la factura
            data: Datos extraídos por OCR
            user_id: ID del usuario
        """

        establecimiento = data.get("establecimiento", "Desconocido")
        cadena = detectar_cadena(establecimiento)
        total_factura = limpiar_precio_colombiano(data.get("total", 0))

        print(f"🏪 Establecimiento: {establecimiento} (Cadena: {cadena})")
        print(f"💵 Total factura: ${total_factura:,}")

        # ========================================
        # PASO 1: DETECCIÓN DE DUPLICADOS
        # ========================================
        productos_originales = data.get("productos", [])

        print(f"\n{'='*70}")
        print(f"🧹 LIMPIEZA DE DUPLICADOS")
        print(f"{'='*70}")

        if DUPLICATE_DETECTOR_AVAILABLE and len(productos_originales) > 0:
            # Convertir productos al formato esperado
            productos_para_detector = []
            for prod in productos_originales:
                productos_para_detector.append({
                    "codigo": prod.get("codigo", ""),
                    "nombre": prod.get("nombre", ""),
                    "valor": limpiar_precio_colombiano(prod.get("precio", 0)),
                    "cantidad": prod.get("cantidad", 1)
                })

            # Ejecutar detección de duplicados
            resultado_limpieza = detectar_duplicados_automaticamente(
                productos=productos_para_detector,
                total_factura=total_factura,
                umbral_similitud=0.85,
                tolerancia_total=0.15
            )

            # Mostrar resultado
            if resultado_limpieza["duplicados_detectados"]:
                print(f"📊 Productos originales: {len(productos_originales)}")
                print(f"✅ Productos limpios: {len(resultado_limpieza['productos_limpios'])}")
                print(f"🗑️ Duplicados eliminados: {len(resultado_limpieza['productos_eliminados'])}")

                for prod_eliminado in resultado_limpieza["productos_eliminados"]:
                    print(f"   ❌ {prod_eliminado['nombre'][:40]} (${prod_eliminado['valor']:,})")
                    print(f"      Razón: {prod_eliminado['razon']}")
            else:
                print(f"✅ No se detectaron duplicados ({len(productos_originales)} productos)")

            # Convertir productos limpios de vuelta al formato original
            productos_a_procesar = []
            for prod_limpio in resultado_limpieza["productos_limpios"]:
                productos_a_procesar.append({
                    "codigo": prod_limpio.get("codigo", ""),
                    "nombre": prod_limpio.get("nombre", ""),
                    "precio": prod_limpio.get("valor", 0),
                    "cantidad": prod_limpio.get("cantidad", 1)
                })
        else:
            # Sin detector de duplicados, usar todos los productos
            productos_a_procesar = productos_originales
            print(f"⚠️ Detector de duplicados no disponible")
            print(f"📦 Procesando {len(productos_originales)} productos sin filtrar")

        print(f"{'='*70}\n")

        # ========================================
        # PASO 2: PROCESAMIENTO DE PRODUCTOS
        # ========================================
        print(f"{'='*70}")
        print(f"📦 PROCESAMIENTO DE PRODUCTOS")
        print(f"{'='*70}\n")

        productos_guardados = 0
        productos_rechazados = 0
        errores_detalle = []

        for idx, product in enumerate(productos_a_procesar, 1):
            print(f"[{idx}/{len(productos_a_procesar)}] Procesando: {product.get('nombre', 'SIN NOMBRE')[:50]}")

            item_id = self._save_product_to_items_factura(
                cursor, conn, product, factura_id, user_id, establecimiento, cadena
            )

            if item_id:
                productos_guardados += 1
            else:
                productos_rechazados += 1
                errores_detalle.append({
                    'nombre': product.get('nombre', 'SIN NOMBRE'),
                    'codigo': product.get('codigo', ''),
                    'precio': product.get('precio', 0)
                })

        # Mostrar resumen
        print(f"\n{'='*70}")
        print(f"📊 RESUMEN DE PROCESAMIENTO")
        print(f"{'='*70}")
        print(f"✅ Productos guardados: {productos_guardados}")
        print(f"❌ Productos rechazados: {productos_rechazados}")

        if productos_rechazados > 0 and errores_detalle:
            print(f"\n❌ Productos rechazados:")
            for error in errores_detalle[:5]:  # Mostrar máximo 5
                print(f"   • {error['nombre'][:40]} (${error['precio']:,})")

        print(f"{'='*70}\n")

        # Actualizar factura con estadísticas
        cursor.execute("""
            UPDATE facturas
            SET productos_detectados = %s,
                productos_guardados = %s,
                estado_validacion = 'procesado',
                fecha_procesamiento = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (len(productos_a_procesar), productos_guardados, factura_id))

    def _save_product_to_items_factura(
        self,
        cursor,
        conn,
        product: Dict,
        factura_id: int,
        user_id: int,
        establecimiento: str,
        cadena: str
    ) -> Optional[int]:
        """
        ✅ VERSIÓN 3.0 - CON PRODUCTRESOLVER INTEGRADO

        Guarda un producto en items_factura usando:
        1. Normalización de códigos según establecimiento
        2. ProductResolver para gestión de productos canónicos
        3. Validaciones robustas
        4. Sistema de auditoría

        Args:
            cursor: Cursor de base de datos
            conn: Conexión a base de datos
            product: Diccionario con datos del producto
            factura_id: ID de la factura
            user_id: ID del usuario
            establecimiento: Nombre del establecimiento
            cadena: Cadena comercial

        Returns:
            int: ID del item creado, o None si falló
        """
        try:
            # ========================================
            # EXTRACCIÓN Y LIMPIEZA DE DATOS
            # ========================================
            codigo_raw = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()
            precio_raw = product.get("precio", 0)
            precio = limpiar_precio_colombiano(precio_raw)
            cantidad = int(product.get("cantidad", 1))

            # ========================================
            # VALIDACIÓN DEL PRODUCTO
            # ========================================
            es_valido, razon_rechazo = validar_producto(nombre, precio, codigo_raw)

            if not es_valido:
                print(f"   ❌ RECHAZADO: {razon_rechazo}")
                return None

            # ========================================
            # NORMALIZACIÓN DE CÓDIGO
            # ========================================
            codigo, tipo_codigo, confianza = normalizar_codigo_por_establecimiento(
                codigo_raw, establecimiento
            )

            print(f"   💰 ${precio:,} x{cantidad}")
            if codigo_raw != codigo:
                print(f"   📟 Código normalizado: {codigo_raw} → {codigo} ({tipo_codigo})")
            else:
                print(f"   📟 Código: {codigo or 'SIN CÓDIGO'} ({tipo_codigo})")

            # ========================================
            # RESOLVER PRODUCTO CON PRODUCTRESOLVER
            # ========================================
            if not PRODUCT_RESOLVER_AVAILABLE:
                print(f"   ❌ ProductResolver no disponible")
                return None

            resolver = ProductResolver()
            try:
                # Usar código normalizado, o código raw si no hay normalizado
                codigo_final = codigo if codigo else codigo_raw

                canonico_id, variante_id, accion = resolver.resolver_producto(
                    codigo=codigo_final if codigo_final else f"INTERNO_{hash(nombre) % 100000}",
                    nombre=nombre,
                    establecimiento=establecimiento,
                    precio=precio,
                    marca=None,  # TODO: Extraer de Claude en futuras versiones
                    categoria=None  # TODO: Extraer de Claude en futuras versiones
                )

                accion_emoji = {
                    'found_variant': '🔍',
                    'found_canonical': '🆕',
                    'created_new': '✨'
                }.get(accion, '❓')

                print(f"   {accion_emoji} ProductResolver: Canónico={canonico_id}, Variante={variante_id}")

                # Obtener producto_maestro_id desde el canónico
                cursor.execute("""
                    SELECT id FROM productos_maestros
                    WHERE producto_canonico_id = %s
                    LIMIT 1
                """, (canonico_id,))

                result = cursor.fetchone()
                producto_maestro_id = result[0] if result else None

                if not producto_maestro_id:
                    # El ProductResolver debería haber creado el maestro
                    # Hacer commit y reintentar
                    conn.commit()

                    cursor.execute("""
                        SELECT id FROM productos_maestros
                        WHERE producto_canonico_id = %s
                        LIMIT 1
                    """, (canonico_id,))

                    result = cursor.fetchone()
                    producto_maestro_id = result[0] if result else None

                if not producto_maestro_id:
                    print(f"   ⚠️ No se pudo obtener producto_maestro_id para canónico {canonico_id}")
                    return None

            except Exception as e:
                print(f"   ❌ Error en ProductResolver: {e}")
                traceback.print_exc()
                return None
            finally:
                resolver.close()

            # ========================================
            # GUARDAR EN ITEMS_FACTURA
            # ========================================
            cursor.execute("""
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    producto_maestro_id,
                    producto_canonico_id,
                    variante_id,
                    codigo_leido,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    matching_confianza,
                    fecha_creacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                factura_id,
                user_id,
                producto_maestro_id,
                canonico_id,
                variante_id,
                codigo_final if codigo_final else None,
                nombre,
                precio,
                cantidad,
                confianza
            ))

            item_id = cursor.fetchone()[0]
            conn.commit()

            print(f"   ✅ Item guardado (ID: {item_id})")

            return item_id

        except Exception as e:
            print(f"   ❌ Error guardando producto: {e}")
            traceback.print_exc()
            conn.rollback()
            return None

    def _process_failed_ocr(self, cursor, factura_id: int, error: str):
        """
        Procesa un fallo de OCR

        Args:
            cursor: Cursor de base de datos
            factura_id: ID de la factura
            error: Mensaje de error
        """
        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'error_ocr',
                notas = %s,
                fecha_procesamiento = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (f"Error OCR: {error}", factura_id))

    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estadísticas del procesador

        Returns:
            Dict con estadísticas:
            - is_running: Si está activo
            - processed_count: Facturas procesadas
            - error_count: Facturas con error
            - success_rate: Tasa de éxito (%)
            - last_processed: Última factura procesada
            - queue_size: Tamaño de la cola
            - processing_count: Facturas en proceso
            - recent_errors: Últimos errores
        """
        return {
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'success_rate': round(self.success_rate, 2),
            'last_processed': self.last_processed.isoformat() if self.last_processed else None,
            'queue_size': ocr_queue.qsize(),
            'processing_count': len([p for p in processing.values() if p.get('status') == 'processing']),
            'recent_errors': [
                {
                    'timestamp': err['timestamp'].isoformat(),
                    'error': err['error']
                }
                for err in error_log[-10:]
            ] if error_log else []
        }


# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================

print("=" * 80)
print("✅ OCR PROCESSOR V3.0 CARGADO")
print("=" * 80)
print("📟 Normalización inteligente de códigos: ✅")
print("🧹 Detección automática de duplicados: ✅" if DUPLICATE_DETECTOR_AVAILABLE else "🧹 Detección automática de duplicados: ❌")
print("🎯 ProductResolver (sistema canónico): ✅" if PRODUCT_RESOLVER_AVAILABLE else "🎯 ProductResolver (sistema canónico): ❌")
print("💰 Validación robusta de precios: ✅")
print("📦 Actualización automática de inventario: ✅")
print("🏪 Soporta: ARA, D1, Éxito, Jumbo, Olímpica, Carulla, y más")
print("=" * 80)

# Crear instancia global del procesador
processor = OCRProcessor()
