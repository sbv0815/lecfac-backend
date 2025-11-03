"""
Sistema de Procesamiento Automático de OCR para Facturas
VERSIÓN 2.2 - CON DETECCIÓN DE DUPLICADOS AUTOMÁTICA
Maneja múltiples establecimientos y tipos de códigos (EAN, PLU, internos)
Detecta y elimina productos duplicados dentro de cada factura
"""

import threading
from queue import Queue
import time
import os
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

# Importar funciones de base de datos
from database import get_db_connection, detectar_cadena, actualizar_inventario_desde_factura
from claude_invoice import parse_invoice_with_claude

# ✅ Importar normalizador de códigos
from normalizador_codigos import (
    normalizar_codigo_por_establecimiento,
    buscar_o_crear_producto_inteligente
)

# ✅ NUEVO: Importar detector de duplicados
from duplicate_detector import detectar_duplicados_automaticamente

# Colas y tracking globales
ocr_queue = Queue()
processing = {}
error_log = []


# ==============================================================================
# FUNCIÓN PARA LIMPIAR PRECIOS COLOMBIANOS
# ==============================================================================

def limpiar_precio_colombiano(precio_str):
    """
    Convierte precio colombiano a entero (sin decimales).
    En Colombia NO se usan decimales/centavos, solo pesos enteros.
    """
    # Caso 1: None o vacío
    if precio_str is None or precio_str == "":
        return 0

    # Caso 2: Ya es un entero
    if isinstance(precio_str, int):
        return precio_str

    # Caso 3: Es un float
    if isinstance(precio_str, float):
        if precio_str == int(precio_str):
            return int(precio_str)
        return int(precio_str * 100)

    # Caso 4: Es string - procesar
    precio_str = str(precio_str).strip()

    # Eliminar espacios y símbolos de moneda
    precio_str = precio_str.replace(" ", "")
    precio_str = precio_str.replace("$", "")
    precio_str = precio_str.replace("COP", "")
    precio_str = precio_str.replace("cop", "")
    precio_str = precio_str.strip()

    # Caso 4A: Tiene múltiples puntos o comas (separador de miles)
    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        precio_str = precio_str.replace(",", "").replace(".", "")

    # Caso 4B: Tiene un solo punto o coma
    elif '.' in precio_str or ',' in precio_str:
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        # Si hay 3 dígitos después, es separador de miles
        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        # Si hay 1-2 dígitos, eliminar igual (no hay decimales en Colombia)
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
# CLASE OCPROCESSOR - VERSIÓN 2.2 CON DETECCIÓN DE DUPLICADOS
# ==============================================================================

class OCRProcessor:
    """Procesador automático de facturas con OCR - Versión 2.2"""

    def __init__(self):
        self.is_running = False
        self.processed_count = 0
        self.error_count = 0
        self.success_rate = 100.0
        self.last_processed = None
        self.worker_thread = None

    def start(self):
        """Inicia el procesador en background"""
        if self.is_running:
            print("⚠️ Procesador ya está en ejecución")
            return

        self.is_running = True
        self.worker_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.worker_thread.start()
        print("🤖 Procesador OCR automático iniciado")
        print("   ✅ VERSIÓN 2.2 - Normalización inteligente de códigos")
        print("   🧹 VERSIÓN 2.2 - Detección automática de duplicados")

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
        """Procesa una factura individual"""
        factura_id = task.get('factura_id')
        image_path = task.get('image_path')
        user_id = task.get('user_id', 1)

        if not factura_id or not image_path:
            print(f"❌ Task inválido: {task}")
            return

        try:
            print(f"🔄 Procesando factura #{factura_id}")
            processing[factura_id] = {
                'status': 'processing',
                'started_at': datetime.now()
            }

            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Imagen no encontrada: {image_path}")

            result = parse_invoice_with_claude(image_path)

            conn = get_db_connection()
            if not conn:
                raise Exception("No se pudo conectar a la base de datos")

            cursor = conn.cursor()

            if result.get("success"):
                self._process_successful_ocr(
                    cursor, conn, factura_id, result["data"], user_id
                )
                conn.commit()

                self.processed_count += 1
                self.last_processed = datetime.now()

                processing[factura_id] = {
                    'status': 'completed',
                    'completed_at': datetime.now()
                }

                print(f"✅ Factura #{factura_id} procesada exitosamente")

            else:
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
        """Procesa un resultado exitoso de OCR"""

        establecimiento = data.get("establecimiento", "Desconocido")
        cadena = detectar_cadena(establecimiento)

        # ========================================
        # ✅ NUEVO: DETECCIÓN DE DUPLICADOS
        # ========================================
        productos_originales = data.get("productos", [])
        total_factura = data.get("total", 0)

        print(f"\n{'='*70}")
        print(f"🧹 LIMPIEZA DE DUPLICADOS - Factura #{factura_id}")
        print(f"{'='*70}")

        # Convertir productos al formato esperado por duplicate_detector
        # Claude retorna: {"codigo": "...", "nombre": "...", "precio": 123}
        # duplicate_detector espera: {"codigo": "...", "nombre": "...", "valor": 123}
        productos_para_detector = []
        for prod in productos_originales:
            productos_para_detector.append({
                "codigo": prod.get("codigo", ""),
                "nombre": prod.get("nombre", ""),
                "valor": prod.get("precio", 0),  # duplicate_detector usa "valor"
                "cantidad": prod.get("cantidad", 1)
            })

        # Ejecutar detección de duplicados
        resultado_limpieza = detectar_duplicados_automaticamente(
            productos=productos_para_detector,
            total_factura=total_factura,
            umbral_similitud=0.85,
            tolerancia_total=0.15
        )

        # Registrar resultado de limpieza
        if resultado_limpieza["duplicados_detectados"]:
            print(f"\n📊 RESULTADO LIMPIEZA:")
            print(f"   📦 Productos originales: {len(productos_originales)}")
            print(f"   ✅ Productos limpios: {len(resultado_limpieza['productos_limpios'])}")
            print(f"   🗑️ Duplicados eliminados: {len(resultado_limpieza['productos_eliminados'])}")

            # Mostrar qué se eliminó
            for prod_eliminado in resultado_limpieza["productos_eliminados"]:
                print(f"      ❌ Eliminado: {prod_eliminado['nombre'][:40]} (${prod_eliminado['valor']:,})")
                print(f"         Razón: {prod_eliminado['razon']}")
        else:
            print(f"   ✅ No se detectaron duplicados en esta factura")

        print(f"{'='*70}\n")

        # Convertir productos limpios de vuelta al formato original
        # duplicate_detector retorna "valor", convertir a "precio"
        productos_a_procesar = []
        for prod_limpio in resultado_limpieza["productos_limpios"]:
            productos_a_procesar.append({
                "codigo": prod_limpio.get("codigo", ""),
                "nombre": prod_limpio.get("nombre", ""),
                "precio": prod_limpio.get("valor", 0),  # Convertir "valor" a "precio"
                "cantidad": prod_limpio.get("cantidad", 1)
            })

        # ========================================
        # PROCESAMIENTO DE PRODUCTOS LIMPIOS
        # ========================================
        productos_guardados = 0
        productos_rechazados = 0

        for product in productos_a_procesar:  # ⭐ Usar productos limpios
            item_id = self._save_product_to_items_factura(
                cursor, conn, product, factura_id, user_id, establecimiento, cadena
            )

            if item_id:
                productos_guardados += 1
            else:
                productos_rechazados += 1

        print(f"📊 Factura #{factura_id}: {productos_guardados} productos guardados, {productos_rechazados} rechazados")

    def _save_product_to_items_factura(self, cursor, conn, product: Dict, factura_id: int,
                                       user_id: int, establecimiento: str, cadena: str) -> Optional[int]:
        """
        ✅ VERSIÓN 2.2 - CON NORMALIZACIÓN INTELIGENTE DE CÓDIGOS

        Guarda un producto con:
        1. Normalización de códigos según establecimiento (ARA, D1, etc.)
        2. Detección de tipo de código (EAN, PLU, INTERNO)
        3. Búsqueda inteligente (por código o nombre)
        4. Creación automática si no existe
        """
        try:
            codigo_raw = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()
            precio_raw = product.get("precio", 0)
            precio = limpiar_precio_colombiano(precio_raw)
            cantidad = int(product.get("cantidad", 1))

            # ========================================
            # VALIDACIONES BÁSICAS
            # ========================================
            if not nombre:
                print(f"   ⚠️ Producto sin nombre, omitiendo")
                return None

            if precio <= 0:
                print(f"   ⚠️ Precio inválido para '{nombre}': {precio_raw} → {precio}")
                return None

            if precio < 10:
                print(f"   ⚠️ Precio muy bajo para '{nombre}': ${precio:,}, omitiendo")
                return None

            if precio > 10_000_000:
                print(f"   ⚠️ Precio sospechoso para '{nombre}': ${precio:,}, verificar")

            # ========================================
            # ✅ NORMALIZAR CÓDIGO SEGÚN ESTABLECIMIENTO
            # ========================================
            codigo, tipo_codigo, confianza = normalizar_codigo_por_establecimiento(
                codigo_raw, establecimiento
            )

            print(f"   💰 '{nombre}': ${precio:,}")
            print(f"      📟 Código: {codigo_raw} → {codigo} ({tipo_codigo}, {confianza}%)")

            # ========================================
            # ✅ BUSCAR O CREAR PRODUCTO INTELIGENTE
            # ========================================
            producto_maestro_id, accion = buscar_o_crear_producto_inteligente(
                cursor, conn, codigo, tipo_codigo, nombre, establecimiento, precio,
                codigo_raw=codigo_raw
            )

            if not producto_maestro_id:
                print(f"   ❌ No se pudo obtener producto_maestro_id")
                return None

            # ========================================
            # GUARDAR EN ITEMS_FACTURA
            # ========================================
            cursor.execute("""
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    producto_maestro_id,
                    codigo_leido,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    matching_confianza,
                    fecha_creacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (
                factura_id,
                user_id,
                producto_maestro_id,
                codigo if codigo else None,
                nombre,
                precio,
                cantidad,
                confianza
            ))

            item_id = cursor.fetchone()[0]
            conn.commit()

            print(f"   ✅ Item guardado: ID={item_id}, Producto={producto_maestro_id}, Acción={accion}")

            return item_id

        except Exception as e:
            print(f"   ❌ Error guardando producto '{nombre}': {e}")
            traceback.print_exc()
            conn.rollback()
            return None

    def _process_failed_ocr(self, cursor, factura_id: int, error: str):
        """Procesa un fallo de OCR"""
        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'error_ocr',
                notas = %s
            WHERE id = %s
        """, (f"Error OCR: {error}", factura_id))

    def get_stats(self):
        """Retorna estadísticas del procesador"""
        return {
            'is_running': self.is_running,
            'processed_count': self.processed_count,
            'error_count': self.error_count,
            'success_rate': round(self.success_rate, 2),
            'last_processed': self.last_processed.isoformat() if self.last_processed else None,
            'queue_size': ocr_queue.qsize(),
            'processing_count': len([p for p in processing.values() if p.get('status') == 'processing']),
            'recent_errors': error_log[-10:] if error_log else []
        }


print("✅ OCR Processor V2.2 cargado")
print("   📟 Normalización inteligente de códigos habilitada")
print("   🧹 Detección automática de duplicados habilitada")
print("   🏪 Soporta: ARA, D1, Éxito, Jumbo, y cualquier establecimiento")

# Crear instancia global del procesador
processor = OCRProcessor()
