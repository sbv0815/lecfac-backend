"""
Sistema de Procesamiento Automático de OCR para Facturas
VERSIÓN 2.0 - INTEGRADO CON PRODUCTOS CANÓNICOS
Usa ProductResolver para evitar duplicados y comparar precios
"""

import threading
from queue import Queue
import time
import os
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
import traceback
import unicodedata
import re

# Importar funciones de base de datos
from database import get_db_connection, detectar_cadena, actualizar_inventario_desde_factura
from claude_invoice import parse_invoice_with_claude

# ✅ NUEVO: Importar ProductResolver para sistema canónico
from product_resolver import ProductResolver

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
# CLASE OCPROCESSOR - VERSIÓN 2.0 CON PRODUCTOS CANÓNICOS
# ==============================================================================

class OCRProcessor:
    """Procesador automático de facturas con OCR - Versión 2.0"""

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
        print("🤖 Procesador OCR automático iniciado (VERSIÓN 2.0 - PRODUCTOS CANÓNICOS)")

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

        productos_guardados = 0
        productos_rechazados = 0

        for product in data.get("productos", []):
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
        ✅ VERSIÓN 2.0 - USA PRODUCTRESOLVER PARA SISTEMA CANÓNICO

        Guarda un producto usando el sistema de productos canónicos:
        1. Resuelve identidad del producto (canónico + variante)
        2. Guarda en items_factura con referencias canónicas
        3. Guarda precio en precios_productos para comparación
        """
        try:
            codigo = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()

            # Limpiar precio colombiano
            precio_raw = product.get("precio", 0)
            precio = limpiar_precio_colombiano(precio_raw)

            cantidad = int(product.get("cantidad", 1))

            # Validación 1: Producto debe tener nombre
            if not nombre:
                print(f"   ⚠️ Producto sin nombre, omitiendo")
                return None

            # Validación 2: Precio debe ser positivo
            if precio <= 0:
                print(f"   ⚠️ Precio inválido para '{nombre}': {precio_raw} → {precio}")
                return None

            # Validación 3: Precio razonable (entre $10 y $10 millones)
            if precio < 10:
                print(f"   ⚠️ Precio muy bajo para '{nombre}': ${precio:,}, omitiendo")
                return None

            if precio > 10_000_000:
                print(f"   ⚠️ Precio sospechoso para '{nombre}': ${precio:,}, verificar")

            print(f"   💰 '{nombre}': {precio_raw} → ${precio:,} pesos")

            # ========================================
            # ✅ NUEVO: USAR PRODUCTRESOLVER
            # ========================================
            print(f"   🧠 Resolviendo identidad del producto...")

            resolver = ProductResolver()

            try:
                # Resolver producto: busca o crea canónico + variante
                canonico_id, variante_id, accion = resolver.resolver_producto(
                    codigo=codigo,
                    nombre=nombre,
                    establecimiento=establecimiento,
                    precio=precio,
                    marca=None,
                    categoria=None
                )

                print(f"   ✅ Resuelto: Canónico={canonico_id}, Variante={variante_id}, Acción={accion}")

            except Exception as e:
                print(f"   ❌ Error en ProductResolver: {e}")
                traceback.print_exc()
                resolver.close()
                return None

            finally:
                resolver.close()

            # ========================================
            # GUARDAR EN ITEMS_FACTURA (con referencias canónicas)
            # ========================================
            cursor.execute("""
                INSERT INTO items_factura (
                    factura_id,
                    usuario_id,
                    producto_canonico_id,
                    codigo_leido,
                    nombre_leido,
                    precio_pagado,
                    cantidad,
                    matching_confianza,
                    fecha_creacion
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                factura_id,
                user_id,
                canonico_id,
                codigo if codigo else None,
                nombre,
                precio,
                cantidad,
                95 if accion == 'found_ean' else 85 if accion == 'found_similar' else 70,
                datetime.now()
            ))

            item_id = cursor.fetchone()[0]
            conn.commit()

            print(f"   ✅ Item guardado: ID={item_id}, Canónico={canonico_id}, Precio=${precio:,}")

            # ========================================
            # GUARDAR PRECIO EN precios_productos (para comparación)
            # ========================================
            try:
                cursor.execute("""
                    INSERT INTO precios_productos (
                        producto_canonico_id,
                        variante_id,
                        establecimiento,
                        precio,
                        fecha,
                        usuario_id,
                        factura_id
                    ) VALUES (%s, %s, %s, %s, CURRENT_DATE, %s, %s)
                    ON CONFLICT (producto_canonico_id, establecimiento, fecha)
                    DO UPDATE SET
                        precio = EXCLUDED.precio,
                        usuario_id = EXCLUDED.usuario_id,
                        factura_id = EXCLUDED.factura_id
                """, (
                    canonico_id,
                    variante_id,
                    establecimiento,
                    precio,
                    user_id,
                    factura_id
                ))
                conn.commit()
                print(f"   💰 Precio guardado en precios_productos para comparación")
            except Exception as e:
                print(f"   ⚠️ Error guardando precio: {e}")
                conn.rollback()

            return item_id

        except Exception as e:
            print(f"   ⚠️ Error guardando producto '{nombre}': {e}")
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


print("✅ OCR Processor V2.0 cargado - INTEGRADO CON PRODUCTOS CANÓNICOS")

# Crear instancia global del procesador
processor = OCRProcessor()
