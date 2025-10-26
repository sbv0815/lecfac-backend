"""
Sistema de Procesamiento Automático de OCR para Facturas
FIX CRÍTICO: Eliminado método duplicado + agregado conn=conn
"""

import threading
from queue import Queue
import time
import os
import tempfile
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

# Importar funciones necesarias del proyecto
from database import get_db_connection, detectar_cadena, actualizar_inventario_desde_factura
from claude_invoice import parse_invoice_with_claude
from product_matching import buscar_o_crear_producto_inteligente

# Colas y tracking globales
ocr_queue = Queue()
processing = {}
error_log = []


class OCRProcessor:
    """Procesador automático de facturas con OCR"""

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

                    # Pequeña pausa entre procesamientos
                    time.sleep(1)
                else:
                    # No hay tareas, esperar
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

            # Verificar que el archivo existe
            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Imagen no encontrada: {image_path}")

            # Procesar con Claude
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

            # Intentar guardar el error en BD
            self._save_error_to_db(factura_id, str(e))

        finally:
            # Limpiar archivo temporal
            if image_path and os.path.exists(image_path):
                try:
                    os.unlink(image_path)
                    print(f"🗑️ Archivo temporal eliminado: {image_path}")
                except Exception as e:
                    print(f"⚠️ No se pudo eliminar archivo temporal: {e}")

            # Limpiar del tracking después de 5 minutos
            if factura_id in processing:
                threading.Timer(300, lambda: processing.pop(factura_id, None)).start()

    def _process_successful_ocr(self, cursor, conn, factura_id: int, data: Dict, user_id: int):
        """Procesa un resultado exitoso de OCR"""

        # Extraer datos principales
        establecimiento = data.get("establecimiento", "Desconocido")
        cadena = detectar_cadena(establecimiento)
        total = float(data.get("total", 0))
        fecha = data.get("fecha")
        productos = data.get("productos", [])

        print(f"📦 Procesando {len(productos)} productos de la factura #{factura_id}")

        # Calcular score de calidad
        score = self.calculate_quality_score(data)

        # Determinar estado basado en calidad
        if score >= 80:
            estado = "procesado"
        elif score >= 50:
            estado = "revision"
        else:
            estado = "baja_calidad"

        # Actualizar factura
        cursor.execute("""
            UPDATE facturas
            SET establecimiento = %s,
                cadena = %s,
                total_factura = %s,
                fecha_factura = %s,
                estado_validacion = %s,
                puntaje_calidad = %s,
                productos_detectados = %s,
                fecha_procesamiento = %s,
                procesado_por = 'OCR_AUTO'
            WHERE id = %s
        """, (
            establecimiento,
            cadena,
            total,
            fecha,
            estado,
            score,
            len(productos),
            datetime.now(),
            factura_id
        ))

        # Procesar y guardar productos
        productos_guardados = 0
        productos_rechazados = 0

        for idx, prod in enumerate(productos):
            if self.validate_product(prod):
                try:
                    # Guardar producto usando la nueva función
                    producto_maestro_id = self._save_product_to_items_factura(
                        cursor, conn, prod, factura_id, user_id, establecimiento, cadena
                    )

                    if producto_maestro_id:
                        productos_guardados += 1
                        print(f"   ✅ Producto {idx+1}/{len(productos)}: {prod.get('nombre')[:30]}")
                    else:
                        productos_rechazados += 1
                        print(f"   ⚠️ No se pudo guardar: {prod.get('nombre')[:30]}")

                except Exception as e:
                    productos_rechazados += 1
                    print(f"   ❌ Error guardando producto: {e}")
                    traceback.print_exc()
            else:
                productos_rechazados += 1
                print(f"   🚫 Producto rechazado: {prod.get('nombre', 'sin nombre')[:30]}")

        # Actualizar contador de productos guardados
        cursor.execute("""
            UPDATE facturas
            SET productos_guardados = %s
            WHERE id = %s
        """, (productos_guardados, factura_id))

        # Commit antes de actualizar inventario
        conn.commit()

        # Actualizar inventario del usuario
        if productos_guardados > 0:
            print(f"📦 Actualizando inventario del usuario {user_id}...")
            try:
                actualizar_inventario_desde_factura(factura_id, user_id)
                print(f"✅ Inventario actualizado correctamente")
            except Exception as e:
                print(f"⚠️ Error actualizando inventario: {e}")
                traceback.print_exc()

        # Registrar en log
        try:
            cursor.execute("""
                INSERT INTO ocr_logs (factura_id, status, message, details, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                factura_id,
                "success",
                f"Procesado exitosamente",
                f"Score: {score}, Productos: {productos_guardados}/{len(productos)}, Rechazados: {productos_rechazados}",
                datetime.now()
            ))
            conn.commit()
        except Exception as e:
            print(f"⚠️ No se pudo guardar log: {e}")

        print(f"📊 Factura #{factura_id}: {productos_guardados} productos guardados, {productos_rechazados} rechazados")

    def _save_product_to_items_factura(self, cursor, conn, product: Dict, factura_id: int,
                                       user_id: int, establecimiento: str, cadena: str) -> Optional[int]:
        """
        Guarda un producto en items_factura usando el sistema de matching inteligente
        🔧 FIX FINAL: Parámetros correctos + conn agregado
        """
        try:
            codigo = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()
            precio = int(float(product.get("precio", 0)))
            cantidad = int(product.get("cantidad", 1))

            if not nombre:
                print(f"   ⚠️ Producto sin nombre, omitiendo")
                return None

            if precio <= 0:
                print(f"   ⚠️ Precio inválido: {precio}")
                return None

            # 🔧 FIX: Usar los parámetros CORRECTOS según product_matching.py
            # CRÍTICO: Incluir conn=conn para que las funciones puedan hacer commit
            producto_maestro_id = buscar_o_crear_producto_inteligente(
                codigo=codigo,
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
                conn=conn
            )

            # 🚨 CRÍTICO: Si no se pudo obtener producto_maestro_id, NO continuar
            if not producto_maestro_id:
                print(f"   ❌ No se pudo obtener producto_maestro_id para: {nombre} ({codigo})")
                print(f"      → Saltando este producto")
                return None

            # ✅ Solo si llegamos aquí, producto_maestro_id es válido
            # Guardar en items_factura
            cursor.execute("""
                INSERT INTO items_factura (
                    factura_id,
                    producto_maestro_id,
                    usuario_id,
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
                producto_maestro_id,
                user_id,
                codigo if codigo else None,
                nombre,
                precio,
                cantidad,
                90,
                datetime.now()
            ))

            item_id = cursor.fetchone()[0]
            conn.commit()

            print(f"   ✅ Item guardado - ID: {item_id}, producto_maestro_id: {producto_maestro_id}, precio: ${precio:,}")

            return producto_maestro_id

        except Exception as e:
            print(f"   ❌ Error guardando producto en items_factura: {e}")
            traceback.print_exc()
            conn.rollback()
            return None

    def _process_failed_ocr(self, cursor, factura_id: int, error: str):
        """Procesa un fallo de OCR"""

        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'error_ocr',
                notas = %s,
                fecha_procesamiento = %s
            WHERE id = %s
        """, (f"Error OCR: {error}", datetime.now(), factura_id))

        # Registrar en log
        try:
            cursor.execute("""
                INSERT INTO ocr_logs (factura_id, status, message, created_at)
                VALUES (%s, %s, %s, %s)
            """, (factura_id, "error", error, datetime.now()))
        except Exception as e:
            print(f"⚠️ No se pudo guardar log de error: {e}")

    def calculate_quality_score(self, data: Dict) -> int:
        """Calcula un score de calidad para el resultado del OCR"""
        score = 100

        # Penalizaciones por datos faltantes o dudosos

        # Establecimiento
        establecimiento = data.get("establecimiento", "")
        if not establecimiento or establecimiento.lower() in ["desconocido", "sin nombre", "procesando"]:
            score -= 30
        elif len(establecimiento) < 3:
            score -= 15

        # Total
        total = data.get("total", 0)
        if total <= 0:
            score -= 25
        elif total > 100000000:  # Total sospechosamente alto
            score -= 10

        # Productos
        productos = data.get("productos", [])
        if len(productos) == 0:
            score -= 40
        elif len(productos) == 1:
            score -= 15

        # Análisis de productos
        if productos:
            # Productos sin código
            sin_codigo = sum(1 for p in productos if
                           not p.get("codigo") or p.get("codigo") == "SIN_CODIGO")
            if sin_codigo > len(productos) * 0.5:
                score -= 15
            elif sin_codigo > len(productos) * 0.25:
                score -= 8

            # Productos sin precio válido
            sin_precio = sum(1 for p in productos if
                           not p.get("precio") or p.get("precio") <= 0)
            if sin_precio > len(productos) * 0.3:
                score -= 10

            # Productos con nombres muy cortos o genéricos
            nombres_malos = sum(1 for p in productos if
                              len(str(p.get("nombre", ""))) < 3)
            if nombres_malos > len(productos) * 0.2:
                score -= 5

            # Verificar si el total coincide aproximadamente con suma de productos
            suma_productos = sum(float(p.get("precio", 0)) for p in productos)
            if suma_productos > 0 and total > 0:
                diferencia_porcentaje = abs(total - suma_productos) / total * 100
                if diferencia_porcentaje > 20:
                    score -= 10

        # Fecha
        if not data.get("fecha"):
            score -= 5

        return max(0, min(100, score))

    def validate_product(self, product: Dict) -> bool:
        """Valida si un producto debe ser guardado"""

        # Debe tener al menos nombre
        if not product.get("nombre"):
            return False

        nombre = str(product.get("nombre", "")).lower().strip()

        # Filtrar líneas que claramente no son productos
        palabras_excluir = [
            'subtotal', 'total', 'cambio', 'efectivo', 'tarjeta',
            'descuento', 'ahorro', 'puntos', 'iva', 'impuesto',
            'propina', 'servicio', 'domicilio', 'envio', 'delivery',
            'recibido', 'devuelto', 'vuelto', 'pago', 'saldo',
            '----------', '==========', '***', '...'
        ]

        for palabra in palabras_excluir:
            if palabra in nombre:
                return False

        # Validar precio
        try:
            precio = float(product.get("precio", 0))
            # Rechazar precios negativos o absurdamente altos
            if precio < 0 or precio > 100000000:
                return False
        except:
            return False

        # Validar que el nombre no sea solo números o caracteres especiales
        if nombre.replace(" ", "").replace(".", "").replace("-", "").isdigit():
            return False

        # Nombre muy corto probablemente es basura
        if len(nombre) < 2:
            return False

        return True

    def _save_error_to_db(self, factura_id: int, error: str):
        """Intenta guardar un error en la base de datos"""
        try:
            conn = get_db_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE facturas
                    SET estado_validacion = 'error_sistema',
                        notas = %s
                    WHERE id = %s
                """, (f"Error sistema: {error[:500]}", factura_id))

                try:
                    cursor.execute("""
                        INSERT INTO ocr_logs (factura_id, status, message, created_at)
                        VALUES (%s, %s, %s, %s)
                    """, (factura_id, "error_sistema", error[:500], datetime.now()))
                except:
                    pass  # Tabla ocr_logs puede no existir

                conn.commit()
                conn.close()
        except Exception as e:
            print(f"❌ No se pudo guardar error en BD: {e}")

    def get_stats(self) -> Dict:
        """Obtiene estadísticas del procesador"""
        return {
            "is_running": self.is_running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "success_rate": round(self.success_rate, 2),
            "queue_size": ocr_queue.qsize(),
            "processing_now": len([p for p in processing.values()
                                 if p.get('status') == 'processing']),
            "last_processed": self.last_processed.isoformat() if self.last_processed else None
        }

    def add_to_queue(self, factura_id: int, image_path: str, user_id: int = 1) -> bool:
        """Agrega una factura a la cola de procesamiento"""
        try:
            task = {
                'factura_id': factura_id,
                'image_path': image_path,
                'user_id': user_id,
                'timestamp': datetime.now()
            }

            ocr_queue.put(task)
            processing[factura_id] = {
                'status': 'queued',
                'queued_at': datetime.now()
            }

            print(f"📥 Factura #{factura_id} agregada a la cola (posición: {ocr_queue.qsize()})")
            return True

        except Exception as e:
            print(f"❌ Error agregando a cola: {e}")
            return False

    def get_queue_position(self, factura_id: int) -> Optional[int]:
        """Obtiene la posición aproximada en la cola"""
        if factura_id in processing:
            status = processing[factura_id].get('status')
            if status == 'queued':
                # Aproximar posición (no es 100% exacto en multi-threading)
                return ocr_queue.qsize()
            elif status == 'processing':
                return 0
        return None


# Instancia global del procesador
processor = OCRProcessor()

# Force deploy: 10/25/2025 15:28:36
