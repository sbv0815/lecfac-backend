"""
Sistema de Procesamiento Automático de OCR para Facturas
VERSIÓN STANDALONE - TODO EL CÓDIGO INTEGRADO
NO REQUIERE IMPORTS EXTERNOS DE MATCHING
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

# Importar solo funciones de base de datos
from database import get_db_connection, detectar_cadena, actualizar_inventario_desde_factura
from claude_invoice import parse_invoice_with_claude

# Colas y tracking globales
ocr_queue = Queue()
processing = {}
error_log = []


# ==============================================================================
# FUNCIÓN PARA LIMPIAR PRECIOS COLOMBIANOS - VERSIÓN MEJORADA
# ==============================================================================

def limpiar_precio_colombiano(precio_str):
    """
    Convierte precio colombiano a entero (sin decimales).

    VERSIÓN MEJORADA: Maneja strings, integers y floats correctamente.

    En Colombia NO se usan decimales/centavos, solo pesos enteros.
    Las facturas muestran separadores de miles con comas o puntos.

    Args:
        precio_str: Precio en cualquier formato (string, int, float)

    Returns:
        int: Precio en pesos enteros

    Examples:
        >>> limpiar_precio_colombiano("15,540")
        15540
        >>> limpiar_precio_colombiano("15.540")
        15540
        >>> limpiar_precio_colombiano(39.45)  # Float de Claude API
        3945
        >>> limpiar_precio_colombiano(15540)
        15540
        >>> limpiar_precio_colombiano("$ 1.234.567")
        1234567
    """
    # Caso 1: None o vacío
    if precio_str is None or precio_str == "":
        return 0

    # Caso 2: Ya es un entero
    if isinstance(precio_str, int):
        return precio_str

    # Caso 3: Es un float (puede venir de Claude API)
    if isinstance(precio_str, float):
        # Si tiene decimales pequeños (ej: 15540.0), es solo formateo
        if precio_str == int(precio_str):
            return int(precio_str)
        # Si tiene decimales significativos, puede ser error de OCR
        # Ej: 39.45 probablemente significa 3945 pesos (faltó un cero)
        return int(precio_str * 100)

    # Caso 4: Es string - procesar
    precio_str = str(precio_str).strip()

    # Eliminar espacios
    precio_str = precio_str.replace(" ", "")

    # Eliminar símbolos de moneda
    precio_str = precio_str.replace("$", "")
    precio_str = precio_str.replace("COP", "")
    precio_str = precio_str.replace("cop", "")
    precio_str = precio_str.strip()

    # CRÍTICO: Determinar si usa punto o coma como separador
    # En Colombia, ambos pueden usarse para separar miles

    # Caso 4A: Tiene múltiples puntos o comas (separador de miles)
    # Ej: "1.234.567" o "1,234,567"
    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        # Eliminar TODOS los separadores
        precio_str = precio_str.replace(",", "").replace(".", "")

    # Caso 4B: Tiene un solo punto o coma
    # Ej: "15.540" o "15,540"
    elif '.' in precio_str or ',' in precio_str:
        # Verificar cantidad de dígitos después del separador
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        # Si hay 3 dígitos después, es separador de miles
        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        # Si hay 1-2 dígitos, puede ser decimal mal leído
        elif len(partes) == 2 and len(partes[1]) <= 2:
            # En Colombia NO hay decimales, así que eliminamos el separador
            precio_str = precio_str.replace(",", "").replace(".", "")
        else:
            # Caso raro, eliminar todos
            precio_str = precio_str.replace(",", "").replace(".", "")

    # Convertir a entero
    try:
        precio = int(float(precio_str))

        # Validación de sanidad
        if precio < 0:
            print(f"   ⚠️ Precio negativo detectado: {precio}, retornando 0")
            return 0

        return precio

    except (ValueError, TypeError) as e:
        print(f"   ⚠️ No se pudo convertir precio '{precio_str}': {e}")
        return 0


# ==============================================================================
# FUNCIONES DE MATCHING INTEGRADAS (NO REQUIEREN IMPORT)
# ==============================================================================

def normalizar_nombre_producto(nombre: str) -> str:
    """Normaliza nombre de producto para comparación"""
    if not nombre:
        return ""

    texto = nombre.upper()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r'[^A-Z0-9\s]', ' ', texto)
    texto = ' '.join(texto.split())

    return texto


def clasificar_codigo_producto(codigo: str, establecimiento: str = None) -> dict:
    """Clasifica un código según su tipo"""

    if not codigo or not isinstance(codigo, str):
        return {"tipo": "INVALIDO", "codigo_normalizado": None}

    codigo = codigo.strip()

    # EAN-13 completo
    if len(codigo) == 13 and codigo.isdigit():
        return {
            "tipo": "EAN13",
            "codigo_normalizado": codigo,
            "es_unico_global": True
        }

    # EAN-13 incompleto (10 dígitos)
    if len(codigo) == 10 and codigo.isdigit():
        return {
            "tipo": "EAN13_INCOMPLETO",
            "codigo_normalizado": f"770{codigo}",
            "es_unico_global": True
        }

    # PLU o código interno
    if codigo.isdigit():
        return {
            "tipo": "INTERNO",
            "codigo_normalizado": codigo,
            "es_unico_global": False,
            "requiere_establecimiento": True
        }

    # Código alfanumérico
    return {
        "tipo": "ALFANUMERICO",
        "codigo_normalizado": codigo,
        "es_unico_global": False,
        "requiere_establecimiento": True
    }


def buscar_o_crear_por_ean_inline(codigo_ean: str, nombre: str, precio: int, cursor, conn) -> Optional[int]:
    """Buscar o crear producto por EAN"""
    nombre_norm = normalizar_nombre_producto(nombre)

    try:
        print(f"      🔎 Buscando EAN: {codigo_ean}")

        cursor.execute("""
            SELECT id, precio_promedio_global, total_reportes
            FROM productos_maestros
            WHERE codigo_ean = %s
            LIMIT 1
        """, (codigo_ean,))

        resultado = cursor.fetchone()

        if resultado:
            producto_id = resultado[0]
            print(f"      ✅ Producto encontrado por EAN: ID={producto_id}")

            # Actualizar precio promedio
            precio_actual = resultado[1] or 0
            reportes_actuales = resultado[2] or 0
            nuevo_total_reportes = reportes_actuales + 1
            nuevo_precio_promedio = ((precio_actual * reportes_actuales) + precio) / nuevo_total_reportes

            cursor.execute("""
                UPDATE productos_maestros
                SET precio_promedio_global = %s,
                    total_reportes = %s,
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (nuevo_precio_promedio, nuevo_total_reportes, producto_id))
            conn.commit()

            return producto_id

        # Crear nuevo producto
        print(f"      ➕ Creando nuevo producto con EAN")
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                precio_promedio_global,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """, (codigo_ean, nombre_norm, nombre, precio))

        nuevo_id = cursor.fetchone()[0]
        conn.commit()
        print(f"      ✅ Producto creado con EAN: ID={nuevo_id}")
        return nuevo_id

    except Exception as e:
        print(f"      ❌ Error en buscar_o_crear_por_ean: {e}")
        conn.rollback()
        return None


def buscar_o_crear_por_codigo_interno_inline(codigo: str, nombre: str, precio: int, establecimiento: str, cursor, conn) -> Optional[int]:
    """Buscar o crear producto por código interno"""
    nombre_norm = normalizar_nombre_producto(nombre)
    codigo_interno_compuesto = f"{codigo}|{establecimiento}"

    try:
        print(f"      🔎 Buscando código interno: {codigo_interno_compuesto}")

        cursor.execute("""
            SELECT id, precio_promedio_global, total_reportes
            FROM productos_maestros
            WHERE subcategoria = %s
            AND nombre_normalizado = %s
            AND codigo_ean IS NULL
            LIMIT 1
        """, (codigo_interno_compuesto, nombre_norm))

        resultado = cursor.fetchone()

        if resultado:
            producto_id = resultado[0]
            print(f"      ✅ Producto encontrado por código interno: ID={producto_id}")

            # Actualizar precio promedio
            precio_actual = resultado[1] or 0
            reportes_actuales = resultado[2] or 0
            nuevo_total_reportes = reportes_actuales + 1
            nuevo_precio_promedio = ((precio_actual * reportes_actuales) + precio) / nuevo_total_reportes

            cursor.execute("""
                UPDATE productos_maestros
                SET precio_promedio_global = %s,
                    total_reportes = %s,
                    ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (nuevo_precio_promedio, nuevo_total_reportes, producto_id))
            conn.commit()

            return producto_id

        # Crear nuevo producto
        print(f"      ➕ Creando nuevo producto con código interno")
        cursor.execute("""
            INSERT INTO productos_maestros (
                codigo_ean,
                nombre_normalizado,
                nombre_comercial,
                precio_promedio_global,
                subcategoria,
                total_reportes,
                primera_vez_reportado,
                ultima_actualizacion
            ) VALUES (NULL, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """, (nombre_norm, nombre, precio, codigo_interno_compuesto))

        nuevo_id = cursor.fetchone()[0]
        conn.commit()
        print(f"      ✅ Producto creado con código interno: ID={nuevo_id}")
        return nuevo_id

    except Exception as e:
        print(f"      ❌ Error en buscar_o_crear_por_codigo_interno: {e}")
        conn.rollback()
        return None


def buscar_o_crear_producto_inteligente_inline(codigo: str, nombre: str, precio: int, establecimiento: str, cursor, conn) -> Optional[int]:
    """
    FUNCIÓN INTEGRADA - NO REQUIERE IMPORT
    Busca o crea producto maestro usando clasificación inteligente
    """

    print(f"\n🔍 [INLINE] buscar_o_crear_producto_inteligente:")
    print(f"   - codigo: {codigo}")
    print(f"   - nombre: {nombre}")
    print(f"   - precio: ${precio:,} pesos")
    print(f"   - establecimiento: {establecimiento}")

    if not nombre or not nombre.strip():
        print(f"   ❌ Nombre vacío")
        return None

    if precio <= 0:
        print(f"   ❌ Precio inválido ({precio})")
        return None

    # Clasificar código
    clasificacion = clasificar_codigo_producto(codigo, establecimiento)
    print(f"   📊 Clasificación: {clasificacion['tipo']}")

    try:
        if clasificacion["tipo"] in ["EAN13", "EAN13_INCOMPLETO"]:
            print(f"   ➡️ Usando estrategia EAN")
            resultado = buscar_o_crear_por_ean_inline(
                codigo_ean=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                cursor=cursor,
                conn=conn
            )
            print(f"   ✅ Resultado EAN: {resultado}")
            return resultado

        elif clasificacion.get("requiere_establecimiento"):
            print(f"   ➡️ Usando estrategia código interno")
            resultado = buscar_o_crear_por_codigo_interno_inline(
                codigo=clasificacion["codigo_normalizado"],
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
                conn=conn
            )
            print(f"   ✅ Resultado interno: {resultado}")
            return resultado

        else:
            print(f"   ⚠️ Tipo no manejado: {clasificacion['tipo']}")
            return None

    except Exception as e:
        print(f"   ❌ EXCEPCIÓN: {e}")
        traceback.print_exc()
        conn.rollback()
        return None


# ==============================================================================
# CLASE OCPROCESSOR
# ==============================================================================

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
        print("🤖 Procesador OCR automático iniciado (VERSIÓN MEJORADA)")

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
        Guarda un producto en items_factura usando matching INLINE
        VERSIÓN CORREGIDA: Maneja correctamente precios colombianos (sin decimales)
        """
        try:
            codigo = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()

            # ✅ CORRECCIÓN CRÍTICA: Usar función mejorada de limpieza
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
                # No rechazar, solo advertir

            print(f"   💰 '{nombre}': {precio_raw} → ${precio:,} pesos")

            # ✅ USAR FUNCIÓN INLINE (NO IMPORT)
            producto_maestro_id = buscar_o_crear_producto_inteligente_inline(
                codigo=codigo,
                nombre=nombre,
                precio=precio,
                establecimiento=establecimiento,
                cursor=cursor,
                conn=conn
            )

            if not producto_maestro_id:
                print(f"   ❌ No se pudo obtener producto_maestro_id para: {nombre}")
                return None

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

            print(f"   ✅ Item guardado: ID={item_id}, Producto={producto_maestro_id}, Precio=${precio:,}")
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


print("✅ OCR Processor cargado - VERSIÓN MEJORADA CON PRECIOS COLOMBIANOS")

# Crear instancia global del procesador
processor = OCRProcessor()
