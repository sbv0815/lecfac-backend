"""
============================================================================
SISTEMA DE PROCESAMIENTO AUTOMATICO DE OCR PARA FACTURAS
VERSION 3.3 - CON GUARDADO AUTOMATICO DE PLUs
============================================================================

CAMBIOS EN ESTA VERSION:
- AGREGADO: Guardado automático de PLUs en productos_por_establecimiento
- MEJORADO: Asociación código-establecimiento desde el OCR
- OPTIMIZADO: Un solo escaneo = producto + PLU guardados

ARQUITECTURA:
- productos_maestros: Sistema principal
- items_factura: Items con producto_maestro_id
- productos_por_establecimiento: PLUs por establecimiento (NUEVO)

AUTOR: LecFac Team
ULTIMA ACTUALIZACION: 2025-11-06 (Auto-guardado PLUs)
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

# Importar normalizador de codigos (solo la función de normalización)
from normalizador_codigos import normalizar_codigo_por_establecimiento

# Importar product_matcher (sistema funcional para buscar/crear productos)
try:
    from product_matcher import buscar_o_crear_producto_inteligente as buscar_producto_v2
    PRODUCT_MATCHING_AVAILABLE = True
    print("✅ product_matcher cargado correctamente")
except ImportError as e:
    PRODUCT_MATCHING_AVAILABLE = False
    print(f"❌ product_matcher no disponible: {e}")
    print("   El sistema NO funcionara sin product_matcher")

# Importar detector de duplicados
try:
    from duplicate_detector import detectar_duplicados_automaticamente
    DUPLICATE_DETECTOR_AVAILABLE = True
    print("✅ Detector de duplicados cargado")
except ImportError:
    DUPLICATE_DETECTOR_AVAILABLE = False
    print("⚠️  Detector de duplicados no disponible")

# Colas y tracking globales
ocr_queue = Queue()
processing = {}
error_log = []


def limpiar_precio_colombiano(precio_str) -> int:
    """Convierte precio colombiano a entero (sin decimales)"""
    if precio_str is None or precio_str == "":
        return 0

    if isinstance(precio_str, int):
        return max(0, precio_str)

    if isinstance(precio_str, float):
        if precio_str == int(precio_str):
            return max(0, int(precio_str))
        return max(0, int(precio_str * 100))

    precio_str = str(precio_str).strip()
    precio_str = precio_str.replace(" ", "").replace("$", "").replace("COP", "").replace("cop", "").strip()

    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        precio_str = precio_str.replace(",", "").replace(".", "")
    elif '.' in precio_str or ',' in precio_str:
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        elif len(partes) == 2 and len(partes[1]) <= 2:
            precio_str = precio_str.replace(",", "").replace(".", "")
        else:
            precio_str = precio_str.replace(",", "").replace(".", "")

    try:
        precio = int(float(precio_str))
        if precio < 0:
            print(f"   ⚠️  Precio negativo detectado: {precio}, retornando 0")
            return 0
        return precio
    except (ValueError, TypeError) as e:
        print(f"   ⚠️  No se pudo convertir precio '{precio_str}': {e}")
        return 0


def validar_producto(nombre: str, precio: int, codigo: str = "") -> Tuple[bool, Optional[str]]:
    """Valida que un producto cumpla con los requisitos minimos"""
    if not nombre or nombre.strip() == "":
        return False, "Producto sin nombre"

    if len(nombre.strip()) < 2:
        return False, f"Nombre muy corto: '{nombre}'"

    if precio <= 0:
        return False, f"Precio invalido: ${precio:,}"

    if precio < 10:
        return False, f"Precio muy bajo: ${precio:,} (posible error OCR)"

    if precio > 10_000_000:
        return False, f"Precio sospechosamente alto: ${precio:,} (verificar)"

    nombre_lower = nombre.lower()
    palabras_basura = ['total', 'subtotal', 'iva', 'descuento', 'cambio', 'efectivo']
    if any(palabra in nombre_lower for palabra in palabras_basura):
        return False, f"Nombre parece ser campo de totales: '{nombre}'"

    return True, None


def obtener_o_crear_establecimiento_id(cursor, cadena: str) -> Optional[int]:
    """
    Obtiene el ID del establecimiento basado en la cadena detectada.
    Si no existe, intenta crearlo.
    """
    try:
        # Primero buscar si existe
        cursor.execute("""
            SELECT id FROM establecimientos
            WHERE nombre_normalizado ILIKE %s
               OR nombre ILIKE %s
            LIMIT 1
        """, (cadena, cadena))

        result = cursor.fetchone()
        if result:
            return result[0]

        # Si no existe, crear uno nuevo
        cursor.execute("""
            INSERT INTO establecimientos (nombre, nombre_normalizado)
            VALUES (%s, %s)
            RETURNING id
        """, (cadena, cadena))

        new_id = cursor.fetchone()[0]
        print(f"   📍 Nuevo establecimiento creado: {cadena} (ID: {new_id})")
        return new_id

    except Exception as e:
        print(f"   ⚠️ Error con establecimiento {cadena}: {e}")
        return None

# AGREGAR ESTAS FUNCIONES A ocr_processor.py
# Colocarlas después de las importaciones y antes de la clase OCRProcessor

def clasificar_codigo(codigo: str) -> Tuple[str, str]:
    """
    Clasifica un código como EAN o PLU basándose en su longitud

    Args:
        codigo: Código a clasificar

    Returns:
        tuple: (tipo_codigo, codigo_limpio) donde tipo es 'EAN', 'PLU' o 'DESCONOCIDO'
    """
    if not codigo or not isinstance(codigo, str):
        return 'DESCONOCIDO', ''

    # Limpiar código - solo dígitos
    codigo_limpio = ''.join(filter(str.isdigit, str(codigo)))

    if not codigo_limpio:
        return 'DESCONOCIDO', ''

    longitud = len(codigo_limpio)

    # Clasificación por longitud
    if longitud >= 8:
        return 'EAN', codigo_limpio
    elif 3 <= longitud <= 7:
        return 'PLU', codigo_limpio
    elif longitud < 3:
        return 'CODIGO_MUY_CORTO', codigo_limpio
    else:
        return 'DESCONOCIDO', codigo_limpio


def es_producto_fresco(nombre: str) -> bool:
    """
    Determina si un producto es fresco basándose en el nombre

    Args:
        nombre: Nombre del producto

    Returns:
        bool: True si es producto fresco
    """
    if not nombre:
        return False

    productos_frescos = [
        # Frutas
        'manzana', 'pera', 'naranja', 'limón', 'mandarina', 'toronja',
        'banano', 'plátano', 'fresa', 'mora', 'uva', 'mango', 'papaya',
        'piña', 'sandía', 'melón', 'durazno', 'ciruela', 'kiwi',

        # Verduras
        'tomate', 'cebolla', 'zanahoria', 'papa', 'yuca', 'lechuga',
        'espinaca', 'acelga', 'repollo', 'coliflor', 'brócoli', 'apio',
        'cilantro', 'perejil', 'albahaca', 'pimentón', 'ají', 'pepino',
        'calabaza', 'ahuyama', 'berenjena', 'remolacha', 'rábano',

        # Carnes
        'carne', 'res', 'cerdo', 'pollo', 'pescado', 'mariscos',
        'camarón', 'pulpo', 'calamar', 'chuleta', 'costilla', 'lomo',
        'pechuga', 'muslo', 'alas', 'molida', 'bistec',

        # Lácteos frescos
        'leche', 'yogurt', 'yogur', 'queso', 'crema', 'mantequilla',
        'requesón', 'cuajada', 'suero',

        # Otros frescos
        'huevo', 'pan', 'arepa', 'tortilla', 'fresco', 'fresca',
        'verdura', 'hortaliza', 'fruta', 'orgánico', 'orgánica'
    ]

    nombre_lower = nombre.lower()
    return any(fresco in nombre_lower for fresco in productos_frescos)


def guardar_plu_establecimiento(cursor, conn, producto_maestro_id: int,
                               establecimiento_id: int, codigo_plu: str,
                               precio: int, descripcion: str = None):
    """
    Guarda un PLU en productos_por_establecimiento

    Args:
        cursor: Cursor de base de datos
        conn: Conexión a base de datos
        producto_maestro_id: ID del producto maestro
        establecimiento_id: ID del establecimiento
        codigo_plu: Código PLU
        precio: Precio del producto
        descripcion: Descripción opcional del PLU
    """
    try:
        # Verificar si ya existe
        cursor.execute("""
            SELECT id FROM productos_por_establecimiento
            WHERE producto_maestro_id = %s
              AND establecimiento_id = %s
        """, (producto_maestro_id, establecimiento_id))

        existe = cursor.fetchone()

        if existe:
            # Actualizar
            cursor.execute("""
                UPDATE productos_por_establecimiento
                SET codigo_plu = %s,
                    precio_unitario = %s,
                    descripcion_plu = %s,
                    fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE producto_maestro_id = %s
                  AND establecimiento_id = %s
            """, (codigo_plu, precio, descripcion,
                  producto_maestro_id, establecimiento_id))
            print(f"      📝 PLU actualizado: {codigo_plu}")
        else:
            # Insertar nuevo
            cursor.execute("""
                INSERT INTO productos_por_establecimiento
                (producto_maestro_id, establecimiento_id, codigo_plu,
                 precio_unitario, descripcion_plu)
                VALUES (%s, %s, %s, %s, %s)
            """, (producto_maestro_id, establecimiento_id,
                  codigo_plu, precio, descripcion))
            print(f"      ✅ PLU guardado: {codigo_plu}")

    except Exception as e:
        print(f"      ❌ Error guardando PLU: {e}")
        # No hacer rollback para no perder el producto


# MODIFICACIÓN NECESARIA en _save_product_to_items_factura:
# Buscar la línea donde dice:
#   es_plu = tipo_codigo in ['PLU', 'PLU_NORMALIZADO', 'CODIGO_CORTO']
#
# Y reemplazar toda esa sección por:

# NUEVO CÓDIGO PARA CLASIFICAR Y GUARDAR PLUs:
# ---------------------------------------------
# Clasificar el código
tipo_clasificado, codigo_limpio = clasificar_codigo(codigo_final)
print(f"   🏷️ Clasificación: {tipo_clasificado} ({len(codigo_limpio)} dígitos)")

# Determinar si debe guardarse como PLU
debe_guardar_plu = False

if tipo_clasificado == 'PLU':
    # Es definitivamente un PLU
    debe_guardar_plu = True
    print(f"   📌 PLU detectado: {codigo_limpio}")
elif tipo_clasificado == 'EAN' and cadena.upper() in ['ARA', 'D1']:
    # Ara y D1 a veces usan EANs cortos como PLUs
    if len(codigo_limpio) <= 10:
        debe_guardar_plu = True
        print(f"   📌 {cadena} usa EAN corto como PLU")

# Guardar PLU si corresponde
if debe_guardar_plu and establecimiento_id and producto_maestro_id:
    guardar_plu_establecimiento(
        cursor=cursor,
        conn=conn,
        producto_maestro_id=producto_maestro_id,
        establecimiento_id=establecimiento_id,
        codigo_plu=codigo_limpio,
        precio=precio,
        descripcion=nombre
    )

# Si es un producto fresco, marcarlo
if es_producto_fresco(nombre):
    try:
        cursor.execute("""
            UPDATE productos_maestros
            SET es_producto_fresco = TRUE
            WHERE id = %s
        """, (producto_maestro_id,))
        print(f"   🥬 Marcado como producto fresco")
    except Exception as e:
        print(f"   ⚠️ No se pudo marcar como fresco: {e}")


class OCRProcessor:
    """Procesador automatico de facturas con OCR - Version 3.3 con PLUs"""

    def __init__(self):
        self.is_running = False
        self.processed_count = 0
        self.error_count = 0
        self.success_rate = 100.0
        self.last_processed = None
        self.worker_thread = None

        if not PRODUCT_MATCHING_AVAILABLE:
            print("❌ ADVERTENCIA: product_matching_v2 no esta disponible")
            print("   El sistema NO funcionara sin product_matching_v2")

    def start(self):
        if self.is_running:
            print("⚠️  Procesador ya esta en ejecucion")
            return

        if not PRODUCT_MATCHING_AVAILABLE:
            print("❌ No se puede iniciar: product_matching_v2 no disponible")
            return

        self.is_running = True
        self.worker_thread = threading.Thread(target=self.process_queue, daemon=True)
        self.worker_thread.start()

        print("=" * 80)
        print("🚀 PROCESADOR OCR AUTOMATICO INICIADO")
        print("=" * 80)
        print("VERSION 3.3 - CON GUARDADO AUTOMATICO DE PLUs")
        print("✅ product_matcher integrado")
        print("✅ Normalizacion inteligente de codigos")
        print("✅ Deteccion automatica de duplicados")
        print("✅ Validacion robusta de productos")
        print("✅ Actualizacion automatica de inventario")
        print("✅ NUEVO: Guardado automático de PLUs por establecimiento")
        print("🏪 Soporta: ARA, D1, Exito, Jumbo, Olimpica y mas")
        print("=" * 80)

    def stop(self):
        self.is_running = False
        print("⏹️  Deteniendo procesador OCR...")

    def process_queue(self):
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
        factura_id = task.get('factura_id')
        image_path = task.get('image_path')
        user_id = task.get('user_id', 1)

        if not factura_id or not image_path:
            print(f"❌ Task invalido: {task}")
            return

        try:
            print(f"\n{'='*80}")
            print(f"📄 PROCESANDO FACTURA #{factura_id}")
            print(f"{'='*80}")

            processing[factura_id] = {
                'status': 'processing',
                'started_at': datetime.now()
            }

            if not os.path.exists(image_path):
                raise FileNotFoundError(f"Imagen no encontrada: {image_path}")

            print("🔍 Extrayendo datos con Claude Vision...")
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

                print(f"\n📦 Actualizando inventario del usuario {user_id}...")
                try:
                    actualizar_inventario_desde_factura(factura_id, user_id)
                    print(f"   ✅ Inventario actualizado")
                except Exception as e:
                    print(f"   ⚠️  Error actualizando inventario: {e}")

                print(f"\n💰 Guardando precios historicos...")
                try:
                    resultado_precios = procesar_items_factura_y_guardar_precios(factura_id, user_id)
                    if resultado_precios.get('precios_guardados', 0) > 0:
                        print(f"   ✅ {resultado_precios['precios_guardados']} precios guardados")
                except Exception as e:
                    print(f"   ⚠️  Error guardando precios: {e}")

                self.processed_count += 1
                self.last_processed = datetime.now()

                processing[factura_id] = {
                    'status': 'completed',
                    'completed_at': datetime.now()
                }

                print(f"\n✅ FACTURA #{factura_id} PROCESADA EXITOSAMENTE")
                print(f"{'='*80}\n")

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

                print(f"❌ Error OCR en factura #{factura_id}: {result.get('error')}")

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
        establecimiento = data.get("establecimiento", "Desconocido")
        cadena = detectar_cadena(establecimiento)
        total_factura = limpiar_precio_colombiano(data.get("total", 0))

        print(f"🏪 Establecimiento: {establecimiento} (Cadena: {cadena})")
        print(f"💵 Total factura: ${total_factura:,}")

        # Obtener o crear el ID del establecimiento
        establecimiento_id = obtener_o_crear_establecimiento_id(cursor, cadena)
        if establecimiento_id:
            print(f"   📍 Establecimiento ID: {establecimiento_id}")
        else:
            print(f"   ⚠️ No se pudo obtener ID del establecimiento")

        productos_originales = data.get("productos", [])

        print(f"\n{'='*70}")
        print(f"🧹 LIMPIEZA DE DUPLICADOS")
        print(f"{'='*70}")

        if DUPLICATE_DETECTOR_AVAILABLE and len(productos_originales) > 0:
            productos_para_detector = []
            for prod in productos_originales:
                productos_para_detector.append({
                    "codigo": prod.get("codigo", ""),
                    "nombre": prod.get("nombre", ""),
                    "valor": limpiar_precio_colombiano(prod.get("precio", 0)),
                    "cantidad": prod.get("cantidad", 1)
                })

            resultado_limpieza = detectar_duplicados_automaticamente(
                productos=productos_para_detector,
                total_factura=total_factura,
                umbral_similitud=0.85,
                tolerancia_total=0.15
            )

            if resultado_limpieza["duplicados_detectados"]:
                print(f"📊 Productos originales: {len(productos_originales)}")
                print(f"✅ Productos limpios: {len(resultado_limpieza['productos_limpios'])}")
                print(f"🗑️  Duplicados eliminados: {len(resultado_limpieza['productos_eliminados'])}")

                for prod_eliminado in resultado_limpieza["productos_eliminados"]:
                    print(f"   • {prod_eliminado['nombre'][:40]} (${prod_eliminado['valor']:,})")
                    print(f"      Razon: {prod_eliminado['razon']}")
            else:
                print(f"✅ No se detectaron duplicados ({len(productos_originales)} productos)")

            productos_a_procesar = []
            for prod_limpio in resultado_limpieza["productos_limpios"]:
                productos_a_procesar.append({
                    "codigo": prod_limpio.get("codigo", ""),
                    "nombre": prod_limpio.get("nombre", ""),
                    "precio": prod_limpio.get("valor", 0),
                    "cantidad": prod_limpio.get("cantidad", 1)
                })
        else:
            productos_a_procesar = productos_originales
            print(f"⚠️  Detector de duplicados no disponible")
            print(f"📦 Procesando {len(productos_originales)} productos sin filtrar")

        print(f"{'='*70}\n")

        print(f"{'='*70}")
        print(f"⚙️  PROCESAMIENTO DE PRODUCTOS")
        print(f"{'='*70}\n")

        productos_guardados = 0
        productos_rechazados = 0
        errores_detalle = []

        for idx, product in enumerate(productos_a_procesar, 1):
            nombre_producto = product.get('nombre', 'SIN NOMBRE')[:50]
            print(f"[{idx}/{len(productos_a_procesar)}] 🔄 Procesando: {nombre_producto}")

            item_id = self._save_product_to_items_factura(
                cursor, conn, product, factura_id, user_id,
                establecimiento, cadena, establecimiento_id
            )

            if item_id:
                productos_guardados += 1
                print(f"   ✅ Guardado (Item ID: {item_id})")
            else:
                productos_rechazados += 1
                print(f"   ❌ Rechazado")
                errores_detalle.append({
                    'nombre': nombre_producto,
                    'codigo': product.get('codigo', ''),
                    'precio': product.get('precio', 0)
                })

        print(f"\n{'='*70}")
        print(f"📊 RESUMEN DE PROCESAMIENTO")
        print(f"{'='*70}")
        print(f"✅ Productos guardados: {productos_guardados}")
        print(f"❌ Productos rechazados: {productos_rechazados}")

        if productos_rechazados > 0 and errores_detalle:
            print(f"\n🚫 Productos rechazados:")
            for error in errores_detalle[:5]:
                print(f"   • {error['nombre'][:40]} (${error['precio']:,})")

        print(f"{'='*70}\n")

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
        cadena: str,
        establecimiento_id: Optional[int] = None
    ) -> Optional[int]:
        """
        VERSION 3.3 - Con guardado automático de PLUs

        Flujo mejorado:
        1. Validar producto
        2. Normalizar codigo
        3. Buscar/crear en productos_maestros
        4. Guardar en items_factura
        5. NUEVO: Guardar PLU en productos_por_establecimiento
        """
        try:
            codigo_raw = str(product.get("codigo", "")).strip()
            nombre = str(product.get("nombre", "")).strip()
            precio_raw = product.get("precio", 0)
            precio = limpiar_precio_colombiano(precio_raw)
            cantidad = int(product.get("cantidad", 1))

            # PASO 1: Validar producto
            es_valido, razon_rechazo = validar_producto(nombre, precio, codigo_raw)

            if not es_valido:
                print(f"   ⚠️  RECHAZADO: {razon_rechazo}")
                return None

            # PASO 2: Normalizar codigo
            codigo, tipo_codigo, confianza = normalizar_codigo_por_establecimiento(
                codigo_raw, establecimiento
            )

            print(f"   💰 ${precio:,} x{cantidad}")
            if codigo_raw != codigo and codigo:
                print(f"   🔄 Codigo normalizado: {codigo_raw} -> {codigo} ({tipo_codigo})")
            else:
                print(f"   🔖 Codigo: {codigo or 'SIN CODIGO'} ({tipo_codigo})")

            # PASO 3: Buscar/crear producto maestro
            if not PRODUCT_MATCHING_AVAILABLE:
                print(f"   ❌ product_matching_v2 no disponible")
                return None

            try:
                codigo_final = codigo if codigo else codigo_raw if codigo_raw else ""

                # Llamar a product_matcher
                producto_maestro_id = buscar_producto_v2(
                    codigo=codigo_final,
                    nombre=nombre,
                    precio=precio,
                    establecimiento=establecimiento,
                    cursor=cursor,
                    conn=conn
                )

                if not producto_maestro_id:
                    print(f"   ❌ No se pudo obtener producto_maestro_id")
                    return None

                print(f"   ✅ Producto Maestro ID: {producto_maestro_id}")

            except Exception as e:
                print(f"   ❌ Error en product_matcher: {e}")
                traceback.print_exc()
                return None

            # PASO 4: Guardar en items_factura
            try:
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
                    codigo_final if codigo_final else None,
                    nombre,
                    precio,
                    cantidad,
                    confianza
                ))

                item_id = cursor.fetchone()[0]

                # PASO 5: NUEVO - Guardar PLU en productos_por_establecimiento
                if codigo_final and establecimiento_id and producto_maestro_id:
                    try:
                        # Determinar si es PLU o EAN
                        es_plu = tipo_codigo in ['PLU', 'PLU_NORMALIZADO', 'CODIGO_CORTO']

                        # Para Ara y Jumbo, el EAN también puede ser el PLU
                        if cadena.upper() in ['ARA', 'JUMBO'] and tipo_codigo == 'EAN':
                            es_plu = True
                            print(f"   📌 {cadena} usa EAN como PLU")

                        if es_plu or tipo_codigo == 'EAN':
                            cursor.execute("""
                                INSERT INTO productos_por_establecimiento
                                    (producto_maestro_id, establecimiento_id, codigo_plu, precio_unitario, fecha_creacion)
                                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                                ON CONFLICT (producto_maestro_id, establecimiento_id)
                                DO UPDATE SET
                                    codigo_plu = EXCLUDED.codigo_plu,
                                    precio_unitario = EXCLUDED.precio_unitario,
                                    fecha_actualizacion = CURRENT_TIMESTAMP
                            """, (producto_maestro_id, establecimiento_id, codigo_final, precio))

                            print(f"   📌 PLU guardado automáticamente: {codigo_final} en {cadena}")

                    except Exception as e:
                        print(f"   ⚠️ No se pudo guardar PLU automático: {e}")
                        # No hacer rollback, el producto ya está guardado

                return item_id

            except Exception as e:
                print(f"   ❌ Error insertando en items_factura: {e}")
                traceback.print_exc()
                conn.rollback()
                return None

        except Exception as e:
            print(f"   ❌ Error guardando producto: {e}")
            traceback.print_exc()
            conn.rollback()
            return None

    def _process_failed_ocr(self, cursor, factura_id: int, error: str):
        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'error_ocr',
                notas = %s,
                fecha_procesamiento = CURRENT_TIMESTAMP
            WHERE id = %s
        """, (f"Error OCR: {error}", factura_id))

    def get_stats(self) -> Dict[str, Any]:
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


print("=" * 80)
print("🚀 OCR PROCESSOR V3.3 CARGADO - CON GUARDADO AUTOMATICO DE PLUs")
print("=" * 80)
print("✅ Sistema simplificado con product_matcher")
print("✅ Normalizacion inteligente de codigos: OK")
print("✅ Deteccion automatica de duplicados: OK" if DUPLICATE_DETECTOR_AVAILABLE else "⚠️  Deteccion automatica de duplicados: NO")
print("✅ product_matcher (sistema funcional): OK" if PRODUCT_MATCHING_AVAILABLE else "❌ product_matcher: NO")
print("✅ Validacion robusta de precios: OK")
print("✅ Actualizacion automatica de inventario: OK")
print("✅ NUEVO: Guardado automático de PLUs por establecimiento")
print("🏪 Soporta: ARA, D1, Exito, Jumbo, Olimpica, Carulla, y mas")
print("=" * 80)

processor = OCRProcessor()
