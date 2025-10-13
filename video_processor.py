"""
Procesador de videos - SISTEMA OPTIMIZADO PARA PRECISIÓN
Extrae más frames y asegura capturar el FINAL de la factura
"""

import cv2
import tempfile
import os
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def extraer_frames_video(video_path, intervalo=1.0):
    """
    Extrae frames de un video de manera OPTIMIZADA PARA MÁXIMA PRECISIÓN

    Args:
        video_path: Ruta al archivo de video
        intervalo: No se usa (parámetro legacy)

    Returns:
        Lista de rutas a los frames extraídos
    """
    import cv2
    import os

    frames = []

    try:
        cap = cv2.VideoCapture(video_path)

        if not cap.isOpened():
            print(f"❌ Error: No se pudo abrir el video {video_path}")
            return frames

        # Obtener propiedades del video
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # ⭐ CALCULAR DURACIÓN
        duracion = total_frames / fps if fps > 0 else 0

        print(
            f"📹 Video: {duracion:.1f}s, {fps:.1f} FPS, {total_frames} frames totales"
        )

        # 🎯 SISTEMA MEJORADO - MÁS FRAMES = MEJOR PRECISIÓN
        if duracion <= 10:
            max_frames = 12  # Videos cortos: 12 frames (era 4)
        elif duracion <= 20:
            max_frames = 18  # Videos medianos: 18 frames (era 6)
        elif duracion <= 30:
            max_frames = 25  # Videos largos: 25 frames (era 8)
        else:
            max_frames = 30  # Videos muy largos: 30 frames (era 10)

        print(f"🎯 Extraeremos {max_frames} frames para máxima precisión")

        # Calcular intervalo entre frames
        if duracion > 0:
            frame_interval = max(1, total_frames // max_frames)
        else:
            frame_interval = 1

        frame_count = 0
        extracted_count = 0

        # PRIMERA PASADA: Frames uniformemente distribuidos
        while cap.isOpened() and extracted_count < max_frames:
            ret, frame = cap.read()

            if not ret:
                break

            # Extraer frame cada 'frame_interval' frames
            if frame_count % frame_interval == 0:
                # Guardar frame como imagen temporal
                frame_filename = f"/tmp/frame_{os.getpid()}_{extracted_count:03d}.jpg"
                cv2.imwrite(frame_filename, frame)
                frames.append(frame_filename)
                extracted_count += 1

                print(f"  ✓ Frame {extracted_count}/{max_frames} extraído")

            frame_count += 1

        cap.release()

        # 🎯 CRÍTICO: SEGUNDA PASADA - Asegurar frames del FINAL
        print(f"🎯 Extrayendo frames FINALES para capturar el TOTAL...")

        cap = cv2.VideoCapture(video_path)

        # Calcular posiciones de los últimos 5 frames
        ultimos_frames_posiciones = [
            total_frames - 1,  # Último frame
            total_frames - int(fps * 0.5),  # 0.5s antes del final
            total_frames - int(fps * 1),  # 1s antes del final
            total_frames - int(fps * 1.5),  # 1.5s antes del final
            total_frames - int(fps * 2),  # 2s antes del final
        ]

        # Filtrar posiciones válidas
        ultimos_frames_posiciones = [
            pos for pos in ultimos_frames_posiciones if 0 <= pos < total_frames
        ]

        # Extraer últimos frames
        for i, pos in enumerate(ultimos_frames_posiciones):
            cap.set(cv2.CAP_PROP_POS_FRAMES, pos)
            ret, frame = cap.read()

            if ret:
                frame_filename = f"/tmp/frame_{os.getpid()}_final_{i:03d}.jpg"
                cv2.imwrite(frame_filename, frame)
                frames.append(frame_filename)
                print(
                    f"  ✓ Frame FINAL {i+1}/{len(ultimos_frames_posiciones)} extraído (frame #{pos})"
                )

        cap.release()

        print(f"✅ {len(frames)} frames extraídos exitosamente")
        print(f"   - Frames regulares: {max_frames}")
        print(f"   - Frames finales: {len(ultimos_frames_posiciones)}")

        return frames

    except Exception as e:
        print(f"❌ Error extrayendo frames: {e}")
        import traceback

        traceback.print_exc()
        return frames


"""
REEMPLAZAR la función deduplicar_productos() en video_processor.py
"""


"""
DEDUPLICACIÓN OPTIMIZADA - PRIORIZA: Código, Nombre, Precio
Reemplazar en video_processor.py
"""


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Deduplicación OPTIMIZADA que prioriza:
    1. Códigos únicos (EAN/PLU)
    2. Nombres sin duplicados
    3. Precios correctos
    """
    from difflib import SequenceMatcher
    import re

    if not productos:
        return []

    logger.info(
        f"🔍 Iniciando deduplicación OPTIMIZADA de {len(productos)} productos..."
    )

    # ========================================
    # PASO 1: FILTRAR LÍNEAS NO-PRODUCTO
    # ========================================
    palabras_filtro = [
        # Descuentos y promociones
        "ahorro",
        "descuento",
        "desc",
        "dto",
        "rebaja",
        "promocion",
        "promo",
        "oferta",
        "2x1",
        "3x2",
        "dcto",
        "precio final",
        # Información de pago
        "iva",
        "impuesto",
        "subtotal",
        "total",
        "cambio",
        "efectivo",
        "tarjeta",
        "redeban",
        "credito",
        "debito",
        "visa",
        "mastercard",
        # Otros
        "establecimiento",
        "fecha",
        "hora",
        "cajero",
        "caja",
        "gracias",
        "vuelva pronto",
        "nit",
    ]

    productos_validos = []
    lineas_filtradas = 0

    for prod in productos:
        nombre = str(prod.get("nombre", "")).lower().strip()
        codigo = str(prod.get("codigo", "")).strip()
        precio = prod.get("precio", 0)

        # ❌ FILTRO 1: Palabras de descuento/pago
        if any(palabra in nombre for palabra in palabras_filtro):
            lineas_filtradas += 1
            continue

        # ❌ FILTRO 2: Nombres muy cortos (ruido)
        if len(nombre) < 3:
            lineas_filtradas += 1
            continue

        # ❌ FILTRO 3: Solo números (no es nombre de producto)
        if re.match(r"^[\d\s\-\.]+$", nombre):
            lineas_filtradas += 1
            continue

        # ❌ FILTRO 4: Precio cero o negativo (error de OCR)
        if precio <= 0:
            lineas_filtradas += 1
            continue

        # ✅ Producto válido
        productos_validos.append(prod)

    logger.info(f"   🗑️ Paso 1: {lineas_filtradas} líneas no-producto eliminadas")
    productos = productos_validos

    # ========================================
    # PASO 2: AGRUPAR POR CÓDIGO (MÁXIMA PRIORIDAD)
    # ========================================
    productos_con_codigo = {}
    productos_sin_codigo = []

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        precio = prod.get("precio", 0)

        # ✅ TIENE CÓDIGO VÁLIDO (6+ dígitos)
        if codigo and len(codigo) >= 6 and codigo.isdigit():

            if codigo not in productos_con_codigo:
                # Primera vez que vemos este código
                productos_con_codigo[codigo] = prod
            else:
                # Ya existe este código - mantener el mejor
                prod_existente = productos_con_codigo[codigo]

                # Criterio 1: Nombre más largo (más completo)
                if len(nombre) > len(prod_existente.get("nombre", "")):
                    productos_con_codigo[codigo] = prod
                # Criterio 2: Si igual longitud, mantener precio más alto (más confiable)
                elif len(nombre) == len(prod_existente.get("nombre", "")):
                    if precio > prod_existente.get("precio", 0):
                        productos_con_codigo[codigo] = prod

        else:
            # Sin código válido - requiere análisis por nombre
            productos_sin_codigo.append(prod)

    logger.info(f"   ✅ Paso 2: {len(productos_con_codigo)} productos con código único")
    logger.info(
        f"   🔍 Paso 3: {len(productos_sin_codigo)} sin código (analizando por nombre...)"
    )

    # ========================================
    # PASO 3: DEDUPLICAR SIN CÓDIGO (POR NOMBRE + PRECIO)
    # ========================================

    def normalizar_para_comparacion(texto: str) -> str:
        """Normaliza texto para comparación más flexible"""
        # Convertir a mayúsculas
        texto = texto.upper().strip()
        # Remover caracteres especiales comunes en OCR malo
        texto = re.sub(r"[^\w\s]", "", texto)
        # Remover espacios múltiples
        texto = re.sub(r"\s+", " ", texto)
        # Remover números al final (peso/tamaño) para mejor matching
        # Ej: "ARROZ DIANA 500G" y "ARROZ DIANA 5OOG" se vuelven "ARROZ DIANA"
        texto = re.sub(r"\s+\d+[A-Z]*$", "", texto)
        return texto

    productos_unicos_sin_codigo = []

    for prod in productos_sin_codigo:
        nombre = str(prod.get("nombre", "")).strip()
        precio = prod.get("precio", 0)

        nombre_normalizado = normalizar_para_comparacion(nombre)

        # Buscar si ya existe un producto similar
        es_duplicado = False

        for prod_existente in productos_unicos_sin_codigo:
            nombre_existente = str(prod_existente.get("nombre", "")).strip()
            nombre_existente_normalizado = normalizar_para_comparacion(nombre_existente)
            precio_existente = prod_existente.get("precio", 0)

            # Calcular similitud de nombre
            similitud = SequenceMatcher(
                None, nombre_normalizado, nombre_existente_normalizado
            ).ratio()

            # Calcular similitud de precio
            if precio > 0 and precio_existente > 0:
                diff_precio = abs(precio - precio_existente) / max(
                    precio, precio_existente
                )
                precios_similares = diff_precio < 0.05  # Máximo 5% de diferencia
            else:
                precios_similares = False

            # ✅ CRITERIO DE DUPLICADO:
            # Opción A: 85%+ de similitud en nombre
            # Opción B: 70%+ de similitud Y precios casi idénticos
            if similitud >= 0.85:
                es_duplicado = True
                # Actualizar con el nombre más completo
                if len(nombre) > len(nombre_existente):
                    prod_existente["nombre"] = nombre
                break
            elif similitud >= 0.70 and precios_similares:
                es_duplicado = True
                if len(nombre) > len(nombre_existente):
                    prod_existente["nombre"] = nombre
                break

        # Si no es duplicado, agregarlo
        if not es_duplicado:
            productos_unicos_sin_codigo.append(prod)

    # ========================================
    # PASO 4: COMBINAR RESULTADOS
    # ========================================
    resultado = list(productos_con_codigo.values()) + productos_unicos_sin_codigo

    # ========================================
    # PASO 5: VALIDACIÓN FINAL DE CALIDAD
    # ========================================
    resultado_final = []
    productos_rechazados = 0

    for prod in resultado:
        nombre = str(prod.get("nombre", "")).strip()
        codigo = str(prod.get("codigo", "")).strip()
        precio = prod.get("precio", 0)

        # Validaciones finales
        validaciones_ok = True

        # Validación 1: Nombre no vacío
        if not nombre or len(nombre) < 3:
            validaciones_ok = False

        # Validación 2: Precio válido
        if precio <= 0:
            validaciones_ok = False

        # Validación 3: Si tiene código, debe ser numérico y >= 6 dígitos
        if codigo and (not codigo.isdigit() or len(codigo) < 6):
            # Código inválido - limpiar pero mantener producto
            prod["codigo"] = ""

        if validaciones_ok:
            resultado_final.append(prod)
        else:
            productos_rechazados += 1

    # ========================================
    # ESTADÍSTICAS FINALES
    # ========================================
    total_original = len(productos) + lineas_filtradas
    duplicados_eliminados = len(productos) - len(resultado_final)

    logger.info(f"=" * 70)
    logger.info(f"✅ DEDUPLICACIÓN COMPLETADA")
    logger.info(f"   📊 Productos detectados: {total_original}")
    logger.info(f"   🗑️  Líneas filtradas: {lineas_filtradas}")
    logger.info(f"   ✅ Productos únicos: {len(resultado_final)}")
    logger.info(f"   🔢 Con código EAN/PLU: {len(productos_con_codigo)}")
    logger.info(f"   📝 Sin código (por nombre): {len(productos_unicos_sin_codigo)}")
    logger.info(f"   ❌ Rechazados (calidad): {productos_rechazados}")
    logger.info(
        f"   🎯 Tasa de precisión: {(len(resultado_final) / max(len(productos), 1) * 100):.1f}%"
    )
    logger.info(f"=" * 70)

    # Log de productos finales (para debugging)
    logger.info(f"📦 PRODUCTOS FINALES:")
    for i, prod in enumerate(resultado_final, 1):
        codigo = prod.get("codigo", "SIN CÓDIGO")
        nombre = prod.get("nombre", "")
        precio = prod.get("precio", 0)
        logger.info(f"   {i}. [{codigo}] {nombre} - ${precio:,.0f}")

    return resultado_final


def limpiar_frames_temporales(frames_paths: List[str]):
    """Elimina archivos temporales de forma segura"""
    eliminados = 0
    errores = 0

    for frame_path in frames_paths:
        try:
            if os.path.exists(frame_path):
                os.remove(frame_path)
                eliminados += 1
        except Exception as e:
            errores += 1
            logger.warning(f"⚠️ Error limpiando {frame_path}: {e}")

    if eliminados > 0:
        logger.info(f"🧹 {eliminados} frames temporales eliminados")
    if errores > 0:
        logger.warning(f"⚠️ {errores} archivos no se pudieron eliminar")


def combinar_frames_vertical(
    frames_paths: List[str], output_path: str = None, max_height: int = 15000
) -> str:
    """
    Combina múltiples frames en una sola imagen vertical

    Args:
        frames_paths: Lista de rutas a las imágenes de frames
        output_path: Ruta donde guardar la imagen combinada (opcional)
        max_height: Altura máxima permitida en píxeles (default: 15000)

    Returns:
        Ruta al archivo de imagen combinada
    """
    if not frames_paths:
        raise ValueError("No hay frames para combinar")

    try:
        # Leer todos los frames
        images = []
        for path in frames_paths:
            if os.path.exists(path):
                img = cv2.imread(path)
                if img is not None:
                    images.append(img)

        if not images:
            raise ValueError("No se pudieron leer los frames")

        # Obtener ancho máximo
        max_width = max(img.shape[1] for img in images)

        # Redimensionar todas las imágenes al mismo ancho
        resized_images = []
        for img in images:
            if img.shape[1] != max_width:
                height = int(img.shape[0] * max_width / img.shape[1])
                img = cv2.resize(img, (max_width, height))
            resized_images.append(img)

        # Calcular altura total
        total_height = sum(img.shape[0] for img in resized_images)

        # Si excede el máximo, redimensionar proporcionalmente
        if total_height > max_height:
            scale = max_height / total_height
            resized_images = [
                cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)))
                for img in resized_images
            ]
            total_height = sum(img.shape[0] for img in resized_images)
            max_width = int(max_width * scale)

        # Crear imagen combinada
        combined = np.vstack(resized_images)

        # Guardar
        if output_path is None:
            output_path = f"/tmp/combined_{os.getpid()}.jpg"

        cv2.imwrite(output_path, combined, [cv2.IMWRITE_JPEG_QUALITY, 85])

        logger.info(
            f"✅ Imagen combinada: {len(images)} frames, {total_height}px altura"
        )

        return output_path

    except Exception as e:
        logger.error(f"❌ Error combinando frames: {e}")
        # Fallback: devolver el primer frame
        if frames_paths and os.path.exists(frames_paths[0]):
            return frames_paths[0]
        raise
