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


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Deduplicación ULTRA MEJORADA con validaciones estrictas

    Prioriza:
    1. Códigos EAN/PLU válidos
    2. Nombres descriptivos (no genéricos)
    3. Precios razonables
    """
    from difflib import SequenceMatcher
    import re

    if not productos:
        return []

    logger.info(
        f"🔍 Iniciando deduplicación ULTRA MEJORADA de {len(productos)} productos..."
    )

    # ========================================
    # FUNCIONES AUXILIARES
    # ========================================

    def es_codigo_valido(codigo: str) -> bool:
        """Valida si un código es realmente un código de producto"""
        if not codigo or not isinstance(codigo, str):
            return False

        codigo = codigo.strip()

        # Muy corto
        if len(codigo) < 4:
            return False

        # Muy largo (probablemente error)
        if len(codigo) > 14:
            return False

        # Debe ser numérico
        if not codigo.isdigit():
            return False

        # Códigos repetitivos (666666, 777777, etc.)
        digitos_unicos = len(set(codigo))
        if digitos_unicos == 1:  # Todos los dígitos iguales
            return False

        # Códigos con más del 70% del mismo dígito
        if digitos_unicos <= 2 and len(codigo) >= 6:
            contador_max = max(codigo.count(d) for d in set(codigo))
            if contador_max / len(codigo) > 0.7:
                return False

        return True

    def es_nombre_generico(nombre: str) -> bool:
        """Detecta nombres genéricos que NO son productos reales"""
        nombre_upper = nombre.upper().strip()

        # Nombres muy cortos
        if len(nombre_upper) < 3:
            return True

        # Unidades de medida
        unidades = ["KGM", "KG", "/KGM", "/KG", "UND", "UNIDADES", "/U", "1/U"]
        if nombre_upper in unidades:
            return True

        # Códigos internos
        codigos_internos = ["PLU", "SKU", "COD", "REF"]
        if nombre_upper in codigos_internos:
            return True

        # Solo contiene unidades de medida y números
        if re.match(r"^[\d\s/KGM]+$", nombre_upper):
            return True

        # Nombres que son solo ratios (875/KGM, 220/KGM, etc.)
        if re.match(r"^\d+/\w+$", nombre_upper):
            return True

        return False

    def es_precio_valido(precio: float, nombre: str = "") -> bool:
        """Valida si un precio es razonable"""
        # Precio debe ser positivo
        if precio <= 0:
            return False

        # Precios sospechosamente bajos (< $100)
        # Estos suelen ser errores o líneas de descuento
        if precio < 100:
            return False

        # Precios extremadamente altos (> $500,000)
        # Probablemente error de lectura
        if precio > 500000:
            return False

        return True

    # ========================================
    # PASO 1: FILTRAR LÍNEAS NO-PRODUCTO (MEJORADO)
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
        "v ahorro",
        "vahorro",
        "ahorro v",
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
        # Unidades de medida (NO son productos)
        "kgm",
        "/kgm",
        "kg/",
        "/kg",
        "und",
        "unidades",
        # Información de tienda
        "establecimiento",
        "fecha",
        "hora",
        "cajero",
        "caja",
        "gracias",
        "vuelva pronto",
        "nit",
        "autoretenedor",
        "toshiba",
        "global commerce",
        "cadena s.a",
        "cadena s.r",
        "cadena",
        "commerce solutions",
        # Términos administrativos
        "rpad",
        "sito la colina",
        "sitio la colina",
        "bito la colina",
    ]

    productos_validos = []
    lineas_filtradas = 0
    razones_filtrado = {}

    for prod in productos:
        nombre = str(prod.get("nombre", "")).lower().strip()
        codigo = str(prod.get("codigo", "")).strip()
        precio = prod.get("precio", 0)

        razon = None

        # ❌ FILTRO 1: Palabras de descuento/pago/administrativa
        if any(palabra in nombre for palabra in palabras_filtro):
            razon = "palabra_filtro"
            lineas_filtradas += 1
            razones_filtrado[razon] = razones_filtrado.get(razon, 0) + 1
            continue

        # ❌ FILTRO 2: Nombres genéricos
        if es_nombre_generico(nombre):
            razon = "nombre_generico"
            lineas_filtradas += 1
            razones_filtrado[razon] = razones_filtrado.get(razon, 0) + 1
            continue

        # ❌ FILTRO 3: Solo números o caracteres especiales
        if re.match(r"^[\d\s\-\./]+$", nombre):
            razon = "solo_numeros"
            lineas_filtradas += 1
            razones_filtrado[razon] = razones_filtrado.get(razon, 0) + 1
            continue

        # ❌ FILTRO 4: Precio inválido
        if not es_precio_valido(precio, nombre):
            razon = "precio_invalido"
            lineas_filtradas += 1
            razones_filtrado[razon] = razones_filtrado.get(razon, 0) + 1
            continue

        # ✅ Producto válido
        productos_validos.append(prod)

    logger.info(f"   🗑️ Paso 1: {lineas_filtradas} líneas no-producto eliminadas")
    if razones_filtrado:
        for razon, count in razones_filtrado.items():
            logger.info(f"      - {razon}: {count}")

    productos = productos_validos

    # ========================================
    # PASO 2: AGRUPAR POR CÓDIGO VÁLIDO (MÁXIMA PRIORIDAD)
    # ========================================
    productos_con_codigo = {}
    productos_sin_codigo = []

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        precio = prod.get("precio", 0)

        # ✅ TIENE CÓDIGO VÁLIDO
        if es_codigo_valido(codigo):
            if codigo not in productos_con_codigo:
                # Primera vez que vemos este código
                productos_con_codigo[codigo] = prod
            else:
                # Ya existe este código - mantener el mejor
                prod_existente = productos_con_codigo[codigo]

                # Criterio 1: Nombre más largo y descriptivo
                if len(nombre) > len(prod_existente.get("nombre", "")):
                    productos_con_codigo[codigo] = prod
                # Criterio 2: Si igual longitud, mantener precio más confiable
                elif len(nombre) == len(prod_existente.get("nombre", "")):
                    if abs(precio - prod_existente.get("precio", 0)) < 100:
                        # Precios similares, mantener el más alto
                        if precio > prod_existente.get("precio", 0):
                            productos_con_codigo[codigo] = prod
        else:
            # Sin código válido - requiere análisis por nombre
            productos_sin_codigo.append(prod)

    logger.info(
        f"   ✅ Paso 2: {len(productos_con_codigo)} productos con código válido"
    )
    logger.info(
        f"   🔍 Paso 3: {len(productos_sin_codigo)} sin código (analizando por nombre...)"
    )

    # ========================================
    # PASO 3: DEDUPLICAR SIN CÓDIGO (MÁS ESTRICTO)
    # ========================================

    def normalizar_para_comparacion(texto: str) -> str:
        """Normaliza texto para comparación más flexible"""
        texto = texto.upper().strip()
        # Remover caracteres especiales
        texto = re.sub(r"[^\w\s]", "", texto)
        # Remover espacios múltiples
        texto = re.sub(r"\s+", " ", texto)
        # Remover números y unidades al final
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
                precios_similares = (
                    diff_precio < 0.03
                )  # Máximo 3% de diferencia (más estricto)
            else:
                precios_similares = False

            # ✅ CRITERIO DE DUPLICADO (MÁS ESTRICTO):
            # Opción A: 90%+ de similitud en nombre (antes era 85%)
            # Opción B: 80%+ de similitud Y precios casi idénticos
            if similitud >= 0.90:
                es_duplicado = True
                # Mantener el nombre más completo
                if len(nombre) > len(nombre_existente):
                    prod_existente["nombre"] = nombre
                break
            elif similitud >= 0.80 and precios_similares:
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
    # PASO 5: VALIDACIÓN FINAL ESTRICTA
    # ========================================
    resultado_final = []
    productos_rechazados = 0

    for prod in resultado:
        nombre = str(prod.get("nombre", "")).strip()
        codigo = str(prod.get("codigo", "")).strip()
        precio = prod.get("precio", 0)

        # Validaciones finales
        validaciones_ok = True

        # Validación 1: Nombre descriptivo
        if not nombre or len(nombre) < 4 or es_nombre_generico(nombre):
            validaciones_ok = False

        # Validación 2: Precio válido
        if not es_precio_valido(precio, nombre):
            validaciones_ok = False

        # Validación 3: Si tiene código, debe ser válido
        if codigo and not es_codigo_valido(codigo):
            # Código inválido - limpiar pero mantener producto si es bueno
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
    logger.info(f"   🔢 Con código válido: {len(productos_con_codigo)}")
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
