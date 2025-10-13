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


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Deduplicación MEJORADA con análisis inteligente
    """
    from difflib import SequenceMatcher
    import re

    if not productos:
        return []

    logger.info(f"🔍 Iniciando deduplicación de {len(productos)} productos...")

    # ========================================
    # NIVEL 1: Filtrar descuentos e IVA
    # ========================================
    palabras_descuento = [
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
    ]

    productos_limpios = []
    lineas_filtradas = 0

    for prod in productos:
        nombre = str(prod.get("nombre", "")).lower().strip()

        # Filtrar por palabras clave
        es_basura = any(palabra in nombre for palabra in palabras_descuento)

        # Filtrar nombres muy cortos
        if len(nombre) < 3:
            es_basura = True

        # Filtrar si solo tiene números y espacios
        if re.match(r"^[\d\s]+$", nombre):
            es_basura = True

        if not es_basura:
            productos_limpios.append(prod)
        else:
            lineas_filtradas += 1

    if lineas_filtradas > 0:
        logger.info(f"   🗑️ Nivel 1: {lineas_filtradas} líneas basura eliminadas")

    productos = productos_limpios

    # ========================================
    # NIVEL 2: Deduplicar por código + nombre normalizado
    # ========================================

    def normalizar_nombre(nombre: str) -> str:
        """Normaliza nombre para mejor comparación"""
        nombre = nombre.upper().strip()
        # Remover espacios múltiples
        nombre = re.sub(r"\s+", " ", nombre)
        # Remover caracteres especiales comunes en OCR
        nombre = re.sub(r"[^\w\s]", "", nombre)
        return nombre

    def extraer_palabras_clave(nombre: str) -> set:
        """Extrae palabras significativas del nombre"""
        nombre_norm = normalizar_nombre(nombre)
        palabras = nombre_norm.split()
        # Filtrar palabras muy cortas
        return set(p for p in palabras if len(p) >= 3)

    productos_unicos = {}
    productos_sin_codigo = []

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        precio = prod.get("precio", 0)

        if not nombre:
            continue

        # ========================================
        # ESTRATEGIA 1: Código EAN/PLU confiable
        # ========================================
        if codigo and len(codigo) >= 6 and codigo.isdigit():
            # Normalizar nombre para comparación
            nombre_norm = normalizar_nombre(nombre)

            # Buscar si ya existe este código
            encontrado = False
            for key in list(productos_unicos.keys()):
                if key.startswith(f"cod_{codigo}_"):
                    # Ya existe este código, verificar si es el mismo producto
                    prod_existente = productos_unicos[key]
                    nombre_existente_norm = normalizar_nombre(
                        prod_existente.get("nombre", "")
                    )

                    # Calcular similitud
                    similitud = SequenceMatcher(
                        None, nombre_norm, nombre_existente_norm
                    ).ratio()

                    # Si son muy similares (>70%), es duplicado
                    if similitud > 0.70:
                        # Mantener el nombre más completo
                        if len(nombre) > len(prod_existente.get("nombre", "")):
                            productos_unicos[key] = prod
                        encontrado = True
                        break

            if not encontrado:
                # Crear clave única con código y hash del nombre
                key = f"cod_{codigo}_{hash(nombre_norm) % 10000}"
                productos_unicos[key] = prod

        # Sin código confiable - analizar por nombre
        else:
            productos_sin_codigo.append(prod)

    logger.info(f"   ✅ Nivel 2: {len(productos_unicos)} productos con código único")
    logger.info(
        f"   🔍 Nivel 3: {len(productos_sin_codigo)} sin código (analizando...)"
    )

    # ========================================
    # NIVEL 3: Deduplicar por similitud de nombre + precio
    # ========================================
    for prod in productos_sin_codigo:
        nombre = str(prod.get("nombre", "")).strip()
        precio = prod.get("precio", 0)

        if not nombre or len(nombre) < 3:
            continue

        nombre_norm = normalizar_nombre(nombre)
        palabras_clave = extraer_palabras_clave(nombre)

        encontrado = False

        # Comparar con productos existentes
        for key, prod_existente in list(productos_unicos.items()):
            nombre_existente = str(prod_existente.get("nombre", "")).strip()
            nombre_existente_norm = normalizar_nombre(nombre_existente)
            palabras_clave_existente = extraer_palabras_clave(nombre_existente)
            precio_existente = prod_existente.get("precio", 0)

            # ✅ Estrategia 1: Similitud de texto alta
            similitud_texto = SequenceMatcher(
                None, nombre_norm, nombre_existente_norm
            ).ratio()

            # ✅ Estrategia 2: Palabras clave en común
            if palabras_clave and palabras_clave_existente:
                palabras_comunes = palabras_clave & palabras_clave_existente
                similitud_palabras = len(palabras_comunes) / max(
                    len(palabras_clave), len(palabras_clave_existente)
                )
            else:
                similitud_palabras = 0

            # ✅ Estrategia 3: Similitud de precio
            if precio > 0 and precio_existente > 0:
                diferencia_precio = abs(precio - precio_existente) / max(
                    precio, precio_existente
                )
                similitud_precio = 1 - diferencia_precio
            else:
                similitud_precio = 0

            # DECISIÓN: Es duplicado si cumple CUALQUIERA de estos criterios:
            # 1. Similitud de texto > 80%
            # 2. Similitud de palabras > 70% Y precio similar (>90%)
            # 3. Similitud de texto > 70% Y precio idéntico o muy similar (>95%)

            es_duplicado = False

            if similitud_texto > 0.80:
                es_duplicado = True
            elif similitud_palabras > 0.70 and similitud_precio > 0.90:
                es_duplicado = True
            elif similitud_texto > 0.70 and similitud_precio > 0.95:
                es_duplicado = True

            if es_duplicado:
                encontrado = True
                # Mantener el nombre más completo
                if len(nombre) > len(nombre_existente):
                    productos_unicos[key]["nombre"] = prod["nombre"]
                break

        # Si no es duplicado, agregarlo
        if not encontrado:
            palabras = nombre_norm.split()[:5]
            key = f"nombre_{'_'.join(palabras)}"

            # Asegurar clave única
            counter = 1
            original_key = key
            while key in productos_unicos:
                key = f"{original_key}_{counter}"
                counter += 1

            productos_unicos[key] = prod

    resultado = list(productos_unicos.values())

    # ========================================
    # ESTADÍSTICAS FINALES
    # ========================================
    duplicados_eliminados = len(productos) - len(resultado)
    tasa_deduplicacion = (
        (duplicados_eliminados / len(productos) * 100) if len(productos) > 0 else 0
    )

    logger.info(f"=" * 70)
    logger.info(f"✅ DEDUPLICACIÓN COMPLETADA")
    logger.info(f"   Productos originales: {len(productos) + lineas_filtradas}")
    logger.info(f"   Basura eliminada: {lineas_filtradas}")
    logger.info(f"   Productos únicos: {len(resultado)}")
    logger.info(f"   Duplicados eliminados: {duplicados_eliminados}")
    logger.info(f"   Tasa de deduplicación: {tasa_deduplicacion:.1f}%")
    logger.info(f"=" * 70)

    return resultado


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
