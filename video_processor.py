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
    Deduplicación ULTRA INTELIGENTE con 3 niveles
    """
    from difflib import SequenceMatcher

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
    ]

    productos_limpios = []
    lineas_filtradas = 0

    for prod in productos:
        nombre = str(prod.get("nombre", "")).lower().strip()

        # Filtrar por palabras clave
        es_basura = any(palabra in nombre for palabra in palabras_descuento)

        # Filtrar nombres muy cortos (ruido)
        if len(nombre) < 3:
            es_basura = True

        if not es_basura:
            productos_limpios.append(prod)
        else:
            lineas_filtradas += 1

    if lineas_filtradas > 0:
        logger.info(f"   🗑️ Nivel 1: {lineas_filtradas} líneas basura eliminadas")

    productos = productos_limpios

    # ========================================
    # NIVEL 2: Deduplicar por código EAN/PLU
    # ========================================
    productos_unicos = {}
    productos_sin_codigo = []

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()

        if not nombre:
            continue

        # Con código EAN (8+ dígitos) - MUY CONFIABLE
        if codigo and len(codigo) >= 8 and codigo.isdigit():
            key = f"ean_{codigo}"
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                # Mantener el nombre más completo
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get("nombre", "")):
                    productos_unicos[key] = prod

        # Con código PLU (1-7 dígitos) - CONFIABLE
        elif codigo and 1 <= len(codigo) <= 7 and codigo.isdigit():
            key = f"plu_{codigo}"
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get("nombre", "")):
                    productos_unicos[key] = prod

        # Sin código - requiere análisis por nombre
        else:
            productos_sin_codigo.append(prod)

    logger.info(f"   ✅ Nivel 2: {len(productos_unicos)} productos con código único")
    logger.info(
        f"   🔍 Nivel 3: {len(productos_sin_codigo)} sin código (analizando...)"
    )

    # ========================================
    # NIVEL 3: Deduplicar SIN código (MÁS DIFÍCIL)
    # ========================================
    for prod in productos_sin_codigo:
        nombre = str(prod.get("nombre", "")).strip().lower()
        precio = prod.get("precio", 0)

        if not nombre or len(nombre) < 3:
            continue

        encontrado = False

        # Comparar con productos existentes
        for key, prod_existente in list(productos_unicos.items()):
            nombre_existente = str(prod_existente.get("nombre", "")).strip().lower()
            precio_existente = prod_existente.get("precio", 0)

            # ✅ Similitud de texto (Levenshtein)
            similitud = SequenceMatcher(None, nombre, nombre_existente).ratio()

            # ✅ Umbral ALTO para evitar falsos positivos
            if similitud > 0.90:  # 90% de similitud
                # ✅ Validar también similitud de precio (±10%)
                if precio > 0 and precio_existente > 0:
                    diferencia_precio = abs(precio - precio_existente) / max(
                        precio, precio_existente
                    )

                    if diferencia_precio < 0.10:  # Máximo 10% de diferencia
                        encontrado = True
                        # Mantener el nombre más largo (más completo)
                        if len(nombre) > len(nombre_existente):
                            productos_unicos[key]["nombre"] = prod["nombre"]
                        break
                else:
                    # Si no hay precio, usar solo similitud de texto
                    encontrado = True
                    if len(nombre) > len(nombre_existente):
                        productos_unicos[key]["nombre"] = prod["nombre"]
                    break

        # Si no es duplicado, agregarlo
        if not encontrado:
            palabras = nombre.split()[:5]  # Primeras 5 palabras como clave
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
