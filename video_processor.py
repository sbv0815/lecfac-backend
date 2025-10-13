"""
Procesador de videos - SISTEMA DE 3 NIVELES DE CONFIANZA
Prioriza NO PERDER INFORMACIÓN sobre tener datos perfectos
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
        duracion = total_frames / fps if fps > 0 else 0

        print(
            f"📹 Video: {duracion:.1f}s, {fps:.1f} FPS, {total_frames} frames totales"
        )

        # Sistema adaptativo de frames
        if duracion <= 10:
            max_frames = 12
        elif duracion <= 20:
            max_frames = 18
        elif duracion <= 30:
            max_frames = 25
        else:
            max_frames = 30

        print(f"🎯 Extraeremos {max_frames} frames para máxima precisión")

        frame_interval = max(1, total_frames // max_frames) if duracion > 0 else 1
        frame_count = 0
        extracted_count = 0

        # Primera pasada: frames uniformes
        while cap.isOpened() and extracted_count < max_frames:
            ret, frame = cap.read()
            if not ret:
                break

            if frame_count % frame_interval == 0:
                frame_filename = f"/tmp/frame_{os.getpid()}_{extracted_count:03d}.jpg"
                cv2.imwrite(frame_filename, frame)
                frames.append(frame_filename)
                extracted_count += 1
                print(f"  ✓ Frame {extracted_count}/{max_frames} extraído")

            frame_count += 1

        cap.release()

        # Segunda pasada: frames finales
        print(f"🎯 Extrayendo frames FINALES para capturar el TOTAL...")
        cap = cv2.VideoCapture(video_path)

        ultimos_frames_posiciones = [
            total_frames - 1,
            total_frames - int(fps * 0.5),
            total_frames - int(fps * 1),
            total_frames - int(fps * 1.5),
            total_frames - int(fps * 2),
        ]

        ultimos_frames_posiciones = [
            pos for pos in ultimos_frames_posiciones if 0 <= pos < total_frames
        ]

        for i, pos in enumerate(ultimos_frames_posiciones):
            cap.set(cv2.CAP_PROP_POS_FRAMES, pos)
            ret, frame = cap.read()
            if ret:
                frame_filename = f"/tmp/frame_{os.getpid()}_final_{i:03d}.jpg"
                cv2.imwrite(frame_filename, frame)
                frames.append(frame_filename)
                print(
                    f"  ✓ Frame FINAL {i+1}/{len(ultimos_frames_posiciones)} extraído"
                )

        cap.release()

        print(f"✅ {len(frames)} frames extraídos exitosamente")
        return frames

    except Exception as e:
        print(f"❌ Error extrayendo frames: {e}")
        import traceback

        traceback.print_exc()
        return frames


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Deduplicación con SISTEMA DE 3 NIVELES DE CONFIANZA

    NIVEL 1 (Alta): Código + Nombre + Precio
    NIVEL 2 (Media): Nombre + Precio (sin código)
    NIVEL 3 (Baja): Solo Nombre o Solo Precio

    🎯 FILOSOFÍA: Es mejor tener 1 producto con 80% confianza
                  que perder completamente ese producto
    """
    from difflib import SequenceMatcher
    import re

    if not productos:
        return []

    logger.info(f"🔍 Iniciando deduplicación de {len(productos)} productos...")

    # ========================================
    # FUNCIONES DE VALIDACIÓN (MÁS PERMISIVAS)
    # ========================================

    def es_codigo_valido(codigo: str) -> bool:
        """Valida códigos - MÁS PERMISIVO"""
        if not codigo or not isinstance(codigo, str):
            return False

        codigo = codigo.strip()

        # ✅ Aceptar desde 3 dígitos (PLU de frutas/verduras)
        if len(codigo) < 3:
            return False

        if len(codigo) > 14:
            return False

        # Debe ser numérico
        if not codigo.isdigit():
            return False

        # Rechazar solo códigos OBVIAMENTE falsos
        digitos_unicos = len(set(codigo))
        if digitos_unicos == 1:  # 666666, 777777
            return False

        return True

    def es_nombre_generico(nombre: str) -> bool:
        """Detecta SOLO nombres OBVIAMENTE genéricos"""
        nombre_upper = nombre.upper().strip()

        # ✅ Aceptar nombres de 2+ caracteres (antes 3+)
        if len(nombre_upper) < 2:
            return True

        # Solo unidades de medida puras
        unidades = ["KGM", "KG", "/KGM", "/KG", "UND", "/U"]
        if nombre_upper in unidades:
            return True

        # Solo códigos internos
        if nombre_upper in ["PLU", "SKU", "COD", "REF"]:
            return True

        # Solo números y barras
        if re.match(r"^[\d\s/]+$", nombre_upper):
            return True

        return False

    def es_precio_valido(precio: float) -> bool:
        """Valida precios - MÁS PERMISIVO"""
        if precio <= 0:
            return False

        # ✅ Aceptar desde $50 (antes $100)
        # Hay productos de $50, $80, $90 válidos
        if precio < 50:
            return False

        # Rechazar solo extremos obvios
        if precio > 1000000:  # 1 millón
            return False

        return True

    def normalizar_para_comparacion(texto: str) -> str:
        """Normaliza texto para comparación estricta"""
        texto = texto.upper().strip()
        texto = re.sub(r"[^\w\s]", "", texto)  # Quitar puntuación
        texto = re.sub(r"\s+", " ", texto)  # Espacios únicos
        texto = re.sub(r"\s+\d+[A-Z]*$", "", texto)  # Quitar números finales
        return texto

    def calcular_nivel_confianza(codigo: str, nombre: str, precio: float) -> int:
        """
        Calcula el nivel de confianza del producto

        NIVEL 1: Código + Nombre + Precio ✅
        NIVEL 2: Nombre + Precio (sin código) ⚠️
        NIVEL 3: Solo nombre o solo precio ⚡
        """
        tiene_codigo = bool(codigo and es_codigo_valido(codigo))
        tiene_nombre = bool(
            nombre and len(nombre) >= 2 and not es_nombre_generico(nombre)
        )
        tiene_precio = bool(precio and es_precio_valido(precio))

        if tiene_codigo and tiene_nombre and tiene_precio:
            return 1  # Alta confianza
        elif tiene_nombre and tiene_precio:
            return 2  # Media confianza
        elif tiene_nombre or tiene_precio:
            return 3  # Baja confianza
        else:
            return 0  # Rechazar

    # ========================================
    # PASO 1: FILTRAR SOLO BASURA OBVIA
    # ========================================
    palabras_filtro = [
        "ahorro",
        "descuento",
        "desc",
        "dto",
        "rebaja",
        "promocion",
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
        "establecimiento",
        "gracias",
        "vuelva pronto",
        "nit",
        "autoretenedor",
        "cadena",
    ]

    productos_validos = []
    lineas_filtradas = 0

    for prod in productos:
        nombre = str(prod.get("nombre", "")).lower().strip()
        codigo = str(prod.get("codigo", "")).strip()
        precio = prod.get("precio", 0)

        # ❌ FILTRO 1: Palabras administrativas
        if any(palabra in nombre for palabra in palabras_filtro):
            lineas_filtradas += 1
            continue

        # ❌ FILTRO 2: Solo números sin sentido
        if re.match(r"^[\d\s\-\./]+$", nombre):
            lineas_filtradas += 1
            continue

        # ✅ CALCULAR NIVEL DE CONFIANZA
        nivel = calcular_nivel_confianza(codigo, nombre, precio)

        if nivel > 0:
            # Agregar nivel de confianza al producto
            prod["nivel_confianza"] = nivel
            productos_validos.append(prod)
        else:
            lineas_filtradas += 1

    logger.info(f"   🗑️ Paso 1: {lineas_filtradas} líneas basura eliminadas")
    logger.info(f"   ✅ Productos válidos: {len(productos_validos)}")

    productos = productos_validos

    # ========================================
    # PASO 2: AGRUPAR POR NOMBRE + PRECIO (CRÍTICO PARA VIDEOS)
    # ========================================
    # En videos, Claude lee mal los códigos creando "falsos diferentes"
    # SOLUCIÓN: Agrupar por (NOMBRE + PRECIO) primero

    productos_unicos = {}

    for prod in productos:
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip().upper()
        precio = prod.get("precio", 0)
        nivel = prod.get("nivel_confianza", 3)

        # Normalizar nombre para comparación
        nombre_norm = normalizar_para_comparacion(nombre)

        # Clave ÚNICA: Nombre normalizado + Precio
        # Esto agrupa "CREMA DE LECHE U $22865" aunque tenga códigos diferentes
        clave_unica = (nombre_norm, precio)

        if clave_unica not in productos_unicos:
            productos_unicos[clave_unica] = prod
        else:
            # Ya existe producto similar - mantener el MEJOR
            prod_existente = productos_unicos[clave_unica]

            # Prioridad 1: Con código válido
            tiene_codigo_actual = es_codigo_valido(codigo)
            tiene_codigo_existente = es_codigo_valido(prod_existente.get("codigo", ""))

            if tiene_codigo_actual and not tiene_codigo_existente:
                productos_unicos[clave_unica] = prod
            elif not tiene_codigo_actual and tiene_codigo_existente:
                pass  # Mantener el existente
            else:
                # Ambos tienen o no tienen código
                # Prioridad 2: Mejor nivel de confianza
                if nivel < prod_existente.get("nivel_confianza", 3):
                    productos_unicos[clave_unica] = prod
                # Prioridad 3: Nombre más completo
                elif nivel == prod_existente.get("nivel_confianza", 3):
                    if len(nombre) > len(prod_existente.get("nombre", "")):
                        productos_unicos[clave_unica] = prod

    # Convertir a lista
    productos_con_codigo = list(productos_unicos.values())
    productos_sin_codigo = []  # Ya no necesitamos este paso

    logger.info(
        f"   📊 Agrupados por Nombre+Precio: {len(productos_unicos)} productos únicos"
    )

    # ========================================
    # PASO 3: RESULTADO FINAL
    # ========================================
    resultado_final = list(productos_unicos.values())

    # ========================================
    # ESTADÍSTICAS FINALES
    # ========================================
    total_original = len(productos) + lineas_filtradas

    nivel_1 = len([p for p in resultado_final if p.get("nivel_confianza") == 1])
    nivel_2 = len([p for p in resultado_final if p.get("nivel_confianza") == 2])
    nivel_3 = len([p for p in resultado_final if p.get("nivel_confianza") == 3])

    logger.info(f"=" * 70)
    logger.info(f"✅ DEDUPLICACIÓN COMPLETADA (Sistema 3 Niveles)")
    logger.info(f"   📊 Líneas detectadas: {total_original}")
    logger.info(f"   🗑️  Basura eliminada: {lineas_filtradas}")
    logger.info(f"   ✅ Productos guardados: {len(resultado_final)}")
    logger.info(f"")
    logger.info(f"   📊 POR NIVEL DE CONFIANZA:")
    logger.info(f"   ✅ NIVEL 1 (Código+Nombre+Precio): {nivel_1}")
    logger.info(f"   ⚠️  NIVEL 2 (Nombre+Precio): {nivel_2}")
    logger.info(f"   ⚡ NIVEL 3 (Parcial): {nivel_3}")
    logger.info(f"=" * 70)

    # Log de productos finales
    logger.info(f"📦 PRODUCTOS GUARDADOS:")
    for i, prod in enumerate(resultado_final[:10], 1):
        codigo = prod.get("codigo", "SIN CÓDIGO")
        nombre = prod.get("nombre", "")
        precio = prod.get("precio", 0)
        nivel = prod.get("nivel_confianza", 3)
        emoji = "✅" if nivel == 1 else "⚠️" if nivel == 2 else "⚡"
        logger.info(f"   {emoji} {i}. [{codigo}] {nombre} - ${precio:,.0f}")

    if len(resultado_final) > 10:
        logger.info(f"   ... y {len(resultado_final) - 10} más")

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
    """Combina múltiples frames en una sola imagen vertical"""
    if not frames_paths:
        raise ValueError("No hay frames para combinar")

    try:
        images = []
        for path in frames_paths:
            if os.path.exists(path):
                img = cv2.imread(path)
                if img is not None:
                    images.append(img)

        if not images:
            raise ValueError("No se pudieron leer los frames")

        max_width = max(img.shape[1] for img in images)

        resized_images = []
        for img in images:
            if img.shape[1] != max_width:
                height = int(img.shape[0] * max_width / img.shape[1])
                img = cv2.resize(img, (max_width, height))
            resized_images.append(img)

        total_height = sum(img.shape[0] for img in resized_images)

        if total_height > max_height:
            scale = max_height / total_height
            resized_images = [
                cv2.resize(img, (int(img.shape[1] * scale), int(img.shape[0] * scale)))
                for img in resized_images
            ]
            total_height = sum(img.shape[0] for img in resized_images)
            max_width = int(max_width * scale)

        combined = np.vstack(resized_images)

        if output_path is None:
            output_path = f"/tmp/combined_{os.getpid()}.jpg"

        cv2.imwrite(output_path, combined, [cv2.IMWRITE_JPEG_QUALITY, 85])

        logger.info(
            f"✅ Imagen combinada: {len(images)} frames, {total_height}px altura"
        )

        return output_path

    except Exception as e:
        logger.error(f"❌ Error combinando frames: {e}")
        if frames_paths and os.path.exists(frames_paths[0]):
            return frames_paths[0]
        raise
