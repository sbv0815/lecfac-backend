"""
Procesador de videos de facturas - VERSIÓN OPTIMIZADA Y SEGURA
Extrae frames inteligentemente y deduplica productos
"""
import cv2
import tempfile
import os
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def extraer_frames_video(video_path: str, intervalo: float = 1.0) -> List[str]:
    """
    Extrae frames de un video de forma optimizada
    
    Args:
        video_path: Ruta al archivo de video
        intervalo: Intervalo en segundos (default: 1.0 = 1 frame por segundo)
        
    Returns:
        Lista de rutas a imágenes temporales extraídas
    """
    try:
        logger.info(f"📹 Abriendo video: {video_path}")
        
        cap = cv2.VideoCapture(video_path)
        
        if not cap.isOpened():
            logger.error("❌ No se pudo abrir el video")
            return []
        
        # Obtener información del video
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = total_frames / fps if fps > 0 else 0
        
        logger.info(f"   FPS: {fps:.2f}")
        logger.info(f"   Frames totales: {total_frames}")
        logger.info(f"   Duración: {duration:.2f}s")
        
        # Si FPS es inválido, usar fallback
        if fps <= 0 or fps > 240:
            logger.warning(f"⚠️ FPS inválido ({fps}), usando 30 como fallback")
            fps = 30
        
        # Ajustar intervalo según duración
        if duration <= 5:
            intervalo = 0.8  # Videos cortos: más frames
        elif duration <= 10:
            intervalo = 1.0  # Videos medios: 1 frame/seg
        else:
            intervalo = 1.5  # Videos largos: menos frames
        
        frame_interval = max(1, int(fps * intervalo))
        logger.info(f"   Extrayendo 1 frame cada {frame_interval} frames ({intervalo}s)")
        
        frames_paths = []
        frame_count = 0
        saved_count = 0
        
        # Límite de seguridad: máximo 20 frames
        max_frames = 20
        
        while True:
            ret, frame = cap.read()
            
            if not ret:
                break
            
            # Verificar límite
            if saved_count >= max_frames:
                logger.warning(f"⚠️ Límite de {max_frames} frames alcanzado")
                break
            
            # Guardar frame cada intervalo
            if frame_count % frame_interval == 0:
                try:
                    # Crear archivo temporal
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg', dir='/tmp') as tmp_frame:
                        frame_path = tmp_frame.name
                    
                    # Guardar frame con buena calidad
                    success = cv2.imwrite(
                        frame_path, 
                        frame, 
                        [cv2.IMWRITE_JPEG_QUALITY, 90]
                    )
                    
                    if success:
                        frames_paths.append(frame_path)
                        saved_count += 1
                        logger.info(f"   ✓ Frame {saved_count} guardado (frame #{frame_count})")
                    else:
                        logger.warning(f"   ⚠️ No se pudo guardar frame {frame_count}")
                        
                except Exception as e:
                    logger.error(f"   ❌ Error guardando frame {frame_count}: {e}")
            
            frame_count += 1
        
        cap.release()
        
        logger.info(f"✅ Extracción completa:")
        logger.info(f"   Total frames analizados: {frame_count}")
        logger.info(f"   Frames guardados: {saved_count}")
        
        return frames_paths
        
    except Exception as e:
        logger.error(f"❌ Error procesando video: {e}")
        import traceback
        traceback.print_exc()
        return []


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Elimina productos duplicados detectados en múltiples frames
    """
    from difflib import SequenceMatcher
    
    if not productos:
        return []
    
    logger.info(f"🔍 Deduplicando {len(productos)} productos...")
    
    # PASO 1: Filtrar descuentos
    palabras_descuento = [
        'ahorro', 'descuento', 'desc', 'dto', 'rebaja', 'promocion', 'promo', 
        'oferta', '2x1', '3x2', 'precio final', 'valor final', 'dcto'
    ]
    
    productos_limpios = []
    descuentos_filtrados = 0
    
    for prod in productos:
        nombre = str(prod.get('nombre', '')).lower().strip()
        es_descuento = any(palabra in nombre for palabra in palabras_descuento)
        
        if not es_descuento:
            productos_limpios.append(prod)
        else:
            descuentos_filtrados += 1
    
    if descuentos_filtrados > 0:
        logger.info(f"   🗑️ {descuentos_filtrados} descuentos eliminados")
    
    productos = productos_limpios
    
    # PASO 2: Deduplicar por código
    productos_unicos = {}
    productos_sin_codigo = []
    
    for prod in productos:
        codigo = str(prod.get('codigo', '')).strip()
        nombre = str(prod.get('nombre', '')).strip().lower()
        
        if not nombre or len(nombre) < 3:
            continue
        
        # Con código EAN (8+ dígitos)
        if codigo and len(codigo) >= 8 and codigo.isdigit():
            key = f"codigo_{codigo}"
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
        
        # Con código PLU (1-7 dígitos)
        elif codigo and 1 <= len(codigo) <= 7 and codigo.isdigit():
            key = f"plu_{codigo}"
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
        
        # Sin código
        else:
            productos_sin_codigo.append(prod)
    
    # PASO 3: Deduplicar sin código por nombre
    logger.info(f"   🔍 Analizando {len(productos_sin_codigo)} productos sin código...")
    
    for prod in productos_sin_codigo:
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        if not nombre or len(nombre) < 3:
            continue
        
        encontrado = False
        
        for key, prod_existente in productos_unicos.items():
            nombre_existente = str(prod_existente.get('nombre', '')).strip().lower()
            precio_existente = prod_existente.get('precio', 0)
            
            similitud = SequenceMatcher(None, nombre, nombre_existente).ratio()
            
            if similitud > 0.85:
                diferencia_precio = abs(precio - precio_existente) / max(precio, precio_existente, 1)
                
                if diferencia_precio < 0.20:
                    encontrado = True
                    if len(nombre) > len(nombre_existente):
                        productos_unicos[key]['nombre'] = prod['nombre']
                    break
        
        if not encontrado:
            palabras = nombre.split()[:4]
            key = f"nombre_{'_'.join(palabras)}"
            
            counter = 1
            original_key = key
            while key in productos_unicos:
                key = f"{original_key}_{counter}"
                counter += 1
            
            productos_unicos[key] = prod
    
    resultado = list(productos_unicos.values())
    
    logger.info(f"✅ Deduplicación completa:")
    logger.info(f"   Productos originales: {len(productos) + descuentos_filtrados}")
    logger.info(f"   Descuentos filtrados: {descuentos_filtrados}")
    logger.info(f"   Productos únicos: {len(resultado)}")
    logger.info(f"   Duplicados eliminados: {len(productos) - len(resultado)}")
    
    return resultado


def limpiar_frames_temporales(frames_paths: List[str]):
    """Elimina los archivos temporales de frames"""
    for frame_path in frames_paths:
        try:
            if os.path.exists(frame_path):
                os.remove(frame_path)
        except Exception as e:
            logger.warning(f"⚠️ No se pudo eliminar frame: {e}")
