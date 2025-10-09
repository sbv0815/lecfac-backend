"""
Procesador de videos de facturas
Extrae frames y los procesa con OCR
"""
import cv2
import tempfile
import os
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def extraer_frames_video(video_path: str, intervalo: float = 0.5) -> List[str]:
    """
    Extrae frames de un video cada X segundos
    
    Args:
        video_path: Ruta al archivo de video
        intervalo: Intervalo en segundos entre frames (default: 0.5s)
        
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
        
        # Si FPS es 0 o inválido, usar 30 como fallback
        if fps <= 0 or fps > 240:
            logger.warning(f"⚠️ FPS inválido ({fps}), usando 30 como fallback")
            fps = 30
        
        # Calcular intervalo de frames
        frame_interval = max(1, int(fps * intervalo))
        logger.info(f"   Extrayendo 1 frame cada {frame_interval} frames ({intervalo}s)")
        
        frames_paths = []
        frame_count = 0
        saved_count = 0
        
        while True:
            ret, frame = cap.read()
            
            if not ret:
                break
            
            # Guardar frame cada intervalo
            if frame_count % frame_interval == 0:
                try:
                    # Crear archivo temporal para el frame
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg', dir='/tmp') as tmp_frame:
                        frame_path = tmp_frame.name
                    
                    # Guardar frame como JPEG con buena calidad
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
    
    Usa una combinación de:
    - Código de barras (si existe)
    - Similitud de nombre
    - Precio similar
    
    Args:
        productos: Lista de productos detectados en todos los frames
        
    Returns:
        Lista de productos únicos sin duplicados
    """
    from difflib import SequenceMatcher
    
    if not productos:
        return []
    
    logger.info(f"🔍 Deduplicando {len(productos)} productos...")
    
    productos_unicos = {}
    productos_sin_codigo = []
    
    for prod in productos:
        codigo = str(prod.get('codigo', '')).strip()
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        # CASO 1: Producto con código válido (8+ dígitos)
        if codigo and len(codigo) >= 8 and codigo.isdigit():
            key = f"codigo_{codigo}"
            
            if key not in productos_unicos:
                productos_unicos[key] = prod
                logger.info(f"   ✓ Producto con código: {codigo} - {nombre[:30]}")
            else:
                # Si está duplicado, quedarse con el que tenga mejor información
                prod_existente = productos_unicos[key]
                
                # Preferir el que tenga nombre más largo (más completo)
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
                    logger.info(f"   ↻ Actualizado: {codigo} (mejor nombre)")
        
        # CASO 2: Producto con código PLU corto (3-7 dígitos)
        elif codigo and 3 <= len(codigo) <= 7 and codigo.isdigit():
            key = f"plu_{codigo}"
            
            if key not in productos_unicos:
                productos_unicos[key] = prod
                logger.info(f"   ✓ PLU: {codigo} - {nombre[:30]}")
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
        
        # CASO 3: Producto sin código - usar nombre + precio
        else:
            productos_sin_codigo.append(prod)
    
    # Deduplicar productos sin código usando similitud de nombre
    logger.info(f"🔍 Analizando {len(productos_sin_codigo)} productos sin código...")
    
    for prod in productos_sin_codigo:
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        if not nombre or len(nombre) < 3:
            continue
        
        # Buscar si ya existe un producto similar
        encontrado = False
        
        for key, prod_existente in productos_unicos.items():
            nombre_existente = str(prod_existente.get('nombre', '')).strip().lower()
            precio_existente = prod_existente.get('precio', 0)
            
            # Calcular similitud de nombres
            similitud = SequenceMatcher(None, nombre, nombre_existente).ratio()
            
            # Si son muy similares (>85%) y precio cercano (±20%)
            if similitud > 0.85:
                diferencia_precio = abs(precio - precio_existente) / max(precio, precio_existente, 1)
                
                if diferencia_precio < 0.20:  # Menos de 20% de diferencia
                    encontrado = True
                    # Actualizar con el nombre más largo
                    if len(nombre) > len(nombre_existente):
                        productos_unicos[key]['nombre'] = prod['nombre']
                    break
        
        if not encontrado:
            # Crear key único con primeras palabras del nombre
            palabras = nombre.split()[:4]
            key = f"nombre_{'_'.join(palabras)}"
            
            # Evitar colisiones
            counter = 1
            original_key = key
            while key in productos_unicos:
                key = f"{original_key}_{counter}"
                counter += 1
            
            productos_unicos[key] = prod
            logger.info(f"   ✓ Sin código: {nombre[:30]}")
    
    resultado = list(productos_unicos.values())
    
    logger.info(f"✅ Deduplicación completa:")
    logger.info(f"   Productos originales: {len(productos)}")
    logger.info(f"   Productos únicos: {len(resultado)}")
    logger.info(f"   Duplicados eliminados: {len(productos) - len(resultado)}")
    
    return resultado


def limpiar_frames_temporales(frames_paths: List[str]):
    """
    Elimina los archivos temporales de frames
    """
    for frame_path in frames_paths:
        try:
            if os.path.exists(frame_path):
                os.remove(frame_path)
        except Exception as e:
            logger.warning(f"⚠️ No se pudo eliminar frame temporal {frame_path}: {e}")
