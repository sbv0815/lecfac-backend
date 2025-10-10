"""
Procesador de videos de facturas - VERSIÓN OPTIMIZADA
Extrae frames inteligentemente y deduplica productos
"""
import cv2
import tempfile
import os
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def detectar_cambio_significativo(frame_actual, frame_anterior, umbral=0.12):
    """
    Detecta si hay cambio significativo entre dos frames
    Umbral bajo = más sensible a cambios (más frames)
    Umbral alto = menos sensible (menos frames)
    """
    if frame_anterior is None:
        return True
    
    try:
        # Convertir a escala de grises
        gray1 = cv2.cvtColor(frame_anterior, cv2.COLOR_BGR2GRAY)
        gray2 = cv2.cvtColor(frame_actual, cv2.COLOR_BGR2GRAY)
        
        # Calcular diferencia absoluta
        diff = cv2.absdiff(gray1, gray2)
        
        # Porcentaje de píxeles que cambiaron significativamente
        porcentaje_cambio = np.sum(diff > 30) / diff.size
        
        return porcentaje_cambio > umbral
    except Exception as e:
        logger.warning(f"Error detectando cambio: {e}")
        return True  # En caso de error, procesar el frame


def estrategia_frames_por_duracion(duration: float) -> tuple:
    """
    Determina estrategia de extracción según duración del video
    Retorna (intervalo_segundos, usar_deteccion_cambios)
    """
    if duration <= 3:
        # Video muy corto: cada 0.5s, sin detección
        return (0.5, False)
    elif duration <= 8:
        # Video corto: cada 0.8s, con detección
        return (0.8, True)
    elif duration <= 15:
        # Video medio: cada 1.2s, con detección
        return (1.2, True)
    else:
        # Video largo: cada 2s, con detección
        return (2.0, True)


def extraer_frames_video(video_path: str, intervalo: float = None) -> List[str]:
    """
    Extrae frames de un video de forma INTELIGENTE
    - Detecta duración y ajusta estrategia automáticamente
    - Detecta cambios significativos para evitar frames duplicados
    - Optimizado para balance entre velocidad y calidad
    
    Args:
        video_path: Ruta al archivo de video
        intervalo: Intervalo en segundos (None = automático según duración)
        
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
        
        logger.info(f"   📊 Información del video:")
        logger.info(f"      - FPS: {fps:.2f}")
        logger.info(f"      - Frames totales: {total_frames}")
        logger.info(f"      - Duración: {duration:.2f}s")
        
        # Validar FPS
        if fps <= 0 or fps > 240:
            logger.warning(f"⚠️ FPS inválido ({fps}), usando 30 como fallback")
            fps = 30
        
        # Estrategia inteligente
        if intervalo is None:
            intervalo, usar_deteccion = estrategia_frames_por_duracion(duration)
            logger.info(f"   🎯 Estrategia automática:")
            logger.info(f"      - Intervalo: {intervalo}s")
            logger.info(f"      - Detección cambios: {'Sí' if usar_deteccion else 'No'}")
        else:
            usar_deteccion = True  # Siempre usar si se especifica intervalo manual
        
        frame_interval = max(1, int(fps * intervalo))
        logger.info(f"   ⚙️ Extrayendo 1 frame cada {frame_interval} frames ({intervalo}s)")
        
        frames_paths = []
        frame_count = 0
        saved_count = 0
        skipped_count = 0
        frame_anterior = None
        
        # Límite de seguridad: máximo 25 frames
        max_frames = 25
        
        while True:
            ret, frame = cap.read()
            
            if not ret:
                break
            
            # Verificar límite de seguridad
            if saved_count >= max_frames:
                logger.warning(f"⚠️ Alcanzado límite de {max_frames} frames")
                break
            
            # Guardar frame cada intervalo
            if frame_count % frame_interval == 0:
                
                # Verificar cambio significativo (si está habilitado)
                hay_cambio = True
                if usar_deteccion and frame_anterior is not None:
                    hay_cambio = detectar_cambio_significativo(frame, frame_anterior)
                
                if hay_cambio:
                    try:
                        # Crear archivo temporal
                        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg', dir='/tmp') as tmp_frame:
                            frame_path = tmp_frame.name
                        
                        # Guardar frame con buena calidad
                        success = cv2.imwrite(
                            frame_path, 
                            frame, 
                            [cv2.IMWRITE_JPEG_QUALITY, 85]  # 85% calidad (balance)
                        )
                        
                        if success:
                            frames_paths.append(frame_path)
                            saved_count += 1
                            frame_anterior = frame.copy()
                            logger.info(f"   ✅ Frame {saved_count} guardado (#{frame_count}, {frame_count/fps:.1f}s)")
                        else:
                            logger.warning(f"   ⚠️ No se pudo guardar frame {frame_count}")
                            
                    except Exception as e:
                        logger.error(f"   ❌ Error guardando frame {frame_count}: {e}")
                else:
                    skipped_count += 1
                    logger.debug(f"   ⏭️ Frame {frame_count} saltado (sin cambios)")
            
            frame_count += 1
        
        cap.release()
        
        logger.info(f"✅ Extracción completa:")
        logger.info(f"   📊 Resultados:")
        logger.info(f"      - Frames analizados: {frame_count}")
        logger.info(f"      - Frames guardados: {saved_count}")
        logger.info(f"      - Frames saltados: {skipped_count}")
        logger.info(f"      - Eficiencia: {(skipped_count/frame_count*100):.1f}% frames evitados")
        
        return frames_paths
        
    except Exception as e:
        logger.error(f"❌ Error procesando video: {e}")
        import traceback
        traceback.print_exc()
        return []


def deduplicar_productos(productos: List[Dict]) -> List[Dict]:
    """
    Elimina productos duplicados detectados en múltiples frames
    MEJORADO: Pre-filtra descuentos y optimiza deduplicación
    """
    from difflib import SequenceMatcher
    
    if not productos:
        return []
    
    logger.info(f"🔍 Deduplicando {len(productos)} productos...")
    
    # ========== PASO 1: FILTRADO PREVIO DE DESCUENTOS ==========
    palabras_descuento = [
        'ahorro', 'descuento', 'desc', 'dto', 'rebaja', 'promocion', 'promo', 
        'oferta', '2x1', '3x2', 'precio final', 'valor final', 'dto',
        'dcto', 'descu', 'dcto.', 'ahorro%', 'desc%', 'ahorro ', ' ahorro',
        'descto', 'descuent'
    ]
    
    productos_limpios = []
    descuentos_filtrados = 0
    
    for prod in productos:
        nombre = str(prod.get('nombre', '')).lower().strip()
        
        # Verificar si es descuento
        es_descuento = any(palabra in nombre for palabra in palabras_descuento)
        
        if es_descuento:
            descuentos_filtrados += 1
        else:
            productos_limpios.append(prod)
    
    if descuentos_filtrados > 0:
        logger.info(f"   🗑️ {descuentos_filtrados} descuentos eliminados")
    
    productos = productos_limpios
    
    # ========== PASO 2: DEDUPLICACIÓN POR CÓDIGO ==========
    productos_unicos = {}
    productos_sin_codigo = []
    
    for prod in productos:
        codigo = str(prod.get('codigo', '')).strip()
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        # Validar nombre mínimo
        if not nombre or len(nombre) < 3:
            continue
        
        # CASO 1: Producto con código EAN (8+ dígitos)
        if codigo and len(codigo) >= 8 and codigo.isdigit():
            key = f"ean_{codigo}"
            
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                # Quedarse con el que tenga mejor nombre
                prod_existente = productos_unicos[key]
                if len(nombre) > len(str(prod_existente.get('nombre', '')).lower()):
                    productos_unicos[key] = prod
        
        # CASO 2: Producto con código PLU (1-7 dígitos)
        elif codigo and 1 <= len(codigo) <= 7 and codigo.isdigit():
            key = f"plu_{codigo}"
            
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(str(prod_existente.get('nombre', '')).lower()):
                    productos_unicos[key] = prod
        
        # CASO 3: Producto sin código
        else:
            productos_sin_codigo.append(prod)
    
    # ========== PASO 3: DEDUPLICAR PRODUCTOS SIN CÓDIGO ==========
    logger.info(f"   🔍 Analizando {len(productos_sin_codigo)} productos sin código...")
    
    for prod in productos_sin_codigo:
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        if not nombre or len(nombre) < 3:
            continue
        
        # Buscar similar
        encontrado = False
        
        for key, prod_existente in productos_unicos.items():
            nombre_existente = str(prod_existente.get('nombre', '')).strip().lower()
            precio_existente = prod_existente.get('precio', 0)
            
            # Similitud de nombres
            similitud = SequenceMatcher(None, nombre, nombre_existente).ratio()
            
            # Si son muy similares (>85%) y precio cercano (±20%)
            if similitud > 0.85:
                diferencia_precio = abs(precio - precio_existente) / max(precio, precio_existente, 1) if precio and precio_existente else 1
                
                if diferencia_precio < 0.20:
                    encontrado = True
                    # Actualizar con el nombre más largo
                    if len(nombre) > len(nombre_existente):
                        productos_unicos[key]['nombre'] = prod['nombre']
                    break
        
        if not encontrado:
            # Crear key único
            palabras = nombre.split()[:4]
            key = f"nombre_{'_'.join(palabras)}"
            
            # Evitar colisiones
            counter = 1
            original_key = key
            while key in productos_unicos:
                key = f"{original_key}_{counter}"
                counter += 1
            
            productos_unicos[key] = prod
    
    resultado = list(productos_unicos.values())
    
    logger.info(f"✅ Deduplicación completa:")
    logger.info(f"   📊 Productos originales: {len(productos) + descuentos_filtrados}")
    logger.info(f"   🗑️ Descuentos filtrados: {descuentos_filtrados}")
    logger.info(f"   ✨ Productos únicos: {len(resultado)}")
    logger.info(f"   📉 Duplicados eliminados: {len(productos) - len(resultado)}")
    
    return resultado


def limpiar_frames_temporales(frames_paths: List[str]):
    """Elimina los archivos temporales de frames"""
    for frame_path in frames_paths:
        try:
            if os.path.exists(frame_path):
                os.remove(frame_path)
        except Exception as e:
            logger.warning(f"⚠️ No se pudo eliminar frame: {e}")
