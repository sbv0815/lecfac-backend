"""
Procesador de videos - SISTEMA ADAPTATIVO INTELIGENTE
Ajusta frames según duración del video
"""
import cv2
import tempfile
import os
import numpy as np
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)


def extraer_frames_video(video_path: str, intervalo: float = None) -> List[str]:
    """
    Extrae frames de forma INTELIGENTE según duración
    
    SISTEMA ADAPTATIVO:
    - Video corto (≤5s): 3-4 frames (factura pequeña)
    - Video medio (6-12s): 6-8 frames (factura normal)
    - Video largo (13-20s): 10-12 frames (factura grande)
    - Video muy largo (>20s): 15 frames (factura enorme)
    
    Returns:
        Lista de rutas a imágenes optimizada
    """
    try:
        logger.info(f"📹 Abriendo video: {video_path}")
        
        cap = cv2.VideoCapture(video_path)
        
        if not cap.isOpened():
            logger.error("❌ No se pudo abrir el video")
            return []
        
        # Info del video
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        duration = total_frames / fps if fps > 0 else 0
        
        logger.info(f"   FPS: {fps:.2f}")
        logger.info(f"   Frames totales: {total_frames}")
        logger.info(f"   Duración: {duration:.2f}s")
        
        # FPS fallback
        if fps <= 0 or fps > 240:
            logger.warning(f"⚠️ FPS inválido ({fps}), usando 30")
            fps = 30
        
        # ✅ SISTEMA ADAPTATIVO: Frames según duración
        if duration <= 5:
            max_frames = 4
            intervalo = 1.2
            tipo_factura = "CORTA"
        elif duration <= 12:
            max_frames = 8
            intervalo = 1.5
            tipo_factura = "NORMAL"
        elif duration <= 20:
            max_frames = 12
            intervalo = 1.8
            tipo_factura = "GRANDE"
        else:
            max_frames = 15
            intervalo = 2.0
            tipo_factura = "MUY GRANDE"
        
        logger.info(f"   📊 Factura {tipo_factura}: {max_frames} frames")
        logger.info(f"   ⏱️ Intervalo: {intervalo}s entre frames")
        
        frame_interval = max(1, int(fps * intervalo))
        
        frames_paths = []
        frame_count = 0
        saved_count = 0
        
        while True:
            ret, frame = cap.read()
            
            if not ret:
                break
            
            # Verificar límite
            if saved_count >= max_frames:
                logger.info(f"✅ Meta alcanzada: {max_frames} frames")
                break
            
            # Guardar cada intervalo
            if frame_count % frame_interval == 0:
                try:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg', dir='/tmp') as tmp_frame:
                        frame_path = tmp_frame.name
                    
                    # ✅ Calidad alta (95) para mejor OCR
                    success = cv2.imwrite(
                        frame_path, 
                        frame, 
                        [cv2.IMWRITE_JPEG_QUALITY, 95]
                    )
                    
                    if success:
                        frames_paths.append(frame_path)
                        saved_count += 1
                        logger.info(f"   ✓ Frame {saved_count}/{max_frames} @ {frame_count/fps:.1f}s")
                    else:
                        logger.warning(f"   ⚠️ Error guardando frame")
                        
                except Exception as e:
                    logger.error(f"   ❌ Error: {e}")
            
            frame_count += 1
        
        cap.release()
        
        # ✅ Estadísticas finales
        cobertura = (saved_count * intervalo / duration * 100) if duration > 0 else 0
        logger.info(f"✅ Extracción completa:")
        logger.info(f"   Frames guardados: {saved_count}")
        logger.info(f"   Cobertura estimada: {cobertura:.0f}% del video")
        
        return frames_paths
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return []


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
        'ahorro', 'descuento', 'desc', 'dto', 'rebaja', 'promocion', 'promo', 
        'oferta', '2x1', '3x2', 'dcto', 'precio final', 'iva', 'impuesto',
        'subtotal', 'total', 'cambio', 'efectivo', 'tarjeta'
    ]
    
    productos_limpios = []
    lineas_filtradas = 0
    
    for prod in productos:
        nombre = str(prod.get('nombre', '')).lower().strip()
        
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
        codigo = str(prod.get('codigo', '')).strip()
        nombre = str(prod.get('nombre', '')).strip()
        
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
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
        
        # Con código PLU (1-7 dígitos) - CONFIABLE
        elif codigo and 1 <= len(codigo) <= 7 and codigo.isdigit():
            key = f"plu_{codigo}"
            if key not in productos_unicos:
                productos_unicos[key] = prod
            else:
                prod_existente = productos_unicos[key]
                if len(nombre) > len(prod_existente.get('nombre', '')):
                    productos_unicos[key] = prod
        
        # Sin código - requiere análisis por nombre
        else:
            productos_sin_codigo.append(prod)
    
    logger.info(f"   ✅ Nivel 2: {len(productos_unicos)} productos con código único")
    logger.info(f"   🔍 Nivel 3: {len(productos_sin_codigo)} sin código (analizando...)")
    
    # ========================================
    # NIVEL 3: Deduplicar SIN código (MÁS DIFÍCIL)
    # ========================================
    for prod in productos_sin_codigo:
        nombre = str(prod.get('nombre', '')).strip().lower()
        precio = prod.get('precio', 0)
        
        if not nombre or len(nombre) < 3:
            continue
        
        encontrado = False
        
        # Comparar con productos existentes
        for key, prod_existente in list(productos_unicos.items()):
            nombre_existente = str(prod_existente.get('nombre', '')).strip().lower()
            precio_existente = prod_existente.get('precio', 0)
            
            # ✅ Similitud de texto (Levenshtein)
            similitud = SequenceMatcher(None, nombre, nombre_existente).ratio()
            
            # ✅ Umbral ALTO para evitar falsos positivos
            if similitud > 0.90:  # 90% de similitud
                # ✅ Validar también similitud de precio (±10%)
                if precio > 0 and precio_existente > 0:
                    diferencia_precio = abs(precio - precio_existente) / max(precio, precio_existente)
                    
                    if diferencia_precio < 0.10:  # Máximo 10% de diferencia
                        encontrado = True
                        # Mantener el nombre más largo (más completo)
                        if len(nombre) > len(nombre_existente):
                            productos_unicos[key]['nombre'] = prod['nombre']
                        break
                else:
                    # Si no hay precio, usar solo similitud de texto
                    encontrado = True
                    if len(nombre) > len(nombre_existente):
                        productos_unicos[key]['nombre'] = prod['nombre']
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
    tasa_deduplicacion = (duplicados_eliminados / len(productos) * 100) if len(productos) > 0 else 0
    
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
