"""
Sistema de aprendizaje automático de correcciones de productos.
Permite que el sistema aprenda de correcciones manuales.
"""
import re
from typing import Optional, List, Dict
import psycopg2
from difflib import SequenceMatcher
import logging

logger = logging.getLogger(__name__)


def normalizar_nombre(nombre: str) -> str:
    """Normaliza un nombre para búsqueda fuzzy"""
    if not nombre:
        return ""
    
    # Lowercase
    nombre = nombre.lower()
    
    # Quitar tildes
    tildes = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'Á': 'a', 'É': 'e', 'Í': 'i', 'Ó': 'o', 'Ú': 'u',
        'ñ': 'n', 'Ñ': 'n'
    }
    for origen, destino in tildes.items():
        nombre = nombre.replace(origen, destino)
    
    # Quitar caracteres especiales (excepto espacios)
    nombre = re.sub(r'[^a-z0-9\s]', '', nombre)
    
    # Normalizar espacios
    nombre = ' '.join(nombre.split())
    
    return nombre.strip()


def similitud_nombres(nombre1: str, nombre2: str) -> float:
    """
    Calcula similitud entre dos nombres (0.0 a 1.0)
    Usa SequenceMatcher de difflib
    """
    norm1 = normalizar_nombre(nombre1)
    norm2 = normalizar_nombre(nombre2)
    
    if not norm1 or not norm2:
        return 0.0
    
    return SequenceMatcher(None, norm1, norm2).ratio()


def guardar_correccion(
    conn,
    nombre_ocr: str,
    codigo_ocr: str,
    codigo_correcto: str,
    nombre_correcto: Optional[str] = None,
    establecimiento_id: Optional[int] = None,
    factura_id: Optional[int] = None,
    usuario_id: Optional[int] = None
) -> int:
    """
    Guarda una corrección manual en la base de datos.
    
    Returns:
        ID de la corrección guardada
    """
    cursor = conn.cursor()
    
    try:
        nombre_normalizado = normalizar_nombre(nombre_ocr)
        
        # Verificar si ya existe esta corrección
        cursor.execute("""
            SELECT id, veces_aplicado 
            FROM correcciones_productos
            WHERE nombre_normalizado = %s 
            AND (establecimiento_id = %s OR establecimiento_id IS NULL)
        """, (nombre_normalizado, establecimiento_id))
        
        existing = cursor.fetchone()
        
        if existing:
            # Actualizar corrección existente
            correccion_id, veces = existing
            cursor.execute("""
                UPDATE correcciones_productos
                SET codigo_correcto = %s,
                    nombre_correcto = %s,
                    fecha_correccion = CURRENT_TIMESTAMP
                WHERE id = %s
                RETURNING id
            """, (codigo_correcto, nombre_correcto or nombre_ocr, correccion_id))
            
            logger.info(f"✏️ Corrección actualizada: '{nombre_ocr}' -> {codigo_correcto}")
        else:
            # Insertar nueva corrección
            cursor.execute("""
                INSERT INTO correcciones_productos (
                    nombre_ocr, codigo_ocr, codigo_correcto, nombre_correcto,
                    nombre_normalizado, establecimiento_id, factura_id, usuario_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                nombre_ocr, codigo_ocr, codigo_correcto, nombre_correcto or nombre_ocr,
                nombre_normalizado, establecimiento_id, factura_id, usuario_id
            ))
            
            logger.info(f"✅ Nueva corrección guardada: '{nombre_ocr}' -> {codigo_correcto}")
        
        correccion_id = cursor.fetchone()[0]
        conn.commit()
        return correccion_id
        
    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Error guardando corrección: {e}")
        raise
    finally:
        cursor.close()


def buscar_correccion(
    conn,
    nombre_producto: str,
    establecimiento_id: Optional[int] = None,
    umbral_similitud: float = 0.85
) -> Optional[Dict]:
    """
    Busca una corrección existente para un producto.
    
    Args:
        nombre_producto: Nombre del producto a buscar
        establecimiento_id: ID del establecimiento (opcional, prioriza correcciones específicas)
        umbral_similitud: Mínimo de similitud requerido (0.0 a 1.0)
    
    Returns:
        Dict con la corrección encontrada o None
    """
    cursor = conn.cursor()
    
    try:
        nombre_normalizado = normalizar_nombre(nombre_producto)
        
        # 1. Buscar coincidencia exacta (mismo establecimiento)
        if establecimiento_id:
            cursor.execute("""
                SELECT id, nombre_ocr, codigo_correcto, nombre_correcto, veces_aplicado
                FROM correcciones_productos
                WHERE nombre_normalizado = %s 
                AND establecimiento_id = %s
                ORDER BY veces_aplicado DESC
                LIMIT 1
            """, (nombre_normalizado, establecimiento_id))
            
            result = cursor.fetchone()
            if result:
                logger.info(f"🎯 Match exacto encontrado: '{nombre_producto}' -> {result[2]}")
                return {
                    'id': result[0],
                    'nombre_ocr': result[1],
                    'codigo_correcto': result[2],
                    'nombre_correcto': result[3],
                    'veces_aplicado': result[4],
                    'tipo_match': 'exacto_establecimiento',
                    'similitud': 1.0
                }
        
        # 2. Buscar coincidencia exacta (cualquier establecimiento)
        cursor.execute("""
            SELECT id, nombre_ocr, codigo_correcto, nombre_correcto, veces_aplicado, establecimiento_id
            FROM correcciones_productos
            WHERE nombre_normalizado = %s
            ORDER BY 
                CASE WHEN establecimiento_id = %s THEN 0 ELSE 1 END,
                veces_aplicado DESC
            LIMIT 1
        """, (nombre_normalizado, establecimiento_id))
        
        result = cursor.fetchone()
        if result:
            logger.info(f"🎯 Match exacto encontrado: '{nombre_producto}' -> {result[2]}")
            return {
                'id': result[0],
                'nombre_ocr': result[1],
                'codigo_correcto': result[2],
                'nombre_correcto': result[3],
                'veces_aplicado': result[4],
                'tipo_match': 'exacto_general',
                'similitud': 1.0
            }
        
        # 3. Buscar similitud fuzzy (más costoso, solo si no hay match exacto)
        cursor.execute("""
            SELECT id, nombre_ocr, codigo_correcto, nombre_correcto, 
                   veces_aplicado, establecimiento_id, nombre_normalizado
            FROM correcciones_productos
            WHERE establecimiento_id = %s OR establecimiento_id IS NULL
            ORDER BY veces_aplicado DESC
            LIMIT 100
        """, (establecimiento_id,))
        
        correcciones = cursor.fetchall()
        
        mejor_match = None
        mejor_similitud = 0.0
        
        for corr in correcciones:
            similitud = similitud_nombres(nombre_producto, corr[1])
            if similitud > mejor_similitud and similitud >= umbral_similitud:
                mejor_similitud = similitud
                mejor_match = {
                    'id': corr[0],
                    'nombre_ocr': corr[1],
                    'codigo_correcto': corr[2],
                    'nombre_correcto': corr[3],
                    'veces_aplicado': corr[4],
                    'tipo_match': 'fuzzy',
                    'similitud': similitud
                }
        
        if mejor_match:
            logger.info(f"🔍 Match fuzzy encontrado ({mejor_similitud:.2%}): '{nombre_producto}' -> {mejor_match['codigo_correcto']}")
        
        return mejor_match
        
    except Exception as e:
        logger.error(f"❌ Error buscando corrección: {e}")
        return None
    finally:
        cursor.close()


def marcar_correccion_aplicada(conn, correccion_id: int):
    """Incrementa el contador de veces aplicado"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE correcciones_productos
            SET veces_aplicado = veces_aplicado + 1
            WHERE id = %s
        """, (correccion_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"❌ Error marcando corrección aplicada: {e}")
        conn.rollback()
    finally:
        cursor.close()


def aplicar_correcciones_automaticas(
    conn,
    productos: List[Dict],
    establecimiento_id: Optional[int] = None,
    umbral_similitud: float = 0.85
) -> List[Dict]:
    """
    Aplica correcciones automáticas a una lista de productos del OCR.
    
    Args:
        productos: Lista de productos detectados por OCR
        establecimiento_id: ID del establecimiento
        umbral_similitud: Umbral para matches fuzzy
    
    Returns:
        Lista de productos con correcciones aplicadas y metadata
    """
    productos_corregidos = []
    stats = {'corregidos': 0, 'sin_cambios': 0, 'fuzzy': 0}
    
    for producto in productos:
        codigo_original = producto.get('codigo', '')
        nombre = producto.get('nombre', '')
        
        # Si ya tiene código válido (8+ dígitos), no buscar corrección
        if codigo_original and len(codigo_original) >= 8:
            productos_corregidos.append({
                **producto,
                'correccion_aplicada': False
            })
            stats['sin_cambios'] += 1
            continue
        
        # Buscar corrección
        correccion = buscar_correccion(conn, nombre, establecimiento_id, umbral_similitud)
        
        if correccion:
            # Aplicar corrección
            productos_corregidos.append({
                **producto,
                'codigo': correccion['codigo_correcto'],
                'nombre': correccion['nombre_correcto'],
                'correccion_aplicada': True,
                'correccion_id': correccion['id'],
                'tipo_match': correccion['tipo_match'],
                'similitud': correccion['similitud'],
                'codigo_original': codigo_original
            })
            
            # Marcar como aplicada
            marcar_correccion_aplicada(conn, correccion['id'])
            
            stats['corregidos'] += 1
            if correccion['tipo_match'] == 'fuzzy':
                stats['fuzzy'] += 1
        else:
            # No hay corrección, mantener original
            productos_corregidos.append({
                **producto,
                'correccion_aplicada': False
            })
            stats['sin_cambios'] += 1
    
    logger.info(f"📊 Correcciones aplicadas: {stats['corregidos']}/{len(productos)} "
                f"(fuzzy: {stats['fuzzy']}, sin cambios: {stats['sin_cambios']})")
    
    return productos_corregidos
