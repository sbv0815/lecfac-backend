"""
Sistema Completo de Auditoría para Base de Datos de Facturas
Incluye auditorías avanzadas, análisis inteligente de precios y corrección automática de problemas
"""

import threading
import time
import json
import logging
import decimal
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
from database import get_db_connection

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("audit_system.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AuditSystem")

# Encoder personalizado para manejar decimales en json
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super().default(o)

class AuditSystem:
    """Sistema completo de auditoría automática para facturas"""
    
    def __init__(self):
        self.audit_logs = []
        self.price_tolerance = {
            'mismo_establecimiento': 0.15,
            'misma_cadena': 0.25,
            'diferente_cadena': 0.50,
            'productos_frescos': 0.40
        }
        self.logger = logger
    
    def run_daily_audit(self) -> Dict:
        """Ejecuta todas las auditorías diarias y mejoras de calidad"""
        self.logger.info("🔍 Iniciando auditoría completa...")
        
        # Primero ejecutar mejoras de calidad de datos
        quality_improvement = self.improve_data_quality()
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'duplicates': self.detect_duplicate_invoices(),
            'math_errors': self.verify_invoice_math(),
            'price_anomalies': self.detect_price_anomalies(),
            'product_issues': self.audit_product_catalog(),
            'fresh_products': self.audit_fresh_products(),
            'data_quality': self.assess_data_quality(),
            'quality_improvement': quality_improvement
        }
        
        # Guardar resultados
        self._save_audit_log(results)
        
        # Aplicar correcciones automáticas según los resultados
        self._apply_automatic_corrections(results)
        
        self.logger.info(f"✅ Auditoría completada: {json.dumps(results, cls=DecimalEncoder)}")
        return results
    
    def detect_duplicate_invoices(self) -> Dict:
        """
        Detecta facturas duplicadas utilizando múltiples criterios
        para una identificación más precisa
        """
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Consulta avanzada para detectar duplicados
            # Usa varios criterios y tiene mayor tolerancia en el total para detectar más duplicados
            cursor.execute("""
                WITH potential_duplicates AS (
                    SELECT 
                        usuario_id, 
                        establecimiento, 
                        ROUND(total_factura, -2) as rounded_total, -- Redondear a centenas
                        DATE(fecha_cargue) as fecha,
                        COUNT(*) as duplicados,
                        STRING_AGG(id::text, ',') as ids
                    FROM facturas
                    WHERE fecha_cargue >= (CURRENT_DATE - INTERVAL '90 days')
                      AND (estado_validacion IS NULL OR estado_validacion != 'duplicado')
                    GROUP BY usuario_id, establecimiento, ROUND(total_factura, -2), DATE(fecha_cargue)
                    HAVING COUNT(*) > 1
                )
                SELECT * FROM potential_duplicates
                ORDER BY duplicados DESC
            """)
            
            duplicates = cursor.fetchall()
            self.logger.info(f"Encontrados {len(duplicates)} grupos de duplicados potenciales")
            
            processed = 0
            duplicates_details = []
            
            for dup in duplicates:
                usuario_id, establecimiento, total, fecha, count, ids_str = dup
                ids = ids_str.split(',')
                
                # Asegurar que el id original existe
                if not ids or len(ids) < 2:
                    continue
                
                # El primer ID se mantiene como original
                original_id = int(ids[0])
                
                # Información para el reporte
                dup_info = {
                    "establecimiento": establecimiento,
                    "total": float(total) if total else 0,
                    "fecha": fecha.isoformat() if hasattr(fecha, 'isoformat') else str(fecha),
                    "ids_afectados": []
                }
                
                for dup_id in ids[1:]:
                    try:
                        # Convertir explícitamente a enteros
                        duplicate_id = int(dup_id)
                        
                        # Verificación adicional - comprobar si realmente es un duplicado
                        # mediante el análisis de productos
                        cursor.execute("""
                            WITH orig_prods AS (
                                SELECT STRING_AGG(COALESCE(codigo, nombre), ',' ORDER BY codigo) as productos
                                FROM productos
                                WHERE factura_id = %s
                            ),
                            dup_prods AS (
                                SELECT STRING_AGG(COALESCE(codigo, nombre), ',' ORDER BY codigo) as productos
                                FROM productos
                                WHERE factura_id = %s
                            )
                            SELECT 
                                orig_prods.productos, 
                                dup_prods.productos,
                                CASE
                                    WHEN orig_prods.productos = dup_prods.productos THEN 'exacto'
                                    WHEN orig_prods.productos LIKE '%' || dup_prods.productos || '%' 
                                      OR dup_prods.productos LIKE '%' || orig_prods.productos || '%' THEN 'parcial'
                                    ELSE 'diferente'
                                END as similitud
                            FROM orig_prods, dup_prods
                        """, (original_id, duplicate_id))
                        
                        result = cursor.fetchone()
                        
                        # Solo marcar como duplicado si hay similitud en productos
                        if result and result[2] != 'diferente':
                            cursor.execute("""
                                UPDATE facturas 
                                SET estado_validacion = 'duplicado',
                                    notas = CONCAT(COALESCE(notas, ''), ' | Duplicado de factura #', %s),
                                    puntaje_calidad = 0
                                WHERE id = %s
                            """, (original_id, duplicate_id))
                            
                            processed += 1
                            dup_info["ids_afectados"].append(duplicate_id)
                    except (ValueError, TypeError) as e:
                        self.logger.error(f"Error al procesar IDs de facturas duplicadas: {e}")
                        continue
                
                if dup_info["ids_afectados"]:
                    duplicates_details.append(dup_info)
            
            conn.commit()
            
            return {
                'found': len(duplicates),
                'processed': processed,
                'details': duplicates_details,
                'status': 'success'
            }
            
        except Exception as e:
            self.logger.error(f"Error detectando duplicados: {e}", exc_info=True)
            conn.rollback()
            return {'error': str(e), 'status': 'failed'}
        finally:
            cursor.close()
            conn.close()
    
    def verify_invoice_math(self) -> Dict:
        """Verifica discrepancias matemáticas entre totales y sumas de productos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Consulta mejorada para verificar matemáticas de facturas
            # Incluye más facturas (30 días) y mejora el cálculo de errores
            cursor.execute("""
                WITH factura_math AS (
                    SELECT 
                        f.id,
                        f.establecimiento,
                        f.total_factura,
                        COALESCE(SUM(pp.precio), 0) as suma_productos,
                        COUNT(pp.id) as num_productos,
                        f.puntaje_calidad
                    FROM facturas f
                    LEFT JOIN precios_productos pp ON f.id = pp.factura_id
                    WHERE f.estado_validacion NOT IN ('duplicado', 'error_matematico')
                      AND f.fecha_cargue >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY f.id, f.establecimiento, f.total_factura, f.puntaje_calidad
                )
                SELECT 
                    id, 
                    establecimiento,
                    total_factura, 
                    suma_productos,
                    ABS(total_factura - suma_productos) as diferencia,
                    CASE 
                        WHEN total_factura = 0 THEN 100
                        ELSE ABS(total_factura - suma_productos) * 100.0 / total_factura
                    END as error_porcentaje,
                    puntaje_calidad
                FROM factura_math
                WHERE total_factura > 0
            """)
            
            all_invoices = cursor.fetchall()
            errors_found = 0
            warnings_found = 0
            math_errors_details = []
            
            for invoice in all_invoices:
                factura_id, estab, total, suma, diff, error_pct, puntaje = invoice
                
                error_info = {
                    "factura_id": factura_id,
                    "establecimiento": estab,
                    "total_factura": float(total) if total else 0,
                    "suma_productos": float(suma) if suma else 0,
                    "diferencia_porcentual": float(error_pct) if error_pct else 0
                }
                
                if error_pct > 20:
                    cursor.execute("""
                        UPDATE facturas 
                        SET estado_validacion = 'error_matematico',
                            notas = %s,
                            puntaje_calidad = GREATEST(0, COALESCE(puntaje_calidad, 0) - 30)
                        WHERE id = %s
                    """, (
                        f"Error matemático: Total ${total} vs Suma ${suma} ({error_pct:.1f}% diferencia)",
                        factura_id
                    ))
                    errors_found += 1
                    error_info["gravedad"] = "error"
                    error_info["accion"] = "Marcado como error matemático y reducido puntaje en 30 puntos"
                    math_errors_details.append(error_info)
                    
                elif error_pct > 10:
                    cursor.execute("""
                        UPDATE facturas 
                        SET notas = CONCAT(COALESCE(notas, ''), ' | Advertencia: diferencia ', %s, '%'),
                            puntaje_calidad = GREATEST(0, COALESCE(puntaje_calidad, 0) - 10)
                        WHERE id = %s
                    """, (f"{error_pct:.1f}", factura_id))
                    warnings_found += 1
                    error_info["gravedad"] = "advertencia"
                    error_info["accion"] = "Reducido puntaje de calidad en 10 puntos"
                    math_errors_details.append(error_info)
            
            conn.commit()
            
            return {
                'total_checked': len(all_invoices),
                'errors': errors_found,
                'warnings': warnings_found,
                'details': math_errors_details,
                'status': 'success'
            }
            
        except Exception as e:
            self.logger.error(f"Error verificando matemáticas: {e}", exc_info=True)
            return {'error': str(e), 'status': 'failed'}
        finally:
            conn.close()

    def detect_price_anomalies(self) -> Dict:
        """Detecta anomalías en precios de productos comparando entre establecimientos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Detectar anomalías de precios comparando con precios históricos
            cursor.execute("""
                WITH precio_promedio AS (
                    SELECT 
                        pc.codigo_ean,
                        pc.nombre_producto,
                        AVG(pp.precio) as precio_promedio,
                        STDDEV(pp.precio) as desviacion_estandar,
                        COUNT(pp.id) as total_precios
                    FROM productos_catalogo pc
                    JOIN precios_productos pp ON pc.id = pp.producto_id
                    JOIN facturas f ON pp.factura_id = f.id
                    WHERE f.fecha_cargue >= CURRENT_DATE - INTERVAL '90 days'
                      AND f.estado_validacion != 'duplicado'
                    GROUP BY pc.codigo_ean, pc.nombre_producto
                    HAVING COUNT(pp.id) > 3 -- Solo productos con suficientes datos
                ),
                precios_recientes AS (
                    SELECT 
                        pp.id,
                        f.id as factura_id,
                        f.establecimiento,
                        f.cadena,
                        pc.codigo_ean,
                        pc.nombre_producto,
                        pp.precio,
                        pa.precio_promedio,
                        pa.desviacion_estandar,
                        CASE 
                            WHEN pa.desviacion_estandar = 0 THEN 0 -- Evitar división por cero
                            ELSE (pp.precio - pa.precio_promedio) / pa.desviacion_estandar
                        END as z_score
                    FROM precio_promedio pa
                    JOIN productos_catalogo pc ON pa.codigo_ean = pc.codigo_ean
                    JOIN precios_productos pp ON pc.id = pp.producto_id
                    JOIN facturas f ON pp.factura_id = f.id
                    WHERE f.fecha_cargue >= CURRENT_DATE - INTERVAL '30 days'
                      AND f.estado_validacion != 'duplicado'
                )
                SELECT * FROM precios_recientes
                WHERE ABS(z_score) > 2 -- Más de 2 desviaciones estándar
                ORDER BY ABS(z_score) DESC
                LIMIT 100
            """)
            
            anomalies = cursor.fetchall()
            anomaly_details = []
            
            for anomaly in anomalies:
                precio_id, factura_id, establecimiento, cadena, codigo, nombre, precio, promedio, desviacion, z_score = anomaly
                
                # Calcular variación porcentual de manera segura
                variacion_porcentual = 0
                if promedio and promedio != 0:
                    variacion_porcentual = (precio - promedio) * 100 / promedio
                
                # Añadir a detalles
                anomaly_details.append({
                    "precio_id": precio_id,
                    "factura_id": factura_id,
                    "establecimiento": establecimiento,
                    "cadena": cadena,
                    "producto": {
                        "codigo": codigo,
                        "nombre": nombre
                    },
                    "precio_actual": float(precio) if precio else 0,
                    "precio_promedio": float(promedio) if promedio else 0,
                    "desviacion": float(desviacion) if desviacion else 0,
                    "z_score": float(z_score) if z_score else 0,
                    "variacion_porcentual": float(variacion_porcentual)
                })
                
                # Marcar la anomalía en la base de datos
                cursor.execute("""
                    UPDATE precios_productos
                    SET es_anomalia = TRUE,
                        notas = %s
                    WHERE id = %s
                """, (
                    f"Precio anómalo: {precio} vs promedio {promedio:.2f} (z-score: {z_score:.2f})",
                    precio_id
                ))
            
            # También actualizar las facturas afectadas
            if anomaly_details:
                factura_ids = [a["factura_id"] for a in anomaly_details]
                placeholders = ','.join(['%s'] * len(factura_ids))
                
                cursor.execute(f"""
                    UPDATE facturas
                    SET tiene_anomalias_precio = TRUE,
                        notas = CONCAT(COALESCE(notas, ''), ' | Detectadas anomalías de precios'),
                        puntaje_calidad = GREATEST(0, COALESCE(puntaje_calidad, 0) - 5)
                    WHERE id IN ({placeholders})
                """, factura_ids)
            
            conn.commit()
            
            return {
                'checked': cursor.rowcount,
                'anomalies': len(anomaly_details),
                'details': anomaly_details,
                'status': 'success'
            }
            
        except Exception as e:
            self.logger.error(f"Error detectando anomalías de precios: {e}", exc_info=True)
            return {
                'checked': 0, 
                'anomalies': 0, 
                'details': [],
                'error': str(e),
                'status': 'failed'
            }
        finally:
            conn.close()

    def audit_product_catalog(self) -> Dict:
        """Audita y mejora el catálogo de productos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 1. Detectar productos duplicados (mismo nombre, diferente código)
            cursor.execute("""
                WITH producto_similares AS (
                    SELECT 
                        LOWER(REGEXP_REPLACE(nombre_producto, '[^a-zA-Z0-9]', '', 'g')) as nombre_normalizado,
                        COUNT(*) as total,
                        STRING_AGG(id::text, ',') as ids,
                        STRING_AGG(nombre_producto, ' | ') as nombres
                    FROM productos_catalogo
                    GROUP BY LOWER(REGEXP_REPLACE(nombre_producto, '[^a-zA-Z0-9]', '', 'g'))
                    HAVING COUNT(*) > 1
                )
                SELECT * FROM producto_similares
                ORDER BY total DESC
                LIMIT 50
            """)
            
            duplicates = cursor.fetchall()
            duplicates_fixed = 0
            duplicate_details = []
            
            for dup in duplicates:
                nombre_norm, total, ids_str, nombres = dup
                # Solo mostrar en reporte, no corregir automáticamente
                duplicate_details.append({
                    "nombre_normalizado": nombre_norm,
                    "total_duplicados": total,
                    "ids": ids_str,
                    "nombres": nombres
                })
            
            # 2. Corregir nombres imprecisos (muy cortos o genéricos)
            cursor.execute("""
                UPDATE productos_catalogo
                SET requiere_revision = TRUE
                WHERE LENGTH(nombre_producto) < 5
                   OR nombre_producto IN ('producto', 'articulo', 'item', 'varios')
                   OR nombre_producto LIKE '%?%'
                   OR nombre_producto LIKE '%sin nombre%'
                RETURNING id
            """)
            
            nombres_imprecisos = cursor.rowcount
            
            # 3. Identificar productos sin movimiento reciente
            cursor.execute("""
                UPDATE productos_catalogo
                SET activo = FALSE
                WHERE id NOT IN (
                    SELECT DISTINCT pp.producto_id 
                    FROM precios_productos pp
                    JOIN facturas f ON pp.factura_id = f.id
                    WHERE f.fecha_cargue >= CURRENT_DATE - INTERVAL '180 days'
                )
                RETURNING id
            """)
            
            sin_movimiento = cursor.rowcount
            
            conn.commit()
            
            return {
                'issues_fixed': duplicates_fixed + nombres_imprecisos + sin_movimiento,
                'duplicates_found': len(duplicates),
                'imprecise_names': nombres_imprecisos,
                'inactive_products': sin_movimiento,
                'details': duplicate_details,
                'status': 'success'
            }
            
        except Exception as e:
            self.logger.error(f"Error auditando catálogo: {e}", exc_info=True)
            return {
                'issues_fixed': 0, 
                'details': [], 
                'error': str(e),
                'status': 'failed'
            }
        finally:
            conn.close()

    def audit_fresh_products(self) -> Dict:
        """Audita productos frescos por cadena"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Productos frescos por cadena
            cursor.execute("""
                WITH productos_por_cadena AS (
                    SELECT 
                        c.cadena,
                        COUNT(DISTINCT c.producto_id) as total_productos
                    FROM codigos_locales c
                    JOIN productos_catalogo pc ON c.producto_id = pc.id
                    WHERE pc.es_producto_fresco = TRUE
                    GROUP BY c.cadena
                )
                SELECT * FROM productos_por_cadena
                ORDER BY total_productos DESC
            """)
            
            mapeos_por_cadena = []
            for row in cursor.fetchall():
                cadena, total = row
                mapeos_por_cadena.append({
                    "cadena": cadena,
                    "total_productos": total
                })
            
            # Productos huérfanos (sin mapeo a código local)
            cursor.execute("""
                SELECT COUNT(*) 
                FROM productos_catalogo pc
                WHERE pc.es_producto_fresco = TRUE
                  AND NOT EXISTS (
                    SELECT 1 FROM codigos_locales c
                    WHERE c.producto_id = pc.id
                  )
            """)
            
            codigos_huerfanos = cursor.fetchone()[0]
            
            return {
                'mapeos_por_cadena': mapeos_por_cadena,
                'codigos_huerfanos': codigos_huerfanos,
                'status': 'success'
            }
            
        except Exception as e:
            self.logger.error(f"Error auditando productos frescos: {e}", exc_info=True)
            return {
                'mapeos_por_cadena': [], 
                'codigos_huerfanos': 0,
                'error': str(e),
                'status': 'failed'
            }
        finally:
            conn.close()

    def assess_data_quality(self) -> Dict:
        """Evalúa la calidad general de los datos de manera más completa"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Consulta completa de calidad
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_facturas,
                    AVG(puntaje_calidad) as calidad_promedio,
                    COUNT(CASE WHEN estado_validacion = 'procesado' THEN 1 END) as procesadas,
                    COUNT(CASE WHEN estado_validacion LIKE '%error%' THEN 1 END) as con_error,
                    COUNT(CASE WHEN estado_validacion = 'revision' THEN 1 END) as en_revision,
                    COUNT(CASE WHEN imagen_data IS NOT NULL THEN 1 END) as con_imagen,
                    COUNT(CASE WHEN puntaje_calidad IS NULL THEN 1 END) as sin_puntaje,
                    COUNT(CASE WHEN puntaje_calidad >= 80 THEN 1 END) as calidad_alta,
                    COUNT(CASE WHEN puntaje_calidad < 30 THEN 1 END) as calidad_baja
                FROM facturas
                WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '30 days'
            """)
            
            stats = cursor.fetchone()
            
            # Calcular el health score usando más factores
            total_facturas = stats[0] or 1  # Evitar división por cero
            facturas_procesadas = stats[2] or 0
            facturas_con_error = stats[3] or 0
            facturas_con_imagen = stats[5] or 0
            facturas_calidad_alta = stats[7] or 0
            
            # Puntaje basado en:
            # - Porcentaje de facturas procesadas correctamente (30%)
            # - Porcentaje de facturas con imágenes (40%)
            # - Porcentaje de facturas con alta calidad (30%)
            score_procesadas = (facturas_procesadas / total_facturas) * 30
            score_imagenes = (facturas_con_imagen / total_facturas) * 40
            score_calidad = (facturas_calidad_alta / total_facturas) * 30
            
            # Penalizar por facturas con errores
            penalizacion_errores = (facturas_con_error / total_facturas) * 15
            
            health_score = min(100, score_procesadas + score_imagenes + score_calidad - penalizacion_errores)
            
            # Estadísticas por fecha
            cursor.execute("""
                SELECT 
                    DATE(fecha_cargue) as fecha,
                    COUNT(*) as total_facturas,
                    AVG(puntaje_calidad) as calidad_promedio,
                    COUNT(CASE WHEN imagen_data IS NOT NULL THEN 1 END) as con_imagen
                FROM facturas
                WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(fecha_cargue)
                ORDER BY DATE(fecha_cargue) DESC
            """)
            
            daily_stats = []
            for row in cursor.fetchall():
                fecha, total, calidad, con_imagen = row
                # Calcular porcentaje con imagen de forma segura
                porcentaje_con_imagen = 0
                if total and total > 0:
                    porcentaje_con_imagen = (con_imagen / total * 100)
                
                daily_stats.append({
                    "fecha": fecha.isoformat() if hasattr(fecha, 'isoformat') else str(fecha),
                    "total_facturas": total,
                    "calidad_promedio": float(calidad) if calidad else 0,
                    "porcentaje_con_imagen": porcentaje_con_imagen
                })
            
            # Crear resultado detallado
            quality_result = {
                'health_score': health_score,
                'total_invoices': stats[0],
                'avg_quality': float(stats[1]) if stats[1] else 0,
                'processed': stats[2],
                'errors': stats[3],
                'pending_review': stats[4],
                'with_images': stats[5],
                'without_score': stats[6],
                'high_quality': stats[7],
                'low_quality': stats[8],
                'daily_stats': daily_stats,
                'improvement_needed': health_score < 70,
                'critical_state': health_score < 40,
                'status': 'success'
            }
            
            return quality_result
            
        except Exception as e:
            self.logger.error(f"Error evaluando calidad: {e}", exc_info=True)
            return {
                'health_score': 0, 
                'total_invoices': 0, 
                'avg_quality': 0,
                'error': str(e),
                'status': 'failed'
            }
        finally:
            if conn:
                conn.close()
    
    def improve_data_quality(self) -> Dict:
        """Mejora la calidad de datos existentes"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            improvements = {
                'facturas_actualizadas': 0,
                'sin_imagen_mejoradas': 0,
                'sin_puntaje_evaluadas': 0,
                'errores_matematicos_corregidos': 0,
                'estado_validacion_actualizado': 0,
            }
            
            # 1. Aplicar puntaje a facturas sin puntaje
            cursor.execute("""
                WITH factura_updates AS (
                    SELECT 
                        id,
                        CASE
                            WHEN imagen_data IS NOT NULL THEN 50 -- Base para facturas con imagen
                            ELSE 20 -- Base para facturas sin imagen
                        END +
                        CASE
                            WHEN establecimiento IS NOT NULL AND LENGTH(establecimiento) > 3 THEN 10
                            ELSE 0
                        END +
                        CASE
                            WHEN total_factura > 0 THEN 20
                            ELSE 0
                        END as nuevo_puntaje
                    FROM facturas
                    WHERE puntaje_calidad IS NULL
                )
                UPDATE facturas f
                SET puntaje_calidad = fu.nuevo_puntaje
                FROM factura_updates fu
                WHERE f.id = fu.id
                RETURNING f.id
            """)
            
            improvements['sin_puntaje_evaluadas'] = cursor.rowcount
            
            # 2. Mejorar facturas sin imagen pero con buenos datos
            cursor.execute("""
                UPDATE facturas
                SET puntaje_calidad = 30,
                    notas = CONCAT(COALESCE(notas, ''), ' | Puntaje ajustado en auditoría')
                WHERE imagen_data IS NULL
                  AND puntaje_calidad < 30
                  AND establecimiento IS NOT NULL
                  AND total_factura > 0
                  AND estado_validacion != 'duplicado'
                RETURNING id
            """)
            
            improvements['sin_imagen_mejoradas'] = cursor.rowcount
            
            # 3. Corregir facturas con errores matemáticos leves
            cursor.execute("""
                WITH factura_math AS (
                    SELECT 
                        f.id,
                        f.total_factura,
                        COALESCE(SUM(pp.precio), 0) as suma_productos,
                        CASE 
                            WHEN f.total_factura = 0 THEN 100
                            ELSE ABS(f.total_factura - COALESCE(SUM(pp.precio), 0)) * 100.0 / f.total_factura
                        END as error_porcentaje
                    FROM facturas f
                    JOIN precios_productos pp ON f.id = pp.factura_id
                    WHERE f.estado_validacion NOT IN ('duplicado', 'error_matematico')
                      AND f.total_factura > 0
                    GROUP BY f.id, f.total_factura
                    HAVING ABS(f.total_factura - COALESCE(SUM(pp.precio), 0)) * 100.0 / f.total_factura BETWEEN 5 AND 15
                )
                UPDATE facturas f
                SET total_factura = fm.suma_productos,
                    puntaje_calidad = GREATEST(COALESCE(puntaje_calidad, 0), 60),
                    notas = CONCAT(COALESCE(notas, ''), ' | Total ajustado automáticamente')
                FROM factura_math fm
                WHERE f.id = fm.id
                  AND fm.suma_productos > 0
                RETURNING f.id
            """)
            
            improvements['errores_matematicos_corregidos'] = cursor.rowcount
            
            # 4. Actualizar estados de validación
            cursor.execute("""
                UPDATE facturas
                SET estado_validacion = 
                    CASE 
                        WHEN puntaje_calidad >= 70 THEN 'procesado'
                        WHEN puntaje_calidad >= 40 THEN 'revision'
                        ELSE 'error'
                    END
                WHERE estado_validacion IS NULL
                  OR (estado_validacion NOT IN ('duplicado', 'error_matematico') 
                      AND estado_validacion != 
                        CASE 
                            WHEN puntaje_calidad >= 70 THEN 'procesado'
                            WHEN puntaje_calidad >= 40 THEN 'revision'
                            ELSE 'error'
                        END
                     )
                RETURNING id
            """)
            
            improvements['estado_validacion_actualizado'] = cursor.rowcount
            
            # Calcular total
            improvements['facturas_actualizadas'] = (
                improvements['sin_puntaje_evaluadas'] + 
                improvements['sin_imagen_mejoradas'] + 
                improvements['errores_matematicos_corregidos'] + 
                improvements['estado_validacion_actualizado']
            )
            
            conn.commit()
            
            # Registrar mejoras
            self.logger.info(f"✅ Mejoras de calidad aplicadas: {improvements}")
            
            improvements['status'] = 'success'
            return improvements
            
        except Exception as e:
            self.logger.error(f"Error mejorando calidad de datos: {e}", exc_info=True)
            conn.rollback()
            return {'error': str(e), 'status': 'failed'}
        finally:
            cursor.close()
            conn.close()

    def _apply_automatic_corrections(self, results: Dict) -> None:
        """Aplica correcciones automáticas basadas en los resultados de la auditoría"""
        try:
            # Verificar si hay problemas críticos
            quality = results.get('data_quality', {})
            health_score = quality.get('health_score', 0)
            
            if health_score < 40:
                self.logger.warning(f"🚨 Salud del sistema crítica: {health_score}/100. Aplicando correcciones...")
                
                # Aplicar mejoras más agresivas
                conn = get_db_connection()
                cursor = conn.cursor()
                
                try:
                    # 1. Corregir facturas con precios anómalos extremos
                    cursor.execute("""
                        WITH precio_extremo AS (
                            SELECT 
                                pp.id,
                                pp.precio,
                                pc.nombre_producto,
                                AVG(pp2.precio) as precio_promedio
                            FROM precios_productos pp
                            JOIN productos_catalogo pc ON pp.producto_id = pc.id
                            JOIN precios_productos pp2 ON pc.id = pp2.producto_id AND pp2.id != pp.id
                            GROUP BY pp.id, pp.precio, pc.nombre_producto
                            HAVING (pp.precio > 10 * AVG(pp2.precio) AND AVG(pp2.precio) > 0)
                               OR (pp.precio < 0.1 * AVG(pp2.precio) AND AVG(pp2.precio) > 0)
                        )
                        UPDATE precios_productos pp
                        SET precio = pe.precio_promedio,
                            notas = CONCAT('Precio corregido automáticamente. Original: ', pp.precio, ' -> Nuevo: ', pe.precio_promedio)
                        FROM precio_extremo pe
                        WHERE pp.id = pe.id
                    """)
                    
                    # 2. Marcar facturas problemáticas para revisión manual
                    cursor.execute("""
                        UPDATE facturas
                        SET estado_validacion = 'requiere_intervencion',
                            notas = CONCAT(COALESCE(notas, ''), ' | Marcado para revisión por auditoría automática')
                        WHERE puntaje_calidad < 20
                          AND estado_validacion NOT IN ('duplicado', 'error_matematico')
                          AND fecha_cargue >= CURRENT_DATE - INTERVAL '30 days'
                    """)
                    
                    conn.commit()
                    self.logger.info("✅ Correcciones automáticas aplicadas con éxito")
                    
                except Exception as e:
                    self.logger.error(f"Error en correcciones automáticas: {e}", exc_info=True)
                    conn.rollback()
                finally:
                    cursor.close()
                    conn.close()
        except Exception as e:
            self.logger.error(f"Error al aplicar correcciones: {e}", exc_info=True)

    def _save_audit_log(self, results: Dict) -> None:
        """Guarda log de auditoría en la base de datos"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Convertir el resultado a JSON
            results_json = json.dumps(results, cls=DecimalEncoder)
            
            # Guardar en la tabla de logs
            cursor.execute("""
                INSERT INTO audit_logs (
                    fecha_ejecucion, 
                    resultado, 
                    health_score,
                    facturas_revisadas,
                    duplicados_encontrados,
                    errores_matematicos
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                datetime.now(),
                results_json,
                results.get('data_quality', {}).get('health_score', 0),
                results.get('data_quality', {}).get('total_invoices', 0),
                results.get('duplicates', {}).get('found', 0),
                results.get('math_errors', {}).get('errors', 0)
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Mantener historial local
            self.audit_logs.append(results)
            # Solo mantener los últimos 10 logs
            if len(self.audit_logs) > 10:
                self.audit_logs = self.audit_logs[-10:]
                
        except Exception as e:
            self.logger.error(f"Error guardando log de auditoría: {e}", exc_info=True)
    
    def _get_recent_audit_logs(self, limit: int = 5) -> List[Dict]:
        """Obtiene logs recientes de auditoría"""
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    id, 
                    fecha_ejecucion, 
                    health_score, 
                    facturas_revisadas, 
                    duplicados_encontrados, 
                    errores_matematicos
                FROM audit_logs
                ORDER BY fecha_ejecucion DESC
                LIMIT %s
            """, (limit,))
            
            logs = []
            for row in cursor.fetchall():
                log_id, fecha, health_score, facturas, duplicados, errores = row
                logs.append({
                    "id": log_id,
                    "fecha": fecha.isoformat() if hasattr(fecha, 'isoformat') else str(fecha),
                    "health_score": float(health_score) if health_score else 0,
                    "facturas_revisadas": facturas,
                    "duplicados_encontrados": duplicados,
                    "errores_matematicos": errores
                })
            
            cursor.close()
            conn.close()
            return logs
            
        except Exception as e:
            self.logger.error(f"Error obteniendo logs recientes: {e}", exc_info=True)
            return []

    def _calculate_system_health(self, quality: Dict) -> str:
        """Calcula el estado de salud del sistema"""
        score = quality.get('health_score', 100)
        
        if score >= 90:
            return "🟢 Excelente"
        elif score >= 70:
            return "🟡 Bueno"
        elif score >= 50:
            return "🟠 Regular"
        else:
            return "🔴 Requiere atención"

    def generate_audit_report(self) -> Dict:
        """Genera reporte completo de auditoría con recomendaciones detalladas"""
        # Obtener métricas actuales
        quality = self.assess_data_quality()
        recent_logs = self._get_recent_audit_logs(5)
        
        # Generar recomendaciones basadas en datos reales
        recommendations = []
        
        # Recomendación 1: Calidad de imágenes
        if quality.get('with_images', 0) < quality.get('total_invoices', 1) * 0.7:
            image_percent = (quality.get('with_images', 0) / quality.get('total_invoices', 1) * 100)
            recommendations.append(
                f"⬆️ Aumentar porcentaje de facturas con imágenes (actualmente {image_percent:.1f}%). "
                "Modifica la app móvil para exigir imágenes de alta calidad."
            )
        
        # Recomendación 2: Validación matemática
        if quality.get('errors', 0) > 0:
            recommendations.append(
                f"🧮 Corregir los {quality.get('errors', 0)} errores matemáticos detectados. "
                "Verificar totales y sumas de productos."
            )
        
        # Recomendación 3: Validación de datos
        if quality.get('improvement_needed', False):
            recommendations.append(
                "✅ Implementar validación estricta de datos antes de aceptar facturas. "
                "Utilizar el validador para rechazar datos incorrectos."
            )
        
        # Recomendación 4: Revisar facturas de baja calidad
        if quality.get('low_quality', 0) > 0:
            recommendations.append(
                f"🔍 Revisar manualmente las {quality.get('low_quality', 0)} facturas con puntaje menor a 30. "
                "Corregir datos o eliminar si son inválidas."
            )
        
        # Si no hay recomendaciones específicas
        if not recommendations:
            recommendations.append("✅ Sistema funcionando correctamente. Mantener monitoreo regular.")
        
        # Calcular tendencia
        trend = "estable"
        if recent_logs and len(recent_logs) > 1:
            first_score = recent_logs[-1].get('health_score', 0)
            last_score = recent_logs[0].get('health_score', 0)
            
            if last_score > first_score + 5:
                trend = "mejorando"
            elif last_score < first_score - 5:
                trend = "empeorando"
        
        # Generar reporte completo
        return {
            'generated_at': datetime.now().isoformat(),
            'data_quality': quality,
            'price_intelligence': self.detect_price_anomalies(),
            'recent_audits': recent_logs,
            'recommendations': recommendations,
            'system_health': self._calculate_system_health(quality),
            'trend': trend
        }
    
    def create_missing_tables(self) -> bool:
        """Crea tablas necesarias para el sistema de auditoría si no existen"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Verificar y crear tabla de logs de auditoría
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    fecha_ejecucion TIMESTAMP NOT NULL,
                    resultado JSONB,
                    health_score NUMERIC(5,2),
                    facturas_revisadas INTEGER,
                    duplicados_encontrados INTEGER,
                    errores_matematicos INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Actualizar tabla de facturas con campos necesarios si no existen
            # Nota: Este enfoque es seguro, verificará primero si la columna existe
            cursor.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'facturas' AND column_name = 'puntaje_calidad') THEN
                        ALTER TABLE facturas ADD COLUMN puntaje_calidad NUMERIC(5,2);
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'facturas' AND column_name = 'tiene_anomalias_precio') THEN
                        ALTER TABLE facturas ADD COLUMN tiene_anomalias_precio BOOLEAN DEFAULT FALSE;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'precios_productos' AND column_name = 'es_anomalia') THEN
                        ALTER TABLE precios_productos ADD COLUMN es_anomalia BOOLEAN DEFAULT FALSE;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'precios_productos' AND column_name = 'notas') THEN
                        ALTER TABLE precios_productos ADD COLUMN notas TEXT;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'productos_catalogo' AND column_name = 'requiere_revision') THEN
                        ALTER TABLE productos_catalogo ADD COLUMN requiere_revision BOOLEAN DEFAULT FALSE;
                    END IF;
                    
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name = 'productos_catalogo' AND column_name = 'activo') THEN
                        ALTER TABLE productos_catalogo ADD COLUMN activo BOOLEAN DEFAULT TRUE;
                    END IF;
                END $$;
            """)
            
            conn.commit()
            self.logger.info("✅ Tablas de auditoría verificadas/creadas correctamente")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creando tablas de auditoría: {e}", exc_info=True)
            conn.rollback()
            return False
        finally:
            cursor.close()
            conn.close()


class AuditScheduler:
    """Programador de auditorías automáticas mejorado"""
    
    def __init__(self):
        self.audit_system = AuditSystem()
        self.is_running = False
        self.thread = None
        self.logger = logging.getLogger("AuditScheduler")
        
        # Asegurar que las tablas necesarias existen
        self.audit_system.create_missing_tables()
    
    def start(self):
        """Inicia el programador de auditorías"""
        if self.is_running:
            self.logger.warning("⚠️ Scheduler ya está en ejecución")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._run_schedule, daemon=True)
        self.thread.start()
        self.logger.info("🔄 Programador de auditorías iniciado")
    
    def stop(self):
        """Detiene el programador"""
        self.is_running = False
        self.logger.info("🛑 Deteniendo programador de auditorías...")
    
    def _run_schedule(self):
        """Ejecuta auditorías según calendario mejorado"""
        last_full_audit = datetime.now() - timedelta(days=1)  # Forzar auditoría completa al inicio
        
        while self.is_running:
            try:
                now = datetime.now()
                
                # Auditoría horaria (detección rápida)
                if now.minute == 0:
                    self.logger.info(f"⏰ Auditoría horaria - {now.strftime('%H:%M')}")
                    self.audit_system.detect_duplicate_invoices()
                    self.audit_system.verify_invoice_math()
                
                # Auditoría de precios cada 6 horas
                if now.hour % 6 == 0 and now.minute == 0:
                    self.logger.info("💰 Auditoría de precios")
                    self.audit_system.detect_price_anomalies()
                
                # Auditoría completa nocturna
                if now.hour == 3 and now.minute == 0 or (now - last_full_audit).total_seconds() > 86400:
                    self.logger.info("🌙 Auditoría completa")
                    results = self.audit_system.run_daily_audit()
                    last_full_audit = now
                    
                    # Verificar salud del sistema
                    health_score = results.get('data_quality', {}).get('health_score', 0)
                    if health_score < 40:
                        self.logger.warning(f"🚨 ALERTA: Salud crítica del sistema ({health_score}/100)")
                        # Aquí podrías implementar notificaciones
                
                # Dormir por un minuto
                time.sleep(60)
                
            except Exception as e:
                self.logger.error(f"❌ Error en scheduler: {e}", exc_info=True)
                time.sleep(60)  # Esperar un minuto antes de reintentar

    def run_manual_audit(self) -> Dict:
        """Ejecuta una auditoría completa bajo demanda"""
        self.logger.info("🔍 Iniciando auditoría manual")
        return self.audit_system.run_daily_audit()
    
    def improve_quality(self) -> Dict:
        """Ejecuta una mejora de calidad bajo demanda"""
        self.logger.info("🔄 Iniciando mejora de calidad manual")
        quality_result = self.audit_system.improve_data_quality()
        
        # Ejecutar auditoría completa para ver resultados
        if quality_result.get('status') == 'success':
            self.logger.info("📊 Ejecutando auditoría post-mejora")
            self.audit_system.run_daily_audit()
        
        return quality_result


# Instancia global
audit_scheduler = AuditScheduler()


# Si se ejecuta directamente, iniciar el scheduler
if __name__ == "__main__":
    print("🚀 Iniciando sistema de auditoría...")
    audit_scheduler.start()
    
    # Mantener el script corriendo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("👋 Deteniendo sistema de auditoría...")
        audit_scheduler.stop()
