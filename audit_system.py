"""
Sistema Completo de Auditoría para Base de Datos de Facturas
Incluye auditorías básicas y análisis inteligente de precios
"""

import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
from database import get_db_connection

class AuditSystem:
    """Sistema completo de auditoría automática"""
    
    def __init__(self):
        self.audit_logs = []
        self.price_tolerance = {
            'mismo_establecimiento': 0.15,
            'misma_cadena': 0.25,
            'diferente_cadena': 0.50,
            'productos_frescos': 0.40
        }
    
    def run_daily_audit(self) -> Dict:
        """Ejecuta todas las auditorías diarias"""
        print("🔍 Iniciando auditoría completa...")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'duplicates': self.detect_duplicate_invoices(),
            'math_errors': self.verify_invoice_math(),
            'price_anomalies': self.detect_price_anomalies(),
            'product_issues': self.audit_product_catalog(),
            'fresh_products': self.audit_fresh_products(),
            'data_quality': self.assess_data_quality()
        }
        
        self._save_audit_log(results)
        
        print(f"✅ Auditoría completada: {results}")
        return results
    
    def detect_duplicate_invoices(self) -> Dict:
        """Detecta facturas duplicadas"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Buscar duplicados potenciales
            cursor.execute("""
                SELECT 
                    usuario_id, 
                    establecimiento, 
                    total_factura,
                    DATE(fecha_cargue) as fecha,
                    COUNT(*) as duplicados,
                    STRING_AGG(CAST(id AS TEXT), ',') as ids
                FROM facturas
                WHERE fecha_cargue >= (CURRENT_DATE - INTERVAL '7 days')
                  AND estado_validacion != 'duplicado'
                GROUP BY usuario_id, establecimiento, total_factura, DATE(fecha_cargue)
                HAVING COUNT(*) > 1
            """)
            
            duplicates = cursor.fetchall()
            processed = 0
            
            for dup in duplicates:
                usuario_id, establecimiento, total, fecha, count, ids_str = dup
                ids = ids_str.split(',')
                
                for dup_id in ids[1:]:
                    cursor.execute("""
                        UPDATE facturas 
                        SET estado_validacion = 'duplicado',
                            notas = CONCAT(COALESCE(notas, ''), ' | Duplicado de factura #', %s),
                            puntaje_calidad = 0
                        WHERE id = %s
                    """, (ids[0], dup_id))
                    processed += 1
            
            conn.commit()
            return {
                'found': len(duplicates),
                'processed': processed,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error detectando duplicados: {e}")
            return {'error': str(e), 'status': 'failed'}
        finally:
            conn.close()

def verify_invoice_math(self) -> Dict:
    """Verifica matemáticas de las facturas"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Verificar sumas
        cursor.execute("""
            WITH factura_math AS (
                SELECT 
                    f.id,
                    f.establecimiento,
                    f.total_factura,
                    COALESCE(SUM(pp.precio), 0) as suma_productos,
                    COUNT(pp.id) as num_productos
                FROM facturas f
                LEFT JOIN precios_productos pp ON f.id = pp.factura_id
                WHERE f.estado_validacion IN ('procesado', 'revision')
                  AND f.fecha_cargue >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY f.id, f.establecimiento, f.total_factura
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
                END as error_porcentaje
            FROM factura_math
            WHERE total_factura > 0
        """)
        
        all_invoices = cursor.fetchall()
        errors_found = 0
        warnings_found = 0
        
        for invoice in all_invoices:
            factura_id, estab, total, suma, diff, error_pct = invoice
            
            if error_pct > 20:
                cursor.execute("""
                    UPDATE facturas 
                    SET estado_validacion = 'error_matematico',
                        notas = %s,
                        puntaje_calidad = GREATEST(0, puntaje_calidad - 30)
                    WHERE id = %s
                """, (
                    f"Error matemático: Total ${total} vs Suma ${suma} ({error_pct:.1f}% diferencia)",
                    factura_id
                ))
                errors_found += 1
                
            elif error_pct > 10:
                cursor.execute("""
                    UPDATE facturas 
                    SET puntaje_calidad = GREATEST(0, puntaje_calidad - 10),
                        notas = CONCAT(COALESCE(notas, ''), ' | Advertencia: diferencia ', %s, '%')
                    WHERE id = %s
                """, (f"{error_pct:.1f}", factura_id))
                warnings_found += 1
        
        conn.commit()
        return {
            'total_checked': len(all_invoices),
            'errors': errors_found,
            'warnings': warnings_found,
            'status': 'success'
        }
        
    except Exception as e:
        print(f"❌ Error verificando matemáticas: {e}")
        return {'error': str(e), 'status': 'failed'}
    finally:
        conn.close()
    
    def detect_price_anomalies(self) -> Dict:
        """Detecta anomalías de precios usando análisis contextual"""
        conn = get_db_connection()
        cursor = conn.cursor()
        anomalies_found = []
        
        try:
            # Obtener precios recientes para análisis
            cursor.execute("""
                SELECT DISTINCT
                    pp.id,
                    pp.producto_id,
                    pp.precio,
                    pp.establecimiento,
                    pp.cadena,
                    pp.factura_id,
                    pc.nombre_producto,
                    pc.es_producto_fresco
                FROM precios_productos pp
                JOIN productos_catalogo pc ON pp.producto_id = pc.id
                WHERE pp.fecha_reporte >= CURRENT_DATE - INTERVAL '1 day'
            """)
            
            recent_prices = cursor.fetchall()
            
            for price_record in recent_prices:
                price_id, prod_id, precio, estab, cadena, factura_id, nombre, es_fresco = price_record
                
                # Validar precio en contexto
                validation = self._validate_price_context(
                    cursor, prod_id, precio, estab, cadena, es_fresco
                )
                
                if not validation['valido']:
                    anomalies_found.append({
                        'producto': nombre,
                        'precio': precio,
                        'establecimiento': estab,
                        'razon': validation['razon']
                    })
                    
                    # Marcar para revisión
                    cursor.execute("""
                        UPDATE facturas
                        SET estado_validacion = 'revision',
                            notas = CONCAT(COALESCE(notas, ''), ' | Precio anómalo: ', %s)
                        WHERE id = %s
                    """, (nombre, factura_id))
                    
                    # Log
                    cursor.execute("""
                        INSERT INTO ocr_logs (factura_id, status, message, details, created_at)
                        VALUES (%s, 'price_anomaly', %s, %s, %s)
                    """, (
                        factura_id,
                        f"Precio anómalo detectado",
                        f"{nombre}: ${precio} - {validation['razon']}",
                        datetime.now()
                    ))
            
            conn.commit()
            return {
                'checked': len(recent_prices),
                'anomalies': len(anomalies_found),
                'details': anomalies_found[:10],  # Primeras 10 para el reporte
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error detectando anomalías: {e}")
            return {'error': str(e), 'status': 'failed'}
        finally:
            conn.close()
    
    def _validate_price_context(self, cursor, producto_id: int, precio: float, 
                               establecimiento: str, cadena: str, es_fresco: bool) -> Dict:
        """Valida precio en su contexto específico"""
        
        # Obtener histórico del mismo establecimiento
        cursor.execute("""
            SELECT precio
            FROM precios_productos
            WHERE producto_id = %s 
              AND establecimiento = %s
              AND fecha_reporte >= CURRENT_DATE - INTERVAL '30 days'
              AND fecha_reporte < CURRENT_DATE
            ORDER BY fecha_reporte DESC
            LIMIT 10
        """, (producto_id, establecimiento))
        
        mismo_lugar = [row[0] for row in cursor.fetchall()]
        
        if mismo_lugar:
            promedio = sum(mismo_lugar) / len(mismo_lugar)
            tolerancia = self.price_tolerance['productos_frescos' if es_fresco else 'mismo_establecimiento']
            
            if abs(precio - promedio) / promedio <= tolerancia:
                return {'valido': True, 'razon': 'Precio normal para este establecimiento'}
            
            # Verificar tendencia
            if len(mismo_lugar) >= 3:
                if self._check_price_trend(mismo_lugar, precio):
                    return {'valido': True, 'razon': 'Sigue tendencia de precios'}
        
        # Comparar con la cadena
        cursor.execute("""
            SELECT AVG(precio), MIN(precio), MAX(precio), COUNT(*)
            FROM precios_productos
            WHERE producto_id = %s 
              AND cadena = %s
              AND fecha_reporte >= CURRENT_DATE - INTERVAL '30 days'
        """, (producto_id, cadena))
        
        cadena_stats = cursor.fetchone()
        if cadena_stats and cadena_stats[3] >= 5:  # Al menos 5 muestras
            avg_cadena, min_cadena, max_cadena, _ = cadena_stats
            
            if es_fresco:
                # Productos frescos: más tolerancia
                if min_cadena * 0.6 <= precio <= max_cadena * 1.4:
                    return {'valido': True, 'razon': 'Precio de producto fresco dentro de rango'}
            else:
                # Productos normales
                if min_cadena * 0.8 <= precio <= max_cadena * 1.2:
                    return {'valido': True, 'razon': 'Precio dentro del rango de la cadena'}
        
        # Si no hay suficientes datos, aceptar con advertencia
        if not mismo_lugar and cadena_stats[3] < 3:
            return {'valido': True, 'razon': 'Datos insuficientes para validar'}
        
        return {'valido': False, 'razon': 'Precio fuera de rangos esperados'}
    
    def _check_price_trend(self, historico: List[float], nuevo_precio: float) -> bool:
        """Verifica si el precio sigue una tendencia"""
        if len(historico) < 3:
            return False
        
        # Calcular cambios porcentuales
        cambios = []
        for i in range(1, min(5, len(historico))):
            cambio = (historico[i-1] - historico[i]) / historico[i]
            cambios.append(cambio)
        
        if not cambios:
            return False
        
        promedio_cambio = sum(cambios) / len(cambios)
        
        # Si hay tendencia clara (más de 3% cambio consistente)
        if abs(promedio_cambio) > 0.03:
            precio_esperado = historico[0] * (1 + promedio_cambio)
            # Aceptar si está cerca del precio esperado según tendencia
            if abs(nuevo_precio - precio_esperado) / precio_esperado < 0.25:
                return True
        
        return False
    
    def audit_product_catalog(self) -> Dict:
        """Audita y limpia el catálogo de productos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        issues_fixed = []
        
        try:
            # 1. Consolidar productos con múltiples nombres
            cursor.execute("""
                WITH duplicados AS (
                    SELECT 
                        codigo_ean,
                        COUNT(DISTINCT id) as num_registros,
                        MIN(id) as id_principal,
                        STRING_AGG(CAST(id AS VARCHAR), ',') as todos_ids,
                        STRING_AGG(nombre_producto, ' | ') as nombres
                    FROM productos_catalogo
                    WHERE codigo_ean IS NOT NULL 
                      AND codigo_ean != 'SIN_CODIGO'
                      AND LENGTH(codigo_ean) >= 6
                    GROUP BY codigo_ean
                    HAVING COUNT(DISTINCT id) > 1
                )
                SELECT * FROM duplicados
            """)
            
            duplicados = cursor.fetchall()
            
            for dup in duplicados:
                codigo, num, id_principal, ids_str, nombres = dup
                ids = ids_str.split(',')
                
                # Migrar todos los precios al producto principal
                for id_dup in ids[1:]:
                    cursor.execute("""
                        UPDATE precios_productos
                        SET producto_id = %s
                        WHERE producto_id = %s
                    """, (id_principal, int(id_dup)))
                    
                    # Eliminar duplicado
                    cursor.execute("""
                        DELETE FROM productos_catalogo WHERE id = %s
                    """, (int(id_dup),))
                
                issues_fixed.append(f"Consolidado {num} registros para código {codigo}")
            
            # 2. Marcar productos inactivos
            cursor.execute("""
                UPDATE productos_catalogo
                SET es_producto_fresco = FALSE
                WHERE ultimo_reporte < CURRENT_DATE - INTERVAL '180 days'
                  AND total_reportes < 5
            """)
            
            inactivos = cursor.rowcount
            if inactivos > 0:
                issues_fixed.append(f"Marcados {inactivos} productos como inactivos")
            
            # 3. Limpiar códigos inválidos
            cursor.execute("""
                UPDATE productos_catalogo
                SET codigo_ean = CONCAT('FIX_', id)
                WHERE codigo_ean IS NULL OR codigo_ean = ''
            """)
            
            conn.commit()
            return {
                'issues_fixed': len(issues_fixed),
                'details': issues_fixed,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error auditando catálogo: {e}")
            conn.rollback()
            return {'error': str(e), 'status': 'failed'}
        finally:
            conn.close()
    
    def audit_fresh_products(self) -> Dict:
        """Audita productos frescos y sus códigos locales"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Verificar mapeos de códigos locales
            cursor.execute("""
            SELECT 
            cl.cadena,
            COUNT(DISTINCT cl.codigo_local) as codigos_unicos,
            COUNT(DISTINCT cl.producto_id) as productos_mapeados
            FROM codigos_locales cl
            GROUP BY cl.cadena
            """)
            
            mapeos = cursor.fetchall()
            
            # Detectar códigos huérfanos
            cursor.execute("""
                SELECT COUNT(*)
                FROM productos p
                JOIN facturas f ON p.factura_id = f.id
                WHERE LENGTH(p.codigo) < 7
                  AND p.codigo NOT IN (
                    SELECT codigo_local 
                    FROM codigos_locales 
                    WHERE cadena = f.cadena
                  )
                  AND f.fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            huerfanos = cursor.fetchone()[0]
            
            conn.close()
            return {
                'mapeos_por_cadena': mapeos,
                'codigos_huerfanos': huerfanos,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error auditando productos frescos: {e}")
            return {'error': str(e), 'status': 'failed'}
        finally:
            if conn:
                conn.close()
    
    def assess_data_quality(self) -> Dict:
        """Evalúa la calidad general de los datos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Métricas de calidad
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_facturas,
                    AVG(puntaje_calidad) as calidad_promedio,
                    COUNT(CASE WHEN estado_validacion = 'procesado' THEN 1 END) as procesadas,
                    COUNT(CASE WHEN estado_validacion = 'error_ocr' THEN 1 END) as con_error,
                    COUNT(CASE WHEN estado_validacion = 'revision' THEN 1 END) as en_revision,
                    COUNT(CASE WHEN imagen_data IS NOT NULL THEN 1 END) as con_imagen
                FROM facturas
                WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            stats = cursor.fetchone()
            
            # Calcular score de salud
            health_score = 100
            if stats[1]:  # calidad_promedio
                health_score = min(100, stats[1])
            
            # Penalizar por errores
            if stats[0] > 0:
                error_rate = (stats[3] / stats[0]) * 100
                if error_rate > 10:
                    health_score -= 20
                elif error_rate > 5:
                    health_score -= 10
            
            conn.close()
            return {
                'health_score': health_score,
                'total_invoices': stats[0],
                'avg_quality': float(stats[1]) if stats[1] else 0,
                'processed': stats[2],
                'errors': stats[3],
                'pending_review': stats[4],
                'with_images': stats[5],
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error evaluando calidad: {e}")
            return {'error': str(e), 'status': 'failed'}
        finally:
            if conn:
                conn.close()
    
    def clean_old_data(self) -> Dict:
        """Limpia datos antiguos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Archivar facturas antiguas con error
            cursor.execute("""
                UPDATE facturas
                SET estado_validacion = 'archivado'
                WHERE estado_validacion IN ('error_ocr', 'error_sistema', 'duplicado')
                  AND fecha_cargue < CURRENT_DATE - INTERVAL '30 days'
            """)
            archivadas = cursor.rowcount
            
            # Limpiar logs antiguos
            cursor.execute("""
                DELETE FROM ocr_logs
                WHERE created_at < CURRENT_DATE - INTERVAL '90 days'
            """)
            logs_deleted = cursor.rowcount
            
            conn.commit()
            return {
                'archived_invoices': archivadas,
                'logs_deleted': logs_deleted,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"❌ Error limpiando datos: {e}")
            conn.rollback()
            return {'error': str(e), 'status': 'failed'}
        finally:
            conn.close()
    
    def generate_audit_report(self) -> Dict:
        """Genera reporte completo de auditoría"""
        
        # Ejecutar evaluación de calidad
        quality = self.assess_data_quality()
        
        # Generar reporte de inteligencia de precios
        price_intel = self._generate_price_intelligence()
        
        # Obtener últimos logs de auditoría
        recent_audits = self._get_recent_audit_logs()
        
        # Generar recomendaciones
        recommendations = self._generate_recommendations(quality, price_intel)
        
        return {
            'generated_at': datetime.now().isoformat(),
            'data_quality': quality,
            'price_intelligence': price_intel,
            'recent_audits': recent_audits,
            'recommendations': recommendations,
            'system_health': self._calculate_system_health(quality)
        }
    
    def _generate_price_intelligence(self) -> Dict:
        """Genera inteligencia de precios"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Productos con mayor variación
            cursor.execute("""
                WITH price_stats AS (
                    SELECT 
                        pc.nombre_producto,
                        pp.cadena,
                        MIN(pp.precio) as min_precio,
                        MAX(pp.precio) as max_precio,
                        AVG(pp.precio) as avg_precio,
                        COUNT(DISTINCT pp.establecimiento) as num_tiendas
                    FROM precios_productos pp
                    JOIN productos_catalogo pc ON pp.producto_id = pc.id
                    WHERE pp.fecha_reporte >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY pc.nombre_producto, pp.cadena
                    HAVING COUNT(*) >= 5
                )
                SELECT 
                    nombre_producto,
                    cadena,
                    min_precio,
                    max_precio,
                    avg_precio,
                    (max_precio - min_precio) * 100.0 / avg_precio as variacion_pct
                FROM price_stats
                ORDER BY variacion_pct DESC
                LIMIT 10
            """)
            
            high_variance = cursor.fetchall()
            
            # Cadenas más económicas
            cursor.execute("""
                SELECT 
                    cadena,
                    AVG(precio) as precio_promedio,
                    COUNT(DISTINCT producto_id) as productos
                FROM precios_productos
                WHERE fecha_reporte >= CURRENT_DATE - INTERVAL '7 days'
                  AND cadena IS NOT NULL
                GROUP BY cadena
                HAVING COUNT(DISTINCT producto_id) >= 10
                ORDER BY precio_promedio
            """)
            
            chain_prices = cursor.fetchall()
            
            conn.close()
            return {
                'high_variance_products': high_variance,
                'chain_price_ranking': chain_prices
            }
            
        except Exception as e:
            print(f"Error generando inteligencia de precios: {e}")
            return {}
        finally:
            if conn:
                conn.close()
    
    def _get_recent_audit_logs(self) -> List:
        """Obtiene logs recientes de auditoría"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT 
                    DATE(created_at) as fecha,
                    status,
                    COUNT(*) as cantidad
                FROM ocr_logs
                WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY DATE(created_at), status
                ORDER BY fecha DESC, cantidad DESC
                LIMIT 20
            """)
            
            logs = cursor.fetchall()
            conn.close()
            
            return [{'fecha': str(log[0]), 'tipo': log[1], 'cantidad': log[2]} for log in logs]
            
        except:
            return []
        finally:
            if conn:
                conn.close()
    
    def _generate_recommendations(self, quality: Dict, price_intel: Dict) -> List[str]:
        """Genera recomendaciones basadas en los datos"""
        recommendations = []
        
        # Basadas en calidad
        if quality.get('health_score', 100) < 70:
            recommendations.append("⚠️ Calidad de datos baja. Revisar proceso de OCR.")
        
        if quality.get('errors', 0) > quality.get('total_invoices', 1) * 0.1:
            recommendations.append("❌ Alto porcentaje de errores. Verificar calidad de imágenes.")
        
        if quality.get('pending_review', 0) > 20:
            recommendations.append("📋 Muchas facturas pendientes de revisión. Considerar revisión manual.")
        
        # Basadas en precios
        if price_intel.get('high_variance_products'):
            top_variance = price_intel['high_variance_products'][0] if price_intel['high_variance_products'] else None
            if top_variance and top_variance[5] > 50:
                recommendations.append(f"💰 Alta variación en {top_variance[0]} ({top_variance[5]:.0f}%). Verificar precios.")
        
        if not recommendations:
            recommendations.append("✅ Sistema funcionando correctamente.")
        
        return recommendations
    
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
    
    def _save_audit_log(self, results: Dict):
        """Guarda log de auditoría"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO ocr_logs (factura_id, status, message, details, created_at)
                VALUES (NULL, 'audit_complete', 'Auditoría diaria completada', %s, %s)
            """, (str(results), datetime.now()))
            
            conn.commit()
        except Exception as e:
            print(f"Error guardando log de auditoría: {e}")
        finally:
            conn.close()


class AuditScheduler:
    """Programador de auditorías automáticas"""
    
    def __init__(self):
        self.audit_system = AuditSystem()
        self.is_running = False
        self.thread = None
    
    def start(self):
        """Inicia el programador de auditorías"""
        if self.is_running:
            print("⚠️ Scheduler ya está en ejecución")
            return
        
        self.is_running = True
        self.thread = threading.Thread(target=self._run_schedule, daemon=True)
        self.thread.start()
        print("🔄 Programador de auditorías iniciado")
    
    def stop(self):
        """Detiene el programador"""
        self.is_running = False
        print("🛑 Deteniendo programador de auditorías...")
    
    def _run_schedule(self):
        """Ejecuta auditorías según calendario"""
        while self.is_running:
            try:
                now = datetime.now()
                
                # Cada hora: verificación rápida
                if now.minute == 0:
                    print(f"⏰ Auditoría horaria - {now.strftime('%H:%M')}")
                    self.audit_system.detect_duplicate_invoices()
                    self.audit_system.verify_invoice_math()
                
                # Cada 6 horas: auditoría de precios
                if now.hour % 6 == 0 and now.minute == 0:
                    print("💰 Auditoría de precios")
                    self.audit_system.detect_price_anomalies()
                
                # Diaria a las 3 AM: auditoría completa
                if now.hour == 3 and now.minute == 0:
                    print("🌙 Auditoría completa nocturna")
                    results = self.audit_system.run_daily_audit()
                    print(f"Resultados: {results}")
                
                # Semanal domingos 2 AM: limpieza
                if now.weekday() == 6 and now.hour == 2 and now.minute == 0:
                    print("🧹 Limpieza semanal")
                    self.audit_system.clean_old_data()
                
                # Dormir hasta el próximo minuto
                time.sleep(60)
                
            except Exception as e:
                print(f"❌ Error en scheduler: {e}")
                time.sleep(60)


# Instancia global
audit_scheduler = AuditScheduler()
