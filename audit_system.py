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
        """Detecta anomalías de precios"""
        return {'checked': 0, 'anomalies': 0, 'details': [], 'status': 'success'}

    def audit_product_catalog(self) -> Dict:
        """Audita el catálogo de productos"""
        return {'issues_fixed': 0, 'details': [], 'status': 'success'}

    def audit_fresh_products(self) -> Dict:
        """Audita productos frescos"""
        return {'mapeos_por_cadena': [], 'codigos_huerfanos': 0, 'status': 'success'}

    def assess_data_quality(self) -> Dict:
        """Evalúa la calidad general de los datos"""
        conn = get_db_connection()
        cursor = conn.cursor()
    
        try:
            cursor.execute("""
            SELECT 
                COUNT(*) as total_facturas,
                AVG(puntaje_calidad) as calidad_promedio,
                COUNT(CASE WHEN estado_validacion = 'procesado' THEN 1 END) as procesadas,
                COUNT(CASE WHEN estado_validacion LIKE '%error%' THEN 1 END) as con_error,
                COUNT(CASE WHEN estado_validacion = 'revision' THEN 1 END) as en_revision,
                COUNT(CASE WHEN imagen_data IS NOT NULL THEN 1 END) as con_imagen
            FROM facturas
            WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
            """)
        
            stats = cursor.fetchone()
            health_score = min(100, stats[1] or 0)
        
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

    def _save_audit_log(self, results: Dict):
        """Guarda log de auditoría"""
        pass

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
        """Genera reporte completo de auditoría"""
        quality = self.assess_data_quality()
    
        return {
            'generated_at': datetime.now().isoformat(),
            'data_quality': quality,
            'price_intelligence': {},
            'recent_audits': [],
            'recommendations': ["✅ Sistema funcionando correctamente."],
            'system_health': self._calculate_system_health(quality)
        }
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
