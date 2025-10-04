# audit_system.py (nuevo archivo a crear)

class AuditSystem:
    """Sistema de auditoría automática de base de datos"""
    
    def __init__(self):
        self.audit_logs = []
        
    def run_daily_audit(self):
        """Ejecuta auditorías diarias"""
        results = {
            'duplicates': self.detect_duplicate_invoices(),
            'math_errors': self.verify_invoice_math(),
            'price_anomalies': self.detect_price_anomalies(),
            'product_issues': self.audit_product_catalog(),
            'timestamp': datetime.now()
        }
        return results
    
    def detect_duplicate_invoices(self):
        """Detecta facturas duplicadas (mismo usuario, fecha, total)"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                usuario_id, 
                establecimiento, 
                total_factura,
                fecha_factura,
                COUNT(*) as duplicados,
                STRING_AGG(CAST(id AS VARCHAR), ',') as ids
            FROM facturas
            WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY usuario_id, establecimiento, total_factura, fecha_factura
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cursor.fetchall()
        
        # Marcar duplicados
        for dup in duplicates:
            ids = dup[5].split(',')
            # Mantener el primero, marcar resto como duplicados
            for id_dup in ids[1:]:
                cursor.execute("""
                    UPDATE facturas 
                    SET estado_validacion = 'duplicado',
                        notas = CONCAT(notas, ' | Duplicado de factura #', %s)
                    WHERE id = %s
                """, (ids[0], id_dup))
        
        conn.commit()
        conn.close()
        return len(duplicates)
    
    def verify_invoice_math(self):
        """Verifica que la suma de productos = total factura"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            WITH factura_sumas AS (
                SELECT 
                    f.id,
                    f.total_factura,
                    f.establecimiento,
                    SUM(pp.precio) as suma_productos,
                    COUNT(pp.id) as num_productos
                FROM facturas f
                LEFT JOIN precios_productos pp ON f.id = pp.factura_id
                WHERE f.estado_validacion = 'procesado'
                  AND f.fecha_cargue >= CURRENT_DATE - INTERVAL '1 day'
                GROUP BY f.id, f.total_factura, f.establecimiento
            )
            SELECT 
                id, 
                total_factura, 
                suma_productos,
                ABS(total_factura - suma_productos) as diferencia,
                CASE 
                    WHEN suma_productos = 0 THEN 100
                    ELSE ABS(total_factura - suma_productos) * 100.0 / total_factura
                END as porcentaje_error
            FROM factura_sumas
            WHERE ABS(total_factura - suma_productos) > total_factura * 0.1
        """)
        
        math_errors = cursor.fetchall()
        
        for error in math_errors:
            factura_id, total, suma, diferencia, porcentaje = error
            
            if porcentaje > 20:  # Error mayor al 20%
                cursor.execute("""
                    UPDATE facturas 
                    SET estado_validacion = 'error_matematico',
                        notas = %s,
                        puntaje_calidad = puntaje_calidad - 20
                    WHERE id = %s
                """, (
                    f"Error matemático: Total ${total} vs Suma ${suma} (dif: {porcentaje:.1f}%)",
                    factura_id
                ))
        
        conn.commit()
        conn.close()
        return len(math_errors)
    
    def detect_price_anomalies(self):
        """Detecta precios anómalos (outliers)"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Detectar precios fuera de rango por producto
        cursor.execute("""
            WITH precio_stats AS (
                SELECT 
                    producto_id,
                    AVG(precio) as precio_promedio,
                    STDDEV(precio) as desviacion,
                    MIN(precio) as precio_min,
                    MAX(precio) as precio_max,
                    COUNT(*) as muestras
                FROM precios_productos
                GROUP BY producto_id
                HAVING COUNT(*) >= 5
            )
            SELECT 
                pp.id,
                pp.producto_id,
                pc.nombre_producto,
                pp.precio,
                ps.precio_promedio,
                pp.establecimiento,
                ABS(pp.precio - ps.precio_promedio) / ps.desviacion as z_score
            FROM precios_productos pp
            JOIN precio_stats ps ON pp.producto_id = ps.producto_id
            JOIN productos_catalogo pc ON pp.producto_id = pc.id
            WHERE ABS(pp.precio - ps.precio_promedio) > 3 * ps.desviacion
              AND pp.fecha_reporte >= CURRENT_DATE - INTERVAL '7 days'
        """)
        
        anomalies = cursor.fetchall()
        
        # Marcar outliers
        for anomaly in anomalies:
            precio_id = anomaly[0]
            cursor.execute("""
                INSERT INTO ocr_logs (factura_id, status, message, details)
                SELECT 
                    factura_id, 
                    'price_anomaly',
                    'Precio anómalo detectado',
                    %s
                FROM precios_productos 
                WHERE id = %s
            """, (f"Producto: {anomaly[2]}, Precio: ${anomaly[3]}, Promedio: ${anomaly[4]:.0f]}", precio_id))
        
        conn.commit()
        conn.close()
        return len(anomalies)
    
    def audit_product_catalog(self):
        """Audita el catálogo de productos"""
        issues = []
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Productos con múltiples nombres para mismo código
        cursor.execute("""
            SELECT 
                codigo_ean,
                COUNT(DISTINCT nombre_producto) as variaciones,
                STRING_AGG(DISTINCT nombre_producto, ' | ') as nombres
            FROM productos_catalogo
            WHERE codigo_ean != 'SIN_CODIGO'
            GROUP BY codigo_ean
            HAVING COUNT(DISTINCT nombre_producto) > 1
        """)
        
        name_conflicts = cursor.fetchall()
        
        for conflict in name_conflicts:
            # Consolidar al nombre más frecuente
            cursor.execute("""
                WITH nombre_frecuente AS (
                    SELECT 
                        nombre_producto,
                        COUNT(*) as freq
                    FROM precios_productos pp
                    JOIN productos_catalogo pc ON pp.producto_id = pc.id
                    WHERE pc.codigo_ean = %s
                    GROUP BY nombre_producto
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                )
                UPDATE productos_catalogo
                SET nombre_producto = (SELECT nombre_producto FROM nombre_frecuente)
                WHERE codigo_ean = %s
            """, (conflict[0], conflict[0]))
            
            issues.append(f"Consolidado nombres para código {conflict[0]}")
        
        # 2. Códigos EAN inválidos
        cursor.execute("""
            UPDATE productos_catalogo
            SET codigo_ean = CONCAT('INVALID_', id)
            WHERE LENGTH(codigo_ean) = 13 
              AND codigo_ean NOT REGEXP '^[0-9]{13}$'
        """)
        
        # 3. Productos sin reportes en 90 días (inactivos)
        cursor.execute("""
            UPDATE productos_catalogo
            SET es_producto_fresco = FALSE
            WHERE ultimo_reporte < CURRENT_DATE - INTERVAL '90 days'
              AND total_reportes < 3
        """)
        
        conn.commit()
        conn.close()
        return issues
    
    def clean_old_errors(self):
        """Limpia errores antiguos"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Archivar facturas con error de más de 30 días
        cursor.execute("""
            UPDATE facturas
            SET estado_validacion = 'archivado'
            WHERE estado_validacion IN ('error_ocr', 'error_sistema', 'error_matematico')
              AND fecha_cargue < CURRENT_DATE - INTERVAL '30 days'
        """)
        
        # Limpiar logs antiguos
        cursor.execute("""
            DELETE FROM ocr_logs
            WHERE created_at < CURRENT_DATE - INTERVAL '60 days'
        """)
        
        conn.commit()
        conn.close()
    
    def generate_audit_report(self):
        """Genera reporte de auditoría"""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                estado_validacion,
                COUNT(*) as cantidad,
                AVG(puntaje_calidad) as calidad_promedio
            FROM facturas
            WHERE fecha_cargue >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY estado_validacion
        """)
        
        stats = cursor.fetchall()
        
        report = {
            'week_summary': stats,
            'last_audit': datetime.now(),
            'recommendations': []
        }
        
        # Recomendaciones automáticas
        for stat in stats:
            if stat[0] == 'error_matematico' and stat[1] > 10:
                report['recommendations'].append(
                    "Alto número de errores matemáticos. Revisar algoritmo OCR."
                )
            elif stat[0] == 'duplicado' and stat[1] > 5:
                report['recommendations'].append(
                    "Múltiples duplicados detectados. Considerar validación en app."
                )
        
        conn.close()
        return report


# Programador de auditorías
class AuditScheduler:
    """Ejecuta auditorías periódicamente"""
    
    def __init__(self, audit_system):
        self.audit_system = audit_system
        self.is_running = False
        
    def start(self):
        """Inicia el programador"""
        self.is_running = True
        thread = threading.Thread(target=self._run_schedule, daemon=True)
        thread.start()
        
    def _run_schedule(self):
        """Ejecuta auditorías según calendario"""
        while self.is_running:
            now = datetime.now()
            
            # Auditoría cada hora
            if now.minute == 0:
                self.audit_system.verify_invoice_math()
                self.audit_system.detect_duplicate_invoices()
            
            # Auditoría completa a las 3 AM
            if now.hour == 3 and now.minute == 0:
                results = self.audit_system.run_daily_audit()
                print(f"Auditoría diaria completada: {results}")
            
            # Limpieza semanal (domingos a las 2 AM)
            if now.weekday() == 6 and now.hour == 2 and now.minute == 0:
                self.audit_system.clean_old_errors()
                
            time.sleep(60)  # Revisar cada minuto
