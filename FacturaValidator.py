"""
Sistema de validación de facturas para mejorar la calidad de datos
"""

from datetime import datetime, timedelta
import re
import hashlib
import json
from typing import List, Dict, Tuple, Optional, Union, Any

# Conexión a BD (importación segura)
try:
    from database import get_db_connection
except ImportError:
    # Función alternativa para entornos donde no está disponible
    def get_db_connection():
        raise ImportError("Módulo de base de datos no disponible")

class FacturaValidator:
    """Clase para validar la calidad de facturas"""
    
    # Constantes para la validación
    MIN_ESTABLECIMIENTO_LEN = 3
    MIN_TOTAL_FACTURA = 1000  # Ajustar según moneda local
    MIN_IMAGEN_RESOLUCION = 800
    PUNTOS_BASE = 100
    
    # Penalizaciones por tipo de error
    PENALTIES = {
        "establecimiento_invalido": 15,
        "total_invalido": 25,
        "total_sospechoso": 5,
        "sin_imagen": 30,
        "sin_productos": 30,
        "producto_sin_precio": 5,  # Por producto
        "producto_sin_nombre": 3,   # Por producto
        "producto_sospechoso": 10,  # Por producto
        "error_matematico_grave": 25,
        "error_matematico_medio": 15,
        "error_matematico_leve": 5,
        "duplicado_potencial": 10
    }
    
    @classmethod
    def validar_factura(cls, establecimiento: str, total: float, 
                      tiene_imagen: bool, productos: List[Dict], 
                      cadena: Optional[str] = None,
                      usuario_id: Optional[int] = None,
                      fecha: Optional[str] = None) -> Tuple[int, str, List[str]]:
        """
        Valida una factura y calcula su puntaje de calidad
        
        Args:
            establecimiento: Nombre del establecimiento
            total: Total de la factura
            tiene_imagen: Si la factura incluye imagen
            productos: Lista de productos en la factura
            cadena: Cadena del establecimiento (opcional)
            usuario_id: ID del usuario que sube la factura (opcional)
            fecha: Fecha de la factura en formato ISO (opcional)
            
        Returns:
            tuple: (puntaje, estado, alertas)
        """
        puntaje = cls.PUNTOS_BASE  # Comienza con puntaje perfecto
        alertas = []
        
        # 1. Validar establecimiento
        if not establecimiento or establecimiento == "Desconocido" or len(establecimiento) < cls.MIN_ESTABLECIMIENTO_LEN:
            puntaje -= cls.PENALTIES["establecimiento_invalido"]
            alertas.append("Establecimiento no identificado o inválido")
        
        # 2. Validar total
        if not total or total <= 0:
            puntaje -= cls.PENALTIES["total_invalido"]
            alertas.append("Total de factura inválido")
        elif total < cls.MIN_TOTAL_FACTURA:
            puntaje -= cls.PENALTIES["total_sospechoso"]
            alertas.append(f"Total de factura sospechosamente bajo ({total})")
        
        # 3. Validar imagen
        if not tiene_imagen:
            puntaje -= cls.PENALTIES["sin_imagen"]
            alertas.append("No incluye imagen de respaldo")
        
        # 4. Validar productos
        if not productos or len(productos) == 0:
            puntaje -= cls.PENALTIES["sin_productos"]
            alertas.append("No se detectaron productos")
        else:
            # Verificar calidad de los productos
            productos_sin_precio = 0
            productos_sin_nombre = 0
            productos_sospechosos = 0
            
            for producto in productos:
                # Obtener precio (manejar diferentes formatos)
                precio_valor = producto.get('precio', producto.get('valor', 0))
                try:
                    if isinstance(precio_valor, str):
                        precio_valor = precio_valor.replace(',', '.').strip()
                    precio = float(precio_valor)
                except (ValueError, TypeError):
                    precio = 0
                
                nombre = producto.get('nombre', '')
                codigo = producto.get('codigo', '')
                
                # Precios inválidos
                if precio <= 0:
                    productos_sin_precio += 1
                
                # Nombres inválidos
                if not nombre or len(nombre) < 3:
                    productos_sin_nombre += 1
                
                # Verificar productos sospechosos (precio desproporcionado)
                if precio > 0 and total > 0 and precio > total * 0.9:
                    productos_sospechosos += 1
            
            if productos_sin_precio > 0:
                penalty = min(20, cls.PENALTIES["producto_sin_precio"] * productos_sin_precio)
                puntaje -= penalty
                alertas.append(f"{productos_sin_precio} productos sin precio válido")
            
            if productos_sin_nombre > 0:
                penalty = min(15, cls.PENALTIES["producto_sin_nombre"] * productos_sin_nombre)
                puntaje -= penalty
                alertas.append(f"{productos_sin_nombre} productos sin nombre válido")
            
            if productos_sospechosos > 0:
                penalty = min(20, cls.PENALTIES["producto_sospechoso"] * productos_sospechosos)
                puntaje -= penalty
                alertas.append(f"{productos_sospechosos} productos con precios sospechosos")
        
        # 5. Verificación matemática
        if productos and len(productos) > 0 and total > 0:
            # Sumar todos los precios/valores de los productos
            suma_productos = 0
            for p in productos:
                precio_valor = p.get('precio', p.get('valor', 0))
                try:
                    if isinstance(precio_valor, str):
                        precio_valor = precio_valor.replace(',', '.').strip()
                    suma_productos += float(precio_valor)
                except (ValueError, TypeError):
                    pass
            
            # Calcular diferencia porcentual
            if suma_productos > 0:
                diferencia_porcentual = abs(suma_productos - total) / total * 100
                
                if diferencia_porcentual > 20:
                    puntaje -= cls.PENALTIES["error_matematico_grave"]
                    alertas.append(f"Error matemático: diferencia de {diferencia_porcentual:.1f}% entre total ({total}) y suma de productos ({suma_productos:.2f})")
                elif diferencia_porcentual > 10:
                    puntaje -= cls.PENALTIES["error_matematico_medio"]
                    alertas.append(f"Advertencia: diferencia de {diferencia_porcentual:.1f}% entre total y suma de productos")
                elif diferencia_porcentual > 5:
                    puntaje -= cls.PENALTIES["error_matematico_leve"]
                    alertas.append(f"Pequeña discrepancia de {diferencia_porcentual:.1f}% en el total")
        
        # 6. Verificar duplicados potenciales
        if establecimiento and total > 0 and usuario_id:
            try:
                duplicados = cls.detectar_duplicados_potenciales(
                    establecimiento=establecimiento, 
                    total=total,
                    usuario_id=usuario_id,
                    fecha=fecha
                )
                
                if duplicados and len(duplicados) > 0:
                    puntaje -= cls.PENALTIES["duplicado_potencial"]
                    alertas.append(f"Posible duplicado: {len(duplicados)} facturas similares encontradas")
            except Exception:
                # Error al detectar duplicados, no penalizar
                pass
        
        # Asegurar que el puntaje esté entre 0 y 100
        puntaje = max(0, min(100, int(puntaje)))
        
        # Determinar estado basado en puntaje
        estado = cls.calcular_estado(puntaje)
        
        return puntaje, estado, alertas
    
    @classmethod
    def calcular_estado(cls, puntaje: int) -> str:
        """Determina el estado de validación basado en el puntaje"""
        if puntaje >= 90:
            return 'validado'
        elif puntaje >= 70:
            return 'aceptable'
        elif puntaje >= 40:
            return 'revision'
        else:
            return 'error'
    
    @staticmethod
    def validar_imagen(ancho: int, alto: int, formato: str, 
                      tamano_bytes: Optional[int] = None) -> Tuple[int, List[str]]:
        """
        Valida la calidad de una imagen de factura
        
        Args:
            ancho: Ancho de la imagen en píxeles
            alto: Alto de la imagen en píxeles
            formato: Formato de la imagen (jpg, png, etc.)
            tamano_bytes: Tamaño del archivo en bytes
            
        Returns:
            tuple: (puntaje, alertas)
        """
        alertas = []
        puntaje = 100
        
        # Verificar dimensiones mínimas
        if ancho < 800 or alto < 800:
            puntaje -= 30
            alertas.append(f"Imagen de baja resolución ({ancho}x{alto})")
        
        # Verificar formato
        formatos_preferidos = ['jpg', 'jpeg', 'png']
        if formato.lower() not in formatos_preferidos:
            puntaje -= 10
            alertas.append(f"Formato de imagen no ideal ({formato})")
        
        # Verificar tamaño si está disponible
        if tamano_bytes:
            if tamano_bytes < 50 * 1024:  # Menos de 50KB es sospechosamente pequeño
                puntaje -= 20
                alertas.append(f"Imagen demasiado comprimida ({tamano_bytes/1024:.1f}KB)")
        
        # Verificar proporción
        if ancho > 0 and alto > 0:
            ratio = max(ancho, alto) / min(ancho, alto)
            if ratio > 4:  # Demasiado alargada
                puntaje -= 15
                alertas.append(f"Proporción de imagen inusual ({ratio:.1f}:1)")
        
        # Asegurar que el puntaje esté entre 0 y 100
        puntaje = max(0, min(100, puntaje))
        
        return puntaje, alertas
    
    @staticmethod
    def sugerir_correcciones(factura_datos: Dict[str, Any]) -> List[str]:
        """
        Sugiere correcciones para una factura
        
        Args:
            factura_datos: Diccionario con los datos de la factura
            
        Returns:
            list: Lista de sugerencias de corrección
        """
        sugerencias = []
        
        # Corregir establecimiento si es necesario
        establecimiento = factura_datos.get('establecimiento', '')
        if establecimiento and len(establecimiento) < 3:
            sugerencias.append("El nombre del establecimiento parece incompleto")
        
        # Validar fecha
        fecha = factura_datos.get('fecha')
        if fecha:
            try:
                fecha_dt = datetime.fromisoformat(fecha.replace('Z', '+00:00'))
                if fecha_dt > datetime.now() + timedelta(days=1):
                    sugerencias.append(f"La fecha ({fecha}) está en el futuro")
                elif fecha_dt < datetime.now() - timedelta(days=365):
                    sugerencias.append(f"La fecha ({fecha}) es de hace más de un año")
            except (ValueError, TypeError):
                sugerencias.append("El formato de fecha no es válido")
        
        # Verificar precios de productos
        productos = factura_datos.get('productos', [])
        total = float(factura_datos.get('total', 0))
        
        if not productos and total > 0:
            sugerencias.append("La factura tiene un total pero no se detectaron productos")
        
        for i, producto in enumerate(productos):
            try:
                precio_valor = producto.get('precio', producto.get('valor', 0))
                if isinstance(precio_valor, str):
                    precio_valor = precio_valor.replace(',', '.').strip()
                precio = float(precio_valor)
            except (ValueError, TypeError):
                precio = 0
                
            nombre = producto.get('nombre', '')
            
            if precio <= 0 and nombre:
                sugerencias.append(f"Producto '{nombre}' no tiene precio válido")
            
            if not nombre and precio > 0:
                sugerencias.append(f"Producto #{i+1} tiene precio ({precio}) pero no nombre")
            
            if precio > total * 0.9:
                sugerencias.append(f"El precio del producto '{nombre or i+1}' ({precio}) es casi igual al total ({total})")
        
        # Verificar suma total
        if productos and total > 0:
            suma = 0
            for p in productos:
                try:
                    precio_valor = p.get('precio', p.get('valor', 0))
                    if isinstance(precio_valor, str):
                        precio_valor = precio_valor.replace(',', '.').strip()
                    suma += float(precio_valor)
                except (ValueError, TypeError):
                    pass
            
            diferencia = abs(suma - total)
            if diferencia > total * 0.1:  # Más del 10% de diferencia
                sugerencias.append(f"La suma de productos ({suma:.2f}) difiere significativamente del total ({total})")
        
        return sugerencias
    
    @staticmethod
    def detectar_duplicados_potenciales(establecimiento: str, total: float, 
                                      usuario_id: Optional[int] = None,
                                      fecha: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Detecta facturas potencialmente duplicadas en la base de datos
        
        Args:
            establecimiento: Nombre del establecimiento
            total: Total de la factura
            usuario_id: ID del usuario (opcional)
            fecha: Fecha de la factura (opcional)
            
        Returns:
            list: Lista de facturas potencialmente duplicadas
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Calcular rango de búsqueda para el total (±2%)
            min_total = total * 0.98
            max_total = total * 1.02
            
            # Buscar facturas similares en los últimos 30 días
            query = """
                SELECT f.id, f.establecimiento, f.total_factura, f.fecha_cargue
                FROM facturas f
                WHERE f.establecimiento ILIKE %s
                  AND f.total_factura BETWEEN %s AND %s
                  AND f.fecha_cargue >= CURRENT_DATE - INTERVAL '30 days'
            """
            
            params = [f"%{establecimiento}%", min_total, max_total]
            
            # Filtrar por usuario si se proporciona
            if usuario_id:
                query += " AND f.usuario_id = %s"
                params.append(usuario_id)
            
            # Ejecutar consulta
            cursor.execute(query, params)
            resultados = cursor.fetchall()
            
            # Cerrar conexiones
            cursor.close()
            conn.close()
            
            # Convertir resultados a una lista de diccionarios
            duplicados = []
            for r in resultados:
                duplicados.append({
                    "id": r[0],
                    "establecimiento": r[1],
                    "total": float(r[2]) if r[2] else 0,
                    "fecha": r[3].isoformat() if r[3] else None
                })
            
            return duplicados
            
        except Exception as e:
            print(f"Error al detectar duplicados: {e}")
            # Devolver lista vacía en caso de error
            return []
    
    @staticmethod
    def generar_recomendaciones_sistema(estadisticas: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Genera recomendaciones para mejorar la calidad del sistema
        basado en estadísticas generales
        
        Args:
            estadisticas: Diccionario con estadísticas del sistema
            
        Returns:
            list: Lista de recomendaciones con prioridad e impacto
        """
        recomendaciones = []
        
        # Extraer estadísticas relevantes
        total_facturas = estadisticas.get('total_invoices', 0)
        facturas_con_imagen = estadisticas.get('with_images', 0)
        puntaje_promedio = estadisticas.get('avg_quality', 0)
        
        # Calcular porcentajes
        porcentaje_con_imagen = (facturas_con_imagen / total_facturas * 100) if total_facturas > 0 else 0
        
        # Recomendación 1: Mejorar capturas de imágenes
        if porcentaje_con_imagen < 70:
            recomendaciones.append({
                "titulo": "Aumentar el porcentaje de facturas con imágenes",
                "descripcion": f"Solo el {porcentaje_con_imagen:.1f}% de facturas tienen imágenes. Modifica la app para exigir imágenes de alta calidad.",
                "prioridad": "Alta" if porcentaje_con_imagen < 50 else "Media",
                "impacto": "Aumento de 30 puntos en el puntaje de calidad",
                "pasos": [
                    "Modificar CaptureInvoiceScreen.dart para validar calidad de imágenes",
                    "Rechazar facturas sin imágenes o con imágenes de baja calidad",
                    "Implementar revisión manual de facturas existentes sin imágenes"
                ]
            })
        
        # Recomendación 2: Validación más estricta
        if puntaje_promedio < 50:
            recomendaciones.append({
                "titulo": "Implementar validación más estricta de datos",
                "descripcion": f"El puntaje promedio de calidad ({puntaje_promedio:.1f}) indica problemas estructurales.",
                "prioridad": "Alta",
                "impacto": "Mejora de 20 puntos en el puntaje promedio",
                "pasos": [
                    "Aplicar FacturaValidator a todas las facturas nuevas",
                    "Crear interfaz para corregir facturas existentes",
                    "Implementar validación en tiempo real en el frontend"
                ]
            })
        
        # Recomendación 3: Revisar facturas existentes
        recomendaciones.append({
            "titulo": "Programar revisión de facturas de baja calidad",
            "descripcion": "Ejecutar un proceso automatizado de mejora de las facturas existentes.",
            "prioridad": "Media",
            "impacto": "Corrección inmediata de problemas en datos históricos",
            "pasos": [
                "Identificar facturas con puntaje < 30",
                "Aplicar correcciones automáticas donde sea posible",
                "Priorizar la adición de imágenes faltantes"
            ]
        })
        
        return recomendaciones
    
    @staticmethod
    def corregir_facturas_existentes() -> Dict[str, Any]:
        """
        Ejecuta un proceso para corregir facturas existentes
        con problemas de calidad
        
        Returns:
            dict: Resultados del proceso de corrección
        """
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Identificar facturas sin imagen pero con datos buenos
            cursor.execute("""
                UPDATE facturas
                SET puntaje_calidad = 50
                WHERE imagen_data IS NULL
                  AND estado_validacion IS NULL
                  AND total_factura > 0
                  AND establecimiento != 'Desconocido'
                RETURNING id
            """)
            
            sin_imagen_corregidas = cursor.rowcount
            
            # Corregir facturas con problemas matemáticos
            cursor.execute("""
                SELECT f.id, f.total_factura, SUM(pp.precio) as suma_productos
                FROM facturas f
                JOIN precios_productos pp ON f.id = pp.factura_id
                GROUP BY f.id, f.total_factura
                HAVING ABS(f.total_factura - SUM(pp.precio)) / f.total_factura > 0.2
                  AND f.total_factura > 0
                LIMIT 100
            """)
            
            problemas_matematicos = cursor.fetchall()
            corregidos_matematicos = 0
            
            for factura in problemas_matematicos:
                factura_id, total_registrado, suma_actual = factura
                
                # Si la diferencia es significativa, ajustar el total
                cursor.execute("""
                    UPDATE facturas
                    SET total_factura = %s,
                        notas = CONCAT(COALESCE(notas, ''), ' | Total ajustado por validación automática'),
                        puntaje_calidad = 70
                    WHERE id = %s
                """, (suma_actual, factura_id))
                
                corregidos_matematicos += 1
            
            conn.commit()
            cursor.close()
            conn.close()
            
            return {
                "sin_imagen_corregidas": sin_imagen_corregidas,
                "problemas_matematicos_corregidos": corregidos_matematicos,
                "total_corregidas": sin_imagen_corregidas + corregidos_matematicos
            }
            
        except Exception as e:
            print(f"Error en corrección automática: {e}")
            return {
                "error": str(e),
                "sin_imagen_corregidas": 0,
                "problemas_matematicos_corregidos": 0,
                "total_corregidas": 0
            }
