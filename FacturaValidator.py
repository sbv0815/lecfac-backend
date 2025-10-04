"""
Sistema de validación de facturas para mejorar la calidad de datos
"""

from datetime import datetime
import re

class FacturaValidator:
    """Clase para validar la calidad de facturas"""
    
    @staticmethod
    def validar_factura(establecimiento, total, tiene_imagen, productos, cadena=None):
        """
        Valida una factura y calcula su puntaje de calidad
        
        Args:
            establecimiento (str): Nombre del establecimiento
            total (float): Total de la factura
            tiene_imagen (bool): Si la factura incluye imagen
            productos (list): Lista de productos en la factura
            cadena (str, optional): Cadena del establecimiento
            
        Returns:
            tuple: (puntaje, estado, alertas)
        """
        puntaje = 100  # Comienza con puntaje perfecto
        alertas = []
        
        # 1. Validar establecimiento
        if not establecimiento or establecimiento == "Desconocido" or len(establecimiento) < 3:
            puntaje -= 15
            alertas.append("Establecimiento no identificado o inválido")
        
        # 2. Validar total
        if total <= 0:
            puntaje -= 25
            alertas.append("Total de factura inválido")
        elif total < 1000:  # Para prevenir totales sospechosamente bajos (ajustar según país/moneda)
            puntaje -= 5
            alertas.append("Total de factura sospechosamente bajo")
        
        # 3. Validar imagen
        if not tiene_imagen:
            puntaje -= 30
            alertas.append("No incluye imagen de respaldo")
        
        # 4. Validar productos
        if not productos or len(productos) == 0:
            puntaje -= 30
            alertas.append("No se detectaron productos")
        else:
            # Verificar calidad de los productos
            productos_sin_precio = 0
            productos_sin_nombre = 0
            productos_sospechosos = 0
            
            for producto in productos:
                precio = float(producto.get('precio', 0))
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
                puntaje -= min(20, 5 * productos_sin_precio)
                alertas.append(f"{productos_sin_precio} productos sin precio válido")
            
            if productos_sin_nombre > 0:
                puntaje -= min(15, 3 * productos_sin_nombre)
                alertas.append(f"{productos_sin_nombre} productos sin nombre válido")
            
            if productos_sospechosos > 0:
                puntaje -= min(20, 10 * productos_sospechosos)
                alertas.append(f"{productos_sospechosos} productos con precios sospechosos")
        
        # 5. Verificación matemática
        if productos and len(productos) > 0 and total > 0:
            suma_productos = sum(float(p.get('precio', 0)) for p in productos)
            diferencia_porcentual = abs(suma_productos - total) / total * 100
            
            if diferencia_porcentual > 20:
                puntaje -= 25
                alertas.append(f"Error matemático: diferencia de {diferencia_porcentual:.1f}% entre total y suma de productos")
            elif diferencia_porcentual > 10:
                puntaje -= 15
                alertas.append(f"Advertencia: diferencia de {diferencia_porcentual:.1f}% entre total y suma de productos")
            elif diferencia_porcentual > 5:
                puntaje -= 5
                alertas.append(f"Pequeña discrepancia de {diferencia_porcentual:.1f}% en el total")
        
        # Asegurar que el puntaje esté entre 0 y 100
        puntaje = max(0, min(100, puntaje))
        
        # Determinar estado basado en puntaje
        if puntaje >= 90:
            estado = 'validado'
        elif puntaje >= 70:
            estado = 'aceptable'
        elif puntaje >= 40:
            estado = 'revision'
        else:
            estado = 'error'
        
        return puntaje, estado, alertas
    
    @staticmethod
    def validar_imagen(ancho, alto, formato):
        """Valida la calidad de una imagen de factura"""
        alertas = []
        puntaje = 100
        
        # Verificar dimensiones mínimas
        if ancho < 800 or alto < 800:
            puntaje -= 30
            alertas.append("Imagen de baja resolución")
        
        # Verificar formato
        formatos_preferidos = ['jpg', 'jpeg', 'png']
        if formato.lower() not in formatos_preferidos:
            puntaje -= 10
            alertas.append("Formato de imagen no ideal")
        
        # Asegurar que el puntaje esté entre 0 y 100
        puntaje = max(0, min(100, puntaje))
        
        return puntaje, alertas
    
    @staticmethod
    def sugerir_correcciones(factura_datos):
        """Sugiere correcciones para una factura"""
        sugerencias = []
        
        # Corregir establecimiento si es necesario
        establecimiento = factura_datos.get('establecimiento', '')
        if establecimiento and len(establecimiento) < 3:
            sugerencias.append("El nombre del establecimiento parece incompleto")
        
        # Verificar precios de productos
        productos = factura_datos.get('productos', [])
        for i, producto in enumerate(productos):
            precio = float(producto.get('precio', 0))
            nombre = producto.get('nombre', '')
            
            if precio <= 0 and nombre:
                sugerencias.append(f"Producto '{nombre}' no tiene precio válido")
            
            if not nombre and precio > 0:
                sugerencias.append(f"Producto #{i+1} tiene precio ({precio}) pero no nombre")
        
        return sugerencias
    
    @staticmethod
    def detectar_duplicados_potenciales(establecimiento, total, fecha=None, productos=None):
        """
        Genera una huella digital para detectar duplicados potenciales
        Debe implementarse luego con acceso a la base de datos
        """
        # Implementar lógica de detección de duplicados
        # Esto requiere acceso a la base de datos
        pass
