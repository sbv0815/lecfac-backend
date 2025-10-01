from datetime import datetime
from typing import Dict, List, Tuple

class FacturaValidator:
    """Sistema de validación de calidad de facturas"""
    
    PUNTAJE_MAXIMO = 100
    
    # Pesos para el score de calidad
    PESOS = {
        'establecimiento': 20,
        'total': 25,
        'imagen': 20,
        'productos_calidad': 20,
        'productos_cantidad': 15
    }
    
    @staticmethod
    def validar_factura(
        establecimiento: str,
        total: float,
        tiene_imagen: bool,
        productos: List[Dict],
        cadena: str
    ) -> Tuple[int, str, List[str]]:
        """
        Valida una factura y retorna (puntaje, estado, alertas)
        
        Returns:
            puntaje: 0-100
            estado: 'pendiente' | 'revision_requerida' | 'validada'
            alertas: Lista de problemas detectados
        """
        puntaje = 0
        alertas = []
        
        # 1. Validar establecimiento (20 puntos)
        if establecimiento and establecimiento != "Desconocido":
            puntaje += FacturaValidator.PESOS['establecimiento']
        else:
            alertas.append("⚠️ Establecimiento no identificado")
        
        # 2. Validar total (25 puntos)
        if total and total > 0:
            puntaje += FacturaValidator.PESOS['total']
        else:
            alertas.append("❌ Total de factura faltante o inválido")
        
        # 3. Validar imagen (20 puntos)
        if tiene_imagen:
            puntaje += FacturaValidator.PESOS['imagen']
        else:
            alertas.append("❌ Imagen no guardada")
        
        # 4. Validar calidad de productos (20 puntos)
        productos_validos = 0
        productos_sin_codigo = 0
        productos_sin_nombre = 0
        productos_sin_precio = 0
        
        for prod in productos:
            es_valido = True
            
            if not prod.get('codigo') or len(prod.get('codigo', '')) < 8:
                productos_sin_codigo += 1
                es_valido = False
            
            if not prod.get('nombre') or len(prod.get('nombre', '')) < 3:
                productos_sin_nombre += 1
                es_valido = False
            
            if not prod.get('precio') or prod.get('precio', 0) <= 0:
                productos_sin_precio += 1
                es_valido = False
            
            if es_valido:
                productos_validos += 1
        
        if len(productos) > 0:
            calidad_productos = (productos_validos / len(productos)) * 100
            puntaje += int((calidad_productos / 100) * FacturaValidator.PESOS['productos_calidad'])
            
            if productos_sin_codigo > 0:
                alertas.append(f"⚠️ {productos_sin_codigo} productos sin código válido")
            if productos_sin_nombre > 0:
                alertas.append(f"⚠️ {productos_sin_nombre} productos sin nombre")
            if productos_sin_precio > 0:
                alertas.append(f"⚠️ {productos_sin_precio} productos sin precio")
        else:
            alertas.append("❌ No se detectaron productos")
        
        # 5. Validar cantidad de productos (15 puntos)
        if len(productos) >= 5:
            puntaje += FacturaValidator.PESOS['productos_cantidad']
        elif len(productos) >= 3:
            puntaje += int(FacturaValidator.PESOS['productos_cantidad'] * 0.7)
            alertas.append("⚠️ Pocos productos detectados")
        else:
            alertas.append("❌ Muy pocos productos detectados")
        
        # 6. Validar coherencia de total vs suma de productos
        if total and total > 0 and len(productos) > 0:
            suma_productos = sum(p.get('precio', 0) for p in productos)
            diferencia_porcentual = abs(total - suma_productos) / total * 100
            
            if diferencia_porcentual > 20:
                alertas.append(f"⚠️ Diferencia significativa: Total=${total:.2f} vs Suma=${suma_productos:.2f}")
        
        # Determinar estado
        if puntaje >= 80:
            estado = 'validada'
        elif puntaje >= 50:
            estado = 'revision_requerida'
        else:
            estado = 'pendiente'
        
        return puntaje, estado, alertas
    
    @staticmethod
    def detectar_productos_duplicados(productos: List[Dict]) -> List[Tuple[int, int]]:
        """Detecta productos que parecen duplicados"""
        duplicados = []
        
        for i, p1 in enumerate(productos):
            for j, p2 in enumerate(productos[i+1:], i+1):
                # Mismo código EAN
                if p1.get('codigo') and p1.get('codigo') == p2.get('codigo'):
                    duplicados.append((i, j))
                # Nombres muy similares
                elif FacturaValidator._similitud_nombre(
                    p1.get('nombre', ''), 
                    p2.get('nombre', '')
                ) > 0.85:
                    duplicados.append((i, j))
        
        return duplicados
    
    @staticmethod
    def _similitud_nombre(nombre1: str, nombre2: str) -> float:
        """Calcula similitud entre dos nombres (0-1)"""
        from difflib import SequenceMatcher
        return SequenceMatcher(None, nombre1.lower(), nombre2.lower()).ratio()
