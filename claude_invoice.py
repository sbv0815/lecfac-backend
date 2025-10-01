"""
Procesador de facturas usando Claude Vision API
Reemplaza a Document AI con mayor precisión
"""

import anthropic
import base64
import os
import json
from typing import Dict, List

def parse_invoice_with_claude(image_path: str) -> Dict:
    """
    Procesa una factura usando Claude Vision API
    
    Args:
        image_path: Ruta al archivo de imagen
        
    Returns:
        {
            "success": True/False,
            "data": {
                "establecimiento": str,
                "total": float,
                "fecha": str,
                "productos": [{"codigo": str, "nombre": str, "precio": float}],
                "metadatos": {...}
            },
            "error": str (si success=False)
        }
    """
    
    try:
        print("=== PROCESANDO CON CLAUDE VISION ===")
        
        # Leer y encodear imagen
        with open(image_path, 'rb') as f:
            image_data = base64.b64encode(f.read()).decode('utf-8')
        
        # Determinar tipo MIME
        if image_path.lower().endswith('.png'):
            media_type = "image/png"
        else:
            media_type = "image/jpeg"
        
        # Cliente de Anthropic
        client = anthropic.Anthropic(
            api_key=os.environ.get("ANTHROPIC_API_KEY")
        )
        
        # Crear prompt optimizado
        prompt = """Analiza esta factura de supermercado colombiano y extrae la información en formato JSON.

**INSTRUCCIONES CRÍTICAS:**

1. **Establecimiento**: Nombre completo del supermercado (Jumbo, Éxito, Olímpica, D1, Carulla, etc.)

2. **Total**: Valor total de la factura (busca "TOTAL", "****SUBTOTAL/TOTAL****", etc.). Retorna como número sin puntos ni comas.

3. **Fecha**: En formato YYYY-MM-DD

4. **Productos**: Lista de TODOS los productos comprados. Para cada uno:
   - **codigo**: Código EAN de 13 dígitos (ej: 7702993047842)
     - Si es producto fresco con peso variable (BANANO, TOMATE, ZANAHORIA, etc.), usa el código corto PLU (ej: "116", "1045", "949")
     - Si no tiene código visible, genera uno como "SIN_CODIGO_001"
   
   - **nombre**: Nombre completo del producto (ej: "Chocolate BI", "BANANO URABA")
     - Expande abreviaturas cuando sea obvio (ej: "CUISI" → "CUISINE")
   
   - **precio**: Precio unitario como número (ej: 2190, no "2,190")
     - IGNORA líneas de descuentos (valores negativos o que digan "REF", "DF", "%")
     - IGNORA líneas de subtotales o pesos (ej: "0,878 KG X 16980")

5. **Productos duplicados**: Si un producto aparece varias veces (ej: BANANO URABA comprado 3 veces), inclúyelos todos como entradas separadas.

**FORMATO DE SALIDA:**
```json
{
  "establecimiento": "JUMBO BULEVAR",
  "total": 512352,
  "fecha": "2009-11-23",
  "productos": [
    {"codigo": "7702993047842", "nombre": "Chocolate BI", "precio": 2190},
    {"codigo": "116", "nombre": "BANANO URABA", "precio": 5425},
    {"codigo": "7702175141214", "nombre": "Bicarbonato", "precio": 7790}
  ]
}
