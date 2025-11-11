# claude_invoice.py - VERSIÓN 2.0 CON DETECCIÓN EAN + FILTRO BASURA

import anthropic
import base64
import os
import json
import re
import unicodedata
from typing import Dict, List, Tuple


# ==============================================================================
# FILTRO DE TEXTO BASURA (NUEVO)
# ==============================================================================

PALABRAS_BASURA = [
    # Promociones
    'ahorra', 'ahorro', 'descuento', 'oferta', 'promocion', 'promo',
    '2x1', '3x2', 'lleva', 'paga', 'gratis', 'v.ahorro', 'v.khorro',

    # Textos de factura
    'subtotal', 'total', 'iva', 'propina', 'cambio', 'efectivo',
    'tarjeta', 'credito', 'debito', 'pago', 'recibido',
    'devuelta', 'vuelto', 'recaudo',

    # Textos generales
    'precio final', 'display', 'exhibicion',
    'espaci', 'espaciador', 'separador',

    # Instrucciones
    'guardar', 'refrigerar', 'congelar',
]

def es_texto_basura(nombre: str) -> Tuple[bool, str]:
    """
    Detecta si un texto es basura promocional

    Returns:
        Tuple[bool, str]: (es_basura, razon)
    """
    if not nombre or len(nombre.strip()) < 3:
        return True, "Nombre muy corto"

    nombre_lower = nombre.lower().strip()

    # Verificar palabras basura
    for palabra in PALABRAS_BASURA:
        if palabra in nombre_lower:
            return True, f"Contiene '{palabra}'"

    # Solo números
    if nombre.replace(' ', '').isdigit():
        return True, "Solo números"

    # Patrones basura
    if re.match(r'^\d+x\d+$', nombre_lower):
        return True, "Patrón promocional"

    return False, ""


# ==============================================================================
# NORMALIZACIÓN DE NOMBRES
# ==============================================================================

def normalizar_nombre_producto(nombre: str) -> str:
    """
    Normaliza nombres: MAYÚSCULAS, sin tildes, sin espacios extras
    """
    if not nombre or not nombre.strip():
        return "PRODUCTO SIN NOMBRE"

    # Convertir a mayúsculas
    nombre = nombre.upper().strip()

    # Quitar tildes
    nombre = ''.join(
        c for c in unicodedata.normalize('NFD', nombre)
        if unicodedata.category(c) != 'Mn'
    )

    # Reemplazar caracteres especiales por espacios
    for char in ['-', '_', '.', ',', '/', '\\', '|']:
        nombre = nombre.replace(char, ' ')

    # Quitar espacios múltiples
    nombre = ' '.join(nombre.split())

    # Quitar caracteres no alfanuméricos (excepto espacios)
    nombre = ''.join(c for c in nombre if c.isalnum() or c.isspace())

    return nombre


# ==============================================================================
# CORRECCIONES OCR
# ==============================================================================

CORRECCIONES_OCR = {
    # Errores comunes detectados
    "QSO": "QUESO",
    "FRANC": "FRANCES",
    "BCO": "BLANCO",
    "ZHRIA": "ZANAHORIA",
    "GRL": "GRANEL",
    "PONQ": "PONQUE",
    "PONO": "PONQUE",
    "GGNS": "",  # Ruido OCR
    "CHOCTINA": "CHOCOLATINA",
    "CHOCTINGA": "CHOCOLATINA",
    "CHOCTING": "CHOCOLATINA",
    "CHOCITINA": "CHOCOLATINA",
    "REFRESC": "REFRESCO",
    "DODA": "DOÑA",
    "MARGAR": "MARGARINA",
    "ESPARCI": "MARGARINA",
    "ESPARCIR": "MARGARINA",
    "MEDAL": "MEDALLA",
    "MEDALL": "MEDALLA",
    "MERMEL": "MERMELADA",
    "OSO":"QUESO",

    # Lácteos
    "CREM": "CREMA",
    "VECHE": "LECHE",
    "VEC": "LECHE",
    "LECH": "LECHE",
    "LEC": "LECHE",
    "SEMI": "SEMIDESCREMADA",

    # Marcas
    "ALQUERI": "ALQUERIA",
    "ALQUER": "ALQUERIA",
    "ALQUERIA": "ALQUERIA",  # ← CORRECCIÓN según tu nota
    "ALPNA": "ALPINA",
    "ALPIN": "ALPINA",
    "COLANT": "COLANTA",
}


def corregir_nombre_producto(nombre: str) -> str:
    """
    Corrige errores OCR palabra por palabra
    """
    if not nombre or len(nombre.strip()) < 2:
        return nombre

    nombre_upper = nombre.upper()

    # Corrección palabra por palabra
    palabras = nombre_upper.split()
    palabras_corregidas = []

    for palabra in palabras:
        if palabra in CORRECCIONES_OCR:
            correccion = CORRECCIONES_OCR[palabra]
            if correccion:  # No agregar si es ""
                palabras_corregidas.append(correccion)
        else:
            palabras_corregidas.append(palabra)

    return " ".join(palabras_corregidas)


# ==============================================================================
# LIMPIEZA DE PRECIOS
# ==============================================================================

def limpiar_precio_colombiano(precio_str):
    """
    Convierte precio colombiano a entero (sin decimales)
    """
    if precio_str is None or precio_str == "":
        return 0

    if isinstance(precio_str, int):
        return max(0, precio_str)

    if isinstance(precio_str, float):
        if precio_str == int(precio_str):
            return max(0, int(precio_str))
        return max(0, int(precio_str * 100))

    precio_str = str(precio_str).strip()
    precio_str = precio_str.replace(" ", "").replace("$", "").replace("COP", "").replace("cop", "").strip()

    if precio_str.count('.') > 1 or precio_str.count(',') > 1:
        precio_str = precio_str.replace(",", "").replace(".", "")
    elif '.' in precio_str or ',' in precio_str:
        if '.' in precio_str:
            partes = precio_str.split('.')
        else:
            partes = precio_str.split(',')

        if len(partes) == 2 and len(partes[1]) == 3:
            precio_str = precio_str.replace(",", "").replace(".", "")
        elif len(partes) == 2 and len(partes[1]) <= 2:
            precio_str = precio_str.replace(",", "").replace(".", "")
        else:
            precio_str = precio_str.replace(",", "").replace(".", "")

    try:
        precio = int(float(precio_str))
        return max(0, precio)
    except (ValueError, TypeError):
        return 0


# ==============================================================================
# PROCESAMIENTO PRINCIPAL - PROMPT MEJORADO V2.0
# ==============================================================================

def parse_invoice_with_claude(image_path: str) -> Dict:
    """
    Procesa factura con Claude Vision API
    ✅ VERSIÓN 2.0: Detecta EAN-13 + PLU + Filtro de basura
    """
    try:
        print("=" * 80)
        print("🤖 PROCESANDO CON CLAUDE - v2.0 (EAN + PLU + FILTRO BASURA)")
        print("=" * 80)

        # Leer imagen
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        media_type = "image/png" if image_path.lower().endswith(".png") else "image/jpeg"

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        # ========== PROMPT MEJORADO V2.0 ==========
        prompt = """Eres un experto extractor de productos de facturas colombianas.

# 🎯 TU MISIÓN

Extraer CADA producto que el cliente compró con su código, nombre completo y precio.

# 📋 TIPOS DE CÓDIGOS EN FACTURAS COLOMBIANAS

**1. CÓDIGOS EAN-13 (Códigos de barras universales):**
- Son de 13 dígitos
- Ejemplo: 7702007084542, 7707352920005
- Aparecen en facturas de: JUMBO, ARA, D1, algunos productos en ÉXITO
- Si ves un número de 13 dígitos → SIEMPRE captúralo

**2. CÓDIGOS PLU (Códigos locales del establecimiento):**
- Son de 4-6 dígitos
- Ejemplo: 1220, 2534, 12345
- Comunes en: ÉXITO, OLÍMPICA, CARULLA (productos frescos)
- Si ves un número de 4-6 dígitos al inicio de línea → SIEMPRE captúralo

**3. Sin código:**
- Algunos productos no tienen código visible
- Aún así DEBES extraerlos si tienen nombre + precio

# ⚠️ REGLAS CRÍTICAS

**NOMBRES COMPLETOS:**
❌ MAL: "Crema"
✅ BIEN: "Crema de Leche Alpina Entera"

❌ MAL: "Chocolate"
✅ BIEN: "Chocolatina Jet Leche 45g"

**El nombre termina cuando aparece:**
- "V.Ahorro", "Ahorro", "Descuento"
- "/KGM", "/KG", "/U"
- "x 0.750", "x 1.5"

**IGNORAR estas líneas (NO son productos):**
```
V.Ahorro 0.250               ← Solo descuento
0.750/KGM x 8.800            ← Peso/medida
2x1 Descuento                ← Promoción
Subtotal                     ← Total parcial
Precio Final                 ← Texto promocional
Ahorra 40x                   ← Promoción
Display                      ← No es producto
```

# 📝 FORMATO DE SALIDA

Para CADA producto, responde con:
```json
{
  "codigo": "13 dígitos EAN o 4-6 dígitos PLU",
  "nombre": "Nombre COMPLETO del producto",
  "precio": precio_entero_sin_decimales,
  "cantidad": 1
}
```

**Si NO tiene código visible:**
```json
{
  "codigo": "",
  "nombre": "Nombre completo del producto",
  "precio": precio,
  "cantidad": 1
}
```

# 🔍 EJEMPLOS REALES

**Factura JUMBO (con EAN):**
```
EAN              DESCRIPCIÓN                    PRECIO
7702007084542    Leche Alpina Entera 1100ml     15,900
7707352920005    Atún Van Camp's Agua 140g       4,690
```

Respuesta:
```json
{
  "productos": [
    {
      "codigo": "7702007084542",
      "nombre": "Leche Alpina Entera 1100ml",
      "precio": 15900,
      "cantidad": 1
    },
    {
      "codigo": "7707352920005",
      "nombre": "Atún Van Camp's Agua 140g",
      "precio": 4690,
      "cantidad": 1
    }
  ]
}
```

**Factura ÉXITO (con PLU):**
```
PLU      DETALLE                              PRECIO
1220     Mango                                6,280
         V.Ahorro 0                           ← IGNORAR
2534     Crema de Leche Semidescremada        5,240
```

Respuesta:
```json
{
  "productos": [
    {
      "codigo": "1220",
      "nombre": "Mango",
      "precio": 6280,
      "cantidad": 1
    },
    {
      "codigo": "2534",
      "nombre": "Crema de Leche Semidescremada",
      "precio": 5240,
      "cantidad": 1
    }
  ]
}
```

**Factura sin códigos visibles:**
```
DESCRIPCIÓN                    PRECIO
Pan Tajado Bimbo 450g          8,100
Huevo Rojo AA x30              18,750
```

Respuesta:
```json
{
  "productos": [
    {
      "codigo": "",
      "nombre": "Pan Tajado Bimbo 450g",
      "precio": 8100,
      "cantidad": 1
    },
    {
      "codigo": "",
      "nombre": "Huevo Rojo AA x30",
      "precio": 18750,
      "cantidad": 1
    }
  ]
}
```

# ✅ VALIDACIÓN

Antes de responder:
1. ✅ Cada producto tiene nombre COMPLETO (no truncado)
2. ✅ NO incluiste líneas con "V.Ahorro", "Descuento", "/KGM"
3. ✅ Capturas código EAN (13 dígitos) cuando esté visible
4. ✅ Capturas código PLU (4-6 dígitos) cuando esté visible
5. ✅ Si no hay código, aún incluyes el producto

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON (sin markdown):**

```json
{
  "establecimiento": "NOMBRE DEL ESTABLECIMIENTO",
  "fecha": "YYYY-MM-DD",
  "total": total_entero,
  "productos": [
    {
      "codigo": "EAN13 o PLU o vacío",
      "nombre": "Nombre completo",
      "precio": precio_entero,
      "cantidad": 1
    }
  ]
}
```"""

        # Llamada a Claude
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=8000,
            temperature=0,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": media_type,
                                "data": image_data,
                            },
                        },
                        {"type": "text", "text": prompt},
                    ],
                }
            ],
        )

        response_text = message.content[0].text
        print(f"📄 Respuesta Claude (primeros 300 chars):\n{response_text[:300]}...\n")

        # Extraer JSON
        json_str = response_text

        if "```json" in response_text:
            json_str = response_text.split("```json")[1].split("```")[0]
        elif "```" in response_text:
            json_str = response_text.split("```")[1].split("```")[0]
        elif "{" in response_text:
            start = response_text.find("{")
            end = response_text.rfind("}") + 1
            if start != -1 and end > start:
                json_str = response_text[start:end]

        json_str = json_str.strip()
        data = json.loads(json_str)

        # ========== FILTRADO INTELIGENTE DE BASURA ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])
            productos_filtrados = []
            basura_eliminada = 0

            print(f"🧹 FILTRADO INTELIGENTE DE BASURA...")

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).strip()
                precio = prod.get("precio", 0)

                # Verificar si es basura
                es_basura, razon = es_texto_basura(nombre)

                if es_basura:
                    basura_eliminada += 1
                    print(f"   🗑️  BASURA: '{nombre[:50]}' - {razon}")
                    continue

                # Verificar precio mínimo
                if precio < 100:
                    basura_eliminada += 1
                    print(f"   🗑️  PRECIO BAJO: '{nombre[:50]}' (${precio})")
                    continue

                # Producto válido
                productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if basura_eliminada > 0:
                print(f"✅ {basura_eliminada} productos basura eliminados")
                print(f"📦 {len(productos_filtrados)} productos válidos\n")

        # ========== LIMPIEZA DE NOMBRES ==========
        if "productos" in data and data["productos"]:
            print(f"🧹 LIMPIANDO Y CORRIGIENDO NOMBRES...")

            for prod in data["productos"]:
                nombre_original = str(prod.get("nombre", "")).strip()

                # Eliminar sufijos de error
                nombre_limpio = re.sub(r"\s+V\.?\s*Ahorro.*$", "", nombre_original, flags=re.IGNORECASE)
                nombre_limpio = re.sub(r"\s+\d+\.?\d*/KG[MH]?.*$", "", nombre_limpio, flags=re.IGNORECASE)
                nombre_limpio = nombre_limpio.strip()

                # Corregir errores OCR
                nombre_corregido = corregir_nombre_producto(nombre_limpio)

                # Normalizar
                nombre_final = normalizar_nombre_producto(nombre_corregido)

                if nombre_final != nombre_original:
                    print(f"   🔧 '{nombre_original[:50]}' → '{nombre_final}'")

                prod["nombre"] = nombre_final

        # ========== PROCESAMIENTO FINAL ==========
        productos_procesados = 0
        con_ean = 0
        con_plu = 0
        sin_codigo = 0

        for prod in data.get("productos", []):
            productos_procesados += 1

            # Limpiar precio
            prod["precio"] = limpiar_precio_colombiano(prod.get("precio", 0))
            prod["valor"] = prod["precio"]

            # Cantidad
            prod["cantidad"] = float(prod.get("cantidad", 1))

            # Validar código
            codigo = str(prod.get("codigo", "")).strip()

            if codigo and codigo.isdigit():
                longitud = len(codigo)

                if longitud == 13:
                    prod["codigo"] = codigo
                    con_ean += 1
                    prod["tipo_codigo"] = "EAN-13"
                elif 4 <= longitud <= 6:
                    prod["codigo"] = codigo
                    con_plu += 1
                    prod["tipo_codigo"] = "PLU"
                elif 3 <= longitud <= 13:
                    prod["codigo"] = codigo
                    prod["tipo_codigo"] = "OTRO"
                else:
                    prod["codigo"] = ""
                    sin_codigo += 1
                    prod["tipo_codigo"] = "SIN_CODIGO"
            else:
                prod["codigo"] = ""
                sin_codigo += 1
                prod["tipo_codigo"] = "SIN_CODIGO"

        # Normalizar establecimiento
        data["establecimiento"] = normalizar_establecimiento(
            data.get("establecimiento", "Desconocido")
        )

        # Total
        if "total" not in data or not data["total"]:
            suma = sum(
                p.get("precio", 0) * p.get("cantidad", 1)
                for p in data.get("productos", [])
            )
            data["total"] = suma

        # ========== LOGS FINALES ==========
        print(f"=" * 80)
        print(f"📊 RESULTADOS FINALES:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   💰 Total: ${data.get('total', 0):,}")
        print(f"   📦 Productos válidos: {productos_procesados}")
        print(f"")
        print(f"📊 POR TIPO DE CÓDIGO:")
        print(f"   📦 EAN-13 (13 dígitos): {con_ean}")
        print(f"   🏷️  PLU (4-6 dígitos): {con_plu}")
        print(f"   ❓ Sin código: {sin_codigo}")
        print(f"=" * 80)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-v2.0",
                    "modelo": "claude-3-5-haiku-20241022",
                    "productos_detectados": productos_procesados,
                    "con_ean": con_ean,
                    "con_plu": con_plu,
                    "sin_codigo": sin_codigo,
                },
            },
        }

    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        print(f"Respuesta: {response_text[:500]}")
        return {
            "success": False,
            "error": "Error parseando respuesta. Imagen más clara.",
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": f"Error: {str(e)}"}


def normalizar_establecimiento(nombre_raw: str) -> str:
    """Normaliza nombre del establecimiento"""
    if not nombre_raw:
        return "Desconocido"

    nombre_lower = nombre_raw.lower().strip()

    establecimientos = {
        "jumbo": "JUMBO",
        "exito": "ÉXITO",
        "éxito": "ÉXITO",
        "carulla": "CARULLA",
        "olimpica": "OLÍMPICA",
        "olímpica": "OLÍMPICA",
        "ara": "ARA",
        "d1": "D1",
        "alkosto": "ALKOSTO",
        "makro": "MAKRO",
        "pricesmart": "PRICESMART",
        "surtimax": "SURTIMAX",
        "metro": "METRO",
        "cruz verde": "CRUZ VERDE",
        "cafam": "CAFAM",
        "colsubsidio": "COLSUBSIDIO",
        "jeronimo martins": "ARA",
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    return nombre_raw.strip().upper()[:50]


# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================
print("=" * 80)
print("✅ claude_invoice.py V2.0 CARGADO")
print("=" * 80)
print("🎯 MEJORAS:")
print("   📦 Detecta códigos EAN-13 (13 dígitos)")
print("   🏷️  Detecta códigos PLU (4-6 dígitos)")
print("   🗑️  Filtro inteligente de texto basura")
print("   🔧 Correcciones OCR ampliadas")
print("   📝 Normalización completa")
print("   💰 Manejo robusto de precios colombianos")
print("=" * 80)
