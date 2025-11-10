# claude_invoice.py - VERSIÓN MEJORADA CON MEJOR DETECCIÓN DE PLUs

import anthropic
import base64
import os
import json
import re
import unicodedata
from typing import Dict


# ==============================================================================
# NORMALIZACIÓN DE NOMBRES (NUEVO)
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
# DICCIONARIO DE CORRECCIONES OCR - AMPLIADO
# ==============================================================================

CORRECCIONES_OCR = {
    # Errores comunes detectados
    "QSO": "QUESO",
    "FRANC": "FRANCES",
    "BCO": "BLANCO",
    "ZHRIA": "ZANAHORIA",
    "GRL": "GRANEL",
    "PONQ": "PONQUE",
    "CHOCTINA": "CHOCOLATINA",
    "REFRESC": "REFRESCO",
    "DODA": "DOÑA",
    "MARGAR": "MARGARINA",
    "MEDAL": "MEDALLA",
    "MERMEL": "MERMELADA",
    "ESPARCIB": "ESPARCIR",

    # ✅ NUEVO: Errores específicos de lácteos
    "CREM": "CREMA",
    "CREMA": "CREMA",
    "VECHE": "LECHE",
    "VEC": "LECHE",
    "LECH": "LECHE",
    "LEC": "LECHE",
    "SEMI": "SEMIDESCREMADA",

    # Marcas comunes
    "ALQUERI": "ALQUERIA",
    "ALQUER": "ALQUERIA",
    "ALPNA": "ALPINA",
    "ALPIN": "ALPINA",
    "COLANT": "COLANTA",

    # Otros productos
    "ARE": "AREQUIPE",
    "IMPORT": "IMPORTADO",
    "BOLO": "BOLOÑESA",
    "UND": "UNIDAD",
    "CMAPAN": "COMAPAN",
    "MZRLL": "MOZARELLA",
    "CAS": "CASA",
    "VERD": "VERDE",
    "SUAVIZANT": "SUAVIZANTE",
    "RE": "RES",
    "HARINA HAZ D": "HARINA HAZ DE ORO",
    "LECA KLER": "LACA KLEER",
    "ATUN VAN": "ATUN VAN CAMP",
    "GANSI": "GANSITO",
    "VDE": "VERDE",
    "CHAMPIDON": "CHAMPIÑON",
    "FILET": "FILETE",
    "TILAP": "TILAPIA",
    "MANZ": "MANZANA",
    "PIDA": "PIÑA",
    "DODAPEPA": "DOÑA PEPA",
    "DENT": "DENTAL",
    "FLUOCARD": "FLUOCARDEN",
    "P/HIG": "PAPEL HIGIENICO",
    "PARMES": "PARMESANO",
    "VERDU": "VERDURAS",
    "VINIP": "VINIPEL",
    "CONG": "CONGELADA",
    "SABO": "SABORIZADA",
    "PIDA GOLDEN": "PIÑA GOLDEN",
    "CALDO DE GAT": "CALDO DE GALLINA",
    "FINETE DE 1": "FILETE DE TILAPIA",
    "HARINA HAZ 0": "HARINA HAZ DE ORO",
    "HARINA NAZ": "HARINA HAZ DE ORO",
    "INSTACREM 1D": "INSTACREM",
    "LECA KIFER I": "LACA KLEER",
    "LECA KLEER I": "LACA KLEER",
}


def corregir_nombre_producto(nombre: str) -> str:
    """
    Corrige errores OCR palabra por palabra
    ✅ MEJORADO: Más agresivo con correcciones
    """
    if not nombre or len(nombre.strip()) < 2:
        return nombre

    nombre_upper = nombre.upper()

    # Primero buscar frases completas
    for clave, correccion in CORRECCIONES_OCR.items():
        if " " in clave and clave in nombre_upper:
            return nombre_upper.replace(clave, correccion)

    # Luego palabra por palabra
    palabras = nombre_upper.split()
    palabras_corregidas = []

    for palabra in palabras:
        # Corrección directa
        if palabra in CORRECCIONES_OCR:
            palabras_corregidas.append(CORRECCIONES_OCR[palabra])
        # Correcciones parciales comunes
        elif "CREMA" in palabra and "VECHE" in palabra:
            palabras_corregidas.append("CREMA DE LECHE")
        elif palabra.startswith("LEC") and len(palabra) <= 5:
            palabras_corregidas.append("LECHE")
        elif palabra.startswith("CREM") and len(palabra) <= 5:
            palabras_corregidas.append("CREMA")
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
        return precio_str

    if isinstance(precio_str, float):
        if precio_str == int(precio_str):
            return int(precio_str)
        return int(precio_str * 100)

    precio_str = str(precio_str).strip()
    precio_str = precio_str.replace(" ", "")
    precio_str = precio_str.replace("$", "").replace("COP", "").replace("cop", "")
    precio_str = precio_str.strip()

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
        if precio < 0:
            print(f"   ⚠️ Precio negativo: {precio}, retornando 0")
            return 0
        return precio
    except (ValueError, TypeError) as e:
        print(f"   ⚠️ Error convirtiendo precio '{precio_str}': {e}")
        return 0


# ==============================================================================
# PROCESAMIENTO PRINCIPAL - PROMPT MEJORADO
# ==============================================================================

def parse_invoice_with_claude(image_path: str) -> Dict:
    """
    Procesa factura con Claude Vision API
    ✅ VERSIÓN MEJORADA: Mejor detección de PLUs y nombres completos
    """
    try:
        print("=" * 80)
        print("🤖 PROCESANDO CON CLAUDE HAIKU 3.5 - VERSIÓN MEJORADA PLU")
        print("=" * 80)

        # Leer imagen
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        media_type = "image/png" if image_path.lower().endswith(".png") else "image/jpeg"

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        # ========== PROMPT MEJORADO - ÉNFASIS EN PLUs ==========
        prompt = """Eres un experto en facturas de supermercados COLOMBIANOS. Tu misión es extraer CADA producto que el cliente compró.

# 🎯 ESTRUCTURA DE FACTURAS COLOMBIANAS

Todas las facturas tienen esta estructura de COLUMNAS:
```
PLU/CÓDIGO    DESCRIPCIÓN COMPLETA DEL PRODUCTO          PRECIO
1234          Crema de Leche Alqueria                    5,240
5678          Huevo Rojo AA                              11,450
```

# ⚠️ REGLA CRÍTICA: CÓDIGOS PLU

**Los PLUs son números de 4-6 dígitos que aparecen ANTES del nombre:**
```
1220  Mango                      ← PLU: 1220
      V.Ahorro 0                 ← IGNORAR (no es producto)
3323  Brownie Mini               ← PLU: 3323
```

**IMPORTANTE:**
- Si ves un número de 4-6 dígitos al inicio de línea → ES EL PLU
- Captúralo SIEMPRE en el campo "codigo"
- Los PLUs suelen estar en columna izquierda

**Ejemplos de PLUs válidos:**
- 1220, 2534, 4567 (4 dígitos)
- 12345, 54321 (5 dígitos)
- 123456 (6 dígitos)

**NO son PLUs:**
- "1/u", "2/u" → Son cantidades
- "0.750/KGM" → Es peso
- "V.Ahorro" → Es descuento

# 📝 REGLA CRÍTICA: NOMBRES COMPLETOS

**Extrae el nombre COMPLETO del producto:**

❌ MAL: "Crema"
✅ BIEN: "Crema de Leche Semidescremada"

❌ MAL: "Huevo"
✅ BIEN: "Huevo Rojo AA"

**El nombre TERMINA cuando aparece:**
- "V.Ahorro", "Ahorro"
- "/KGM", "/KG", "/U"
- "x 0.750", "x 1.5"
- "Descuento", "Dto"

**Ejemplo correcto:**
```
PLU    NOMBRE COMPLETO                          PRECIO
2534   Crema de Leche Semidescremada           5,240
       V.Ahorro 0.250                            ← IGNORAR
```

Resultado JSON:
```json
{
  "codigo": "2534",
  "nombre": "Crema de Leche Semidescremada",
  "precio": 5240
}
```

# 🚫 LÍNEAS QUE DEBES IGNORAR

**NO son productos:**
```
1 1/u x 26.900 V.Ahorro 4.035        ← Descuento
0.750/KGM x 8.800                     ← Peso/unidad
2x1 Descuento                         ← Promoción
V.Ahorro                              ← Solo ahorro
Subtotal                              ← Total parcial
```

**Características de líneas basura:**
- Tienen "x" seguido de precio
- Solo dicen "V.Ahorro" o "Ahorro"
- Formato "0.XXX/KG"
- NO tienen PLU al inicio

# 🔍 ALGORITMO PASO A PASO

Para cada línea de la factura:

**PASO 1: ¿Es un producto?**
```
¿Tiene formato "X.XXX/KG x PRECIO"? → NO ES PRODUCTO
¿Solo dice "V.Ahorro"? → NO ES PRODUCTO
¿Empieza con PLU (4-6 dígitos)? → PROBABLEMENTE SÍ
```

**PASO 2: Extraer datos**
```
Columna 1 → codigo (PLU de 4-6 dígitos)
Columna 2 → nombre (COMPLETO, hasta antes de "V.Ahorro")
Columna 3 → precio (última columna con números)
```

**PASO 3: Limpiar nombre**
```
❌ "Crema de Leche V.Ahorro 0"
✅ "Crema de Leche"

❌ "Huevo Rojo 0.750/KGM"
✅ "Huevo Rojo"
```

# 📋 EJEMPLOS REALES

**Factura del ÉXITO:**
```
PLU      DETALLE                              PRECIO
1220     Mango                                6,280
         V.Ahorro 0                           ← IGNORAR
2534     Crema de Leche Semidescremada        5,240
         V.Ahorro 0.250                       ← IGNORAR
3323     Brownie Mini Are                     14,800
```

**Salida JSON correcta:**
```json
{
  "establecimiento": "ÉXITO",
  "fecha": "2024-11-10",
  "total": 26320,
  "productos": [
    {
      "codigo": "1220",
      "nombre": "Mango",
      "cantidad": 1,
      "precio": 6280
    },
    {
      "codigo": "2534",
      "nombre": "Crema de Leche Semidescremada",
      "cantidad": 1,
      "precio": 5240
    },
    {
      "codigo": "3323",
      "nombre": "Brownie Mini Are",
      "cantidad": 1,
      "precio": 14800
    }
  ]
}
```

# ✅ VALIDACIÓN FINAL

Antes de responder, verifica:
1. ✅ Cada producto tiene su PLU (si estaba visible)
2. ✅ Los nombres están COMPLETOS (no truncados)
3. ✅ NO hay líneas con "V.Ahorro", "Descuento", "/KGM"
4. ✅ La suma de precios ≈ SUBTOTAL

# 🏪 ESTABLECIMIENTOS COLOMBIANOS

- Grupo Éxito: ÉXITO, Carulla, Surtimax
- Cencosud: JUMBO, Metro
- Otros: Olímpica, D1, ARA, Alkosto, PriceSmart

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON (sin markdown):**"""

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

        # ========== FILTRADO DE BASURA ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])

            palabras_basura = [
                "ahorro", "descuento", "desc", "dto", "rebaja", "promocion",
                "iva", "impuesto", "subtotal", "total", "cambio", "efectivo",
                "tarjeta", "credito", "debito", "gracias", "vuelva",
                "resolucion", "dian", "nit", "factura", "ticket", "recibo",
                "pago", "autoriza", "aprobado", "cufe", "qr", "fecha", "hora",
            ]

            productos_filtrados = []
            basura_eliminada = 0

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).lower().strip()

                es_basura = any(palabra in nombre for palabra in palabras_basura)
                solo_numeros = not re.search(r"[A-Za-zÀ-ÿ]", nombre)
                es_unidad = nombre in ["kg", "kgm", "/kgm", "/kg", "und", "/u", "x"]
                muy_corto = len(nombre) < 3
                precio = prod.get("precio", 0)
                precio_invalido = precio < 50

                if es_basura or solo_numeros or es_unidad or muy_corto or (precio_invalido and not nombre):
                    basura_eliminada += 1
                    print(f"   🗑️ Basura: '{prod.get('nombre', 'N/A')[:40]}'")
                else:
                    productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if basura_eliminada > 0:
                print(f"✅ {basura_eliminada} líneas basura eliminadas")
                print(f"📦 {len(productos_filtrados)} productos válidos\n")

        # ========== LIMPIEZA DE NOMBRES ==========
        if "productos" in data and data["productos"]:
            productos_limpios = []
            print(f"🧹 LIMPIANDO NOMBRES...")

            for prod in data["productos"]:
                nombre_original = str(prod.get("nombre", "")).strip()
                nombre_limpio = nombre_original

                # Eliminar sufijos de error
                sufijos_error = [
                    r"\s+V\.?\s*Ahorro.*$",
                    r"\s+Ahorro.*$",
                    r"\s+Descuento.*$",
                    r"\s+\d+\.?\d*/KG[MH]?.*$",
                    r"\s+x\s+\d+\.?\d+.*$",
                    r"\s+Khorro.*$",
                ]

                for patron in sufijos_error:
                    nombre_limpio = re.sub(patron, "", nombre_limpio, flags=re.IGNORECASE)

                nombre_limpio = nombre_limpio.strip()

                if nombre_limpio != nombre_original:
                    print(f"   🧹 '{nombre_original[:50]}' → '{nombre_limpio}'")

                if len(nombre_limpio) >= 3:
                    prod["nombre"] = nombre_limpio
                    productos_limpios.append(prod)
                else:
                    print(f"   🗑️ Muy corto: '{nombre_limpio}'")

            data["productos"] = productos_limpios
            print(f"✅ {len(productos_limpios)} productos finales\n")

        # ========== CORRECCIÓN OCR ==========
        if "productos" in data and data["productos"]:
            print(f"🔧 CORRIGIENDO ERRORES OCR...")
            correcciones = 0

            for prod in data["productos"]:
                nombre_original = prod.get("nombre", "")
                nombre_corregido = corregir_nombre_producto(nombre_original)

                if nombre_corregido != nombre_original:
                    print(f"   🔧 '{nombre_original}' → '{nombre_corregido}'")
                    prod["nombre"] = nombre_corregido
                    correcciones += 1

            if correcciones > 0:
                print(f"✅ {correcciones} correcciones OCR\n")

        # ========== NORMALIZACIÓN ==========
        if "productos" in data and data["productos"]:
            print(f"📝 NORMALIZANDO NOMBRES...")

            for prod in data["productos"]:
                nombre_original = prod.get("nombre", "")
                nombre_normalizado = normalizar_nombre_producto(nombre_original)

                if nombre_normalizado != nombre_original:
                    print(f"   📝 '{nombre_original}' → '{nombre_normalizado}'")
                    prod["nombre"] = nombre_normalizado

        # ========== PROCESAMIENTO FINAL ==========
        productos_procesados = 0
        nivel_1 = 0  # Código + Nombre + Precio
        nivel_2 = 0  # Nombre + Precio
        nivel_3 = 0  # Parcial

        for prod in data.get("productos", []):
            productos_procesados += 1

            # Limpiar precio
            if "precio" in prod:
                prod["precio"] = limpiar_precio_colombiano(prod["precio"])
            else:
                prod["precio"] = 0

            prod["valor"] = prod["precio"]

            # Cantidad
            if "cantidad" not in prod:
                prod["cantidad"] = 1
            else:
                try:
                    prod["cantidad"] = float(prod["cantidad"])
                except:
                    prod["cantidad"] = 1

            # Validar código (PLU o EAN)
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()

                # ✅ Acepta códigos de 3-13 dígitos
                if codigo_limpio.isdigit() and 3 <= len(codigo_limpio) <= 13:
                    prod["codigo"] = codigo_limpio
                else:
                    prod["codigo"] = ""
            else:
                prod["codigo"] = ""

            # Limpiar nombre
            nombre = str(prod.get("nombre", "")).strip()
            prod["nombre"] = nombre

            # Calcular nivel de confianza
            tiene_codigo = bool(prod["codigo"])
            tiene_nombre = bool(nombre and len(nombre) >= 3)
            tiene_precio = bool(prod["precio"] >= 50)

            if tiene_codigo and tiene_nombre and tiene_precio:
                prod["nivel_confianza"] = 1
                nivel_1 += 1
            elif tiene_nombre and tiene_precio:
                prod["nivel_confianza"] = 2
                nivel_2 += 1
            else:
                prod["nivel_confianza"] = 3
                nivel_3 += 1

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

        # ========== LOGS ==========
        print(f"=" * 80)
        print(f"📊 RESULTADOS:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   💰 Total: ${data.get('total', 0):,}")
        print(f"   📦 Productos: {productos_procesados}")
        print(f"")
        print(f"📊 POR NIVEL:")
        print(f"   ✅ NIVEL 1 (Código+Nombre+Precio): {nivel_1}")
        print(f"   ⚠️  NIVEL 2 (Nombre+Precio): {nivel_2}")
        print(f"   ⚡ NIVEL 3 (Parcial): {nivel_3}")
        print(f"=" * 80)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-mejorado-plu",
                    "modelo": "claude-3-5-haiku-20241022",
                    "productos_detectados": productos_procesados,
                    "nivel_1": nivel_1,
                    "nivel_2": nivel_2,
                    "nivel_3": nivel_3,
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
print("✅ claude_invoice.py MEJORADO - v2025-11-10")
print("   🔧 Mejor detección de PLUs (4-6 dígitos)")
print("   📝 Normalización completa (MAYÚSCULAS, sin tildes)")
print("   🧹 Correcciones OCR ampliadas")
print("   💰 Manejo robusto de precios colombianos")
