"""
claude_invoice.py - VERSIÓN 3.1 - CON ESTABLECIMIENTO CONFIRMADO
========================================================================

🎯 VERSIÓN 3.1 - NUEVAS CAPACIDADES:
- ✅ Establecimiento confirmado por usuario
- ✅ Corrección automática de fechas antiguas
- ✅ Integración completa con sistema de aprendizaje
- ✅ Auto-corrección de productos conocidos
- ✅ Tracking de ahorro (productos que no llamaron Perplexity)
- ✅ Detección EAN-13 + PLU
- ✅ Filtro inteligente de basura
- ✅ Correcciones OCR mejoradas
- ✅ Estadísticas de aprendizaje

FLUJO V3.1:
1️⃣ Claude OCR → Extrae productos crudos (con establecimiento confirmado)
2️⃣ Filtro basura → Elimina líneas no-producto
3️⃣ Correcciones Python → Arregla errores comunes
4️⃣ Aprendizaje → Busca productos conocidos
5️⃣ Perplexity → Solo si no está en aprendizaje
6️⃣ Guardar → Aprende para próxima vez
"""

import anthropic
import base64
import os
import json
import re
import unicodedata
from typing import Dict, List, Tuple, Optional
from datetime import datetime

# ==============================================================================
# FILTRO DE TEXTO BASURA
# ==============================================================================

PALABRAS_BASURA = [
    # Promociones
    "ahorra",
    "ahorro",
    "descuento",
    "oferta",
    "promocion",
    "promo",
    "2x1",
    "3x2",
    "lleva",
    "paga",
    "gratis",
    "v.ahorro",
    "v.khorro",
    # Textos de factura
    "subtotal",
    "total",
    "iva",
    "propina",
    "cambio",
    "efectivo",
    "tarjeta",
    "credito",
    "debito",
    "pago",
    "recibido",
    "devuelta",
    "vuelto",
    "recaudo",
    # Textos generales
    "precio final",
    "display",
    "exhibicion",
    "espaci",
    "espaciador",
    "separador",
    "domicilio",  # ← AGREGAR ESTA LÍNEA
    "domicilio web",  # ← AGREGAR ESTA LÍNEA
    # Instrucciones
    "guardar",
    "refrigerar",
    "congelar",
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

    # ✅ NUEVO: Validación específica para textos informativos
    textos_informativos = [
        "domicilio web",
        "domicilio",
        "web",
        "display",
        "exhibicion",
        "espaciador",
    ]

    for texto in textos_informativos:
        if nombre_lower == texto or nombre_lower.startswith(texto + " "):
            return True, f"Texto informativo: '{texto}'"

    # Verificar palabras basura
    for palabra in PALABRAS_BASURA:
        if palabra in nombre_lower:
            return True, f"Contiene '{palabra}'"

    # Solo números
    if nombre.replace(" ", "").isdigit():
        return True, "Solo números"

    # Patrones basura
    if re.match(r"^\d+x\d+$", nombre_lower):
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
    nombre = "".join(
        c
        for c in unicodedata.normalize("NFD", nombre)
        if unicodedata.category(c) != "Mn"
    )

    # Reemplazar caracteres especiales por espacios
    for char in ["-", "_", ".", ",", "/", "\\", "|"]:
        nombre = nombre.replace(char, " ")

    # Quitar espacios múltiples
    nombre = " ".join(nombre.split())

    # Quitar caracteres no alfanuméricos (excepto espacios)
    nombre = "".join(c for c in nombre if c.isalnum() or c.isspace())

    return nombre


# ==============================================================================
# CORRECCIONES OCR
# ==============================================================================

CORRECCIONES_OCR = {
    # ========== Errores comunes de OCR ==========
    "QSO": "QUESO",
    "OSO": "QUESO",
    "FRANC": "FRANCES",
    "BCO": "BLANCO",
    "ZHRIA": "ZANAHORIA",
    "GRL": "GRANEL",
    "PONQ": "PONQUE",
    "PONO": "PONQUE",
    "GGNS": "",
    "ZHIRIA": "ZANAHORIA",
    "ZANARIA": "ZANAHORIA",
    "ZANAHRIA": "ZANAHORIA",
    "PONQUE GANS RAMO 48G": "PONQUE GANS RAMO ",
    "PONQUE GANS RAMO 486": "PONQUE GANS RAMO ",
    "PONQUE GNS RAMO 48G": "PONQUE GANS RAMO ",
    "PONQUE GNS RAMO 480": "PONQUE GANS RAMO ",
    "PAN BLANCO BIMBO 730GR": "PAN BLANCO BIMBO 730G",
    "ESPARCIB RAMA C S": "ESPARCIR RAMA C/S",
    "PIDA GOLDEN": "PIÑA GOLDEN",
    "QSO PARMES RALLAD": "QUESO PARMESANO RALLADO",
    "QUESO PARKES RALLAD": "QUESO PARMESANO RALLADO",
    "ARROZ DODA PEPA": "ARROZ DONA PEPA",
    "AREPA DODO PAISA": "AREPAS DONA PAISA",
    "AREPA DORA PAISA": "AREPAS DONA PAISA",
    "AREPA DONA PAISA": "AREPAS DONA PAISA",
    "ATUN NESTLE AGUA": "ATUN MEDALLA AGUA",
    # ========== Correcciones Doña Pepa ==========
    "ARROZ DONAPEPA": "ARROZ DONA PEPA",
    "ARROZ DONA PAPA": "ARROZ DONA PEPA",
    "ARROZ DODAPEPA": "ARROZ DONA PEPA",
    "ARROZ DOBLEPEPA": "ARROZ DONA PEPA",
    "DODA PEPA": "DONA PEPA",
    "DODA": "DONA",
    # ========== Otras correcciones ==========
    "BANAN": "BANANO",
    "BANANO GRANEL": "BANANO GRANEL",
    # ========== Chocolatinas ==========
    "CHOCTINA": "CHOCOLATINA",
    "CHOCTINGA": "CHOCOLATINA",
    "CHOCTING": "CHOCOLATINA",
    "CHOCITINA": "CHOCOLATINA",
    # ========== Refrescos ==========
    "REFRESC": "REFRESCO",
    "REPESO": "REFRESCO",
    "POL": "POLVO",
    "REFRESC  CLIGHT POL": "REFRESCO  CLIGHT POLVO",
    "PASA FUSIL FERRA": "PASA FUSIL FERRARA",
    "PASA FUSIL TERRA": "PASA FUSIL FERRARA",
    # ========== Mermeladas ==========
    "MERMEL": "MERMELADA",
    "FRUGAL PIDA": "FRUGAL PIÑA",
    "PIDA": "PIÑA",
    # ========== Marcas comunes ==========
    "MARGAR": "MARGARINA",
    "ESPARCI": "MARGARINA",
    "ESPARCIR": "MARGARINA",
    "MEDAL": "MEDALLA",
    "MEDALL": "MEDALLA",
    "GALADITOS": "CALADITOS",
    "LECA KLEEK L": "LACA KLEER",
    "QUESO PARMA": "QUESO PARMESANO",
    # ========== Lácteos ==========
    "CREM": "CREMA",
    "VECHE": "LECHE",
    "VEC": "LECHE",
    "LECH": "LECHE",
    "LEC": "LECHE",
    "SEMI": "SEMIDESCREMADA",
    "ALQUERI": "ALQUERIA",
    "ALQUER": "ALQUERIA",
    "ALPNA": "ALPINA",
    "ALPIN": "ALPINA",
    "COLANT": "COLANTA",
    # ========== Palabras sin sentido (eliminar) ==========
    "BLENG": "",
    "MORCAF": "",
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
    precio_str = (
        precio_str.replace(" ", "")
        .replace("$", "")
        .replace("COP", "")
        .replace("cop", "")
        .strip()
    )

    if precio_str.count(".") > 1 or precio_str.count(",") > 1:
        precio_str = precio_str.replace(",", "").replace(".", "")
    elif "." in precio_str or "," in precio_str:
        if "." in precio_str:
            partes = precio_str.split(".")
        else:
            partes = precio_str.split(",")

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


def normalizar_establecimiento(nombre_raw: str) -> str:
    """Normaliza nombre del establecimiento"""
    if not nombre_raw:
        return "Desconocido"

    nombre_lower = nombre_raw.lower().strip()

    establecimientos = {
        "jumbo": "JUMBO",
        "exito": "EXITO",
        "éxito": "EXITO",
        "carulla": "CARULLA",
        "olimpica": "OLIMPICA",
        "olímpica": "OLIMPICA",
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
        "farmatodo": "FARMATODO",
        "supermercados premium": "SUPERMERCADOS PREMIUM",
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    return nombre_raw.strip().upper()[:50]


# ==============================================================================
# PROCESAMIENTO CON CLAUDE VISION
# ==============================================================================


def parse_invoice_with_claude(
    image_path: str,
    establecimiento_preseleccionado: str = None,  # ← NUEVO
    aplicar_aprendizaje: bool = True,
) -> Dict:
    """
    Procesa factura con Claude Vision API - V3.1

    ✅ NUEVO: Recibe establecimiento confirmado por usuario

    Args:
        image_path: Ruta a la imagen de la factura
        establecimiento_preseleccionado: Nombre del supermercado (ya confirmado)
        aplicar_aprendizaje: Si debe usar el sistema de aprendizaje

    Returns:
        Dict con success, data y estadísticas de aprendizaje
    """
    try:
        print("=" * 80)
        print("🤖 CLAUDE INVOICE V3.1 - CON ESTABLECIMIENTO CONFIRMADO")
        if establecimiento_preseleccionado:
            print(f"🏪 ESTABLECIMIENTO: {establecimiento_preseleccionado.upper()}")
        print("=" * 80)

        # Leer imagen
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        media_type = (
            "image/png" if image_path.lower().endswith(".png") else "image/jpeg"
        )

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        # ========== PROMPT MEJORADO V3.1 CON ESTABLECIMIENTO ==========

        # Construir sección de establecimiento si está confirmado
        prompt_establecimiento = ""
        if establecimiento_preseleccionado:
            prompt_establecimiento = f"""
# 🏪 INFORMACIÓN IMPORTANTE DEL ESTABLECIMIENTO

**El usuario YA confirmó que esta factura es de: {establecimiento_preseleccionado.upper()}**

REGLAS CRÍTICAS:
- Usa EXACTAMENTE el nombre: "{establecimiento_preseleccionado.upper()}"
- NO uses nombres genéricos como "Supermercado No Identificado" o "Domicilio Web"
- NO intentes adivinar el establecimiento
- Si la factura tiene un nombre diferente, IGNÓRALO y usa "{establecimiento_preseleccionado.upper()}"

"""

        prompt = f"""{prompt_establecimiento}Eres un experto extractor de productos de facturas colombianas.

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
DOMICILIO WEB                ← Texto informativo, NO ES UN PRODUCTO
Domicilio                    ← Texto informativo, NO ES UN PRODUCTO
```

**IMPORTANTE: Si una línea tiene estas palabras, NO la incluyas en productos:**
- "Domicilio", "Domicilio Web"
- "Display", "Exhibición"
- "Ahorra", "Ahorro", "Descuento"
- "Subtotal", "Total", "IVA"
````

# 📝 FORMATO DE SALIDA

Para CADA producto, responde con:
````json
{{
  "codigo": "13 dígitos EAN o 4-6 dígitos PLU",
  "nombre": "Nombre COMPLETO del producto",
  "precio": precio_entero_sin_decimales,
  "cantidad": 1
}}
````

**Si NO tiene código visible:**
````json
{{
  "codigo": "",
  "nombre": "Nombre completo del producto",
  "precio": precio,
  "cantidad": 1
}}
````

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON (sin markdown):**
````json
{{
  "establecimiento": "{establecimiento_preseleccionado.upper() if establecimiento_preseleccionado else 'NOMBRE DEL ESTABLECIMIENTO'}",
  "fecha": "YYYY-MM-DD",
  "total": total_entero,
  "productos": [
    {{
      "codigo": "EAN13 o PLU o vacío",
      "nombre": "Nombre completo",
      "precio": precio_entero,
      "cantidad": 1
    }}
  ]
}}
```"""

        # Llamada a Claude
        print("📸 Enviando imagen a Claude Vision API...")

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
        print(f"✅ Respuesta recibida ({len(response_text)} caracteres)")

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

        # ✅ FORZAR establecimiento si fue preseleccionado
        if establecimiento_preseleccionado:
            data["establecimiento"] = establecimiento_preseleccionado.upper()
            print(
                f"✅ Establecimiento forzado a: {establecimiento_preseleccionado.upper()}"
            )

        # ✅ FIX: Validar y corregir fecha sospechosa (año antiguo)
        if "fecha" in data and data["fecha"]:
            try:
                fecha_str = str(data["fecha"])
                if len(fecha_str) >= 4:
                    año = int(fecha_str[:4])
                    if año < 2020:  # Fecha sospechosa (ej: 2013)
                        año_actual = datetime.now().year
                        fecha_corregida = fecha_str.replace(
                            str(año), str(año_actual), 1
                        )
                        print(f"   ⚠️  Año sospechoso ({año}) corregido a {año_actual}")
                        data["fecha"] = fecha_corregida
            except:
                pass

        # ========== FILTRADO INTELIGENTE DE BASURA ==========
        productos_originales = 0
        basura_eliminada = 0

        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])
            productos_filtrados = []

            print(f"\n🧹 FILTRADO DE BASURA:")

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).strip()
                precio = prod.get("precio", 0)

                # Verificar si es basura
                es_basura, razon = es_texto_basura(nombre)

                if es_basura:
                    basura_eliminada += 1
                    print(f"   🗑️  '{nombre[:40]}' - {razon}")
                    continue

                # Verificar precio mínimo
                if precio < 100:
                    basura_eliminada += 1
                    print(f"   🗑️  '{nombre[:40]}' - Precio muy bajo (${precio})")
                    continue

                # Producto válido
                productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if basura_eliminada > 0:
                print(f"   ✅ Eliminados: {basura_eliminada}")
                print(f"   📦 Válidos: {len(productos_filtrados)}")

        # ========== LIMPIEZA Y CORRECCIÓN DE NOMBRES ==========
        if "productos" in data and data["productos"]:
            print(f"\n🔧 CORRECCIONES OCR:")

            for prod in data["productos"]:
                nombre_original = str(prod.get("nombre", "")).strip()

                # Eliminar sufijos de error
                nombre_limpio = re.sub(
                    r"\s+V\.?\s*Ahorro.*$", "", nombre_original, flags=re.IGNORECASE
                )
                nombre_limpio = re.sub(
                    r"\s+\d+\.?\d*/KG[MH]?.*$", "", nombre_limpio, flags=re.IGNORECASE
                )
                nombre_limpio = nombre_limpio.strip()

                # Corregir errores OCR
                nombre_corregido = corregir_nombre_producto(nombre_limpio)

                # Normalizar
                nombre_final = normalizar_nombre_producto(nombre_corregido)

                if nombre_final != nombre_original:
                    print(f"   📝 '{nombre_original[:35]}' → '{nombre_final[:35]}'")

                prod["nombre"] = nombre_final
                prod["nombre_ocr_original"] = (
                    nombre_original  # Guardar para aprendizaje
                )

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
                    prod["tipo_codigo"] = "EAN"
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

        # Normalizar establecimiento (solo si no fue preseleccionado)
        if not establecimiento_preseleccionado:
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

        # ========== ESTADÍSTICAS ==========
        print(f"\n" + "=" * 80)
        print(f"📊 RESULTADOS OCR:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   📅 Fecha: {data.get('fecha', 'N/A')}")
        print(f"   💰 Total: ${data.get('total', 0):,}")
        print(f"   📦 Productos: {productos_procesados}")
        print(f"   🗑️  Basura eliminada: {basura_eliminada}")
        print(f"\n📊 CÓDIGOS DETECTADOS:")
        print(f"   📦 EAN-13: {con_ean}")
        print(f"   🏷️  PLU: {con_plu}")
        print(f"   ❓ Sin código: {sin_codigo}")
        print("=" * 80)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-v3.1",
                    "modelo": "claude-3-5-haiku-20241022",
                    "establecimiento_confirmado": bool(establecimiento_preseleccionado),
                    "productos_detectados": productos_procesados,
                    "productos_originales": productos_originales,
                    "basura_eliminada": basura_eliminada,
                    "con_ean": con_ean,
                    "con_plu": con_plu,
                    "sin_codigo": sin_codigo,
                },
            },
        }

    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        print(
            f"Respuesta: {response_text[:500] if 'response_text' in locals() else 'N/A'}"
        )
        return {
            "success": False,
            "error": "Error parseando respuesta. Imagen más clara.",
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": f"Error: {str(e)}"}


# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================
print("=" * 80)
print("✅ claude_invoice.py V3.1 CARGADO - CON ESTABLECIMIENTO CONFIRMADO")
print("=" * 80)
print("🎯 CAPACIDADES:")
print("   🏪 Establecimiento confirmado por usuario")
print("   📦 Detección EAN-13 (códigos de barras universales)")
print("   🏷️  Detección PLU (códigos locales de establecimientos)")
print("   🗑️  Filtro inteligente de texto basura")
print("   🔧 Correcciones OCR ampliadas")
print("   📝 Normalización completa de nombres")
print("   💰 Manejo robusto de precios colombianos")
print("   📅 Corrección automática de fechas antiguas")
print("   🧠 LISTO para integración con aprendizaje")
print("=" * 80)
