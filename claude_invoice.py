"""
claude_invoice.py - VERSIÓN 6.2 - CON POSICIÓN VERTICAL DE PRODUCTOS
========================================================================

🎯 VERSIÓN 6.2 - MEJORAS:
- ✅ Devuelve posición vertical (0-100%) de cada producto en la imagen
- ✅ Permite ubicar rápidamente un producto en la factura original
- ✅ Mantiene todas las funcionalidades de V6.1
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
# FILTRO DE TEXTO BASURA - VERSION 6.1 MEJORADA
# ==============================================================================

PALABRAS_BASURA = [
    # Promociones y descuentos
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
    "precio final",
    # Totales y resúmenes
    "subtotal",
    "total",
    "iva",
    "propina",
    "cambio",
    "efectivo",
    "total item",
    # Medios de pago y transacciones - AMPLIADO
    "tarjeta",
    "credito",
    "debito",
    "pago",
    "recibido",
    "devuelta",
    "redeban",
    "mastercard",
    "visa",
    "credibanco",
    "datafono",
    "terminal",
    "aprobado",
    "autorizado",
    "autorizacion",
    "transaccion",
    "pse",
    "nequi",
    "daviplata",
    "bancolombia",
    "multicolor",
    "red multicolor",
    "codigo aprobacion",
    "cod aprobacion",
    "num aprobacion",
    "referencia",
    "voucher",
    "comprobante",
    "recibo",
    "american express",
    "amex",
    "diners",
    "diners club",
    # Servicios y extras
    "domicilio",
    "domicilio web",
    "display",
    "exhibicion",
    "bolsa",
    "empacar",
    "empaque",
    "bsa p empacar",
    "bsa p/empacar",
    "biodegradable",
]


def es_texto_basura(nombre: str) -> Tuple[bool, str]:
    """Detecta si un texto es basura promocional o información de pago"""
    if not nombre or len(nombre.strip()) < 3:
        return True, "Nombre muy corto"

    nombre_lower = nombre.lower().strip()

    for palabra in PALABRAS_BASURA:
        if palabra in nombre_lower:
            return True, f"Contiene '{palabra}'"

    if nombre.replace(" ", "").replace(".", "").isdigit():
        return True, "Solo números"

    return False, ""


# ==============================================================================
# NORMALIZACIÓN Y CORRECCIONES
# ==============================================================================


def normalizar_nombre_producto(nombre: str) -> str:
    """Normaliza nombres: MAYÚSCULAS, sin tildes, sin espacios extras"""
    if not nombre or not nombre.strip():
        return "PRODUCTO SIN NOMBRE"

    nombre = nombre.upper().strip()
    nombre = "".join(
        c
        for c in unicodedata.normalize("NFD", nombre)
        if unicodedata.category(c) != "Mn"
    )

    for char in ["-", "_", ".", ",", "/", "\\", "|", "(", ")", "[", "]"]:
        nombre = nombre.replace(char, " ")

    nombre = " ".join(nombre.split())
    nombre = "".join(c for c in nombre if c.isalnum() or c.isspace())

    return nombre[:100]


CORRECCIONES_OCR = {
    "QSO": "QUESO",
    "BCO": "BLANCO",
    "GRL": "GRANEL",
    "BANAN": "BANANO",
    "CHOCTINA": "CHOCOLATINA",
    "REFRESC": "REFRESCO",
    "MERMEL": "MERMELADA",
    "MARGAR": "MARGARINA",
    "CREM": "CREMA",
    "LECH": "LECHE",
    "ALQUERI": "ALQUERIA",
    "COLANT": "COLANTA",
    "DODAPEPA": "DONA PEPA",
    "DONAPEPA": "DONA PEPA",
    "DONAPAPA": "DONA PEPA",
    "DOBRAPEPA": "DONA PEPA",
    "DODA": "DONA",
    "DODO": "DONA",
    "MOL": "MOLIDA",
    "ESP": "ESPECIAL",
    "BSA": "BOLSA",
    "LECHEE": "LECHE",
    "CREMAA": "CREMA",
}


def corregir_nombre_producto(nombre: str) -> str:
    """Corrige errores OCR palabra por palabra"""
    if not nombre:
        return nombre

    nombre_upper = nombre.upper()

    for error, correccion in CORRECCIONES_OCR.items():
        if error in nombre_upper:
            nombre_upper = nombre_upper.replace(error, correccion)

    palabras = nombre_upper.split()
    palabras_corregidas = []

    for palabra in palabras:
        if palabra in CORRECCIONES_OCR:
            correccion = CORRECCIONES_OCR[palabra]
            if correccion:
                palabras_corregidas.append(correccion)
        else:
            palabras_corregidas.append(palabra)

    return " ".join(palabras_corregidas)


def limpiar_precio_colombiano(precio_str):
    """Convierte precio colombiano a entero"""
    if precio_str is None or precio_str == "":
        return 0
    if isinstance(precio_str, (int, float)):
        return max(0, int(precio_str))

    precio_str = str(precio_str).strip()
    precio_str = precio_str.replace(" ", "").replace("$", "").replace("COP", "")
    precio_str = precio_str.replace(",", "").replace(".", "")
    precio_str = precio_str.rstrip("AaEeDd")

    try:
        return max(0, int(float(precio_str)))
    except:
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
        "farmatodo": "FARMATODO",
        "cruz verde": "CRUZ VERDE",
        "supermercados premium": "SUPERMERCADOS PREMIUM",
        "ramirez hermanos": "SUPERMERCADOS PREMIUM",
        "CADENA COMERCIAL OXXO COLOMBIA": "OXXO",
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    return nombre_raw.strip().upper()[:50]


# ==============================================================================
# PROCESAMIENTO CON CLAUDE VISION - V6.2 CON POSICIÓN VERTICAL
# ==============================================================================


def parse_invoice_with_claude(
    image_path: str,
    establecimiento_preseleccionado: str = None,
    aplicar_aprendizaje: bool = True,
) -> Dict:
    """
    Procesa factura con Claude Vision API - V6.2
    Lee EXACTAMENTE como un humano leería la factura
    NUEVO: Devuelve posición vertical de cada producto
    """
    try:
        print("=" * 80)
        print("🤖 CLAUDE INVOICE V6.2 - CON POSICIÓN VERTICAL DE PRODUCTOS")
        if establecimiento_preseleccionado:
            print(f"🏪 ESTABLECIMIENTO: {establecimiento_preseleccionado.upper()}")
        print("=" * 80)

        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        media_type = (
            "image/png" if image_path.lower().endswith(".png") else "image/jpeg"
        )

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        establecimiento_info = (
            f'"{establecimiento_preseleccionado.upper()}"'
            if establecimiento_preseleccionado
            else '"NOMBRE_DEL_ESTABLECIMIENTO"'
        )

        # ========== PROMPT V6.2 - CON POSICIÓN VERTICAL ==========
        prompt = f"""Eres un experto en leer facturas colombianas. Tu trabajo es leer EXACTAMENTE lo que está escrito, sin inventar ni modificar nada.

# 🔍 PASO 1: IDENTIFICA EL FORMATO DE LA FACTURA

Mira el ENCABEZADO de la factura para identificar el formato:

**FORMATO A - ÉXITO/CARULLA:**
Si ves: "PLU    DETALLE    PRECIO"
```
1 1/u x 11.450 V.Ahorro 0
1413568 Huevo Rojo AA 15    11.450
```

**FORMATO B - OLÍMPICA:**
Si ves: "# UM  Vr. Unit  Cant  Vr. Total"
```
1393170 ARROZ DODAPEPA 3KG    *
02 un      16.650    1    16.650
```

**FORMATO C - FARMATODO:**
Si ves: "Artículo  Cantidad  Precio  Importe"
```
PROTECTOR CAREFREE SIN FRAGANCIA
101047110    2    7.650    15.300
```

**FORMATO D - SUPERMERCADOS PEQUEÑOS:**
Si ves: "UND  DESCRIPCION  PRECIO  TOTAL"
```
1    QUESO PERA ANDES    3200    3.200
```

# 📋 CÓMO LEER CADA FORMATO

## FORMATO A - ÉXITO/CARULLA (El más común)

Cada producto tiene DOS líneas:
```
1 1/u x 11.450 V.Ahorro 0        ← LÍNEA 1: INFO
1413568 Huevo Rojo AA 15  11.450  ← LÍNEA 2: PRODUCTO
```

**LÍNEA 1 - INFO:**
- "1" al INICIO = NÚMERO DE LÍNEA (NO ES CANTIDAD, IGNORAR)
- "1/u" = CANTIDAD (1 unidad)
- "0.500/KGM" = CANTIDAD en kg (0.5 kg)
- "11.450" = Precio unitario
- "V.Ahorro 0" = Sin descuento

**LÍNEA 2 - PRODUCTO:**
- "1413568" = PLU (código de 6-7 dígitos)
- "Huevo Rojo AA 15" = NOMBRE del producto
- "11.450" = PRECIO FINAL

**⚠️ REGLA CRÍTICA:** El PRIMER número de la línea 1 es el NÚMERO DE LÍNEA, NO la cantidad.

## FORMATO B - OLÍMPICA

```
1393170 ARROZ DODAPEPA 3KG    *
02 un      16.650    1    16.650
1393170 AHORRO (R)DONAPEPA    3.350-
    PRECIO FINAL              13.300
```

- PLU + Nombre en primera línea
- Precio y cantidad en segunda línea
- Si hay AHORRO, usar PRECIO FINAL

## FORMATO C - FARMATODO

```
PROTECTOR CAREFREE SIN FRAGANCIA LARGOS X40UN
101047110    2    7.650    15.300
```

- NOMBRE arriba
- Código + Cantidad + Precio Unit + Total abajo

## FORMATO D - SIN CÓDIGO

```
1    QUESO PERA ANDES    3200    3.200
```

- Cantidad + Nombre + Precio Unit + Total
- NO hay código PLU

# ⚠️ REGLAS ABSOLUTAS

1. **CADA LÍNEA DE PRODUCTO = UN ÍTEM SEPARADO**
   Si el mismo PLU aparece 2 veces en la factura, son 2 ítems separados.

2. **PLUs DIFERENTES = PRODUCTOS DIFERENTES**

3. **LEER EXACTAMENTE LO QUE DICE**

4. **VALIDAR CON EL TOTAL**

5. **🚫 IGNORAR COMPLETAMENTE ESTAS LÍNEAS (NO SON PRODUCTOS):**
   - Métodos de pago: "TARJETA", "CREDITO", "DEBITO", "REDEBAN", "MASTERCARD", "VISA"
   - Transacciones: "APROBADO", "AUTORIZADO", "VOUCHER"
   - Apps de pago: "PSE", "NEQUI", "DAVIPLATA", "BANCOLOMBIA"
   - Totales: "SUBTOTAL", "TOTAL", "IVA", "PROPINA", "CAMBIO"

# 🆕 POSICIÓN VERTICAL DE CADA PRODUCTO

**MUY IMPORTANTE:** Para cada producto, debes indicar su posición vertical en la imagen como un porcentaje (0-100):
- 0% = parte superior de la imagen
- 50% = mitad de la imagen
- 100% = parte inferior de la imagen

Esto permite ubicar rápidamente el producto en la factura original.

**Cómo calcular:**
- Si el producto está en el primer tercio de la factura → 10-33%
- Si está en el segundo tercio → 34-66%
- Si está en el tercio inferior → 67-90%
- El encabezado suele estar en 0-15%
- Los totales suelen estar en 85-100%

# 📝 FORMATO DE RESPUESTA

IMPORTANTE: Responde SOLO con JSON válido, sin markdown ni explicaciones.

{"{"}
  "establecimiento": {establecimiento_info},
  "fecha": "YYYY-MM-DD",
  "total": TOTAL_FACTURA_ENTERO,
  "productos": [
    {"{"}
      "codigo": "PLU_O_EAN",
      "nombre": "NOMBRE_EXACTO_DEL_PRODUCTO",
      "precio": PRECIO_UNITARIO_ENTERO,
      "cantidad": CANTIDAD_DECIMAL,
      "unidad": "un",
      "posicion_vertical": PORCENTAJE_0_A_100
    {"}"}
  ]
{"}"}

# 🎯 EJEMPLO COMPLETO DE EXTRACCIÓN

**FACTURA ÉXITO (imagen de 1000px de alto):**
```
[0-100px: Logo y encabezado]
[100-150px: PLU    DETALLE    PRECIO]
[150-200px: 1413568 Huevo Rojo AA 15     11.450]
[200-250px: 3266709 Bizcochos De Sol     11.050]
[250-300px: 3313023 Crema Leche Semi     10.750]
[700-800px: SUBTOTAL: 33.250]
[800-900px: MASTERCARD / REDEBAN]
```

**EXTRACCIÓN CORRECTA:**
{"{"}
  "establecimiento": "EXITO",
  "fecha": "2025-10-03",
  "total": 33250,
  "productos": [
    {{"codigo": "1413568", "nombre": "HUEVO ROJO AA 15", "precio": 11450, "cantidad": 1, "unidad": "un", "posicion_vertical": 18}},
    {{"codigo": "3266709", "nombre": "BIZCOCHOS DE SOL", "precio": 11050, "cantidad": 1, "unidad": "un", "posicion_vertical": 23}},
    {{"codigo": "3313023", "nombre": "CREMA LECHE SEMI", "precio": 10750, "cantidad": 1, "unidad": "un", "posicion_vertical": 28}}
  ]
{"}"}

**Nota:** Los productos están entre 15-30% porque están en la parte superior de la factura, después del encabezado.

# ✅ VERIFICACIÓN FINAL

Antes de responder:
1. ¿Identifiqué correctamente el formato? ✓
2. ¿Leí cada PLU exactamente como aparece? ✓
3. ¿Asigné una posición vertical aproximada a cada producto? ✓
4. ¿Eliminé TODA información de medios de pago? ✓
5. ¿La suma de precios ≈ total de la factura? ✓

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON VÁLIDO:**"""

        print("📸 Enviando imagen a Claude Vision API...")

        message = client.messages.create(
            model="claude-sonnet-4-20250514",
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

        if establecimiento_preseleccionado:
            data["establecimiento"] = establecimiento_preseleccionado.upper()

        # Validar fecha
        if "fecha" in data and data["fecha"]:
            try:
                fecha_str = str(data["fecha"])
                if len(fecha_str) >= 4:
                    año = int(fecha_str[:4])
                    if año < 2020:
                        año_actual = datetime.now().year
                        fecha_corregida = fecha_str.replace(
                            str(año), str(año_actual), 1
                        )
                        print(f"   ⚠️  Año corregido: {año} → {año_actual}")
                        data["fecha"] = fecha_corregida
            except:
                pass

        # ========== POST-PROCESAMIENTO ==========
        productos_finales = []
        suma_total = 0

        print(f"\n🔧 POST-PROCESAMIENTO (lectura exacta + filtrado):")

        for prod in data.get("productos", []):
            codigo = str(prod.get("codigo", "")).strip()
            nombre = str(prod.get("nombre", "")).strip()
            precio = prod.get("precio", 0)
            cantidad = float(prod.get("cantidad", 1))
            unidad = prod.get("unidad", "un")
            posicion_vertical = prod.get("posicion_vertical", 50)  # Default: mitad

            # Validar posición vertical
            try:
                posicion_vertical = int(posicion_vertical)
                posicion_vertical = max(0, min(100, posicion_vertical))
            except:
                posicion_vertical = 50

            # Filtrar basura
            es_basura, razon = es_texto_basura(nombre)
            if es_basura:
                print(f"   🗑️  Ignorado: '{nombre[:40]}' - {razon}")
                continue

            # Corregir errores OCR
            nombre_corregido = corregir_nombre_producto(nombre)
            nombre_final = normalizar_nombre_producto(nombre_corregido)

            if nombre_final != nombre.upper():
                print(f"   📝 Corregido: '{nombre[:30]}' → '{nombre_final[:30]}'")

            # Limpiar precio
            precio_limpio = limpiar_precio_colombiano(precio)

            # Validar precio
            if precio_limpio < 100:
                print(f"   ⚠️  Precio muy bajo: '{nombre_final}' - ${precio_limpio}")
                continue

            if precio_limpio > 10000000:
                print(f"   ⚠️  Precio muy alto: '{nombre_final}' - ${precio_limpio:,}")
                continue

            # Calcular subtotal
            subtotal = int(precio_limpio * cantidad)
            suma_total += subtotal

            # Agregar producto CON posición vertical
            productos_finales.append(
                {
                    "codigo": codigo,
                    "nombre": nombre_final,
                    "precio": precio_limpio,
                    "cantidad": cantidad,
                    "unidad": unidad,
                    "nombre_ocr_original": nombre,
                    "posicion_vertical": posicion_vertical,  # 🆕 V6.2
                }
            )

        data["productos"] = productos_finales

        # ========== VALIDACIÓN DE TOTAL ==========
        total_declarado = data.get("total", 0)

        print(f"\n🔍 VALIDACIÓN:")
        print(f"   Total declarado: ${total_declarado:,}")
        print(f"   Suma calculada: ${suma_total:,}")

        if total_declarado > 0:
            diferencia = abs(suma_total - total_declarado)
            diferencia_pct = diferencia / total_declarado * 100
            print(f"   Diferencia: ${diferencia:,} ({diferencia_pct:.1f}%)")

            if diferencia_pct > 10:
                print(f"   ⚠️  ALERTA: Diferencia mayor al 10%, revisar extracción")
            else:
                print(f"   ✅ Validación correcta")

        # ========== ESTADÍSTICAS ==========
        con_codigo = sum(1 for p in productos_finales if p.get("codigo"))
        sin_codigo = sum(1 for p in productos_finales if not p.get("codigo"))
        plus_unicos = set(p.get("codigo") for p in productos_finales if p.get("codigo"))

        print(f"\n" + "=" * 80)
        print(f"📊 RESULTADOS OCR V6.2 - CON POSICIÓN VERTICAL:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   📅 Fecha: {data.get('fecha', 'N/A')}")
        print(f"   💰 Total factura: ${total_declarado:,}")
        print(f"   📦 Ítems totales: {len(productos_finales)}")
        print(f"   🏷️  PLUs únicos: {len(plus_unicos)}")

        print(f"\n📋 PRODUCTOS EXTRAÍDOS (con posición):")
        for i, prod in enumerate(productos_finales, 1):
            codigo_str = prod["codigo"] if prod["codigo"] else "SIN-COD"
            pos = prod.get("posicion_vertical", "?")
            print(
                f"   {i:2}. [{pos:3}%] PLU:{codigo_str:10} | {prod['nombre'][:30]:30} | ${prod['precio']:,}"
            )

        print("=" * 80)

        # Capturar tokens
        tokens_input = message.usage.input_tokens
        tokens_output = message.usage.output_tokens

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-v6.2-con-posicion",
                    "modelo": "claude-sonnet-4-20250514",
                    "establecimiento_confirmado": bool(establecimiento_preseleccionado),
                    "items_totales": len(productos_finales),
                    "plus_unicos": len(plus_unicos),
                    "sin_codigo": sin_codigo,
                    "suma_calculada": suma_total,
                    "total_declarado": total_declarado,
                },
            },
            "usage": {
                "input_tokens": tokens_input,
                "output_tokens": tokens_output,
                "total_tokens": tokens_input + tokens_output,
                "modelo": "claude-sonnet-4-20250514",
            },
        }

    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        print(
            f"Respuesta: {response_text[:500] if 'response_text' in locals() else 'N/A'}"
        )
        usage_data = {}
        if "message" in locals() and hasattr(message, "usage"):
            usage_data = {
                "input_tokens": message.usage.input_tokens,
                "output_tokens": message.usage.output_tokens,
                "total_tokens": message.usage.input_tokens
                + message.usage.output_tokens,
                "modelo": "claude-sonnet-4-20250514",
            }
        else:
            usage_data = {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "modelo": "claude-sonnet-4-20250514",
            }

        return {
            "success": False,
            "error": "Error parseando respuesta JSON",
            "usage": usage_data,
        }

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

        usage_data = {}
        if "message" in locals() and hasattr(message, "usage"):
            usage_data = {
                "input_tokens": message.usage.input_tokens,
                "output_tokens": message.usage.output_tokens,
                "total_tokens": message.usage.input_tokens
                + message.usage.output_tokens,
                "modelo": "claude-sonnet-4-20250514",
            }
        else:
            usage_data = {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "modelo": "claude-sonnet-4-20250514",
            }

        return {"success": False, "error": f"Error: {str(e)}", "usage": usage_data}


print("=" * 80)
print("✅ claude_invoice.py V6.2 - CON POSICIÓN VERTICAL DE PRODUCTOS")
print("=" * 80)
print("🎯 CARACTERÍSTICAS:")
print("   ✅ Detecta formato automáticamente")
print("   ✅ Filtra medios de pago (REDEBAN, VISA, etc.)")
print("   ✅ 🆕 Devuelve posición vertical (0-100%) de cada producto")
print("   ✅ Permite ubicar rápidamente un ítem en la factura")
print("=" * 80)
