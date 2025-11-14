"""
claude_invoice.py - VERSIÓN 5.1 - ANTI-INVENCIÓN + MULTI-FORMATO
========================================================================

🎯 VERSIÓN 5.1 - CORRECCIONES CRÍTICAS:
- ✅ Reglas estrictas para NO inventar productos
- ✅ Validación de cantidad x precio = total
- ✅ Mejor manejo de formato FARMATODO
- ✅ Límite de productos por factura
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
    "domicilio",
    "domicilio web",
    "display",
    "exhibicion",
    "bolsa",
    "empacar",
    "empaque",
    "bsa p empacar",
    "bsa p/empacar",
    "biodegradable",  # ← Nuevo
]


def es_texto_basura(nombre: str) -> Tuple[bool, str]:
    """Detecta si un texto es basura promocional"""
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
    "P/EMPACAR": "PARA EMPACAR",
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
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    return nombre_raw.strip().upper()[:50]


# ==============================================================================
# PROCESAMIENTO CON CLAUDE VISION - V5.1
# ==============================================================================


def parse_invoice_with_claude(
    image_path: str,
    establecimiento_preseleccionado: str = None,
    aplicar_aprendizaje: bool = True,
) -> Dict:
    """
    Procesa factura con Claude Vision API - V5.1
    Con reglas estrictas anti-invención
    """
    try:
        print("=" * 80)
        print("🤖 CLAUDE INVOICE V5.1 - ANTI-INVENCIÓN + MULTI-FORMATO")
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

        # ========== PROMPT V5.1 CON REGLAS ANTI-INVENCIÓN ==========
        prompt = f"""Eres un experto en extracción de datos de facturas colombianas.

# ⚠️ REGLA #1: NO INVENTAR PRODUCTOS

**CRÍTICO:** Solo extraer productos que CLARAMENTE aparecen en la imagen.
- Si no puedes leer claramente un producto, NO LO INCLUYAS
- Si hay duda sobre el nombre, código o precio, OMÍTELO
- NUNCA crear variantes o fragmentos de productos
- Cada producto físico = UNA SOLA entrada en la lista

**VALIDACIÓN OBLIGATORIA:**
- Suma de (precio × cantidad) debe aproximarse al TOTAL de la factura
- Si la suma difiere más del 20% del total, REVISAR extracción
- Máximo 50 productos por factura típica

# 🎯 MISIÓN

Extraer CADA producto REAL con:
1. **Código** (PLU/EAN) - Número a la IZQUIERDA
2. **Nombre COMPLETO** - Texto descriptivo
3. **Precio UNITARIO** - Precio por 1 unidad
4. **Cantidad** - Cuántas unidades o kg

# 📋 FORMATOS DE FACTURAS

## OLÍMPICA
```
1393170 ARROZ DODAPEPA 3KG            *
02 un      16.650    1         16.650
1393170 AHORRO (R)DONAPEPA         3.350-
    PRECIO FINAL                  13.300
```
Extraer: PLU=1393170, Nombre=ARROZ DONA PEPA 3KG, Precio=13300, Cant=1
**NOTA:** Si el mismo PLU aparece 2 veces = 2 unidades del MISMO producto

## ÉXITO / CARULLA
```
13 1/u x 27.800 V.Ahorro 5.560
187687  MINI LYNE                      22.240A
```
Extraer: PLU=187687, Nombre=MINI LYNE, Precio=22240, Cant=1

## FARMATODO (⚠️ FORMATO ESPECIAL)
```
PROTECTOR CAREFREE SIN FRAGANCIA LARGOS X40UN
101047110    2        7.650      15.300
```
- Línea 1: NOMBRE del producto
- Línea 2: Código + Cantidad + Precio Unit + Total
Extraer: Código=101047110, Nombre=PROTECTOR CAREFREE..., Precio=7650, Cant=2

**IMPORTANTE FARMATODO:**
- El NOMBRE está ARRIBA del código
- El código tiene 9 dígitos típicamente
- "Ahorro" es línea separada, IGNORAR

## SIN CÓDIGO
```
1    QUESO PERA ANDES    3200    3.200
```
Extraer: Código="", Nombre=QUESO PERA ANDES, Precio=3200, Cant=1

# ❌ IGNORAR ESTAS LÍNEAS

- "Ahorro" (solo descuento)
- "Bolsa Biodegradable" (empaque)
- "SUBTOTAL", "TOTAL", "IVA"
- Líneas con solo números
- Fragmentos de texto sin sentido

# ✅ CORRECCIONES OBLIGATORIAS

- DODAPEPA → DONA PEPA
- BANAN → BANANO
- QSO → QUESO
- GRL → GRANEL

# 📝 FORMATO DE RESPUESTA

SOLO JSON válido, sin markdown:

{"{"}
  "establecimiento": {establecimiento_info},
  "fecha": "YYYY-MM-DD",
  "total": TOTAL_FACTURA,
  "productos": [
    {"{"}
      "codigo": "CODIGO_O_VACIO",
      "nombre": "NOMBRE_COMPLETO",
      "precio": PRECIO_UNITARIO,
      "cantidad": CANTIDAD,
      "unidad": "un"
    {"}"}
  ]
{"}"}

# 🔍 VERIFICACIÓN FINAL

Antes de responder, verifica:
1. ¿Cada producto tiene nombre claro y legible? ✓
2. ¿Los precios son razonables (500-500,000 pesos)? ✓
3. ¿La suma aproxima al total de la factura? ✓
4. ¿No hay productos duplicados o inventados? ✓

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON:**"""

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

        # ========== POST-PROCESAMIENTO CON VALIDACIÓN ==========
        productos_finales = []
        productos_por_codigo = {}
        suma_total = 0

        print(f"\n🔧 POST-PROCESAMIENTO:")

        for prod in data.get("productos", []):
            codigo = str(prod.get("codigo", "")).strip()
            nombre = str(prod.get("nombre", "")).strip()
            precio = prod.get("precio", 0)
            cantidad = float(prod.get("cantidad", 1))
            unidad = prod.get("unidad", "un")

            # Filtrar basura
            es_basura, razon = es_texto_basura(nombre)
            if es_basura:
                print(f"   🗑️  Ignorado: '{nombre[:40]}' - {razon}")
                continue

            # Corregir nombre
            nombre_corregido = corregir_nombre_producto(nombre)
            nombre_final = normalizar_nombre_producto(nombre_corregido)

            if nombre_final != nombre:
                print(f"   📝 Corregido: '{nombre[:30]}' → '{nombre_final[:30]}'")

            # Limpiar precio
            precio_limpio = limpiar_precio_colombiano(precio)

            # Validar precio mínimo y máximo
            if precio_limpio < 100:
                print(f"   ⚠️  Precio muy bajo: '{nombre_final}' - ${precio_limpio}")
                continue

            if precio_limpio > 10000000:  # 10 millones
                print(f"   ⚠️  Precio muy alto: '{nombre_final}' - ${precio_limpio:,}")
                continue

            # Calcular subtotal
            subtotal = int(precio_limpio * cantidad)
            suma_total += subtotal

            # Agrupar por código
            if codigo and len(codigo) >= 3:
                if codigo in productos_por_codigo:
                    productos_por_codigo[codigo]["cantidad"] += cantidad
                    print(
                        f"   📦 Agrupado {codigo}: +{cantidad} = {productos_por_codigo[codigo]['cantidad']}"
                    )
                else:
                    productos_por_codigo[codigo] = {
                        "codigo": codigo,
                        "nombre": nombre_final,
                        "precio": precio_limpio,
                        "cantidad": cantidad,
                        "unidad": unidad,
                        "nombre_ocr_original": nombre,
                    }
            else:
                productos_finales.append(
                    {
                        "codigo": "",
                        "nombre": nombre_final,
                        "precio": precio_limpio,
                        "cantidad": cantidad,
                        "unidad": unidad,
                        "nombre_ocr_original": nombre,
                    }
                )

        # Agregar productos agrupados
        for codigo, prod_data in productos_por_codigo.items():
            productos_finales.append(prod_data)

        data["productos"] = productos_finales

        # ========== VALIDACIÓN DE TOTAL ==========
        total_declarado = data.get("total", 0)

        print(f"\n🔍 VALIDACIÓN:")
        print(f"   Total declarado: ${total_declarado:,}")
        print(f"   Suma calculada: ${suma_total:,}")

        if total_declarado > 0:
            diferencia_pct = abs(suma_total - total_declarado) / total_declarado * 100
            print(f"   Diferencia: {diferencia_pct:.1f}%")

            if diferencia_pct > 50:
                print(
                    f"   ⚠️  ALERTA: Diferencia muy grande, posible error en extracción"
                )

        # ========== ESTADÍSTICAS ==========
        con_ean = sum(1 for p in productos_finales if len(p.get("codigo", "")) >= 8)
        con_plu = sum(1 for p in productos_finales if 3 <= len(p.get("codigo", "")) < 8)
        sin_codigo = sum(1 for p in productos_finales if not p.get("codigo"))

        print(f"\n" + "=" * 80)
        print(f"📊 RESULTADOS OCR V5.1:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   📅 Fecha: {data.get('fecha', 'N/A')}")
        print(f"   💰 Total factura: ${total_declarado:,}")
        print(f"   📦 Productos únicos: {len(productos_finales)}")
        print(f"\n📊 CÓDIGOS:")
        print(f"   📦 EAN (8+ dígitos): {con_ean}")
        print(f"   🏷️  PLU (3-7 dígitos): {con_plu}")
        print(f"   ❓ Sin código: {sin_codigo}")

        print(f"\n📋 PRODUCTOS EXTRAÍDOS:")
        for i, prod in enumerate(productos_finales, 1):
            codigo_str = prod["codigo"] if prod["codigo"] else "SIN-COD"
            subtotal = int(prod["precio"] * prod["cantidad"])
            print(
                f"   {i:2}. {codigo_str:12} | {prod['nombre'][:30]:30} | ${prod['precio']:,} x {prod['cantidad']} = ${subtotal:,}"
            )

        print("=" * 80)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-v5.1-antiinvencion",
                    "modelo": "claude-sonnet-4-20250514",
                    "establecimiento_confirmado": bool(establecimiento_preseleccionado),
                    "productos_unicos": len(productos_finales),
                    "con_ean": con_ean,
                    "con_plu": con_plu,
                    "sin_codigo": sin_codigo,
                    "suma_calculada": suma_total,
                    "total_declarado": total_declarado,
                },
            },
        }

    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        print(
            f"Respuesta: {response_text[:500] if 'response_text' in locals() else 'N/A'}"
        )
        return {"success": False, "error": "Error parseando respuesta JSON"}
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": f"Error: {str(e)}"}


print("=" * 80)
print("✅ claude_invoice.py V5.1 - ANTI-INVENCIÓN + MULTI-FORMATO")
print("=" * 80)
