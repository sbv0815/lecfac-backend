"""
claude_invoice.py - VERSIÓN 5.0 - UNIVERSAL PARA TODOS LOS FORMATOS
========================================================================

🎯 VERSIÓN 5.0 - SOPORTE MULTI-FORMATO:
- ✅ OLÍMPICA: PLU + Nombre en línea, descuentos separados
- ✅ ÉXITO/CARULLA: Formato línea doble (info + producto)
- ✅ FARMATODO: Código arriba, nombre abajo
- ✅ SUPERMERCADOS PEQUEÑOS: Sin PLU, solo cantidad + nombre + precio
- ✅ Agrupación inteligente por PLU
- ✅ Detección automática de formato
- ✅ Correcciones OCR mejoradas
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
    # Errores comunes
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
    # Doña Pepa y similares
    "DODAPEPA": "DONA PEPA",
    "DONAPEPA": "DONA PEPA",
    "DONAPAPA": "DONA PEPA",
    "DOBRAPEPA": "DONA PEPA",
    "DODA": "DONA",
    "DODO": "DONA",
    # Carnes y otros
    "MOL": "MOLIDA",
    "ESP": "ESPECIAL",
    "CONTRAMUSLO": "CONTRAMUSLO",
    "MOZARELLA": "MOZZARELLA",
    "PERA": "PERA",
    # Frutas
    "PLATANO VERDE SE": "PLATANO VERDE SELECCION",
    "PLATANO MADURO S": "PLATANO MADURO SELECCION",
    "TOMATE CHONTO SE": "TOMATE CHONTO SELECCION",
    "LIMON TAHITI A G": "LIMON TAHITI A GRANEL",
    "CEBOLLA BLANCA B": "CEBOLLA BLANCA",
    # Limpieza
    "BSA": "BOLSA",
    "P/EMPACAR": "PARA EMPACAR",
}


def corregir_nombre_producto(nombre: str) -> str:
    """Corrige errores OCR palabra por palabra"""
    if not nombre:
        return nombre

    nombre_upper = nombre.upper()

    # Primero intentar corrección de frase completa
    for error, correccion in CORRECCIONES_OCR.items():
        if error in nombre_upper:
            nombre_upper = nombre_upper.replace(error, correccion)

    # Luego palabra por palabra
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
    precio_str = precio_str.rstrip("AaEeDd")  # Quitar sufijos de Éxito/Carulla

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
# PROCESAMIENTO CON CLAUDE VISION - V5.0 UNIVERSAL
# ==============================================================================


def parse_invoice_with_claude(
    image_path: str,
    establecimiento_preseleccionado: str = None,
    aplicar_aprendizaje: bool = True,
) -> Dict:
    """
    Procesa factura con Claude Vision API - V5.0 UNIVERSAL
    Soporta múltiples formatos de facturas colombianas
    """
    try:
        print("=" * 80)
        print("🤖 CLAUDE INVOICE V5.0 - UNIVERSAL MULTI-FORMATO")
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

        # ========== PROMPT V5.0 UNIVERSAL ==========

        establecimiento_info = (
            f'"{establecimiento_preseleccionado.upper()}"'
            if establecimiento_preseleccionado
            else '"NOMBRE_DEL_ESTABLECIMIENTO"'
        )

        prompt = f"""Eres un experto en extracción de datos de facturas colombianas. Tu trabajo es identificar CADA producto comprado con su código (si existe), nombre COMPLETO y precio FINAL.

# 🎯 MISIÓN CRÍTICA

Extraer TODOS los productos de la factura. Cada producto debe tener:
1. **Código** (PLU o EAN) - A la IZQUIERDA (puede no existir)
2. **Nombre COMPLETO** - En el CENTRO
3. **Precio FINAL** - A la DERECHA (después de descuentos)
4. **Cantidad** - Puede ser unidades (1, 2, 3...) o peso en kg (0.680, 1.515...)

# 📋 FORMATOS DE FACTURAS COLOMBIANAS

## FORMATO 1: OLÍMPICA
```
1393170 ARROZ DODAPEPA 3KG            *
02 un      16.650    1         16.650
1393170 AHORRO (R)DONAPEPA         3.350-
    PRECIO FINAL                  13.300
```
- PLU: 1393170
- Nombre: ARROZ DONA PEPA 3KG (corregir DODAPEPA)
- Precio FINAL: 13,300 (NO 16,650)
- Cantidad: 1

**REGLA OLÍMPICA:** Si hay "AHORRO" y "PRECIO FINAL", usar el PRECIO FINAL.

## FORMATO 2: ÉXITO / CARULLA
```
13 1/u x 27.800 V.Ahorro 5.560
187687  MINI LYNE                      22.240A
```
- Primera línea: información (cantidad, precio original, ahorro)
- Segunda línea: PLU + Nombre + Precio FINAL
- PLU: 187687
- Nombre: MINI LYNE
- Precio: 22,240 (ignorar la "A" al final)
- Cantidad: 1
```
18 0.460/KGM x 7.980 V.Ahorro 734
1234    Pera                           2.937
```
- Cantidad: 0.460 kg
- Precio total: 2,937

## FORMATO 3: FARMATODO
```
101047110    2        7.650      15.300
GOMA DE MASCAR TRIDENT FRESH HERBAL...
```
- Primera línea: Código + Cantidad + Precio Unit + Total
- Segunda línea: Nombre del producto
- Código: 101047110
- Nombre: GOMA DE MASCAR TRIDENT FRESH HERBAL
- Precio unitario: 7,650
- Cantidad: 2

## FORMATO 4: SUPERMERCADOS PEQUEÑOS (Sin código)
```
1    QUESO PERA ANDES A    3200    3.200
1    HUEVO SANTA REYES     24950   24.950
```
- Cantidad + Nombre + Precio unitario + Total
- NO tienen código PLU/EAN
- Código: "" (vacío)
- Nombre: QUESO PERA ANDES
- Precio: 3,200

# ⚠️ REGLAS CRÍTICAS

1. **CÓDIGO SIEMPRE A LA IZQUIERDA**
   - Si hay número de 4-7 dígitos al inicio = PLU
   - Si hay número de 8-13 dígitos = EAN
   - Si no hay código = dejar vacío ""

2. **PRECIO SIEMPRE A LA DERECHA**
   - Usar el número de la DERECHA
   - Ignorar sufijos como "A", "E", "D" (son códigos internos)
   - Si hay descuento, usar PRECIO FINAL (el menor)

3. **NOMBRE EN EL CENTRO**
   - Capturar nombre COMPLETO del producto
   - Incluir marca, tipo, presentación
   - Corregir errores OCR obvios:
     * DODAPEPA → DONA PEPA
     * BANAN → BANANO
     * QSO → QUESO
     * GRL → GRANEL

4. **AGRUPAR POR CÓDIGO**
   - Si el mismo PLU aparece múltiples veces, SUMAR cantidades
   - Cada código único = UN producto en la lista

5. **IGNORAR ESTAS LÍNEAS:**
   - "AHORRO", "DESCUENTO", "V.Ahorro" (solo info)
   - "PRECIO FINAL" (texto, no producto)
   - "SUBTOTAL", "TOTAL", "IVA"
   - "DOMICILIO WEB", "DISPLAY"
   - "BOLSA", "BSA P/EMPACAR" (materiales)

# 📝 FORMATO DE RESPUESTA

Responde SOLO con JSON válido (sin markdown):

{"{"}
  "establecimiento": {establecimiento_info},
  "fecha": "YYYY-MM-DD",
  "total": TOTAL_FACTURA_ENTERO,
  "productos": [
    {"{"}
      "codigo": "PLU_O_EAN_O_VACIO",
      "nombre": "NOMBRE_COMPLETO_CORREGIDO",
      "precio": PRECIO_UNITARIO_FINAL_ENTERO,
      "cantidad": CANTIDAD_DECIMAL,
      "unidad": "un" | "kg"
    {"}"}
  ]
{"}"}

# 🎯 EJEMPLOS DE EXTRACCIÓN CORRECTA

**OLÍMPICA:**
- 1393170 ARROZ DODAPEPA 3KG, Precio Final 13,300, Cant 2
  → {{"codigo": "1393170", "nombre": "ARROZ DONA PEPA 3KG", "precio": 13300, "cantidad": 2, "unidad": "un"}}

**ÉXITO:**
- 3323923 Brownie Mini Ark, 14.800A
  → {{"codigo": "3323923", "nombre": "BROWNIE MINI ARK", "precio": 14800, "cantidad": 1, "unidad": "un"}}

- 1220 Mango, 0.750 kg, 5.280
  → {{"codigo": "1220", "nombre": "MANGO", "precio": 7040, "cantidad": 0.750, "unidad": "kg"}}

**FARMATODO:**
- 101047110, GOMA DE MASCAR TRIDENT, 7.650 x 2 = 15.300
  → {{"codigo": "101047110", "nombre": "GOMA DE MASCAR TRIDENT FRESH HERBAL", "precio": 7650, "cantidad": 2, "unidad": "un"}}

**SIN CÓDIGO:**
- QUESO PERA ANDES A, 3200
  → {{"codigo": "", "nombre": "QUESO PERA ANDES", "precio": 3200, "cantidad": 1, "unidad": "un"}}

**ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON VÁLIDO:**"""

        # Llamada a Claude
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

        # Forzar establecimiento si fue preseleccionado
        if establecimiento_preseleccionado:
            data["establecimiento"] = establecimiento_preseleccionado.upper()

        # Validar y corregir fecha
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
        productos_por_codigo = {}

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

            # Validar precio mínimo
            if precio_limpio < 100:
                print(f"   ⚠️  Precio bajo: '{nombre_final}' - ${precio_limpio}")
                continue

            # Agrupar por código si existe
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

        # ========== ESTADÍSTICAS ==========
        con_ean = sum(1 for p in productos_finales if len(p.get("codigo", "")) >= 8)
        con_plu = sum(1 for p in productos_finales if 3 <= len(p.get("codigo", "")) < 8)
        sin_codigo = sum(1 for p in productos_finales if not p.get("codigo"))

        print(f"\n" + "=" * 80)
        print(f"📊 RESULTADOS OCR V5.0 UNIVERSAL:")
        print(f"   🏪 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"   📅 Fecha: {data.get('fecha', 'N/A')}")
        print(f"   💰 Total factura: ${data.get('total', 0):,}")
        print(f"   📦 Productos únicos: {len(productos_finales)}")
        print(f"\n📊 CÓDIGOS DETECTADOS:")
        print(f"   📦 EAN (8+ dígitos): {con_ean}")
        print(f"   🏷️  PLU (3-7 dígitos): {con_plu}")
        print(f"   ❓ Sin código: {sin_codigo}")

        print(f"\n📋 PRODUCTOS EXTRAÍDOS:")
        for i, prod in enumerate(productos_finales, 1):
            codigo_str = prod["codigo"] if prod["codigo"] else "SIN-COD"
            print(
                f"   {i:2}. {codigo_str:10} | {prod['nombre'][:35]:35} | ${prod['precio']:,} x {prod['cantidad']}"
            )

        print("=" * 80)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-v5.0-universal",
                    "modelo": "claude-sonnet-4-20250514",
                    "establecimiento_confirmado": bool(establecimiento_preseleccionado),
                    "productos_unicos": len(productos_finales),
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
        return {"success": False, "error": "Error parseando respuesta JSON"}
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": f"Error: {str(e)}"}


# ==============================================================================
# INICIALIZACIÓN
# ==============================================================================
print("=" * 80)
print("✅ claude_invoice.py V5.0 UNIVERSAL CARGADO")
print("=" * 80)
print("🎯 FORMATOS SOPORTADOS:")
print("   ✅ OLÍMPICA: PLU + descuentos separados")
print("   ✅ ÉXITO/CARULLA: Línea doble con V.Ahorro")
print("   ✅ FARMATODO: Código arriba, nombre abajo")
print("   ✅ SUPERMERCADOS PEQUEÑOS: Sin código")
print("   ✅ ARA/D1/JUMBO: Códigos EAN-13")
print("=" * 80)
