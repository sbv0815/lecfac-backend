import anthropic
import base64
import os
import json
from typing import Dict


def parse_invoice_with_claude(image_path: str) -> Dict:
    """Procesa factura con Claude Vision API - OPTIMIZADO PARA FACTURAS LARGAS"""
    try:
        print("=" * 70)
        print("🤖 PROCESANDO CON CLAUDE HAIKU 3.5")
        print("=" * 70)

        # Leer imagen
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")

        # Tipo MIME
        media_type = (
            "image/png" if image_path.lower().endswith(".png") else "image/jpeg"
        )

        # Cliente Anthropic
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            raise ValueError("ANTHROPIC_API_KEY no configurada")

        client = anthropic.Anthropic(api_key=api_key)

        # ========== PROMPT MEJORADO PARA FACTURAS LARGAS ==========
        prompt = """Eres un experto en análisis de facturas de supermercados colombianos.

🎯 CONTEXTO IMPORTANTE:
Esta imagen puede ser UNA de VARIAS imágenes de la MISMA factura.
Las facturas largas se capturan en múltiples fotos secuenciales.

# OBJETIVO
Extraer: establecimiento, fecha, total y CADA producto REAL con su código.

# ⚠️ CRÍTICO: BUSCAR EL TOTAL
El TOTAL suele estar en la ÚLTIMA imagen de la secuencia.
Busca palabras como: TOTAL, GRAN TOTAL, TOTAL A PAGAR, VALOR TOTAL
El total es el número MÁS GRANDE cerca del final de la factura.

# IMPORTANTE: DETECTAR DESCUENTOS
⚠️ NO incluyas líneas de descuentos. Los descuentos tienen palabras:
AHORRO, DESCUENTO, DESC, DTO, REBAJA, PROMOCION, PROMO, OFERTA, %, 2X1, 3X2

EJEMPLOS DE DESCUENTOS (NO incluir):
✗ "14476 AHORRO 20%" → DESCUENTO
✗ "625 DESC ESPECIAL" → DESCUENTO
✗ "750 PROMO 2X1" → DESCUENTO

EJEMPLOS DE PRODUCTOS REALES (SÍ incluir):
✓ "14476 LIMON TAHITI" → PRODUCTO
✓ "625 ZANAHORIA GRL" → PRODUCTO
✓ "7702993047842 LECHE ALPINA" → PRODUCTO

# ESTABLECIMIENTOS
Si contiene: JUMBO, ÉXITO, CARULLA, OLÍMPICA, ARA, D1, ALKOSTO, etc.
Usa SOLO el nombre principal: "JUMBO BULEVAR" → "JUMBO"

# CÓDIGOS DE PRODUCTOS
Códigos están a la IZQUIERDA del nombre.

VÁLIDOS (solo dígitos, 1-13 caracteres):
✓ "116 BANANO" → codigo: "116"
✓ "7702993047842 LECHE" → codigo: "7702993047842"
✓ "09 LIMON" → codigo: "09"

INVÁLIDOS (tienen letras):
✗ "343718DF PRODUCTO" → codigo: ""
✗ "REF123 PRODUCTO" → codigo: ""

# FORMATO JSON (sin comas en precios)
{
  "establecimiento": "JUMBO",
  "fecha": "2024-12-27",
  "total": 234890,
  "productos": [
    {
      "codigo": "7702993047842",
      "nombre": "CHOCOLATE BT",
      "cantidad": 1,
      "precio": 2190
    },
    {
      "codigo": "116",
      "nombre": "BANANO URABA",
      "cantidad": 0.878,
      "precio": 5425
    }
  ]
}

🔍 VALIDACIÓN IMPORTANTE:
Después de extraer productos, VERIFICA:
- La SUMA de (precio × cantidad) de todos los productos debe ser CERCANA al total
- Si hay gran diferencia, puede que falten productos
- Busca si hay productos en la parte INFERIOR de la imagen

REGLAS:
- JSON válido sin errores
- Precios SIN separadores: 2190 (no 2,190)
- Códigos como strings: "116", "09" o ""
- NO incluyas descuentos, IVA, subtotales
- Acepta códigos de 1-13 dígitos
- Lee TODOS los productos visibles, incluso los del final

⚠️ SI ESTA ES UNA IMAGEN PARCIAL:
- Extrae todos los productos visibles
- Si no ves el total, ponlo en 0
- Si no ves el establecimiento, pon "Desconocido"

ANALIZA Y RESPONDE SOLO CON JSON:"""

        # ✅ Llamada con HAIKU 3.5 (más rápido y barato)
        message = client.messages.create(
            model="claude-3-5-haiku-20241022",  # ✅ MODELO CORRECTO
            max_tokens=8000,  # Suficiente para facturas
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

        # Parsear respuesta
        response_text = message.content[0].text
        print(f"📄 Respuesta (primeros 200 chars): {response_text[:200]}...")

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

        # Parsear JSON
        data = json.loads(json_str)

        # ========== FILTRADO DE DESCUENTOS ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])

            palabras_descuento = [
                "ahorro",
                "descuento",
                "desc",
                "dto",
                "rebaja",
                "promocion",
                "promo",
                "oferta",
                "2x1",
                "3x2",
                "dcto",
                "descu",
                "desc%",
            ]

            productos_filtrados = []
            descuentos_eliminados = 0

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).lower().strip()
                es_descuento = any(palabra in nombre for palabra in palabras_descuento)

                if es_descuento:
                    descuentos_eliminados += 1
                    print(f"   🗑️ Descuento: {prod.get('nombre', 'N/A')[:40]}")
                else:
                    productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if descuentos_eliminados > 0:
                print(f"✅ {descuentos_eliminados} descuentos eliminados")

        # Normalizar productos
        productos_procesados = 0
        codigos_validos = 0

        for prod in data.get("productos", []):
            productos_procesados += 1

            # Normalizar precio
            if "precio" in prod:
                try:
                    precio_str = str(prod["precio"]).replace(",", "").replace(".", "")
                    prod["precio"] = int(precio_str) if precio_str.isdigit() else 0
                except:
                    prod["precio"] = 0
            else:
                prod["precio"] = 0

            prod["valor"] = prod["precio"]

            # Cantidad
            if "cantidad" not in prod:
                prod["cantidad"] = 1

            # Validar código
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()

                if codigo_limpio.isdigit() and 1 <= len(codigo_limpio) <= 13:
                    prod["codigo"] = codigo_limpio
                    codigos_validos += 1
                else:
                    prod["codigo"] = ""
            else:
                prod["codigo"] = ""

        # Normalizar establecimiento
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        data["establecimiento"] = normalizar_establecimiento(establecimiento_raw)

        # Asegurar total
        if "total" not in data or not data["total"]:
            data["total"] = sum(p.get("precio", 0) for p in data.get("productos", []))

        print(f"📊 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"💰 Total: ${data.get('total', 0):,}")
        print(f"📦 Productos: {productos_procesados} | Códigos: {codigos_validos}")
        print("=" * 70)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision",
                    "modelo": "claude-3-5-haiku-20241022",
                    "productos_detectados": productos_procesados,
                    "codigos_validos": codigos_validos,
                },
            },
        }

    except json.JSONDecodeError as e:
        print(f"❌ Error JSON: {e}")
        return {"success": False, "error": "No se pudo procesar. Imagen más clara."}
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": "Error procesando imagen."}


def normalizar_establecimiento(nombre_raw: str) -> str:
    """Normaliza nombre del establecimiento"""
    nombre_lower = nombre_raw.lower()

    establecimientos = {
        "jumbo": "JUMBO",
        "exito": "ÉXITO",
        "éxito": "ÉXITO",
        "carulla": "CARULLA",
        "olimpica": "OLÍMPICA",
        "ara": "ARA",
        "d1": "D1",
        "alkosto": "ALKOSTO",
        "makro": "MAKRO",
        "pricesmart": "PRICESMART",
        "dolarcity": "DOLARCITY",
        "camacho": "CAMACHO",
        "cruz verde": "CRUZ VERDE",
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    return nombre_raw.strip().upper()
