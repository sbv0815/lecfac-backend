import anthropic
import base64
import os
import json
from typing import Dict


def parse_invoice_with_claude(image_path: str) -> Dict:
    """
    Procesa factura con Claude Vision API
    Sistema de 3 Niveles de Confianza
    """
    try:
        print("=" * 70)
        print("🤖 PROCESANDO CON CLAUDE HAIKU 3.5 - Sistema 3 Niveles")
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

        # ========== PROMPT MEJORADO - SISTEMA 3 NIVELES ==========
        prompt = """Eres un experto en análisis de facturas de supermercados colombianos.

🎯 OBJETIVO: Extraer SOLO los productos QUE REALMENTE VES en la imagen.

⚠️ CRÍTICO: NO INVENTES productos. Solo incluye lo que CLARAMENTE lees.

# 🔑 REGLA DE ORO
"Si no estás 100% seguro de que es un producto REAL, NO lo incluyas"

Necesitamos:
1. ESTABLECIMIENTO - Para saber dónde compró
2. CÓDIGO del producto - Para identificación única y comparación de precios
3. NOMBRE del producto - Para saber QUÉ compró
4. PRECIO - Para comparar entre tiendas
5. CANTIDAD (si está visible)

# ✅ TIPOS DE PRODUCTOS A INCLUIR

NIVEL 1 - ALTA CONFIANZA (Código + Nombre + Precio):
✓ "7702993047842 LECHE ALPINA 2190" → PERFECTO
✓ "116 BANANO URABA 5425" → PERFECTO

NIVEL 2 - MEDIA CONFIANZA (Nombre + Precio sin código):
✓ "LIMON TAHITI 3500" → INCLUIR (nombre + precio)
✓ "SAL REFISAL 1200" → INCLUIR (nombre + precio)
✓ "AJO NACIONAL 800" → INCLUIR (productos baratos válidos)

NIVEL 3 - BAJA CONFIANZA (Parcial pero útil):
✓ "HUEVOS AA" → INCLUIR (solo nombre, precio 0)
✓ "234 5600" → INCLUIR (código + precio, nombre vacío)

# ❌ LO QUE NO DEBES INCLUIR (BASURA OBVIA)

NO incluir líneas con estas palabras:
✗ AHORRO, DESCUENTO, DESC, DTO, REBAJA, PROMOCION, PROMO, OFERTA
✗ IVA, IMPUESTO, SUBTOTAL, TOTAL A PAGAR, GRAN TOTAL, VALOR TOTAL
✗ CAMBIO, EFECTIVO, ITEMS COMPRADOS, PRECIO FINAL
✗ GRACIAS, VUELVA PRONTO, NIT, RESOLUCION DIAN

MÉTODOS DE PAGO (CRÍTICO - NO SON PRODUCTOS):
✗ TARJETA, CREDITO, DEBITO, REDEBAN, DATAFONO, POS
✗ MASTERCARD, VISA, AMERICAN EXPRESS, AMEX, DINERS
✗ PSE, NEQUI, DAVIPLATA, BANCOLOMBIA, TRANSFERENCIA

EJEMPLOS:
✗ "RM HAS MASTERCARD" → MÉTODO DE PAGO
✗ "TARJ CRE/DEB REDEBAN" → MÉTODO DE PAGO
✗ "14476 AHORRO 20%" → DESCUENTO
✗ "IVA 19%" → IMPUESTO
✗ "SUBTOTAL 45000" → RESUMEN
✗ "GRACIAS POR SU COMPRA" → MENSAJE

# 📝 REGLAS PARA CÓDIGOS

CÓDIGOS VÁLIDOS (solo dígitos, 1-13 caracteres):
✓ "3" → código válido (PLU frutas)
✓ "09" → código válido
✓ "116" → código válido (PLU común)
✓ "7702993047842" → código válido (EAN-13)

CÓDIGOS INVÁLIDOS:
✗ "343718DF" → tiene letras, código: ""
✗ "REF123" → tiene letras, código: ""
✗ "" → vacío, código: ""

# 🔍 NOMBRES DE PRODUCTOS

ACEPTA nombres cortos si son productos REALES:
✓ "SAL" → VÁLIDO (producto real)
✓ "AJO" → VÁLIDO (producto real)
✓ "PAN" → VÁLIDO (producto real)
✓ "TÉ" → VÁLIDO (producto real)

NO aceptes solo unidades o fragmentos:
✗ "KG" → NO es producto
✗ "X" → NO es producto
✗ "/U" → NO es producto

# 💰 PRECIOS

ACEPTA precios desde $50 (productos baratos son válidos):
✓ 50 → válido
✓ 200 → válido (chicles, dulces)
✓ 5600 → válido
✓ 45000 → válido

NO aceptes:
✗ 0 → sin precio
✗ Negativos → descuentos

# 🏪 ESTABLECIMIENTOS

Si ves: JUMBO, ÉXITO, CARULLA, OLÍMPICA, ARA, D1, ALKOSTO, etc.
Usa SOLO el nombre principal sin sucursal:
"JUMBO BULEVAR" → "JUMBO"
"ÉXITO AMERICAS" → "ÉXITO"

# 📅 TOTAL Y FECHA

- El TOTAL suele estar al FINAL de la factura
- Busca: TOTAL, GRAN TOTAL, TOTAL A PAGAR, VALOR TOTAL
- Fecha: formato YYYY-MM-DD (2024-12-27)
- Si no encuentras fecha o total, pon null

# 📦 FORMATO JSON

{
  "establecimiento": "JUMBO",
  "fecha": "2024-12-27",
  "total": 234890,
  "productos": [
    {
      "codigo": "7702993047842",
      "nombre": "LECHE ALPINA ENTERA",
      "cantidad": 2,
      "precio": 8760
    },
    {
      "codigo": "116",
      "nombre": "BANANO URABA",
      "cantidad": 0.878,
      "precio": 5425
    },
    {
      "codigo": "",
      "nombre": "SAL REFISAL",
      "cantidad": 1,
      "precio": 1200
    },
    {
      "codigo": "234",
      "nombre": "LIMON TAHITI",
      "cantidad": 1,
      "precio": 3500
    }
  ]
}

# ⚠️ IMPORTANTE

1. Precios SIN separadores: 2190 (no 2,190)
2. Códigos como strings: "116", "09", ""
3. Si no hay código, pon ""
4. Si no hay precio, pon 0
5. Incluye TODOS los productos visibles, incluso con datos parciales
6. NO incluyas descuentos, IVA, subtotales, mensajes

# 🎯 ESTRATEGIA

Es MEJOR tener:
- 15 productos donde 12 son perfectos y 3 necesitan revisión
QUE:
- 8 productos perfectos pero perdiste 7 productos reales

El usuario puede revisar/corregir después en el editor.

ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON:"""

        # ✅ Llamada con HAIKU 3.5
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

        # Parsear respuesta
        response_text = message.content[0].text
        print(f"📄 Respuesta Claude (primeros 200 chars): {response_text[:200]}...")

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

        # ========== FILTRADO INTELIGENTE DE BASURA ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])

            # Lista REDUCIDA de basura obvia
            palabras_basura = [
                "ahorro",
                "descuento",
                "desc",
                "dto",
                "rebaja",
                "promocion",
                "promo",
                "iva",
                "impuesto",
                "subtotal",
                "total",
                "cambio",
                "efectivo",
                "tarjeta",
                "redeban",
                "credito",
                "debito",
                "gracias",
                "vuelva",
                "resolucion",
                "dian",
                "nit",
                "autoretenedor",
                # Métodos de pago - CRÍTICO
                "mastercard",
                "visa",
                "american express",
                "amex",
                "diners",
                "pse",
                "nequi",
                "daviplata",
                "bancolombia",
                "davivienda",
                "transferencia",
                "datafono",
                "pos",
                "terminal",
                # Mensajes adicionales
                "precio final",
                "gran total",
                "valor total",
                "items comprados",
                "cajero",
                "caja",
                "factura",
                "ticket",
                "recibo",
            ]

            productos_filtrados = []
            basura_eliminada = 0

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).lower().strip()

                # ❌ Filtrar basura obvia
                es_basura = any(palabra in nombre for palabra in palabras_basura)

                # ❌ Filtrar solo números/símbolos (sin letras)
                import re

                solo_numeros = not re.search(r"[A-Za-zÀ-ÿ]", nombre)

                # ❌ Filtrar unidades solas
                es_unidad = nombre in ["kg", "kgm", "/kgm", "/kg", "und", "/u", "x"]

                if es_basura or solo_numeros or es_unidad:
                    basura_eliminada += 1
                    print(f"   🗑️ Basura: {prod.get('nombre', 'N/A')[:40]}")
                else:
                    productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if basura_eliminada > 0:
                print(f"✅ {basura_eliminada} líneas de basura eliminadas")
                print(f"📦 {len(productos_filtrados)} productos válidos guardados")

        # ========== NORMALIZACIÓN Y NIVEL DE CONFIANZA ==========
        productos_procesados = 0
        nivel_1 = 0  # Código + Nombre + Precio
        nivel_2 = 0  # Nombre + Precio
        nivel_3 = 0  # Parcial

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
            else:
                try:
                    prod["cantidad"] = float(prod["cantidad"])
                except:
                    prod["cantidad"] = 1

            # Validar y limpiar código
            if "codigo" in prod and prod["codigo"]:
                codigo_limpio = str(prod["codigo"]).strip()

                # ✅ Acepta códigos de 1-13 dígitos
                if codigo_limpio.isdigit() and 1 <= len(codigo_limpio) <= 13:
                    prod["codigo"] = codigo_limpio
                else:
                    prod["codigo"] = ""
            else:
                prod["codigo"] = ""

            # Limpiar nombre
            nombre = str(prod.get("nombre", "")).strip()
            prod["nombre"] = nombre

            # ✅ Calcular nivel de confianza
            tiene_codigo = bool(prod["codigo"])
            tiene_nombre = bool(nombre and len(nombre) >= 2)
            tiene_precio = bool(prod["precio"] >= 50)

            if tiene_codigo and tiene_nombre and tiene_precio:
                prod["nivel_confianza"] = 1
                nivel_1 += 1
            elif tiene_nombre and tiene_precio:
                prod["nivel_confianza"] = 2
                nivel_2 += 1
            elif tiene_nombre or (tiene_codigo and prod["precio"] > 0):
                prod["nivel_confianza"] = 3
                nivel_3 += 1
            else:
                # Sin suficiente info - marcar para revisión
                prod["nivel_confianza"] = 3
                nivel_3 += 1

        # Normalizar establecimiento
        establecimiento_raw = data.get("establecimiento", "Desconocido")
        data["establecimiento"] = normalizar_establecimiento(establecimiento_raw)

        # Asegurar total
        if "total" not in data or not data["total"]:
            suma_productos = sum(
                p.get("precio", 0) * p.get("cantidad", 1)
                for p in data.get("productos", [])
            )
            data["total"] = suma_productos

        # ========== LOG DE RESULTADOS ==========
        print(f"📊 Establecimiento: {data.get('establecimiento', 'N/A')}")
        print(f"💰 Total: ${data.get('total', 0):,}")
        print(f"📦 Productos procesados: {productos_procesados}")
        print(f"")
        print(f"📊 POR NIVEL DE CONFIANZA:")
        print(f"   ✅ NIVEL 1 (Código+Nombre+Precio): {nivel_1}")
        print(f"   ⚠️  NIVEL 2 (Nombre+Precio): {nivel_2}")
        print(f"   ⚡ NIVEL 3 (Parcial): {nivel_3}")
        print("=" * 70)

        return {
            "success": True,
            "data": {
                **data,
                "metadatos": {
                    "metodo": "claude-vision-3niveles",
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
        print(f"Respuesta recibida: {response_text[:500]}")
        return {
            "success": False,
            "error": "Error parseando respuesta de Claude. Imagen más clara.",
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return {"success": False, "error": f"Error procesando imagen: {str(e)}"}


def normalizar_establecimiento(nombre_raw: str) -> str:
    """Normaliza nombre del establecimiento a formato estándar"""
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
        "dolarcity": "DOLARCITY",
        "surtimax": "SURTIMAX",
        "metro": "METRO",
        "la 14": "LA 14",
        "camacho": "CAMACHO",
        "cruz verde": "CRUZ VERDE",
        "cafam": "CAFAM",
        "colsubsidio": "COLSUBSIDIO",
    }

    for clave, normalizado in establecimientos.items():
        if clave in nombre_lower:
            return normalizado

    # Si no coincide con ninguno, devolver capitalizado
    return nombre_raw.strip().upper()[:50]
