import anthropic
import base64
import os
import json
import re
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

        # ========== PROMPT ULTRA MEJORADO - CONTEXTO COLOMBIANO ==========
        prompt = """Eres un experto en facturas de supermercados COLOMBIANOS.

🎯 OBJETIVO: Extraer SOLO los productos que el cliente COMPRÓ y PAGÓ.

# 🔑 REGLA DE ORO
Lee CUIDADOSAMENTE y extrae SOLO lo que está en la SECCIÓN DE PRODUCTOS.

# 📋 CÓMO LEER UNA FACTURA COLOMBIANA

Las facturas tienen SECCIONES bien definidas:

## SECCIÓN 1: HEADER (No extraer)
- Nombre del establecimiento (ÉXITO, JUMBO, OLÍMPICA, etc)
- NIT, dirección, teléfono
- Número de factura
- Fecha y hora

## SECCIÓN 2: PRODUCTOS (✅ EXTRAER AQUÍ)
Busca la palabra "PLU" o "CODIGO" seguida de "DETALLE" y "PRECIO"

Formato típico:
```
PLU      DETALLE                    PRECIO
123456   PRODUCTO 1                 10,000
789012   PRODUCTO 2                 25,500
```

⚠️ CLAVE: Busca "Total Item: X" al final de esta sección
- Si dice "Total Item: 1" → Solo hay 1 producto
- Si dice "Total Item: 3" → Solo hay 3 productos
- NO extraigas MÁS productos que este número

## SECCIÓN 3: PROMOCIONES Y DESCUENTOS (❌ NO extraer)
Aparece ENTRE los productos y el total final:
```
1 1/u x 26.900  V.Ahorro 4.035  ← ESTO ES DESCUENTO, NO PRODUCTO
SUBTOTAL         26,900
DESCUENTO         4,035
AHORRO            4,035
```

Palabras clave de descuentos:
- V.Ahorro, V. Ahorro, Ahorro
- Descuento, Dto, Rebaja
- Promoción, Promo, Oferta
- 2x1, 3x2, %OFF

## SECCIÓN 4: TOTALES (❌ NO extraer)
```
SUBTOTAL         26,900
DESCUENTO         4,035
VALOR TOTAL      22,865  ← Este es el total DESPUÉS del descuento
```

## SECCIÓN 5: PAGO (❌ NO extraer)
```
FORMA PAGO: CONTADO
EFECTIVO
TARJETA CREDITO
MASTERCARD, VISA, etc
CAMBIO: 17,150
```

## SECCIÓN 6: INFO FISCAL (❌ NO extraer)
- RES DIAN
- Discriminación tarifas
- Códigos QR, Cufe, etc

# ✅ REGLAS PARA EXTRAER PRODUCTOS

1. **Busca "Total Item: X"** - Este número te dice cuántos productos REALMENTE hay
2. **Solo extrae de la sección de productos** (entre el header y los totales)
3. **Cada producto tiene:**
   - Código (PLU o EAN): números a la izquierda
   - Nombre: descripción en el centro
   - Precio: número a la derecha

4. **NO extraigas:**
   - Líneas de descuento (tienen palabras: Ahorro, Descuento, V.Ahorro)
   - Subtotales, totales
   - Métodos de pago
   - Info administrativa

# ⚠️ CASOS ESPECIALES A IGNORAR

❌ **Líneas de numeración:**
- "01 un", "02 un", "03 un" → NO SON PRODUCTOS
- "1/u x 12.900", "2/u x 5.500" → NO SON PRODUCTOS

❌ **Líneas de peso/unidad:**
- "0.875/KGH" → NO ES PRODUCTO
- "1.5/KG" → NO ES PRODUCTO
- Solo indica peso, no es un producto comprado

❌ **Palabras sueltas:**
- "AHORRO", "KG", "KGM", "UN", "%" → NO SON PRODUCTOS
- "REDEBAN", "PAGO", "AUTORIZA", "RECIBO" → NO SON PRODUCTOS

Si ves alguna de estas líneas, IGNÓRALAS COMPLETAMENTE.

# 📦 FORMATO JSON (sin comas en precios)

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
    }
  ]
}

ANALIZA LA IMAGEN CUIDADOSAMENTE Y RESPONDE SOLO CON JSON:"""

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

        # ========== FILTRADO INTELIGENTE DE BASURA - VERSIÓN MEJORADA ==========
        if "productos" in data and data["productos"]:
            productos_originales = len(data["productos"])

            # Lista AMPLIADA de basura obvia
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
                # 🆕 NUEVOS FILTROS
                "pago",
                "autoriza",
                "recibo",
                "aprobado",
                "comprobante",
                "cufe",
                "qr",
                "codigo qr",
                "fecha",
                "hora",
            ]

            productos_filtrados = []
            basura_eliminada = 0

            for prod in data["productos"]:
                nombre = str(prod.get("nombre", "")).lower().strip()

                # ❌ FILTRO 1: Basura obvia
                es_basura = any(palabra in nombre for palabra in palabras_basura)

                # ❌ FILTRO 2: Solo números/símbolos (sin letras)
                solo_numeros = not re.search(r"[A-Za-zÀ-ÿ]", nombre)

                # ❌ FILTRO 3: Unidades solas
                es_unidad = nombre in [
                    "kg",
                    "kgm",
                    "/kgm",
                    "/kg",
                    "und",
                    "/u",
                    "x",
                    "un",
                ]

                # 🆕 FILTRO 4: Formato de peso/medida (0.875/KGH, 1.5/KG, etc.)
                es_peso = bool(re.match(r"^\d+\.?\d*\s*/\s*kg[hm]?$", nombre))

                # 🆕 FILTRO 5: Numeración de líneas (01 un, 02 un, 1/u, 2/u, etc.)
                es_numeracion = bool(re.match(r"^\d{1,2}\s*(un|/u)\b", nombre))

                # 🆕 FILTRO 6: Solo porcentajes o símbolos
                es_simbolo = nombre in ["%", "$", "-", "=", "*", "+"]

                # 🆕 FILTRO 7: Muy corto (menos de 3 caracteres)
                muy_corto = len(nombre) < 3

                # 🆕 FILTRO 8: Precio muy bajo (menos de $50 probablemente es basura)
                precio = prod.get("precio", 0)
                precio_invalido = precio < 50

                # 🆕 FILTRO 9: Nombre repetitivo (ej: "1 1/u x 26.900")
                patron_repetitivo = bool(re.match(r"^\d+\s+\d+/u", nombre))

                # Aplicar TODOS los filtros
                if (
                    es_basura
                    or solo_numeros
                    or es_unidad
                    or es_peso
                    or es_numeracion
                    or es_simbolo
                    or muy_corto
                    or (precio_invalido and not nombre)
                    or patron_repetitivo
                ):

                    basura_eliminada += 1
                    print(f"   🗑️ Basura eliminada: '{prod.get('nombre', 'N/A')[:50]}'")
                    print(f"      Razón: ", end="")
                    if es_basura:
                        print("palabra prohibida", end=" ")
                    if solo_numeros:
                        print("solo números", end=" ")
                    if es_peso:
                        print("formato peso", end=" ")
                    if es_numeracion:
                        print("numeración", end=" ")
                    if muy_corto:
                        print("muy corto", end=" ")
                    if precio_invalido:
                        print("precio inválido", end=" ")
                    print()
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
