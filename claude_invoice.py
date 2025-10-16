import anthropic
import base64
import os
import json
import re
from typing import Dict


def parse_invoice_with_claude(image_path: str) -> Dict:
    """
    Procesa factura con Claude Vision API
    Sistema de 3 Niveles de Confianza + Limpieza Automática
    """
    try:
        print("=" * 70)
        print("🤖 PROCESANDO CON CLAUDE HAIKU 3.5 - Sistema Multi-Establecimiento")
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

        # ========== PROMPT GENERALIZADO MULTI-ESTABLECIMIENTO ==========
        prompt = """Eres un experto en facturas de supermercados COLOMBIANOS.

🎯 OBJETIVO: Extraer SOLO los productos que el cliente COMPRÓ y PAGÓ.

# 📋 ESTRUCTURA COMÚN DE FACTURAS COLOMBIANAS

Todas las facturas tienen COLUMNAS organizadas así:
```
CÓDIGO/PLU    DESCRIPCIÓN/DETALLE          PRECIO/VALOR
123456        Producto X                   12,500
789012        Producto Y                   8,900
```

# ✅ REGLAS UNIVERSALES (Todos los establecimientos)

## 1. IDENTIFICAR COLUMNAS CORRECTAMENTE

**Columna IZQUIERDA - CÓDIGO:**
- Números de 4-13 dígitos
- Etiquetas: PLU, CODIGO, COD, EAN
- ⚠️ NO es código: "1/u", "2/u", "0.750/KGM"

**Columna CENTRO - NOMBRE:**
- Descripción del producto
- ⚠️ **IMPORTANTE**: El nombre TERMINA antes de:
  - "V.Ahorro", "V. Ahorro", "Ahorro"
  - "/KGM", "/KG", "/U", "x 0.750"
  - "Descuento", "Dto", "Khorro" (error OCR)
- **Ejemplo:**
  - ✅ Correcto: "Mango"
  - ❌ Incorrecto: "Mango V.Ahorro 0"

**Columna DERECHA - PRECIO:**
- Precio final pagado
- Formatos: 12.500 o 12,500 o 12500
- ⚠️ NO es precio: "V.Ahorro 1.500"

## 2. DETECTAR LÍNEAS QUE NO SON PRODUCTOS

**Líneas de descuento/peso (IGNORAR):**
```
1 1/u x 26.900 V.Ahorro 4.035        ← DESCUENTO
0.750/KGM x 8.800 V.Ahorro 1.320     ← PESO/UNIDAD
2x1 Descuento                         ← PROMOCIÓN
```

**Características:**
- Contienen "x" seguido de precio
- Tienen "V.Ahorro", "Ahorro", "Descuento"
- Formato de peso: "0.XXX/KGM", "1.5/KG"
- NO tienen código PLU válido

## 3. USAR "Total Item" COMO VALIDACIÓN

Si dice "Total Item: 5", tu respuesta debe tener EXACTAMENTE 5 productos.

# 🔍 ALGORITMO DE EXTRACCIÓN

Para cada línea:

**PASO 1: ¿Es producto o descuento?**
```
¿Formato "X.XXX/KG x PRECIO"? → IGNORAR
¿Tiene "V.Ahorro" sin código? → IGNORAR
¿Solo "Ahorro"/"Descuento"? → IGNORAR
```

**PASO 2: Extraer datos del producto:**
```
Columna 1 → codigo (solo dígitos)
Columna 2 → nombre (SOLO hasta antes de "V.Ahorro"/"KGM")
Columna 3 → precio (número final)
```

**PASO 3: Limpiar nombre:**
- Eliminar todo después de "V.Ahorro"
- Eliminar todo después de "/KGM" o "/KG"
- Eliminar todo después de " x "

# 📝 EJEMPLOS MULTI-ESTABLECIMIENTO

**ÉXITO:**
```
PLU      DETALLE                     PRECIO
1220     Mango                       6.280
         V.Ahorro 0                  ← IGNORAR esta línea
3323923  Brownie Mini Are            14.800
```

**JUMBO:**
```
CODIGO   DESCRIPCION                 VALOR
4756821  LECHE ALPINA 1L             4,200
         Descuento 2x1: -2,100       ← IGNORAR
9182736  PAN TAJADO                  3,500
```

**D1:**
```
COD      PRODUCTO                    PRECIO
123      ARROZ DIANA 500G            2,800
456      ACEITE GIRASOL 1L           8,900
```

**Salida JSON (para todos):**
```json
{
  "establecimiento": "ÉXITO",
  "fecha": "2016-11-16",
  "total": 288486,
  "productos": [
    {
      "codigo": "1220",
      "nombre": "Mango",
      "cantidad": 1,
      "precio": 6280
    },
    {
      "codigo": "3323923",
      "nombre": "Brownie Mini Are",
      "cantidad": 1,
      "precio": 14800
    }
  ]
}
```

# 🚨 VALIDACIÓN FINAL

Antes de responder:
1. ✅ ¿Número productos = "Total Item"?
2. ✅ ¿Ningún nombre contiene "V.Ahorro", "KGM", "Descuento"?
3. ✅ ¿Códigos son 4-13 dígitos numéricos?
4. ✅ ¿Suma precios ≈ SUBTOTAL?

Si hay inconsistencias, elimina líneas sospechosas.

# 🎯 ESTABLECIMIENTOS COLOMBIANOS

Para identificar:
- Grupo Éxito: ÉXITO, Carulla, Surtimax
- Cencosud: JUMBO, Metro
- Otros: Olímpica, D1, ARA, Alkosto, Makro, PriceSmart

ANALIZA LA IMAGEN Y RESPONDE SOLO CON JSON (sin comas en precios):"""

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
                # Métodos de pago
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
                "pago",
                "autoriza",
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

                # ❌ FILTROS DE BASURA
                es_basura = any(palabra in nombre for palabra in palabras_basura)
                solo_numeros = not re.search(r"[A-Za-zÀ-ÿ]", nombre)
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
                es_peso = bool(re.match(r"^\d+\.?\d*\s*/\s*kg[hm]?$", nombre))
                es_numeracion = bool(re.match(r"^\d{1,2}\s*(un|/u)\b", nombre))
                es_simbolo = nombre in ["%", "$", "-", "=", "*", "+"]
                muy_corto = len(nombre) < 3
                precio = prod.get("precio", 0)
                precio_invalido = precio < 50
                patron_repetitivo = bool(re.match(r"^\d+\s+\d+/u", nombre))

                # Aplicar filtros
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
                    print(f"   🗑️ Basura: '{prod.get('nombre', 'N/A')[:40]}'")
                else:
                    productos_filtrados.append(prod)

            data["productos"] = productos_filtrados

            if basura_eliminada > 0:
                print(f"✅ {basura_eliminada} líneas de basura eliminadas")
                print(f"📦 {len(productos_filtrados)} productos pre-limpieza")

        # ========== POST-PROCESAMIENTO: LIMPIEZA DE NOMBRES ==========
        if "productos" in data and data["productos"]:
            productos_limpios = []

            print(f"\n🧹 LIMPIANDO NOMBRES DE PRODUCTOS...")

            for prod in data["productos"]:
                nombre_original = str(prod.get("nombre", "")).strip()
                nombre_limpio = nombre_original

                # 🧹 Patrones a eliminar del FINAL del nombre
                sufijos_error = [
                    r"\s+V\.?\s*Ahorro.*$",  # "V.Ahorro 0", "V. Ahorro 1.500"
                    r"\s+Ahorro.*$",  # "Ahorro 3.200"
                    r"\s+Descuento.*$",  # "Descuento 1.500"
                    r"\s+\d+\.?\d*/KG[MH]?.*$",  # "0.750/KGM x 8.800"
                    r"\s+x\s+\d+\.?\d+.*$",  # "x 26.900"
                    r"\s+Khorro.*$",  # "Khorro G" (error OCR)
                    r"\s+y\s+Khorro.*$",  # "y Khorro G"
                ]

                for patron in sufijos_error:
                    nombre_limpio = re.sub(
                        patron, "", nombre_limpio, flags=re.IGNORECASE
                    )

                nombre_limpio = nombre_limpio.strip()

                # Log de cambios
                if nombre_limpio != nombre_original:
                    print(f"   🧹 '{nombre_original[:40]}' → '{nombre_limpio}'")

                # Solo agregar si tiene contenido válido
                if len(nombre_limpio) >= 3:
                    prod["nombre"] = nombre_limpio
                    productos_limpios.append(prod)
                else:
                    print(f"   🗑️ Nombre muy corto descartado: '{nombre_limpio}'")

            data["productos"] = productos_limpios
            print(
                f"✅ Productos finales después de limpieza: {len(productos_limpios)}\n"
            )

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

            # Limpiar nombre (ya limpiado arriba, pero asegurar)
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
                    "metodo": "claude-vision-multi-establecimiento",
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

    # Si no coincide, devolver capitalizado
    return nombre_raw.strip().upper()[:50]
