import os
import re
import json
import tempfile
import time
from datetime import datetime
from google.cloud import documentai
import traceback
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

# Intentar importar Tesseract (opcional)
try:
    import pytesseract
    TESSERACT_AVAILABLE = True
    print("✅ Tesseract OCR disponible")
except ImportError:
    TESSERACT_AVAILABLE = False
    print("⚠️ Tesseract no disponible - solo Document AI")


# =====================
# Helpers
# =====================

def setup_environment():
    required_vars = [
        "GCP_PROJECT_ID",
        "DOC_AI_LOCATION",
        "DOC_AI_PROCESSOR_ID",
        "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    ]
    for var in required_vars:
        if not os.environ.get(var):
            raise Exception(f"Variable requerida: {var}")

    credentials_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    try:
        json.loads(credentials_json)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(credentials_json)
            temp_file_path = temp_file.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path
        return True
    except Exception:
        raise Exception("JSON inválido")


def preprocess_image(image_path):
    """Preprocesamiento solo con PIL (sin OpenCV)"""
    try:
        img = Image.open(image_path)
        img = img.convert("L")
        img = ImageEnhance.Contrast(img).enhance(2.5)
        img = ImageEnhance.Brightness(img).enhance(1.2)
        img = ImageEnhance.Sharpness(img).enhance(2.0)
        img = img.filter(ImageFilter.MedianFilter(size=3))
        img = img.point(lambda p: 255 if p > 128 else 0, mode="1")
        processed_path = image_path.replace(".jpg", "_processed.png").replace(".png", "_processed.png")
        img.save(processed_path)
        return processed_path
    except Exception as e:
        print(f"⚠️ Error en preprocesamiento: {e}")
        return image_path


def clean_amount(amount_str):
    """Convierte montos a int"""
    if not amount_str:
        return None
    cleaned = re.sub(r"[$\s]", "", str(amount_str))
    cleaned = cleaned.replace(".", "").replace(",", "")
    cleaned = re.sub(r"[^\d]", "", cleaned)
    if not cleaned:
        return None
    try:
        amount = int(cleaned)
        if 10 < amount < 10000000:
            return amount
    except Exception:
        pass
    return None


def _pick_best_price(nums: list[str]) -> int:
    """Elige el número más grande como precio probable"""
    if not nums:
        return 0
    cand = [clean_amount(x) for x in nums]
    cand = [c for c in cand if c is not None]
    return max(cand) if cand else 0


def _looks_like_continuation(line: str) -> bool:
    U = line.upper()
    return (
        bool(re.search(r"\bKG\b", U))
        or bool(re.search(r"\bX\b", U))
        or bool(re.search(r"^\d+\s*X\s*\d+", U))
        or bool(re.search(r"^\d+[.,]\d+\s*KG", U))
    )


def _join_item_blocks(texto: str) -> list[str]:
    raw = [l.strip() for l in texto.splitlines() if l.strip()]
    joined = []
    buf = ""
    for l in raw:
        if not buf:
            buf = l
            continue
        if _looks_like_continuation(l):
            buf = f"{buf}; {l}"
        else:
            joined.append(buf)
            buf = l
    if buf:
        joined.append(buf)
    return joined


def _is_variable_weight_ean(code: str) -> bool:
    return bool(code) and len(code) in (12, 13) and code[:2] in ("20", "28", "29")


def _parse_by_columns(lines: list[str]) -> list[dict]:
    productos = []
    code_idx, total_idx = 0, None
    for l in lines[:40]:
        U = l.upper()
        if "CODIGO" in U and "TOTAL" in U:
            code_idx = U.find("CODIGO")
            total_idx = U.rfind("TOTAL")
            break
    if total_idx is None:
        total_idx = max(30, max((len(x) for x in lines[:50]), default=60) - 8)

    for l in lines:
        if len(l) < 10:
            continue
        U = l.upper()
        if "SUBTOTAL" in U or ("TOTAL" in U and "CODIGO" not in U):
            continue

        codigo = l[: max(0, code_idx + 15)].strip()
        total = l[total_idx:].strip() if total_idx < len(l) else ""
        descr = l[len(codigo) : total_idx].strip() if total_idx > len(codigo) else l.strip()

        codigo_match = re.findall(r"\b\d{6,13}\b", codigo)
        codigo = codigo_match[0] if codigo_match else None

        m = re.findall(r"(\d{1,3}(?:[.,]\d{3})+)", total or l)
        precio = _pick_best_price(m)

        nombre = re.sub(r"\s+", " ", descr).strip()
        nombre = re.sub(r"^\d{3,}\s*", "", nombre)
        nombre = nombre[:80]

        if nombre and len(nombre) >= 3:
            productos.append(
                {"codigo": codigo, "nombre": nombre, "valor": precio or 0, "fuente": "tesseract_columns"}
            )
    return productos
# =====================
# Extractors
# =====================

def extract_vendor_name(raw_text):
    patterns = [
        r"(ALMACENES\s+(?:EXITO|ÉXITO)[^\n]*)",
        r"((?:EXITO|ÉXITO)\s+\w+)",
        r"(CARULLA[^\n]*)",
        r"(TIENDAS\s+D1[^\n]*)",
        r"(JUMBO[^\n]*)",
        r"(OLIMPICA[^\n]*)",
        r"(ALKOSTO[^\n]*)",
        r"(CRUZ\s+VERDE[^\n]*)",
        r"(LA\s+REBAJA[^\n]*)",
        r"(CAFAM[^\n]*)",
        r"(MAKRO[^\n]*)",
        r"(METRO[^\n]*)",
        r"(ARA[^\n]*)",
        r"(D1[^\n]*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            return re.sub(r"\s+", " ", match.group(1).strip())[:50]
    return "Establecimiento no identificado"


def extract_total_invoice(raw_text):
    patterns = [
        r"TOTAL\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)",
        r"TOTAL\s+A\s+PAGAR\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)",
        r"VALOR\s+TOTAL\s*[:\$]?\s*(\d{1,3}[,\.]\d{3}(?:[,\.]\d{3})*)",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw_text, re.IGNORECASE)
        if match:
            total = clean_amount(match.group(1))
            if total and total > 1000:
                return total
    return None


def extract_product_code(text):
    if not text:
        return None
    patterns = [
        r"^\s*(\d{13})\s",
        r"^\s*(\d{12})\s",
        r"^\s*(\d{10})\s",
        r"^\s*(\d{8})\s",
        r"^\s*(\d{7})\s",
        r"^\s*(\d{6})\s",
        r"\b(\d{13})\b",
        r"\b(\d{10})\b",
        r"\b(\d{8})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text.strip())
        if match:
            code = match.group(1)
            if 6 <= len(code) <= 13:
                return code
    return None


def clean_product_name(text, product_code=None):
    if not text:
        return None
    if product_code:
        text = re.sub(rf"^{re.escape(product_code)}\s*", "", text)
    text = re.sub(r"^\d+DF\.[A-Z]{2}\.", "", text)
    text = re.sub(r"^DF\.[A-Z]{2}\.", "", text)
    text = re.sub(r"\d+[,\.]\d{3}.*$", "", text)
    text = re.sub(r"\$\d+.*$", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    words = [w for w in text.split() if re.search(r"[A-Za-zÀ-ÿ]{2,}", w) and len(w) >= 2]
    return " ".join(words[:8])[:80] if words else None


def extract_products_document_ai(document):
    line_items = [e for e in document.entities if e.type_ == "line_item" and e.confidence > 0.3]
    productos = []
    i = 0
    while i < len(line_items):
        entity = line_items[i]
        product_code = extract_product_code(entity.mention_text)
        product_name = clean_product_name(entity.mention_text, product_code)
        unit_price = None
        for prop in entity.properties:
            if prop.type_ == "line_item/amount" and prop.confidence > 0.25:
                unit_price = clean_amount(prop.mention_text)
                if unit_price:
                    break
        if not unit_price and i + 1 < len(line_items):
            for prop in line_items[i + 1].properties:
                if prop.type_ == "line_item/amount":
                    unit_price = clean_amount(prop.mention_text)
                    if unit_price:
                        i += 1
                        break
        if product_code or (product_name and len(product_name) > 2):
            productos.append(
                {"codigo": product_code, "nombre": product_name or "Sin descripción", "valor": unit_price or 0, "fuente": "document_ai"}
            )
        i += 1
    return productos


def extract_products_tesseract_aggressive(image_path):
    if not TESSERACT_AVAILABLE:
        return []
    productos = []
    try:
        processed_path = preprocess_image(image_path)
        img = Image.open(processed_path)
        texto = (
            pytesseract.image_to_string(img, lang="spa", config="--psm 6 --oem 3")
            + "\n"
            + pytesseract.image_to_string(img, lang="spa", config="--psm 4 --oem 3")
            + "\n"
            + pytesseract.image_to_string(img, lang="spa", config="--psm 11 --oem 3")
        )
        lineas = _join_item_blocks(texto)
        print(f"📄 Bloques tras normalizar: {len(lineas)}")

        prods_cols = _parse_by_columns(lineas)
        prods_regex = []

        # Regex fallback
        patterns = [
            r"(\d{6,13})\s+(.+?)\s+(\d{1,3}[,\.]\d{3})",
            r"([A-ZÀ-Ÿ]{3}[A-ZÀ-Ÿ\s/\-\.]{3,60}?)\s+(\d{1,3}[,\.]\d{3})\s*[NXAEH]?\s*",
        ]

        for linea in lineas:
            for pattern in patterns:
                matches = re.finditer(pattern, linea, re.MULTILINE | re.IGNORECASE)
                for m in matches:
                    if len(m.groups()) == 3:
                        g1, g2, g3 = m.groups()
                        if g1.isdigit():
                            codigo, nombre, precio_str = g1, g2, g3
                        else:
                            codigo, nombre, precio_str = None, g1, g2
                    elif len(m.groups()) == 2:
                        g1, g2 = m.groups()
                        codigo, nombre, precio_str = None, g1, g2
                    else:
                        continue
                    precio = clean_amount(precio_str) or 0
                    nombre = re.sub(r"\s+", " ", nombre).strip()[:80]
                    if nombre and len(nombre) >= 3:
                        prods_regex.append(
                            {"codigo": codigo, "nombre": nombre, "valor": precio, "fuente": "tesseract_regex"}
                        )

        def _merge(a, b):
            vistos = set()
            out = []
            for src in (a, b):
                for p in src:
                    key = p.get("codigo") or (p["nombre"].lower().replace(" ", ""), p.get("valor", 0))
                    if key in vistos:
                        continue
                    vistos.add(key)
                    out.append(p)
            return out

        productos = _merge(prods_cols, prods_regex)

        if processed_path != image_path:
            try:
                os.remove(processed_path)
            except Exception:
                pass

        print(f"✓ Productos extraídos por Tesseract: {len(productos)}")
        return productos
    except Exception as e:
        print(f"⚠️ Error Tesseract: {e}")
        traceback.print_exc()
        return []


# =====================
# Combinar y flujo principal
# =====================

def combinar_y_deduplicar(productos_ai, productos_tesseract):
    productos_finales = {}
    import hashlib
    for prod in productos_ai:
        codigo = prod.get("codigo")
        if codigo and len(codigo) >= 8:
            productos_finales[codigo] = prod
    for prod in productos_tesseract:
        codigo = prod.get("codigo")
        if codigo:
            if _is_variable_weight_ean(codigo):
                unique = f"{codigo}_{prod.get('valor',0)}_{prod.get('nombre','')[:20]}"
                codigo = "PLU_" + hashlib.md5(unique.encode()).hexdigest()[:8]
                prod["codigo"] = codigo
            if codigo not in productos_finales:
                productos_finales[codigo] = prod
            elif productos_finales[codigo].get("valor", 0) == 0 and prod.get("valor", 0) > 0:
                productos_finales[codigo]["valor"] = prod["valor"]
        else:
            if prod.get("nombre"):
                nombre_hash = hashlib.md5(prod["nombre"].encode()).hexdigest()[:8]
                temp_codigo = f"TEMP_{nombre_hash}"
                if temp_codigo not in productos_finales:
                    prod["codigo"] = temp_codigo
                    productos_finales[temp_codigo] = prod
    resultado = list(productos_finales.values())
    resultado.sort(key=lambda x: (x.get("codigo", "").startswith("TEMP"), x.get("codigo", "")))
    return resultado


def process_invoice_complete(file_path):
    inicio = time.time()
    try:
        print("\n" + "=" * 70)
        print("🔍 PROCESAMIENTO MEJORADO - OBJETIVO: 50/50 PRODUCTOS")
        print("=" * 70)
        setup_environment()
        print("\n[1/3] 📄 Document AI procesando...")
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"
        with open(file_path, "rb") as image:
            content = image.read()
        mime_type = "image/jpeg"
        if file_path.lower().endswith(".png"):
            mime_type = "image/png"
        elif file_path.lower().endswith(".pdf"):
            mime_type = "application/pdf"
        result = client.process_document(
            request=documentai.ProcessRequest(name=name, raw_document=documentai.RawDocument(content=content, mime_type=mime_type))
        )
        establecimiento = extract_vendor_name(result.document.text)
        total_factura = extract_total_invoice(result.document.text)
        productos_ai = extract_products_document_ai(result.document)
        print(f"   ✓ {len(productos_ai)} productos detectados (AI)")
        print("\n[2/3] 🔬 Tesseract OCR...")
        productos_tesseract = extract_products_tesseract_aggressive(file_path)
        print("\n[3/3] 🔀 Combinando...")
        productos_finales = combinar_y_deduplicar(productos_ai, productos_tesseract)
        tiempo_total = int(time.time() - inicio)
        print("\n" + "=" * 70)
        print("✅ RESULTADO FINAL")
        print("=" * 70)
        print(f"📍 Establecimiento: {establecimiento}")
        print(f"💰 Total factura: {total_factura}" if total_factura else "💰 Total: No detectado")
        print(f"📦 Productos únicos: {len(productos_finales)}")
        print(f"   ├─ Document AI: {len(productos_ai)}")
        print(f"   ├─ Tesseract: {len(productos_tesseract)}")
        print(f"   └─ Finales: {len(productos_finales)}")
        print(f"⏱️ Tiempo: {tiempo_total}s")
        print("=" * 70 + "\n")
        return {
            "establecimiento": establecimiento,
            "total": total_factura,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": "enhanced_columns+regex",
                "productos_ai": len(productos_ai),
                "productos_tesseract": len(productos_tesseract),
                "productos_finales": len(productos_finales),
                "tiempo_segundos": tiempo_total,
            },
        }
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        traceback.print_exc()
        return None


# Mantener compatibilidad
def process_invoice_products(file_path):
    """Versión legacy - redirige al nuevo procesador"""
    return process_invoice_complete(file_path)
