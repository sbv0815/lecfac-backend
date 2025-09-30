import os
import re
import json
import tempfile
import time
import hashlib
import shutil
from datetime import datetime
from google.cloud import documentai
import traceback
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

# ===== Tesseract (opcional) =====
try:
    import pytesseract  # paquete python
    if shutil.which("tesseract"):
        TESSERACT_AVAILABLE = True
        print("✅ Tesseract OCR disponible")
    else:
        TESSERACT_AVAILABLE = False
        print("⚠️ Tesseract no instalado en el sistema - se usará Document AI")
except ImportError:
    TESSERACT_AVAILABLE = False
    print("⚠️ Tesseract no disponible (paquete Python no instalado) - se usará Document AI")

# Reemplaza/añade cerca de tus regex de precio
PRICE_RE_ANY = r"(\d{1,3}(?:[.,]\d{3})+|\d{4,6})"  # 16,390 o 16390

def _looks_like_price_only(s: str) -> bool:
    s = (s or "").strip()
    # 16,390 / 16390 con opcional N/H al final
    if re.fullmatch(r'-?\s*\d{1,3}(?:[.,]\d{3})+\s*[NH]?', s, re.IGNORECASE):
        return True
    if re.fullmatch(r'-?\s*\d{4,6}\s*[NH]?', s, re.IGNORECASE):
        return True
    return False

def _looks_like_continuation(line: str) -> bool:
    U = line.upper()
    return (
        bool(re.search(r"\bKG\b", U))
        or bool(re.search(r"\bX\b", U))
        or bool(re.search(r"^\d+\s*X\s*\d+", U))
        or bool(re.search(r"^\d+[.,]\d+\s*KG", U))
        or _looks_like_price_only(line)            # <— NUEVO
    )


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
        if 10 < amount < 100000000:  # margen alto por seguridad
            return amount
    except Exception:
        pass
    return None


def _pick_best_price(nums):
    """Elige el número más grande como precio probable"""
    if not nums:
        return 0
    cand = [clean_amount(x) for x in nums]
    cand = [c for c in cand if c is not None]
    return max(cand) if cand else 0


def _join_item_blocks(texto: str) -> list[str]:
    """Une líneas de continuación (peso, X, etc.) a su línea principal."""
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


def _is_discount_or_note(line: str) -> bool:
    """Detecta descuentos/ajustes/notas que no son ítems"""
    U = line.upper()
    # monto negativo tipo  -3,278   -12,500
    if re.search(r'-\s*\d{1,3}(?:[.,]\d{3})+', line):
        return True
    # códigos de promo/desc: 344xxxDF.20%REF ...
    if re.search(r'\bDF\.', U):
        return True
    if '%REF' in U or (re.search(r'\bREF\b', U) and '%' in U):
        return True
    return False

def _is_weight_only_or_fragment(text: str) -> bool:
    """True si la 'descripcion' es solo peso/cantidad o texto basura corto."""
    if not text:
        return True
    s = re.sub(r'[;,:]', ' ', text).upper().strip()

    # Líneas de peso/cantidad típicas
    if re.match(r'^\d+(?:[.,]\d+)?\s*KG\b', s):
        return True
    if re.match(r'^\d+\s*X\s*\d{3,6}\b', s):  # "2 X 4200", etc.
        return True
    if re.match(r'^\d{1,3}[.,]\d{3}\b', s) and not re.search(r'[A-ZÀ-Ÿ]', s):
        return True

    # Si solo quedan "palabras" débiles (KG, X, N, H, A, E, etc.)
    tokens = re.findall(r'[A-ZÀ-Ÿ]{2,}', s)
    weak = {'KG','X','N','H','A','E','BA','C','DE','DEL','LA','EL','AL','POR'}
    strong = [t for t in tokens if t not in weak]

    # Requiere al menos una palabra "real" de 4+ letras
    return not any(len(t) >= 4 for t in strong)



# =====================
# Parsers por texto (Document AI / Tesseract)
# =====================

PRICE_RE = r"(\d{1,3}(?:[.,]\d{3})+)"      # 2,190 14,200 231,000
EAN_LONG_RE = r"\b\d{8,13}\b"

def _detect_columns(lines: list[str]):
    """Detecta columnas aprox usando el encabezado 'CODIGO ... TOTAL'."""
    code_idx, total_idx = 0, None
    for l in lines[:50]:
        U = l.upper()
        if "CODIGO" in U and "TOTAL" in U:
            code_idx = U.find("CODIGO")
            total_idx = U.rfind("TOTAL")
            break
    if total_idx is None:
        longest = max((len(x) for x in lines[:60]), default=60)
        total_idx = max(30, longest - 8)
    return code_idx, total_idx


def _parse_text_products(raw_text: str) -> list[dict]:
    """Devuelve lista [{codigo,nombre,valor,fuente}] parseando el texto crudo."""
    lines = _join_item_blocks(raw_text or "")
    print(f"📄 Bloques tras normalizar: {len(lines)}")

    code_idx, total_idx = _detect_columns(lines)

    # 1) Parser por columnas
    matched_idx = set()
    by_cols = []
    for i, l in enumerate(lines):
        if len(l) < 10:
            continue
        U = l.upper()
        if "RESOLUCION DIAN" in U or "RESPONSABLE DE IVA" in U or "AGENTE RETENEDOR" in U:
            continue
        if "****" in U or "SUBTOTAL/TOTAL" in U:
            continue
        if ("CODIGO" in U and "TOTAL" in U) or U.startswith("NRO. CUENTA") or "TARJ CRE/DEB" in U or U.startswith("VISA") or "ITEMS COMPRADOS" in U or "RESUMEN DE IVA" in U:
            continue
        if _is_discount_or_note(l):
            continue

        codigo_zone = l[: max(0, code_idx + 15)].strip()
        total_zone = l[total_idx:].strip() if total_idx < len(l) else ""
        descr_zone = l[len(codigo_zone): total_idx].strip() if total_idx > len(codigo_zone) else l.strip()

        # código (prioriza EAN largo)
        codigo = None
        m = re.findall(EAN_LONG_RE, codigo_zone)
        if m:
            codigo = m[0]

        # precio — permite con o sin separador
        nums = re.findall(PRICE_RE_ANY, total_zone) or re.findall(PRICE_RE_ANY, l)
        precio = _pick_best_price(nums)

        # nombre
        nombre = re.sub(r"\s+", " ", descr_zone).strip()
        nombre = re.sub(r"^\d{3,}\s*", "", nombre)
        nombre = nombre[:80]

        if not nombre or len(nombre) < 3:
            continue
        if not codigo and _is_weight_only_or_fragment(nombre):
            continue

        uid = hashlib.md5(f"col:{i}:{nombre}|{precio}|{codigo or ''}".encode()).hexdigest()[:10]
        by_cols.append({"uid": uid, "codigo": codigo, "nombre": nombre, "valor": precio or 0, "fuente": "text_columns"})
        matched_idx.add(i)

    # 2) Regex SOLO como fallback en líneas no cubiertas por columnas
    by_rx = []
    rx1 = re.compile(rf"(\d{{6,13}})\s+(.+?)\s+{PRICE_RE_ANY}", re.IGNORECASE)
    rx2 = re.compile(rf"([A-Za-zÀ-ÿ]{{2}}[A-Za-zÀ-ÿ0-9\s/\-\.]{{3,80}}?)\s+{PRICE_RE_ANY}(?:\s*[NXAEH])?", re.IGNORECASE)

    for i, linea in enumerate(lines):
        if i in matched_idx:
            continue
        if _is_discount_or_note(linea):
            continue
        U = linea.upper()
        if "SUBTOTAL/TOTAL" in U or "****" in U:
            continue

        m = rx1.search(linea) or rx2.search(linea)
        if not m:
            continue

        if len(m.groups()) == 3:
            g1, g2, g3 = m.groups()
            if g1.isdigit():
                codigo, nombre, precio_s = g1, g2, g3
            else:
                codigo, nombre, precio_s = None, g1, g2
        else:
            continue

        precio = clean_amount(precio_s) or 0
        nombre = re.sub(r"\s+", " ", nombre).strip()[:80]
        if not nombre or len(nombre) < 3:
            continue
        if not codigo and _is_weight_only_or_fragment(nombre):
            continue

        uid = hashlib.md5(f"rx:{i}:{nombre}|{precio}|{codigo or ''}".encode()).hexdigest()[:10]
        by_rx.append({"uid": uid, "codigo": codigo, "nombre": nombre, "valor": precio, "fuente": "text_regex"})

    # 3) merge por uid (no colapsa compras repetidas)
    seen = set()
    out = []
    for src in (by_cols, by_rx):
        for p in src:
            if p["uid"] in seen:
                continue
            seen.add(p["uid"])
            out.append(p)  # conservar uid

    return out
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
    if not raw_text:
        return None
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]

    for l in reversed(lines[-80:]):
        U = l.upper()
        if "SUBTOTAL/TOTAL" in U or re.search(r"\bTOTAL\b", U):
            nums = re.findall(PRICE_RE_ANY, l)   # <- aquí
            val = _pick_best_price(nums)
            if val:
                return val

    cand = []
    for l in lines[-40:]:
        cand += re.findall(PRICE_RE_ANY, l)      # <- y aquí
    return _pick_best_price(cand) if cand else None



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
    """Fusiona entidades de DA con el parser por texto crudo, filtrando descuentos y líneas débiles."""
    productos = []

    # 1) Entidades de Document AI (con filtros)
    line_items = [e for e in getattr(document, "entities", []) if e.type_ == "line_item" and e.confidence > 0.25]
    for e in line_items:
        raw = (e.mention_text or "").strip()
        base = f"{code or ''}|{name}|{price}"
        uid = "da:" + hashlib.md5(base.encode()).hexdigest()[:10]

        productos.append({
        "uid": uid,
        "codigo": code,
        "nombre": name,
        "valor": price,
        "fuente": "document_ai"
        })


        # descarta descuentos/notas
        if _is_discount_or_note(raw) or _is_discount_or_note(name):
            continue

        # EAN si existe
        code = None
        mm = re.search(EAN_LONG_RE, raw)
        if mm:
            code = mm.group(0)

        # si no hay código, exige nombre "fuerte"
        if not code and (_is_weight_only_or_fragment(name) or not _has_two_real_words(name)):
            continue

        # precio; si alguna propiedad viene negativa, descartar
        price = 0
        has_negative = False
        for prop in getattr(e, "properties", []):
            txt = (getattr(prop, "mention_text", "") or "")
            if "-" in txt:
                has_negative = True
            if prop.type_ == "line_item/amount" and prop.confidence > 0.2:
                price = clean_amount(txt) or price
        if has_negative:
            continue

        productos.append({"codigo": code, "nombre": name, "valor": price, "fuente": "document_ai"})

    # 2) Texto crudo SIEMPRE (agrega lo que falte)
    productos_texto = _parse_text_products(getattr(document, "text", "") or "")

    # 3) Merge simple (evita duplicados exactos, mantiene compras repetidas reales)
    seen = set()
    final = []

    def key(p):
        code = p.get("codigo")
        if code and re.fullmatch(EAN_LONG_RE, code):
            return ("ean", code)
        return ("nv", p.get("nombre", "").lower(), p.get("valor", 0))

    for src in (productos, productos_texto):
        for p in src:
            k = key(p)
            if k in seen:
                continue
            seen.add(k)
            final.append(p)

    return final


def extract_products_tesseract_aggressive(image_path):
    """Solo se usa si Tesseract está disponible. En Render normalmente NO lo está."""
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
        productos = _parse_text_products(texto)

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


def _has_two_real_words(name: str) -> bool:
    toks = re.findall(r'[A-Za-zÀ-ÿ]{4,}', name or "")
    return len(toks) >= 2




# =====================
# Combinar y flujo principal
# =====================

def combinar_y_deduplicar(productos_ai, productos_extra):
    """
    Preserva líneas repetidas reales (usa uid).
    Dedup solo si DA y Tesseract detectaron la MISMA línea (clave suave).
    """
    result = []
    seen_uid = set()
    # clave suave para evitar dobles (DA+Tess) de la misma línea
    seen_soft = set()

    def soft_key(p):
        return (
            (p.get("codigo") or "").strip(),
            (p.get("nombre") or "").strip().lower(),
            int(p.get("valor") or 0),
        )

    # Primero, todo lo de Document AI
    for p in productos_ai:
        uid = p.get("uid") or "ai:" + hashlib.md5(
            f"{p.get('codigo','')}|{p.get('nombre','')}|{p.get('valor',0)}".encode()
        ).hexdigest()[:10]
        if uid in seen_uid:
            continue
        seen_uid.add(uid)
        result.append(p)
        seen_soft.add(soft_key(p))

    # Luego, Tesseract (solo si no duplica exacto a nivel de soft_key)
    for p in productos_extra:
        uid = p.get("uid") or "tx:" + hashlib.md5(
            f"{p.get('codigo','')}|{p.get('nombre','')}|{p.get('valor',0)}".encode()
        ).hexdigest()[:10]
        if uid in seen_uid:
            continue
        sk = soft_key(p)
        if sk in seen_soft:
            # ya lo capturó DA, no lo dupliques
            continue
        seen_uid.add(uid)
        seen_soft.add(sk)
        result.append(p)

    # Convertir EAN de balanza a PLU único por línea (si aplica)
    for p in result:
        code = p.get("codigo")
        if code and _is_variable_weight_ean(code):
            h = hashlib.md5(f"{code}|{p.get('valor',0)}|{(p.get('nombre') or '')[:20]}".encode()).hexdigest()[:8]
            p["codigo"] = f"PLU_{h}"

    # Orden estable
    result.sort(key=lambda x: (not bool(x.get("codigo")), x.get("codigo") or "", x.get("nombre","")))
    return result



def process_invoice_complete(file_path):
    inicio = time.time()
    try:
        print("\n" + "=" * 70)
        print("🔍 PROCESAMIENTO MEJORADO - columnas + regex + DA")
        print("=" * 70)
        setup_environment()

        # [1/3] Document AI
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

        raw_text = result.document.text or ""
        establecimiento = extract_vendor_name(raw_text)
        total_factura = extract_total_invoice(raw_text)

        productos_ai = extract_products_document_ai(result.document)
        print(f"   ✓ {len(productos_ai)} productos detectados (DA + texto)")

        # [2/3] Tesseract (opcional) — en Render normalmente no hay
        print("\n[2/3] 🔬 Texto desde imagen (Tesseract, si disponible)...")
        productos_tesseract = extract_products_tesseract_aggressive(file_path) if TESSERACT_AVAILABLE else []

        # [3/3] Combinar
        print("\n[3/3] 🔀 Combinando...")
        productos_finales = combinar_y_deduplicar(productos_ai, productos_tesseract)

        tiempo_total = int(time.time() - inicio)
        print("\n" + "=" * 70)
        print("✅ RESULTADO FINAL")
        print("=" * 70)
        print(f"📍 Establecimiento: {establecimiento}")
        print(f"💰 Total factura: {total_factura}" if total_factura else "💰 Total: No detectado")
        print(f"📦 Productos únicos: {len(productos_finales)}")
        print(f"   ├─ DA + Texto: {len(productos_ai)}")
        print(f"   ├─ Tesseract: {len(productos_tesseract)}")
        print(f"   └F─ Finales: {len(productos_finales)}")
        print(f"⏱️ Tiempo: {tiempo_total}s")
        print("=" * 70 + "\n")

        return {
            "establecimiento": establecimiento,
            "total": total_factura,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": "docai_text_columns+regex(+tess)",
                "docai_text_items": len(productos_ai),
                "tesseract_items": len(productos_tesseract),
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
