import os
import re
import json
import time
import hashlib
import shutil
import tempfile
import traceback
from datetime import datetime

# =============================
# Disponibilidad de librerías
# =============================
try:
    from google.cloud import documentai
    DOCUMENT_AI_AVAILABLE = True
    print("✅ Document AI disponible")
except Exception:
    DOCUMENT_AI_AVAILABLE = False
    print("❌ Document AI no disponible")

try:
    from PIL import Image, ImageEnhance
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

# ===== Tesseract (opcional) =====
try:
    import pytesseract
    # Permite override por ENV o usa /usr/bin/tesseract si existe
    TESSERACT_CMD = os.getenv("TESSERACT_CMD") or shutil.which("tesseract") or "/usr/bin/tesseract"
    if TESSERACT_CMD and os.path.exists(TESSERACT_CMD):
        pytesseract.pytesseract.tesseract_cmd = TESSERACT_CMD
        TESSERACT_AVAILABLE = True
        print(f"✅ Tesseract OCR disponible en: {TESSERACT_CMD}")
    else:
        TESSERACT_AVAILABLE = False
        print("⚠️ Tesseract no encontrado en el sistema - usando solo Document AI")
except Exception:
    TESSERACT_AVAILABLE = False
    print("⚠️ pytesseract no disponible - usando solo Document AI")


# =============================
# Preprocesamiento de imagen
# =============================
def preprocess_for_document_ai(image_path: str) -> str:
    """
    Preprocesa la imagen para subir resolución y nitidez,
    lo que suele mejorar el reconocimiento de Document AI.
    """
    if not PIL_AVAILABLE:
        return image_path
    try:
        img = Image.open(image_path)
        orig_size = img.size

        # Document AI rinde mejor con imágenes "grandes"
        width, height = img.size
        min_width = 3000
        if width < min_width:
            scale = min_width / float(width)
            new_size = (int(width * scale), int(height * scale))
            # PIL>=9: Image.Resampling.LANCZOS; fallback a Image.LANCZOS si no existe
            resampling = getattr(Image, "Resampling", Image).LANCZOS
            img = img.resize(new_size, resampling)
            print(f"🔧 Escalado: {orig_size} -> {new_size}")

        # Contraste + Nitidez
        img = ImageEnhance.Contrast(img).enhance(1.8)
        img = ImageEnhance.Sharpness(img).enhance(1.5)

        processed_path = image_path
        if image_path.lower().endswith(".png"):
            processed_path = image_path[:-4] + "_optimized.jpg"
        elif image_path.lower().endswith(".jpg") or image_path.lower().endswith(".jpeg"):
            processed_path = image_path[:-4] + "_optimized.jpg"
        else:
            processed_path = image_path + "_optimized.jpg"

        img.save(processed_path, "JPEG", quality=95, optimize=False, subsampling=0)
        print(f"🖼️ Imagen optimizada: {processed_path}")
        return processed_path
    except Exception as e:
        print(f"⚠️ Error preprocesando imagen: {e}")
        return image_path


def split_long_invoice(image_path: str) -> list:
    """
    Si la factura es MUY larga, la divide en 2-3 secciones con solape.
    Document AI a veces mejora así el parseo de ítems.
    """
    if not PIL_AVAILABLE:
        return [image_path]
    try:
        img = Image.open(image_path)
        w, h = img.size
        if h <= max(5000, 4 * w):
            return [image_path]

        print(f"🪓 Factura larga detectada {w}x{h}. Dividiendo...")
        sections = []
        num_sections = 2 if h < 8000 else 3
        section_h = h // num_sections
        overlap = 300

        for i in range(num_sections):
            y1 = max(0, i * section_h - overlap)
            y2 = min(h, (i + 1) * section_h + overlap)
            crop = img.crop((0, y1, w, y2))
            out_path = image_path.rsplit(".", 1)[0] + f"_section{i+1}.jpg"
            crop.save(out_path, "JPEG", quality=95)
            sections.append(out_path)
            print(f"  • Sección {i+1}: {y1}..{y2} -> {out_path}")

        return sections
    except Exception as e:
        print(f"⚠️ Error dividiendo imagen: {e}")
        return [image_path]


# =============================
# Configuración de credenciales
# =============================
def setup_document_ai():
    required_vars = [
        "GCP_PROJECT_ID",
        "DOC_AI_LOCATION",
        "DOC_AI_PROCESSOR_ID",
        "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    ]
    for var in required_vars:
        if not os.environ.get(var):
            raise Exception(f"Variable requerida: {var}")

    creds_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    try:
        json.loads(creds_json)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(creds_json)
            cred_path = f.name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path
        return True
    except Exception:
        raise Exception("Credenciales JSON inválidas")


# =============================
# Utilidades de texto / precios
# =============================
PRICE_RE = r"(\d{1,3}(?:[.,]\d{3})+)"            # 16,390
PRICE_RE_ANY = r"(\d{1,3}(?:[.,]\d{3})+|\d{4,6})" # 16,390 o 16390
EAN_LONG_RE = r"\b\d{8,13}\b"

def clean_amount(s):
    if not s:
        return None
    s = str(s)
    m = re.search(PRICE_RE_ANY, s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", "")
    try:
        val = int(num)
        if 10 < val < 100_000_000:
            return val
    except Exception:
        pass
    return None

def _pick_best_price(nums):
    if not nums:
        return 0
    cand = [clean_amount(x) for x in nums]
    cand = [c for c in cand if c is not None]
    return max(cand) if cand else 0

def _looks_like_price_only(s: str) -> bool:
    s = (s or "").strip()
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
        or _looks_like_price_only(line)
    )

def _join_item_blocks(texto: str) -> list:
    raw = [l.strip() for l in (texto or "").splitlines() if l.strip()]
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

def _is_discount_or_note(line: str) -> bool:
    U = (line or "").upper()
    if re.search(r'-\s*\d{1,3}(?:[.,]\d{3})+', line):  # montos negativos
        return True
    if re.search(r'\bDF\.', U) or '%REF' in U or (re.search(r'\bREF\b', U) and '%' in U):
        return True
    if "SUBTOTAL/TOTAL" in U or "RESUMEN DE IVA" in U:
        return True
    return False

def _is_weight_only_or_fragment(text: str) -> bool:
    if not text:
        return True
    s = re.sub(r'[;,:]', ' ', text).upper().strip()
    if re.match(r'^\d+(?:[.,]\d+)?\s*KG\b', s):
        return True
    if re.match(r'^\d+\s*X\s*\d{3,6}\b', s):
        return True
    if re.match(r'^\d{1,3}[.,]\d{3}\b', s) and not re.search(r'[A-ZÀ-Ÿ]', s):
        return True
    tokens = re.findall(r'[A-ZÀ-Ÿ]{2,}', s)
    weak = {'KG','X','N','H','A','E','BA','C','DE','DEL','LA','EL','AL','POR'}
    strong = [t for t in tokens if t not in weak]
    return not any(len(t) >= 4 for t in strong)

def _has_two_real_words(name: str) -> bool:
    toks = re.findall(r'[A-Za-zÀ-ÿ]{4,}', name or "")
    return len(toks) >= 2

def _detect_columns(lines: list):
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


# =============================
# Parsers por texto crudo
# =============================
def _parse_text_products(raw_text: str) -> list:
    lines = _join_item_blocks(raw_text or "")
    print(f"📄 Bloques tras normalizar: {len(lines)}")

    code_idx, total_idx = _detect_columns(lines)

    matched_idx = set()
    by_cols = []
    for i, l in enumerate(lines):
        if len(l) < 10:
            continue
        if _is_discount_or_note(l):
            continue

        U = l.upper()
        if ("CODIGO" in U and "TOTAL" in U) or U.startswith("NRO. CUENTA") or "TARJ CRE/DEB" in U or U.startswith("VISA") or "ITEMS COMPRADOS" in U:
            continue
        if "RESOLUCION DIAN" in U or "RESPONSABLE DE IVA" in U or "AGENTE RETENEDOR" in U:
            continue

        codigo_zone = l[: max(0, code_idx + 15)].strip()
        total_zone = l[total_idx:].strip() if total_idx < len(l) else ""
        descr_zone = l[len(codigo_zone): total_idx].strip() if total_idx > len(codigo_zone) else l.strip()

        codigo = None
        mm = re.findall(EAN_LONG_RE, codigo_zone)
        if mm:
            codigo = mm[0]

        nums = re.findall(PRICE_RE_ANY, total_zone) or re.findall(PRICE_RE_ANY, l)
        precio = _pick_best_price(nums)

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

    seen = set()
    out = []
    for src in (by_cols, by_rx):
        for p in src:
            if p["uid"] in seen:
                continue
            seen.add(p["uid"])
            out.append({k: v for k, v in p.items() if k != "uid"})
    return out


# =============================
# Extractores de metadata
# =============================
def extract_vendor(text):
    patterns = [
        r"(JUMBO\s+[A-ZÁÉÍÓÚÜÑ\s]+)",
        r"(ALMACENES\s+É?XITO[^\n]*)",
        r"(CARULLA[^\n]*)",
        r"(OLIMPICA[^\n]*)",
        r"(D1[^\n]*)",
        r"(CRUZ\s+VERDE[^\n]*)",
        r"(ALKOSTO[^\n]*)",
    ]
    for pat in patterns:
        m = re.search(pat, text or "", re.IGNORECASE)
        if m:
            return re.sub(r"\s+", " ", m.group(1).strip())[:50]
    return "Establecimiento no identificado"

def extract_total(text):
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    for l in reversed(lines[-80:]):
        U = l.upper()
        if "SUBTOTAL/TOTAL" in U or re.search(r"\bTOTAL\b", U):
            nums = re.findall(PRICE_RE_ANY, l)
            val = _pick_best_price(nums)
            if val:
                return val
    cand = []
    for l in lines[-40:]:
        cand += re.findall(PRICE_RE_ANY, l)
    return _pick_best_price(cand) if cand else None


# =============================
# Extractores de productos
# =============================
def extract_products_document_ai(document) -> list:
    """
    Usa DOCAI entities + parser de texto crudo del documento.
    Filtra descuentos, notas y líneas débiles.
    """
    productos = []

    # 1) Entities (line_item)
    line_items = [e for e in getattr(document, "entities", []) if e.type_ == "line_item" and e.confidence > 0.20]
    for e in line_items:
        raw = (getattr(e, "mention_text", "") or "").strip()
        if not raw:
            continue

        if _is_discount_or_note(raw):
            continue

        code = None
        mm = re.search(EAN_LONG_RE, raw)
        if mm:
            code = mm.group(0)

        name = re.sub(r"\s+", " ", raw)[:80] or "Sin descripción"

        if not code and (_is_weight_only_or_fragment(name) or not _has_two_real_words(name)):
            continue

        price = 0
        has_negative = False
        for prop in getattr(e, "properties", []):
            txt = (getattr(prop, "mention_text", "") or "")
            if "-" in txt:
                has_negative = True
            if getattr(prop, "type_", "") == "line_item/amount" and getattr(prop, "confidence", 0) > 0.2:
                price = clean_amount(txt) or price
        if has_negative:
            continue

        productos.append({"codigo": code, "nombre": name, "valor": price, "fuente": "document_ai"})

    # 2) Texto crudo siempre
    prod_text = _parse_text_products(getattr(document, "text", "") or "")

    # 3) Merge simple (EAN exacto o nombre+valor)
    seen = set()
    final = []

    def key(p):
        code = p.get("codigo")
        if code and re.fullmatch(EAN_LONG_RE, code):
            return ("ean", code)
        return ("nv", (p.get("nombre") or "").lower(), p.get("valor", 0))

    for src in (productos, prod_text):
        for p in src:
            k = key(p)
            if k in seen:
                continue
            seen.add(k)
            final.append(p)

    return final


def extract_products_tesseract_aggressive(image_path) -> list:
    """
    Ejecuta pytesseract en varios modos y parsea con el mismo parser de texto crudo.
    En Render puede que Tesseract no esté; en ese caso regresa [].
    """
    if not TESSERACT_AVAILABLE or not PIL_AVAILABLE:
        return []
    try:
        img = Image.open(image_path)
        texts = []
        try:
            texts.append(pytesseract.image_to_string(img, lang="spa", config="--psm 6 --oem 3"))
            texts.append(pytesseract.image_to_string(img, lang="spa", config="--psm 4 --oem 3"))
            texts.append(pytesseract.image_to_string(img, lang="spa", config="--psm 11 --oem 3"))
        except Exception as oe:
            print(f"⚠️ Error pytesseract: {oe}")
            return []
        texto = "\n".join(texts)
        productos = _parse_text_products(texto)
        print(f"✓ Productos Tesseract: {len(productos)}")
        return productos
    except Exception as e:
        print(f"⚠️ Error Tesseract: {e}")
        traceback.print_exc()
        return []


# =============================
# Merge / Dedupe
# =============================
def _is_variable_weight_ean(code: str) -> bool:
    return bool(code) and len(code) in (12, 13) and code[:2] in ("20", "28", "29")

def combinar_y_deduplicar(prod_a, prod_b) -> list:
    out = {}

    for prod in (prod_a or []) + (prod_b or []):
        codigo = prod.get("codigo")

        # EAN de balanza → PLU único
        if codigo and _is_variable_weight_ean(codigo):
            base = f"{codigo}|{prod.get('nombre','')[:20]}|{prod.get('valor',0)}"
            codigo = "PLU_" + hashlib.md5(base.encode()).hexdigest()[:8]
            prod["codigo"] = codigo

        if codigo:
            if codigo not in out:
                out[codigo] = prod
            else:
                if out[codigo].get("valor", 0) == 0 and prod.get("valor", 0) > 0:
                    out[codigo]["valor"] = prod["valor"]
        else:
            key = ("nv", (prod.get("nombre") or "").lower(), prod.get("valor", 0))
            if key not in out:
                out[key] = prod

    result = list(out.values())
    result.sort(key=lambda x: (not bool(x.get("codigo")), x.get("codigo") or "", x.get("nombre", "")))
    return result


# =============================
# Pipeline principal
# =============================
def process_invoice_complete(file_path):
    inicio = time.time()
    try:
        print("\n" + "=" * 70)
        print("🔍 PROCESAMIENTO MEJORADO - columnas + regex + DA (+tess opcional)")
        print("=" * 70)

        if not DOCUMENT_AI_AVAILABLE:
            raise Exception("Document AI no disponible")

        setup_document_ai()

        # Preprocesado para DOCAI y split si es larga
        optimized = preprocess_for_document_ai(file_path)
        sections = split_long_invoice(optimized)

        # Document AI por secciones
        client = documentai.DocumentProcessorServiceClient()
        name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"

        all_products_docai = []
        all_text = []
        for sec in sections:
            with open(sec, "rb") as f:
                content = f.read()
            mime = "image/jpeg" if sec.lower().endswith((".jpg", ".jpeg")) else "image/png"
            resp = client.process_document(
                request=documentai.ProcessRequest(
                    name=name,
                    raw_document=documentai.RawDocument(content=content, mime_type=mime),
                )
            )
            all_products_docai += extract_products_document_ai(resp.document)
            all_text.append(resp.document.text or "")

        raw_text = "\n".join(all_text)

        # Metadata
        establecimiento = extract_vendor(raw_text)
        total_factura = extract_total(raw_text)

        print(f"   ✓ Productos (DOCAI+texto): {len(all_products_docai)}")

        # Tesseract opcional (sobre la imagen optimizada, no las secciones)
        print("\n[2/3] 🔬 Tesseract (si está disponible)...")
        productos_tess = extract_products_tesseract_aggressive(optimized) if TESSERACT_AVAILABLE else []

        # Merge final
        print("\n[3/3] 🔀 Combinando...")
        productos_finales = combinar_y_deduplicar(all_products_docai, productos_tess)

        tiempo = int(time.time() - inicio)
        print("\n" + "=" * 70)
        print("✅ RESULTADO FINAL")
        print("=" * 70)
        print(f"📍 Establecimiento: {establecimiento}")
        print(f"💰 Total factura: {total_factura}" if total_factura else "💰 Total: No detectado")
        print(f"📦 Productos únicos: {len(productos_finales)}")
        print(f"   ├─ DOCAI+Texto: {len(all_products_docai)}")
        print(f"   ├─ Tesseract: {len(productos_tess)}")
        print(f"   └─ Finales: {len(productos_finales)}")
        print(f"⏱️ Tiempo: {tiempo}s")
        print("=" * 70 + "\n")

        # Mostrar primeros 5 para depurar
        if productos_finales:
            print("Primeros 5 productos:")
            for p in productos_finales[:5]:
                precio = f"${p.get('valor',0):,}" if p.get("valor", 0) else "Sin precio"
                print(f"  {p.get('codigo','(s/c)')}: {p.get('nombre')} - {precio}")

        return {
            "establecimiento": establecimiento,
            "total": total_factura,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": "docai_text_columns+regex(+tess)",
                "docai_text_items": len(all_products_docai),
                "tesseract_items": len(productos_tess),
                "productos_finales": len(productos_finales),
                "tiempo_segundos": tiempo,
            },
        }
    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        traceback.print_exc()
        return None


# Alias legacy
def process_invoice_products(file_path):
    return process_invoice_complete(file_path)

