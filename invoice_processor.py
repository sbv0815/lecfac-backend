# invoice_processor.py - SISTEMA 3 NIVELES + FILTROS ANTI-BASURA
import os
import re
import json
import time
import hashlib
import tempfile
import traceback
from datetime import datetime
import shutil

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

    _tcmd = (
        os.getenv("TESSERACT_CMD") or shutil.which("tesseract") or "/usr/bin/tesseract"
    )
    pytesseract.pytesseract.tesseract_cmd = _tcmd
    TESSERACT_AVAILABLE = bool(_tcmd and os.path.exists(_tcmd))
    print(
        f"✅ Tesseract OCR {'disponible' if TESSERACT_AVAILABLE else 'NO disponible'} en: {_tcmd}"
    )
except Exception as e:
    TESSERACT_AVAILABLE = False
    print(f"⚠️ pytesseract no disponible ({e})")


# =============================
# Preprocesamiento de imagen
# =============================
def preprocess_for_document_ai(image_path: str) -> str:
    """Preprocesa la imagen para Document AI"""
    if not PIL_AVAILABLE:
        return image_path
    try:
        img = Image.open(image_path)
        orig_size = img.size

        width, height = img.size
        min_width = 3000
        if width < min_width:
            scale = min_width / float(width)
            new_size = (int(width * scale), int(height * scale))
            resampling = getattr(Image, "Resampling", Image).LANCZOS
            img = img.resize(new_size, resampling)
            print(f"🔧 Escalado: {orig_size} -> {new_size}")

        img = ImageEnhance.Contrast(img).enhance(1.8)
        img = ImageEnhance.Sharpness(img).enhance(1.5)

        processed_path = image_path
        if image_path.lower().endswith(".png"):
            processed_path = image_path[:-4] + "_optimized.jpg"
        elif image_path.lower().endswith(".jpg") or image_path.lower().endswith(
            ".jpeg"
        ):
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
    """Divide facturas muy largas en secciones"""
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


def setup_document_ai():
    """Configura credenciales de Document AI"""
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
PRICE_RE = r"(\d{1,3}(?:[.,]\d{3})+)"
PRICE_RE_ANY = r"(\d{1,3}(?:[.,]\d{3})+|\d{3,6})"  # ✅ Acepta desde $100
EAN_LONG_RE = r"\b\d{6,13}\b"  # ✅ Acepta desde 6 dígitos (PLU)


def clean_amount(s):
    """Limpia y convierte strings a números"""
    if not s:
        return None
    s = str(s)
    m = re.search(PRICE_RE_ANY, s)
    if not m:
        return None
    num = m.group(1).replace(".", "").replace(",", "")
    try:
        val = int(num)
        # ✅ Acepta desde $50 (productos baratos válidos)
        if 50 < val < 100_000_000:
            return val
    except Exception:
        pass
    return None


def _pick_best_price(nums):
    """Selecciona el mejor precio de una lista"""
    if not nums:
        return 0
    cand = [clean_amount(x) for x in nums]
    cand = [c for c in cand if c is not None]
    return max(cand) if cand else 0


# =============================
# FILTROS ANTI-BASURA MEJORADOS
# =============================


def _is_obvious_garbage(text: str) -> bool:
    """
    Detecta SOLO basura OBVIA (no productos)

    ✅ MÁS PERMISIVO: Solo rechaza lo que CLARAMENTE no es producto
    ❌ NO rechaza nombres cortos válidos como "Sal", "Ajo", "Pan"
    """
    if not text:
        return True

    text_upper = text.upper().strip()

    # Lista REDUCIDA de palabras clave de basura
    basura_obvia = [
        # Descuentos y promociones
        "AHORRO",
        "DESCUENTO",
        "DESC",
        "DTO",
        "REBAJA",
        "PROMOCION",
        "DCTO",
        "V AHORRO",
        "VAHORRO",
        "PRECIO FINAL",
        # Impuestos y totales
        "IVA",
        "IMPUESTO",
        "SUBTOTAL",
        "TOTAL A PAGAR",
        "GRAN TOTAL",
        "CAMBIO",
        "EFECTIVO",
        "TARJETA",
        "REDEBAN",
        # Info administrativa
        "RESOLUCION DIAN",
        "RESPONSABLE DE IVA",
        "AGENTE RETENEDOR",
        "NIT",
        "AUTORETENEDOR",
        "GRACIAS POR SU COMPRA",
        "CAJERO",
        "CAJA",
        "FACTURA",
        "TICKET",
        # Cadenas específicas
        "TOSHIBA",
        "GLOBAL COMMERCE",
        "CADENA S.A",
    ]

    # ❌ Rechazar si contiene palabras de basura
    if any(palabra in text_upper for palabra in basura_obvia):
        return True

    # ❌ Rechazar si es SOLO números/símbolos (sin letras)
    if not re.search(r"[A-Za-zÀ-ÿ]", text):
        return True

    # ❌ Rechazar si son SOLO unidades de medida
    if text_upper in ["KG", "KGM", "/KGM", "/KG", "UND", "/U", "X"]:
        return True

    # ✅ TODO LO DEMÁS es potencialmente válido
    return False


def _looks_like_price_only(s: str) -> bool:
    """Detecta si es solo un precio (sin nombre de producto)"""
    s = (s or "").strip()
    if re.fullmatch(r"-?\s*\d{1,3}(?:[.,]\d{3})+\s*[NH]?", s, re.IGNORECASE):
        return True
    if re.fullmatch(r"-?\s*\d{3,6}\s*[NH]?", s, re.IGNORECASE):
        return True
    return False


def _is_discount_or_note(line: str) -> bool:
    """Detecta descuentos o notas (montos negativos)"""
    if not line:
        return False

    U = line.upper()

    # Montos negativos (descuentos)
    if re.search(r"-\s*\d{1,3}(?:[.,]\d{3})+", line):
        return True

    # Referencias a descuentos
    if re.search(r"\bDF\.", U) or "%REF" in U:
        return True

    # Líneas de resumen
    if "SUBTOTAL/TOTAL" in U or "RESUMEN DE IVA" in U:
        return True

    return False


def _calculate_confidence_level(codigo: str, nombre: str, precio: float) -> int:
    """
    Calcula el nivel de confianza del producto

    NIVEL 1 (✅): Código + Nombre + Precio
    NIVEL 2 (⚠️): Nombre + Precio (sin código)
    NIVEL 3 (⚡): Solo Nombre o Solo Precio
    0: Rechazar
    """
    tiene_codigo = bool(codigo and len(codigo) >= 4 and codigo.isdigit())
    tiene_nombre = bool(nombre and len(nombre) >= 2 and not _is_obvious_garbage(nombre))
    tiene_precio = bool(precio and precio >= 50)

    if tiene_codigo and tiene_nombre and tiene_precio:
        return 1  # Alta confianza
    elif tiene_nombre and tiene_precio:
        return 2  # Media confianza
    elif tiene_nombre or (tiene_codigo and precio >= 100):
        return 3  # Baja confianza
    else:
        return 0  # Rechazar


def _looks_like_continuation(line: str) -> bool:
    """Detecta si es una línea de continuación"""
    U = line.upper()
    return (
        bool(re.search(r"\bKG\b", U))
        or bool(re.search(r"\bX\b", U))
        or bool(re.search(r"^\d+\s*X\s*\d+", U))
        or bool(re.search(r"^\d+[.,]\d+\s*KG", U))
        or _looks_like_price_only(line)
    )


def _join_item_blocks(texto: str) -> list:
    """Une bloques de items que están en múltiples líneas"""
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


def _detect_columns(lines: list):
    """Detecta las columnas de código y total en la factura"""
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


def _join_name_with_next_price(lines: list) -> list:
    """Une nombres sin precio con la línea siguiente si es solo precio"""
    out = []
    i = 0
    while i < len(lines):
        cur = lines[i].strip()
        nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""

        # ✅ Detectar nombre real (2+ palabras de 2+ letras)
        has_real_name = len(re.findall(r"[A-Za-zÀ-ÿ]{2,}", cur)) >= 2
        has_price_cur = bool(re.search(PRICE_RE_ANY, cur))

        if has_real_name and not has_price_cur and _looks_like_price_only(nxt):
            out.append(f"{cur} {nxt}")
            i += 2
        else:
            out.append(cur)
            i += 1
    return out


# =============================
# Parser de texto crudo MEJORADO
# =============================
def _parse_text_products(raw_text: str) -> list:
    """
    Parser de texto con SISTEMA DE 3 NIVELES
    Más permisivo pero con filtros anti-basura inteligentes
    """
    lines = _join_item_blocks(raw_text or "")
    lines = _join_name_with_next_price(lines)
    print(f"📄 Bloques tras normalizar: {len(lines)}")

    code_idx, total_idx = _detect_columns(lines)

    matched_idx = set()
    by_cols = []

    for i, l in enumerate(lines):
        # ✅ Aceptar líneas más cortas (5+ caracteres, antes 10+)
        if len(l) < 5:
            continue

        # ❌ Filtrar descuentos y basura obvia
        if _is_discount_or_note(l):
            continue

        U = l.upper()

        # ❌ Líneas de header o footer
        if (
            ("CODIGO" in U and "TOTAL" in U)
            or U.startswith("NRO. CUENTA")
            or "TARJ CRE/DEB" in U
        ):
            continue
        if (
            "RESOLUCION DIAN" in U
            or "RESPONSABLE DE IVA" in U
            or "AGENTE RETENEDOR" in U
        ):
            continue

        # Extraer zonas
        codigo_zone = l[: max(0, code_idx + 15)].strip()
        total_zone = l[total_idx:].strip() if total_idx < len(l) else ""
        descr_zone = (
            l[len(codigo_zone) : total_idx].strip()
            if total_idx > len(codigo_zone)
            else l.strip()
        )

        # Extraer código
        codigo = None
        mm = re.findall(EAN_LONG_RE, codigo_zone)
        if mm:
            codigo = mm[0]

        # Extraer precio
        nums = re.findall(PRICE_RE_ANY, total_zone) or re.findall(PRICE_RE_ANY, l)
        precio = _pick_best_price(nums)

        # Limpiar nombre
        nombre = re.sub(r"\s+", " ", descr_zone).strip()
        nombre = re.sub(r"^\d{3,}\s*", "", nombre)  # Quitar código del inicio
        nombre = nombre[:80]

        # ❌ Filtrar basura obvia
        if _is_obvious_garbage(nombre):
            continue

        # ✅ Calcular nivel de confianza
        nivel = _calculate_confidence_level(codigo, nombre, precio)

        if nivel > 0:
            uid = hashlib.md5(
                f"col:{i}:{nombre}|{precio}|{codigo or ''}".encode()
            ).hexdigest()[:10]
            by_cols.append(
                {
                    "uid": uid,
                    "codigo": codigo,
                    "nombre": nombre,
                    "valor": precio or 0,
                    "fuente": "text_columns",
                    "nivel_confianza": nivel,
                }
            )
            matched_idx.add(i)

    # Parser por regex (fallback)
    by_rx = []
    rx1 = re.compile(rf"(\d{{6,13}})\s+(.+?)\s+{PRICE_RE_ANY}", re.IGNORECASE)
    rx2 = re.compile(
        rf"([A-Za-zÀ-ÿ]{{2}}[A-Za-zÀ-ÿ0-9\s/\-\.]{{2,80}}?)\s+{PRICE_RE_ANY}(?:\s*[NXAEH])?",
        re.IGNORECASE,
    )

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

        # ❌ Filtrar basura
        if _is_obvious_garbage(nombre):
            continue

        # ✅ Calcular nivel
        nivel = _calculate_confidence_level(codigo, nombre, precio)

        if nivel > 0:
            uid = hashlib.md5(
                f"rx:{i}:{nombre}|{precio}|{codigo or ''}".encode()
            ).hexdigest()[:10]
            by_rx.append(
                {
                    "uid": uid,
                    "codigo": codigo,
                    "nombre": nombre,
                    "valor": precio,
                    "fuente": "text_regex",
                    "nivel_confianza": nivel,
                }
            )

    # Combinar y deduplicar
    seen = set()
    out = []
    for src in (by_cols, by_rx):
        for p in src:
            if p["uid"] in seen:
                continue
            seen.add(p["uid"])
            out.append({k: v for k, v in p.items() if k != "uid"})

    print(f"✅ Parser texto: {len(out)} productos extraídos")
    return out


# =============================
# Extractores de metadata
# =============================
def extract_vendor(text):
    """Extrae el nombre del establecimiento"""
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
    """Extrae el total de la factura"""
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
# Extractores de productos (DocAI)
# =============================
def extract_products_document_ai(document) -> list:
    """
    Extrae productos de Document AI con sistema de niveles
    """
    productos = []

    line_items = [
        e
        for e in getattr(document, "entities", [])
        if getattr(e, "type_", "") == "line_item" and getattr(e, "confidence", 0) > 0.20
    ]

    for e in line_items:
        raw = (getattr(e, "mention_text", "") or "").strip()
        if not raw:
            continue

        if _is_discount_or_note(raw):
            continue

        # Extraer código
        code = None
        mm = re.search(EAN_LONG_RE, raw)
        if mm:
            code = mm.group(0)

        name = re.sub(r"\s+", " ", raw)[:80] or "Sin descripción"

        # ❌ Filtrar basura
        if _is_obvious_garbage(name):
            continue

        # Extraer precio
        price = 0
        has_negative = False
        for prop in getattr(e, "properties", []):
            txt = getattr(prop, "mention_text", "") or ""
            if "-" in txt:
                has_negative = True
            if (
                getattr(prop, "type_", "") == "line_item/amount"
                and getattr(prop, "confidence", 0) > 0.2
            ):
                price = clean_amount(txt) or price

        if has_negative:
            continue

        # ✅ Calcular nivel
        nivel = _calculate_confidence_level(code, name, price)

        if nivel > 0:
            productos.append(
                {
                    "codigo": code,
                    "nombre": name,
                    "valor": price,
                    "fuente": "document_ai",
                    "nivel_confianza": nivel,
                }
            )

    # Parser de texto crudo
    prod_text = _parse_text_products(getattr(document, "text", "") or "")

    # Merge por código o nombre+precio
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

    print(f"✅ DocAI + Texto: {len(final)} productos únicos")
    return final


# =============================
# Tesseract (fallback)
# =============================
def extract_products_tesseract_aggressive(image_path) -> list:
    """Extrae productos con Tesseract OCR"""
    if not TESSERACT_AVAILABLE or not PIL_AVAILABLE:
        return []
    try:
        img = Image.open(image_path)
        texts = []
        try:
            texts.append(
                pytesseract.image_to_string(img, lang="spa", config="--psm 6 --oem 3")
            )
            texts.append(
                pytesseract.image_to_string(img, lang="spa", config="--psm 4 --oem 3")
            )
            texts.append(
                pytesseract.image_to_string(img, lang="spa", config="--psm 11 --oem 3")
            )
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
# Merge / Dedupe con niveles
# =============================
def _is_variable_weight_ean(code: str) -> bool:
    """Detecta EAN de productos pesados (balanza)"""
    return bool(code) and len(code) in (12, 13) and code[:2] in ("20", "28", "29")


def combinar_y_deduplicar(prod_a, prod_b) -> list:
    """
    Combina y deduplica productos con prioridad por nivel de confianza
    """
    out = {}

    for prod in (prod_a or []) + (prod_b or []):
        codigo = prod.get("codigo")
        nivel = prod.get("nivel_confianza", 3)

        # EAN de balanza → PLU único
        if codigo and _is_variable_weight_ean(codigo):
            base = f"{codigo}|{prod.get('nombre','')[:20]}|{prod.get('valor',0)}"
            codigo = "PLU_" + hashlib.md5(base.encode()).hexdigest()[:8]
            prod["codigo"] = codigo

        if codigo:
            if codigo not in out:
                out[codigo] = prod
            else:
                # Mantener el de mejor nivel de confianza
                if nivel < out[codigo].get("nivel_confianza", 3):
                    out[codigo] = prod
                elif nivel == out[codigo].get("nivel_confianza", 3):
                    # Mismo nivel - mantener el de mayor precio
                    if prod.get("valor", 0) > out[codigo].get("valor", 0):
                        out[codigo] = prod
        else:
            key = ("nv", (prod.get("nombre") or "").lower(), prod.get("valor", 0))
            if key not in out:
                out[key] = prod
            else:
                # Mantener el de mejor nivel
                if nivel < out[key].get("nivel_confianza", 3):
                    out[key] = prod

    result = list(out.values())
    result.sort(
        key=lambda x: (
            x.get("nivel_confianza", 3),  # Por nivel (1, 2, 3)
            not bool(x.get("codigo")),  # Con código primero
            x.get("codigo") or "",
            x.get("nombre", ""),
        )
    )

    return result


# =============================
# Pipeline principal
# =============================
def process_invoice_complete(file_path: str):
    """Pipeline completo con sistema de 3 niveles"""
    inicio = time.time()
    try:
        print("\n" + "=" * 70)
        print("🔍 PROCESAMIENTO MEJORADO - Sistema 3 Niveles + Anti-Basura")
        print("=" * 70)

        is_pdf = file_path.lower().endswith(".pdf")

        all_products_docai = []
        all_text = []
        establecimiento = None
        total_factura = None

        # [1] Document AI
        used_docai = False
        if DOCUMENT_AI_AVAILABLE:
            try:
                setup_document_ai()
                client = documentai.DocumentProcessorServiceClient()
                name = f"projects/{os.environ['GCP_PROJECT_ID']}/locations/{os.environ['DOC_AI_LOCATION']}/processors/{os.environ['DOC_AI_PROCESSOR_ID']}"

                if is_pdf:
                    with open(file_path, "rb") as f:
                        content = f.read()
                    resp = client.process_document(
                        request=documentai.ProcessRequest(
                            name=name,
                            raw_document=documentai.RawDocument(
                                content=content, mime_type="application/pdf"
                            ),
                        )
                    )
                    all_products_docai += extract_products_document_ai(resp.document)
                    all_text.append(resp.document.text or "")
                else:
                    optimized = (
                        preprocess_for_document_ai(file_path)
                        if PIL_AVAILABLE
                        else file_path
                    )
                    sections = (
                        split_long_invoice(optimized) if PIL_AVAILABLE else [optimized]
                    )
                    for sec in sections:
                        with open(sec, "rb") as f:
                            content = f.read()
                        mime = (
                            "image/jpeg"
                            if sec.lower().endswith((".jpg", ".jpeg"))
                            else "image/png"
                        )
                        resp = client.process_document(
                            request=documentai.ProcessRequest(
                                name=name,
                                raw_document=documentai.RawDocument(
                                    content=content, mime_type=mime
                                ),
                            )
                        )
                        all_products_docai += extract_products_document_ai(
                            resp.document
                        )
                        all_text.append(resp.document.text or "")

                raw_text = "\n".join(all_text)
                establecimiento = extract_vendor(raw_text)
                total_factura = extract_total(raw_text)
                used_docai = True

            except Exception as de:
                print(f"⚠️ Document AI no usable: {de}")
                traceback.print_exc()
        else:
            print("ℹ️ Saltando Document AI (no disponible)")

        # [2] Tesseract (opcional)
        productos_tess = []
        if TESSERACT_AVAILABLE and not is_pdf:
            print("\n[2/3] 🔬 Tesseract (fallback)...")
            optimized_for_tess = (
                preprocess_for_document_ai(file_path) if PIL_AVAILABLE else file_path
            )
            productos_tess = (
                extract_products_tesseract_aggressive(optimized_for_tess) or []
            )

        # [3] Merge y dedupe
        print("\n[3/3] 🔀 Combinando con sistema de niveles...")
        productos_finales = combinar_y_deduplicar(all_products_docai, productos_tess)

        # Estadísticas por nivel
        nivel_1 = len([p for p in productos_finales if p.get("nivel_confianza") == 1])
        nivel_2 = len([p for p in productos_finales if p.get("nivel_confianza") == 2])
        nivel_3 = len([p for p in productos_finales if p.get("nivel_confianza") == 3])

        tiempo = int(time.time() - inicio)
        print("\n" + "=" * 70)
        print("✅ RESULTADO FINAL")
        print("=" * 70)
        if establecimiento:
            print(f"📍 Establecimiento: {establecimiento}")
        print(
            f"💰 Total factura: ${total_factura:,}"
            if total_factura
            else "💰 Total: No detectado"
        )
        print(f"📦 Productos únicos: {len(productos_finales)}")
        print(f"   ✅ NIVEL 1 (Código+Nombre+Precio): {nivel_1}")
        print(f"   ⚠️  NIVEL 2 (Nombre+Precio): {nivel_2}")
        print(f"   ⚡ NIVEL 3 (Parcial): {nivel_3}")
        print(f"⏱️ Tiempo: {tiempo}s")
        print("=" * 70 + "\n")

        if productos_finales:
            print("Primeros 10 productos:")
            for i, p in enumerate(productos_finales[:10], 1):
                nivel = p.get("nivel_confianza", 3)
                emoji = "✅" if nivel == 1 else "⚠️" if nivel == 2 else "⚡"
                precio = f"${p.get('valor',0):,}" if p.get("valor", 0) else "Sin precio"
                codigo = p.get("codigo", "(s/c)")
                print(f"  {emoji} {i}. [{codigo}]: {p.get('nombre')} - {precio}")

        return {
            "establecimiento": establecimiento or "Establecimiento no identificado",
            "total": total_factura,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": productos_finales,
            "metadatos": {
                "metodo": f"{'docai+' if used_docai else ''}text_3niveles{'+tess' if productos_tess else ''}",
                "nivel_1": nivel_1,
                "nivel_2": nivel_2,
                "nivel_3": nivel_3,
                "productos_finales": len(productos_finales),
                "tiempo_segundos": tiempo,
            },
        }

    except Exception as e:
        print(f"❌ ERROR CRÍTICO: {str(e)}")
        traceback.print_exc()
        return {
            "establecimiento": "Establecimiento no identificado",
            "total": None,
            "fecha_cargue": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "productos": [],
            "metadatos": {
                "metodo": "error",
                "error": str(e),
                "tiempo_segundos": int(time.time() - inicio),
            },
        }


# =============================
# Alias legacy
# =============================
def process_invoice_products(file_path: str):
    """Mantiene compatibilidad con main.py"""
    return process_invoice_complete(file_path)
