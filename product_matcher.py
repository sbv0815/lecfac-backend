"""
============================================================================
PRODUCT MATCHER V10.3 - CON DIFERENCIACIÓN POR METROS
============================================================================
Versión: 10.3
Fecha: 2025-12-07

MEJORAS V10.3:
- Extracción de metros (15M, 30M) para papel higiénico/toallas
- Penalización por marca diferente (-20%)
- Penalización por metros diferentes (-25%)
- Bonus por metros coincidentes (+15%)
- Mejor diferenciación entre variantes de misma marca

MEJORAS V10.2:
- Diccionario de abreviaturas de tickets colombianos
- Extracción de marcas conocidas
- Extracción de cantidades
- Búsqueda mejorada por nombre con bonus por marca/cantidad
- Umbral reducido a 45% para mejor match

FLUJO DE VALIDACIÓN (orden de prioridad):
1. PAPA - Producto ya validado 100%
2. AUDITORÍA - EAN escaneado manualmente
3. WEB (VTEX) - API del supermercado + validar contra auditoría
4. CACHE VTEX - Datos guardados de búsquedas anteriores
5. OCR - Última opción, solo datos del ticket

============================================================================
"""

import re
from typing import Optional, Dict, Any, Tuple
from difflib import SequenceMatcher
from datetime import datetime

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

UMBRAL_SIMILITUD_NOMBRE = 0.85  # 85% de similitud para considerar match
UMBRAL_SIMILITUD_AUDITORIA = 0.45  # 45% para match con auditoría (permite abreviaturas)

PALABRAS_IGNORAR = {
    "DE",
    "LA",
    "EL",
    "EN",
    "CON",
    "SIN",
    "POR",
    "PARA",
    "UND",
    "UN",
    "UNA",
    "GR",
    "ML",
    "KG",
    "LT",
    "X",
    "Y",
    "O",
    "A",
    "AL",
    "DEL",
    "LOS",
    "LAS",
    "MAS",
    "MENOS",
    "PACK",
    "PAQUETE",
    "BOLSA",
    "CAJA",
    "BOTELLA",
    "LATA",
}

# ============================================================================
# DICCIONARIO DE ABREVIATURAS COLOMBIANAS (tickets de supermercado)
# ============================================================================

ABREVIATURAS_COLOMBIA = {
    # Papel higiénico y aseo
    "P HIG": "PAPEL HIGIENICO",
    "PAP HIG": "PAPEL HIGIENICO",
    "P.HIG": "PAPEL HIGIENICO",
    "PHIG": "PAPEL HIGIENICO",
    "P HIGI": "PAPEL HIGIENICO",
    "PAP HIGI": "PAPEL HIGIENICO",
    "TOA COC": "TOALLAS COCINA",
    "TOA HIG": "TOALLAS HIGIENICAS",
    "SERV": "SERVILLETAS",
    "SERVILL": "SERVILLETAS",
    "PROT FEM": "PROTECTORES FEMENINOS",
    # Marcas abreviadas en tickets
    "FAM": "FAMILIA",
    "ROSAL30H": "ROSAL 30M",
    "ROSAL15H": "ROSAL 15M",
    "ROSALSON": "ROSAL",
    "SCOTT15": "SCOTT 15M",
    "SCOTT30": "SCOTT 30M",
    # Alimentos básicos
    "ACE VEG": "ACEITE VEGETAL",
    "ACE VEGT": "ACEITE VEGETAL",
    "ACE GIR": "ACEITE GIRASOL",
    "ARR BLCO": "ARROZ BLANCO",
    "ARR BLC": "ARROZ BLANCO",
    "AREPA MAI": "AREPA MAIZ",
    "AREPA MZ": "AREPA MAIZ",
    "PAN TAJ": "PAN TAJADO",
    "PAN MOLD": "PAN MOLDE",
    "HUEV": "HUEVOS",
    "HVO": "HUEVOS",
    "HUEVOS ORO AA": "HUEVOS ORO",
    # Lácteos
    "LCH ENT": "LECHE ENTERA",
    "LCH DESLA": "LECHE DESLACTOSADA",
    "LCH DESC": "LECHE DESCREMADA",
    "LECH ENT": "LECHE ENTERA",
    "YOG": "YOGURT",
    "QUES": "QUESO",
    "QUESO CAMPES": "QUESO CAMPESINO",
    "MANT": "MANTEQUILLA",
    "MARG": "MARGARINA",
    # Carnes y embutidos
    "JAMN": "JAMON",
    "JAM": "JAMON",
    "SALCH": "SALCHICHA",
    "SALCHICH": "SALCHICHA",
    "PECH POL": "PECHUGA POLLO",
    "CARN MOL": "CARNE MOLIDA",
    # Bebidas
    "GAL AGUA": "GALON AGUA",
    "BEB GASEO": "BEBIDA GASEOSA",
    "GASEO": "GASEOSA",
    "JUG NAR": "JUGO NARANJA",
    "JGO NAR": "JUGO NARANJA",
    "GATOR": "GATORADE",
    # Limpieza
    "JAB LAV": "JABON LAVAPLATOS",
    "JAB TOC": "JABON TOCADOR",
    "JAB LIQ": "JABON LIQUIDO",
    "DET LIQ": "DETERGENTE LIQUIDO",
    "DET POL": "DETERGENTE POLVO",
    "LIMP MUL": "LIMPIADOR MULTIUSOS",
    "LIMP VID": "LIMPIADOR VIDRIOS",
    "SUAV ROA": "SUAVIZANTE ROPA",
    "SUAV": "SUAVIZANTE",
    "BLANQ": "BLANQUEADOR",
    # Salsas y condimentos
    "SALSAMEN": "SALSA MAYONESA",
    "SALSA TOM": "SALSA TOMATE",
    "SALSA BBQ": "SALSA BARBECUE",
    "MAYO": "MAYONESA",
    "KETCH": "KETCHUP",
    "MOST": "MOSTAZA",
    # Snacks y dulces
    "CHOC POL": "CHOCOLATE POLVO",
    "CHOC TAB": "CHOCOLATE TABLETA",
    "GAL SAL": "GALLETAS SALADAS",
    "GAL DUL": "GALLETAS DULCES",
    "PAP FRI": "PAPAS FRITAS",
    # Cuidado personal
    "CREM DENT": "CREMA DENTAL",
    "CEP DENT": "CEPILLO DENTAL",
    "DESOD": "DESODORANTE",
    "SHAMPO": "SHAMPOO",
    "SHAMP": "SHAMPOO",
    "ACOND": "ACONDICIONADOR",
    "CREMA CORP": "CREMA CORPORAL",
    # Otros
    "LEV INST": "LEVADURA INSTANTANEA",
    "HAR TRIG": "HARINA TRIGO",
    "AZUC": "AZUCAR",
    "SAL REF": "SAL REFINADA",
    "CAFE MOL": "CAFE MOLIDO",
    "CAFE INST": "CAFE INSTANTANEO",
    # Unidades (corrección OCR)
    "UHD": "UNIDADES",
    "UND": "UNIDADES",
    "UNDS": "UNIDADES",
    "UNID": "UNIDADES",
}

# Marcas comunes colombianas (para extraer de nombres OCR)
MARCAS_CONOCIDAS = {
    # Papel y aseo
    "ROSAL",
    "FAMILIA",
    "SUAVE",
    "SCOTT",
    "ELITE",
    "TECNOQUIMICAS",
    "NOSOTRAS",
    # Alimentos
    "RAMO",
    "BIMBO",
    "COMAPAN",
    "SANTA CLARA",
    "ORO",
    "KIKES",
    "SANTAREYES",
    "HUEVOS ORO",
    "DIANA",
    "FLORHUILA",
    "ROA",
    "ARROZ ROA",
    "ARROZ DIANA",
    "DORIA",
    "PASTAS DORIA",
    "COMARRICO",
    "LA MUÑECA",
    "ALPINA",
    "COLANTA",
    "ALQUERIA",
    "PARMALAT",
    "ZENÚ",
    "RICA",
    "PIETRÁN",
    "SUIZO",
    # Bebidas
    "POSTOBON",
    "COCA COLA",
    "PEPSI",
    "QUATRO",
    "COLOMBIANA",
    "HIT",
    "TAMPICO",
    "FRUTTO",
    "DEL VALLE",
    # Salsas
    "FRUCO",
    "RESPIN",
    "MAGGI",
    "LA CONSTANCIA",
    "SAN JORGE",
    # Limpieza
    "FAB",
    "ARIEL",
    "ACE",
    "DERSA",
    "TOP",
    "VANISH",
    "AXION",
    "LAVAPLATOS",
    "FABULOSO",
    # Cuidado personal
    "COLGATE",
    "FORTIDENT",
    "KOLYNOS",
    "ORAL B",
    "PALMOLIVE",
    "PROTEX",
    "DOVE",
    "REXONA",
    "AXE",
    "HEAD SHOULDERS",
    "SEDAL",
    "PANTENE",
    "ELVIVE",
    # Snacks
    "MARGARITA",
    "DE TODITO",
    "YUPI",
    "SUPER RICAS",
    "FESTIVAL",
    "SALTINAS",
    "DUCALES",
    "CLUB SOCIAL",
    # Otros
    "NESCAFE",
    "SELLO ROJO",
    "AGUILA ROJA",
    "COLCAFE",
    "MANUELITA",
    "INCAUCA",
    "RIOPAILA",
}


# ============================================================================
# FUNCIONES DE UTILIDAD
# ============================================================================


def limpiar_nombre(nombre: str) -> str:
    """Limpia y normaliza un nombre de producto"""
    if not nombre:
        return ""

    nombre = nombre.upper().strip()
    nombre = re.sub(r"[^\w\s]", " ", nombre)
    nombre = re.sub(r"\s+", " ", nombre)

    return nombre.strip()


def expandir_abreviaturas(nombre: str) -> str:
    """
    Expande abreviaturas comunes en nombres de tickets colombianos.
    """
    if not nombre:
        return ""

    nombre_upper = nombre.upper()

    # Ordenar por longitud descendente para evitar reemplazos parciales
    abreviaturas_ordenadas = sorted(
        ABREVIATURAS_COLOMBIA.items(), key=lambda x: len(x[0]), reverse=True
    )

    for abrev, expansion in abreviaturas_ordenadas:
        # Buscar la abreviatura como palabra completa
        patron = r"\b" + re.escape(abrev) + r"\b"
        nombre_upper = re.sub(patron, expansion, nombre_upper)

    # Limpiar espacios múltiples
    nombre_upper = re.sub(r"\s+", " ", nombre_upper).strip()

    return nombre_upper


def extraer_marca(nombre: str) -> Optional[str]:
    """
    Extrae la marca de un nombre de producto si es conocida.
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    # Buscar marcas de más largas a más cortas (para evitar matches parciales)
    marcas_ordenadas = sorted(MARCAS_CONOCIDAS, key=len, reverse=True)

    for marca in marcas_ordenadas:
        if marca in nombre_upper:
            return marca

    return None


def extraer_cantidad(nombre: str) -> Optional[str]:
    """
    Extrae información de cantidad (ej: 12, X12, 30M, 500G).
    Retorna solo el número principal.
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    # Buscar patrones como: X12, 12UND, 30M, 500G, 1LT, X12R
    patrones = [
        r"X(\d+)[RU]?\b",  # X12, X12R, X12U
        r"(\d+)\s*UND",  # 12UND, 12 UND
        r"(\d+)\s*UHD",  # 12UHD (OCR mal leído)
        r"(\d+)\s*UNID",  # 12UNID
        r"(\d+)\s*[R]\b",  # 12R (rollos)
        r"(\d+)\s*GR?\b",  # 500G, 500GR
        r"(\d+)\s*KG\b",  # 1KG
        r"(\d+)\s*ML\b",  # 500ML
        r"(\d+)\s*LT?\b",  # 1L, 1LT
    ]

    for patron in patrones:
        match = re.search(patron, nombre_upper)
        if match:
            return match.group(1)

    return None


def extraer_metros(nombre: str) -> Optional[str]:
    """
    Extrae metros de papel higiénico/toallas (15M, 30M, etc).
    Importante para diferenciar variantes de misma marca.

    OCR a veces confunde M→H, así que buscamos ambos.
    Ejemplo: "ROSAL30H" = "ROSAL 30M"
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    # Patrones para metros (M o H por error OCR)
    patrones = [
        r"(\d+)\s*M\b",  # 30M, 15M
        r"(\d+)\s*H\b",  # 30H, 15H (OCR confunde M→H)
        r"(\d+)\s*MTS?\b",  # 30MTS, 30MT
        r"(\d+)\s*METROS?\b",  # 30 METROS
    ]

    for patron in patrones:
        match = re.search(patron, nombre_upper)
        if match:
            metros = match.group(1)
            # Solo valores típicos de papel higiénico: 15, 20, 25, 30, 40, 50
            if metros in ["15", "20", "25", "30", "40", "50"]:
                return metros

    return None


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """Calcula similitud entre dos nombres (0.0 - 1.0)"""
    if not nombre1 or not nombre2:
        return 0.0

    n1 = limpiar_nombre(nombre1)
    n2 = limpiar_nombre(nombre2)

    # Similitud directa
    ratio = SequenceMatcher(None, n1, n2).ratio()

    # Similitud por palabras significativas
    palabras1 = set(p for p in n1.split() if p not in PALABRAS_IGNORAR and len(p) >= 3)
    palabras2 = set(p for p in n2.split() if p not in PALABRAS_IGNORAR and len(p) >= 3)

    if palabras1 and palabras2:
        interseccion = palabras1 & palabras2
        union = palabras1 | palabras2
        jaccard = len(interseccion) / len(union) if union else 0

        # Promedio ponderado
        return (ratio * 0.6) + (jaccard * 0.4)

    return ratio


def calcular_distancia_plu(plu1: str, plu2: str) -> int:
    """Calcula distancia de Levenshtein entre dos PLUs"""
    if not plu1 or not plu2:
        return 999

    p1 = "".join(c for c in plu1 if c.isdigit())
    p2 = "".join(c for c in plu2 if c.isdigit())

    if len(p1) != len(p2):
        return abs(len(p1) - len(p2)) + 1

    return sum(1 for a, b in zip(p1, p2) if a != b)


# ============================================================================
# PASO 1: BUSCAR PAPA (Producto Aprendido y Perfeccionado por Auditoría)
# ============================================================================


def buscar_papa(plu: str, establecimiento_id: int, cursor) -> Optional[Dict]:
    """
    Busca un producto PAPA (ya validado 100%).
    """
    try:
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.nombre_consolidado,
                pm.codigo_ean,
                pm.marca,
                pm.categoria_id,
                ppe.precio_unitario
            FROM productos_maestros_v2 pm
            JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
            WHERE ppe.codigo_plu = %s
              AND ppe.establecimiento_id = %s
              AND pm.es_producto_papa = TRUE
            LIMIT 1
        """,
            (plu, establecimiento_id),
        )

        row = cursor.fetchone()
        if row:
            print(f"   👑 [PASO 1] PAPA encontrado: {row[1]}")
            return {
                "producto_id": row[0],
                "nombre": row[1],
                "codigo_ean": row[2],
                "marca": row[3],
                "categoria_id": row[4],
                "precio_bd": row[5],
                "fuente": "PAPA",
                "confianza": 1.0,
            }
    except Exception as e:
        print(f"   ⚠️ Error buscando PAPA: {e}")

    return None


# ============================================================================
# PASO 2: BUSCAR EN AUDITORÍA (productos_referencia_ean)
# ============================================================================


def buscar_en_auditoria_por_ean(ean: str, cursor) -> Optional[Dict]:
    """
    Busca un EAN en la tabla de productos escaneados manualmente.
    """
    if not ean or len(ean) < 8:
        return None

    try:
        cursor.execute(
            """
            SELECT
                id,
                codigo_ean,
                nombre,
                marca,
                presentacion,
                categoria,
                validaciones
            FROM productos_referencia_ean
            WHERE codigo_ean = %s
            LIMIT 1
        """,
            (ean,),
        )

        row = cursor.fetchone()
        if row:
            print(f"   📱 [PASO 2] Auditoría por EAN: {row[2]}")
            return {
                "referencia_id": row[0],
                "codigo_ean": row[1],
                "nombre": row[2],
                "marca": row[3],
                "presentacion": row[4],
                "categoria": row[5],
                "validaciones": row[6],
                "fuente": "AUDITORIA",
                "confianza": 0.95,
            }
    except Exception as e:
        print(f"   ⚠️ Error buscando en auditoría por EAN: {e}")

    return None


def buscar_en_auditoria_por_nombre(
    nombre_ocr: str, cursor, umbral: float = UMBRAL_SIMILITUD_AUDITORIA
) -> Optional[Dict]:
    """
    Busca por nombre similar en productos de auditoría.
    MEJORADO V10.2: Expande abreviaturas y busca por marca + cantidad.
    """
    if not nombre_ocr or len(nombre_ocr) < 3:
        return None

    try:
        nombre_limpio = limpiar_nombre(nombre_ocr)

        # PASO 0: Expandir abreviaturas
        nombre_expandido = expandir_abreviaturas(nombre_limpio)
        print(f"   🔄 Nombre expandido: {nombre_limpio} → {nombre_expandido}")

        # Extraer marca, cantidad y metros para búsqueda inteligente
        marca = extraer_marca(nombre_limpio) or extraer_marca(nombre_expandido)
        cantidad = extraer_cantidad(nombre_limpio)
        metros = extraer_metros(nombre_limpio) or extraer_metros(nombre_expandido)
        print(f"   🏷️ Marca: {marca} | Cantidad: {cantidad} | Metros: {metros}")

        # Extraer palabras significativas (del nombre expandido)
        palabras_busqueda = [
            p
            for p in nombre_expandido.split()
            if p not in PALABRAS_IGNORAR and len(p) >= 3
        ]

        # Agregar marca si existe y no está en palabras
        if marca and marca not in palabras_busqueda:
            palabras_busqueda.insert(0, marca)

        print(f"   🔎 Palabras clave: {palabras_busqueda}")

        if not palabras_busqueda:
            return None

        # PASO A: Buscar candidatos que contengan palabras clave
        # Usar hasta 4 palabras para la búsqueda
        palabras_query = palabras_busqueda[:4]
        condiciones = " OR ".join(["UPPER(nombre) LIKE %s" for _ in palabras_query])
        parametros = [f"%{p}%" for p in palabras_query]

        query = f"""
            SELECT
                id,
                codigo_ean,
                nombre,
                marca,
                presentacion,
                categoria,
                validaciones
            FROM productos_referencia_ean
            WHERE {condiciones}
            ORDER BY validaciones DESC
            LIMIT 50
        """

        cursor.execute(query, parametros)
        candidatos = cursor.fetchall()

        print(f"   🔎 Candidatos encontrados: {len(candidatos)}")

        # PASO B: Calcular similitud con múltiples estrategias
        mejor_match = None
        mejor_score = 0

        for row in candidatos:
            nombre_ref = row[2]
            nombre_ref_expandido = expandir_abreviaturas(nombre_ref)

            # Similitud 1: Nombre expandido vs nombre expandido
            sim1 = calcular_similitud(nombre_expandido, nombre_ref_expandido)

            # Similitud 2: Nombre original vs referencia
            sim2 = calcular_similitud(nombre_limpio, nombre_ref)

            # Similitud 3: Nombre expandido vs referencia original
            sim3 = calcular_similitud(nombre_expandido, nombre_ref)

            # Tomar la mejor
            similitud = max(sim1, sim2, sim3)

            # BONUS por coincidencias específicas
            bonus = 0
            penalizacion = 0

            # Bonus por marca coincidente
            marca_ref = extraer_marca(nombre_ref)
            if marca and marca_ref and marca == marca_ref:
                bonus += 0.15
            elif marca and marca_ref and (marca in marca_ref or marca_ref in marca):
                bonus += 0.10
            elif marca and marca_ref and marca != marca_ref:
                # Penalizar si las marcas son diferentes (evita confundir ROSAL con FAMILIA)
                penalizacion += 0.20

            # Bonus/Penalización por metros (crucial para papel higiénico)
            metros_ref = extraer_metros(nombre_ref)
            if metros and metros_ref:
                if metros == metros_ref:
                    bonus += 0.15  # Mismo metraje = muy probable que sea el mismo
                else:
                    penalizacion += (
                        0.25  # Diferente metraje = probablemente otro producto
                    )

            # Bonus por cantidad coincidente
            cantidad_ref = extraer_cantidad(nombre_ref)
            if cantidad and cantidad_ref and cantidad == cantidad_ref:
                bonus += 0.10

            # Bonus por palabras clave que coinciden
            nombre_ref_upper = nombre_ref.upper()
            palabras_coinciden = sum(
                1 for p in palabras_busqueda if p in nombre_ref_upper
            )
            bonus += 0.05 * min(palabras_coinciden, 3)  # Máximo +15% por palabras

            score_final = min(1.0, max(0, similitud + bonus - penalizacion))

            if score_final > mejor_score and score_final >= umbral:
                mejor_score = score_final
                mejor_match = {
                    "referencia_id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "presentacion": row[4],
                    "categoria": row[5],
                    "validaciones": row[6],
                    "similitud": score_final,
                    "fuente": "AUDITORIA_NOMBRE",
                    "confianza": 0.85 * score_final,
                }
                print(
                    f"      📊 {nombre_ref[:35]}: sim={similitud:.0%} +bonus={bonus:.0%} -pen={penalizacion:.0%} = {score_final:.0%}"
                )

        if mejor_match:
            print(
                f"   ✅ [PASO 2] Match encontrado ({mejor_score:.0%}): {mejor_match['nombre']}"
            )
        else:
            print(f"   ❌ No se encontró match >= {umbral:.0%}")

        return mejor_match

    except Exception as e:
        print(f"   ⚠️ Error buscando en auditoría por nombre: {e}")
        import traceback

        traceback.print_exc()

    return None


# ============================================================================
# PASO 3: BUSCAR EN WEB (VTEX) + VALIDAR CONTRA AUDITORÍA
# ============================================================================


def buscar_en_web_y_validar(
    plu: str, nombre_ocr: str, establecimiento: str, precio_ocr: int, cursor
) -> Optional[Dict]:
    """
    Busca en API VTEX y valida el resultado contra auditoría.
    Si el EAN de VTEX existe en auditoría → confianza alta.
    """
    from web_enricher import WebEnricher, es_tienda_vtex

    if not es_tienda_vtex(establecimiento):
        return None

    try:
        enricher = WebEnricher(cursor, None)
        resultado = enricher.enriquecer(
            codigo=plu,
            nombre_ocr=nombre_ocr,
            establecimiento=establecimiento,
            precio_ocr=precio_ocr,
        )

        if not resultado.encontrado:
            return None

        print(f"   🌐 [PASO 3] Web encontró: {resultado.nombre_web}")

        # Validar contra auditoría si tenemos EAN
        confianza = 0.8
        fuente = "WEB"

        if resultado.codigo_ean:
            auditoria = buscar_en_auditoria_por_ean(resultado.codigo_ean, cursor)

            if auditoria:
                # EAN existe en auditoría → validado!
                similitud_nombre = calcular_similitud(
                    resultado.nombre_web, auditoria["nombre"]
                )

                if similitud_nombre >= 0.7:
                    print(
                        f"   ✅ [PASO 3] EAN validado con auditoría ({similitud_nombre:.0%})"
                    )
                    confianza = 0.95
                    fuente = "WEB_VALIDADO"

                    # Usar nombre de auditoría si es mejor
                    if similitud_nombre < 0.95:
                        resultado.nombre_web = auditoria["nombre"]
                        resultado.marca = auditoria.get("marca") or resultado.marca
                else:
                    print(
                        f"   ⚠️ [PASO 3] EAN coincide pero nombres muy diferentes ({similitud_nombre:.0%})"
                    )
                    confianza = 0.6

        return {
            "nombre": resultado.nombre_web,
            "codigo_ean": resultado.codigo_ean,
            "codigo_plu": resultado.codigo_plu,
            "marca": resultado.marca,
            "precio_web": resultado.precio_web,
            "categoria": resultado.categoria,
            "url": resultado.url_producto,
            "imagen": resultado.imagen_url,
            "fuente": fuente,
            "confianza": confianza,
        }

    except Exception as e:
        print(f"   ⚠️ Error buscando en web: {e}")

    return None


# ============================================================================
# PASO 4: BUSCAR EN CACHE VTEX
# ============================================================================


def buscar_en_cache_vtex(plu: str, establecimiento: str, cursor) -> Optional[Dict]:
    """
    Busca en el cache local de productos VTEX.
    """
    try:
        cursor.execute(
            """
            SELECT
                id, nombre, ean, plu, marca, precio, categoria
            FROM productos_vtex_cache
            WHERE (plu = %s OR ean = %s)
              AND establecimiento = %s
            ORDER BY veces_usado DESC
            LIMIT 1
        """,
            (plu, plu, establecimiento.upper()),
        )

        row = cursor.fetchone()
        if row:
            print(f"   💾 [PASO 4] Cache VTEX: {row[1]}")
            return {
                "cache_id": row[0],
                "nombre": row[1],
                "codigo_ean": row[2],
                "codigo_plu": row[3],
                "marca": row[4],
                "precio_web": row[5],
                "categoria": row[6],
                "fuente": "CACHE_VTEX",
                "confianza": 0.7,
            }
    except Exception as e:
        print(f"   ⚠️ Error buscando en cache: {e}")

    return None


# ============================================================================
# PASO 5: BUSCAR PLU EXISTENTE (sin PAPA)
# ============================================================================


def buscar_plu_existente(plu: str, establecimiento_id: int, cursor) -> Optional[Dict]:
    """
    Busca si el PLU ya existe en la BD (aunque no sea PAPA).
    """
    try:
        cursor.execute(
            """
            SELECT
                pm.id,
                pm.nombre_consolidado,
                pm.codigo_ean,
                pm.marca,
                pm.categoria_id,
                pm.fuente_datos,
                pm.confianza_datos,
                ppe.precio_unitario
            FROM productos_maestros_v2 pm
            JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
            WHERE ppe.codigo_plu = %s
              AND ppe.establecimiento_id = %s
            LIMIT 1
        """,
            (plu, establecimiento_id),
        )

        row = cursor.fetchone()
        if row:
            print(f"   📦 [PASO 5] PLU existente: {row[1]}")
            return {
                "producto_id": row[0],
                "nombre": row[1],
                "codigo_ean": row[2],
                "marca": row[3],
                "categoria_id": row[4],
                "fuente": row[5] or "BD",
                "confianza": float(row[6]) if row[6] else 0.5,
                "precio_bd": row[7],
            }
    except Exception as e:
        print(f"   ⚠️ Error buscando PLU existente: {e}")

    return None


# ============================================================================
# FUNCIÓN PRINCIPAL: BUSCAR O CREAR PRODUCTO INTELIGENTE
# ============================================================================


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre_ocr: str,
    precio: int,
    establecimiento_id: int,
    establecimiento_nombre: str,
    cursor,
    conn,
) -> Dict[str, Any]:
    """
    Busca o crea un producto usando el flujo de validación completo.

    Retorna:
        {
            'producto_id': int,
            'nombre': str,
            'codigo_ean': str,
            'es_nuevo': bool,
            'fuente': str,
            'confianza': float
        }
    """
    print(f"\n{'='*60}")
    print(f"🔍 BUSCANDO: PLU={codigo} | {nombre_ocr[:30]}... | ${precio:,}")
    print(f"   Establecimiento: {establecimiento_nombre} (ID: {establecimiento_id})")
    print(f"{'='*60}")

    plu = str(codigo).strip() if codigo else ""
    nombre_limpio = limpiar_nombre(nombre_ocr)

    # ========================================
    # PASO 1: Buscar PAPA
    # ========================================
    print("\n📌 PASO 1: Buscando PAPA...")
    papa = buscar_papa(plu, establecimiento_id, cursor)
    if papa:
        actualizar_precio_si_necesario(
            papa["producto_id"], establecimiento_id, plu, precio, cursor, conn
        )
        return {
            "producto_id": papa["producto_id"],
            "nombre": papa["nombre"],
            "codigo_ean": papa.get("codigo_ean"),
            "es_nuevo": False,
            "fuente": "PAPA",
            "confianza": 1.0,
        }

    # ========================================
    # PASO 2: Buscar en Auditoría
    # ========================================
    print("\n📌 PASO 2: Buscando en Auditoría...")

    # Primero ver si el PLU ya tiene EAN asociado
    plu_existente = buscar_plu_existente(plu, establecimiento_id, cursor)

    if plu_existente and plu_existente.get("codigo_ean"):
        auditoria = buscar_en_auditoria_por_ean(plu_existente["codigo_ean"], cursor)
        if auditoria:
            # Actualizar producto existente con datos de auditoría
            actualizar_producto_con_auditoria(
                plu_existente["producto_id"], auditoria, cursor, conn
            )
            actualizar_precio_si_necesario(
                plu_existente["producto_id"],
                establecimiento_id,
                plu,
                precio,
                cursor,
                conn,
            )
            return {
                "producto_id": plu_existente["producto_id"],
                "nombre": auditoria["nombre"],
                "codigo_ean": auditoria["codigo_ean"],
                "es_nuevo": False,
                "fuente": "AUDITORIA",
                "confianza": 0.95,
            }

    # Buscar por nombre similar (con expansión de abreviaturas)
    auditoria_nombre = buscar_en_auditoria_por_nombre(nombre_limpio, cursor)
    if auditoria_nombre:
        # Crear o actualizar producto con datos de auditoría
        producto_id = crear_o_actualizar_producto(
            plu=plu,
            establecimiento_id=establecimiento_id,
            datos=auditoria_nombre,
            precio=precio,
            cursor=cursor,
            conn=conn,
        )
        return {
            "producto_id": producto_id,
            "nombre": auditoria_nombre["nombre"],
            "codigo_ean": auditoria_nombre["codigo_ean"],
            "es_nuevo": True,
            "fuente": "AUDITORIA_NOMBRE",
            "confianza": auditoria_nombre["confianza"],
        }

    # ========================================
    # PASO 3: Buscar en Web (VTEX)
    # ========================================
    print("\n📌 PASO 3: Buscando en Web (VTEX)...")
    web = buscar_en_web_y_validar(
        plu, nombre_limpio, establecimiento_nombre, precio, cursor
    )
    if web:
        producto_id = crear_o_actualizar_producto(
            plu=plu,
            establecimiento_id=establecimiento_id,
            datos=web,
            precio=precio,
            cursor=cursor,
            conn=conn,
        )
        return {
            "producto_id": producto_id,
            "nombre": web["nombre"],
            "codigo_ean": web.get("codigo_ean"),
            "es_nuevo": True,
            "fuente": web["fuente"],
            "confianza": web["confianza"],
        }

    # ========================================
    # PASO 4: Buscar en Cache VTEX
    # ========================================
    print("\n📌 PASO 4: Buscando en Cache VTEX...")
    cache = buscar_en_cache_vtex(plu, establecimiento_nombre, cursor)
    if cache:
        producto_id = crear_o_actualizar_producto(
            plu=plu,
            establecimiento_id=establecimiento_id,
            datos=cache,
            precio=precio,
            cursor=cursor,
            conn=conn,
        )
        return {
            "producto_id": producto_id,
            "nombre": cache["nombre"],
            "codigo_ean": cache.get("codigo_ean"),
            "es_nuevo": True,
            "fuente": "CACHE_VTEX",
            "confianza": 0.7,
        }

    # ========================================
    # PASO 5: Usar PLU existente o crear con OCR
    # ========================================
    print("\n📌 PASO 5: Usando datos OCR...")

    if plu_existente:
        actualizar_precio_si_necesario(
            plu_existente["producto_id"], establecimiento_id, plu, precio, cursor, conn
        )
        return {
            "producto_id": plu_existente["producto_id"],
            "nombre": plu_existente["nombre"],
            "codigo_ean": plu_existente.get("codigo_ean"),
            "es_nuevo": False,
            "fuente": plu_existente["fuente"],
            "confianza": plu_existente["confianza"],
        }

    # Crear producto nuevo con datos OCR
    producto_id = crear_producto_ocr(
        plu=plu,
        nombre=nombre_limpio,
        precio=precio,
        establecimiento_id=establecimiento_id,
        cursor=cursor,
        conn=conn,
    )

    return {
        "producto_id": producto_id,
        "nombre": nombre_limpio,
        "codigo_ean": None,
        "es_nuevo": True,
        "fuente": "OCR",
        "confianza": 0.5,
    }


# ============================================================================
# FUNCIONES DE ACTUALIZACIÓN Y CREACIÓN
# ============================================================================


def actualizar_precio_si_necesario(
    producto_id: int, establecimiento_id: int, plu: str, precio_nuevo: int, cursor, conn
):
    """Actualiza el precio si es diferente al registrado"""
    try:
        cursor.execute(
            """
            UPDATE productos_por_establecimiento
            SET precio_unitario = %s,
                precio_actual = %s,
                ultima_actualizacion = CURRENT_TIMESTAMP,
                total_reportes = total_reportes + 1
            WHERE producto_maestro_id = %s
              AND establecimiento_id = %s
              AND codigo_plu = %s
        """,
            (precio_nuevo, precio_nuevo, producto_id, establecimiento_id, plu),
        )

        conn.commit()
    except Exception as e:
        print(f"   ⚠️ Error actualizando precio: {e}")
        conn.rollback()


def actualizar_producto_con_auditoria(producto_id: int, auditoria: Dict, cursor, conn):
    """Actualiza un producto existente con datos de auditoría"""
    try:
        cursor.execute(
            """
            UPDATE productos_maestros_v2
            SET nombre_consolidado = %s,
                marca = %s,
                fuente_datos = 'AUDITORIA',
                confianza_datos = 0.95,
                es_producto_papa = TRUE,
                fecha_validacion = CURRENT_TIMESTAMP
            WHERE id = %s
        """,
            (auditoria["nombre"], auditoria.get("marca"), producto_id),
        )

        conn.commit()
        print(f"   ✅ Producto {producto_id} actualizado con datos de auditoría")
    except Exception as e:
        print(f"   ⚠️ Error actualizando con auditoría: {e}")
        conn.rollback()


def crear_o_actualizar_producto(
    plu: str, establecimiento_id: int, datos: Dict, precio: int, cursor, conn
) -> int:
    """Crea o actualiza un producto con los datos proporcionados"""
    try:
        # Verificar si ya existe por EAN
        producto_id = None

        if datos.get("codigo_ean"):
            cursor.execute(
                """
                SELECT id FROM productos_maestros_v2 WHERE codigo_ean = %s
            """,
                (datos["codigo_ean"],),
            )
            row = cursor.fetchone()
            if row:
                producto_id = row[0]

        if producto_id:
            # Actualizar existente
            cursor.execute(
                """
                UPDATE productos_maestros_v2
                SET nombre_consolidado = COALESCE(%s, nombre_consolidado),
                    marca = COALESCE(%s, marca),
                    fuente_datos = %s,
                    confianza_datos = %s
                WHERE id = %s
            """,
                (
                    datos.get("nombre"),
                    datos.get("marca"),
                    datos.get("fuente", "WEB"),
                    datos.get("confianza", 0.8),
                    producto_id,
                ),
            )
        else:
            # Crear nuevo
            cursor.execute(
                """
                INSERT INTO productos_maestros_v2 (
                    nombre_consolidado, codigo_ean, marca,
                    fuente_datos, confianza_datos, veces_visto
                ) VALUES (%s, %s, %s, %s, %s, 1)
                RETURNING id
            """,
                (
                    datos.get("nombre", "SIN NOMBRE"),
                    datos.get("codigo_ean"),
                    datos.get("marca"),
                    datos.get("fuente", "WEB"),
                    datos.get("confianza", 0.8),
                ),
            )
            producto_id = cursor.fetchone()[0]

        # Crear/actualizar relación con establecimiento
        cursor.execute(
            """
            INSERT INTO productos_por_establecimiento (
                producto_maestro_id, establecimiento_id, codigo_plu,
                precio_unitario, precio_actual, ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (producto_maestro_id, establecimiento_id, codigo_plu)
            DO UPDATE SET
                precio_unitario = EXCLUDED.precio_unitario,
                precio_actual = EXCLUDED.precio_actual,
                ultima_actualizacion = CURRENT_TIMESTAMP,
                total_reportes = productos_por_establecimiento.total_reportes + 1
        """,
            (producto_id, establecimiento_id, plu, precio, precio),
        )

        conn.commit()
        print(f"   ✅ Producto {producto_id} creado/actualizado")
        return producto_id

    except Exception as e:
        print(f"   ❌ Error creando producto: {e}")
        conn.rollback()
        raise


def crear_producto_ocr(
    plu: str, nombre: str, precio: int, establecimiento_id: int, cursor, conn
) -> int:
    """Crea un producto nuevo solo con datos OCR"""
    try:
        cursor.execute(
            """
            INSERT INTO productos_maestros_v2 (
                nombre_consolidado, fuente_datos, confianza_datos, veces_visto
            ) VALUES (%s, 'OCR', 0.5, 1)
            RETURNING id
        """,
            (nombre,),
        )

        producto_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO productos_por_establecimiento (
                producto_maestro_id, establecimiento_id, codigo_plu,
                precio_unitario, precio_actual, ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """,
            (producto_id, establecimiento_id, plu, precio, precio),
        )

        conn.commit()
        print(f"   📝 Producto OCR creado: ID {producto_id}")
        return producto_id

    except Exception as e:
        print(f"   ❌ Error creando producto OCR: {e}")
        conn.rollback()
        raise


# ============================================================================
# RESUMEN DE VERSIÓN
# ============================================================================

print("=" * 60)
print("✅ PRODUCT MATCHER V10.3 CARGADO")
print("   Mejoras:")
print("   - Expansión de abreviaturas colombianas")
print("   - Detección de marcas conocidas (FAM→FAMILIA)")
print("   - Extracción de metros (30M, 15M)")
print("   - Penalización marca diferente (-20%)")
print("   - Penalización metros diferentes (-25%)")
print("   - Umbral: 45% con bonus/penalización")
print("   Flujo de validación:")
print("   1. PAPA (100%)")
print("   2. AUDITORÍA - EAN/Nombre (95%)")
print("   3. WEB + Validación (80-95%)")
print("   4. CACHE VTEX (70%)")
print("   5. OCR (50%)")
print("=" * 60)
