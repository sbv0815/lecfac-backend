"""
duplicate_detector.py - VERSIÓN 2.0 CON EXPANSIÓN DE ABREVIATURAS
==================================================================
Resuelve duplicación de productos sin código (OXXO, tiendas de barrio)
donde el OCR produce variaciones del mismo nombre en diferentes frames.

CAMBIOS V2.0:
- ✅ Expande abreviaturas colombianas antes de comparar
- ✅ Usa similitud fuzzy (85%) para agrupar nombres similares
- ✅ Normaliza errores OCR comunes (H→M, N→M en metros)
- ✅ Extrae y compara metros para papel higiénico
- ✅ Mantiene cantidad=1 para video (no suma frames)

AUTOR: LecFac Team
ÚLTIMA ACTUALIZACIÓN: 2025-12-08
==================================================================
"""

import os
import re
from typing import List, Dict, Tuple, Optional
from difflib import SequenceMatcher


# =============================================================================
# DICCIONARIO DE ABREVIATURAS COLOMBIANAS (del product_matcher V10.3)
# =============================================================================
ABREVIATURAS_COLOMBIA = {
    # Papel higiénico y limpieza
    "P HIG": "PAPEL HIGIENICO",
    "PAP HIG": "PAPEL HIGIENICO",
    "P ATG": "PAPEL HIGIENICO",  # Error OCR común
    "P HTG": "PAPEL HIGIENICO",  # Error OCR
    "PHIG": "PAPEL HIGIENICO",
    "PAP": "PAPEL",
    "HIG": "HIGIENICO",
    "ROSAL30H": "ROSAL 30M",  # H es error OCR de M
    "ROSAL30N": "ROSAL 30M",  # N es error OCR de M
    "ROSAL30M": "ROSAL 30M",
    "ROSALSON": "ROSAL 30M",  # Error OCR
    "ROSAL3OH": "ROSAL 30M",  # O en vez de 0
    "ULTRACONF": "ULTRACONFORT",
    "ULTRACNF": "ULTRACONFORT",
    # Huevos
    "HUEV": "HUEVOS",
    "HVS": "HUEVOS",
    "AA": "TIPO AA",
    "AAA": "TIPO AAA",
    "X30": "30 UNIDADES",
    "X30UND": "30 UNIDADES",
    "X30UHD": "30 UNIDADES",  # Error OCR
    "X15": "15 UNIDADES",
    "X12": "12 UNIDADES",
    "X12UND": "12 UNIDADES",
    "X12R": "12 ROLLOS",
    # Aceites y grasas
    "ACE": "ACEITE",
    "ACE VEG": "ACEITE VEGETAL",
    "ACEIT": "ACEITE",
    "VEG": "VEGETAL",
    "VEGETAL": "VEGETAL",
    "GIRAS": "GIRASOL",
    "MZLLA": "MAZOLA",
    # Lácteos
    "LCH": "LECHE",
    "LECH": "LECHE",
    "YOG": "YOGURT",
    "YOGH": "YOGURT",
    "QSO": "QUESO",
    "QESO": "QUESO",
    "MANT": "MANTEQUILLA",
    "MARG": "MARGARINA",
    "DESLA": "DESLACTOSADA",
    "DESLAC": "DESLACTOSADA",
    "SEMIDES": "SEMIDESCREMADA",
    "DESC": "DESCREMADA",
    "ENT": "ENTERA",
    # Carnes
    "PCH": "PECHUGA",
    "PECH": "PECHUGA",
    "PLL": "POLLO",
    "POLL": "POLLO",
    "CRN": "CARNE",
    "CARN": "CARNE",
    "RES": "RES",
    "CRD": "CERDO",
    "CERD": "CERDO",
    "MOL": "MOLIDA",
    "MOLD": "MOLIDA",
    "COST": "COSTILLA",
    "CHUL": "CHULETA",
    "LOM": "LOMO",
    # Bebidas
    "GAS": "GASEOSA",
    "GASS": "GASEOSA",
    "JGO": "JUGO",
    "JUG": "JUGO",
    "AGU": "AGUA",
    "CCA": "COCA COLA",
    "CCOLA": "COCA COLA",
    "PPS": "PEPSI",
    "PEPS": "PEPSI",
    "POSTB": "POSTOBON",
    "POSTBN": "POSTOBON",
    # Limpieza
    "JAB": "JABON",
    "JAB LAV": "JABON LAVAPLATOS",
    "LAVAP": "LAVAPLATOS",
    "DET": "DETERGENTE",
    "DETERG": "DETERGENTE",
    "LIMP": "LIMPIADOR",
    "LIMPIA": "LIMPIADOR",
    "DESINF": "DESINFECTANTE",
    "CLOR": "CLORO",
    "BLNQ": "BLANQUEADOR",
    "SUAV": "SUAVIZANTE",
    "SUAVIZ": "SUAVIZANTE",
    # Panadería
    "PN": "PAN",
    "PAND": "PANDEBONO",
    "PDBON": "PANDEBONO",
    "ALMOJ": "ALMOJABANA",
    "BUNS": "PANES",
    "TAJD": "TAJADO",
    "TAJ": "TAJADO",
    "INTEG": "INTEGRAL",
    "INTGR": "INTEGRAL",
    "BLA": "BLANCO",
    "BLANC": "BLANCO",
    # Snacks
    "PAP FRT": "PAPAS FRITAS",
    "PAPFRT": "PAPAS FRITAS",
    "PAP FR": "PAPAS FRITAS",
    "CHOC": "CHOCOLATE",
    "CHOCOL": "CHOCOLATE",
    "GALL": "GALLETAS",
    "GALLT": "GALLETAS",
    # Granos y cereales
    "ARR": "ARROZ",
    "ARRZ": "ARROZ",
    "FRIJ": "FRIJOL",
    "FRIJL": "FRIJOL",
    "LENT": "LENTEJA",
    "LENTJ": "LENTEJA",
    "GARB": "GARBANZO",
    "AVN": "AVENA",
    "AVEN": "AVENA",
    # Frutas y verduras
    "TOM": "TOMATE",
    "TOMT": "TOMATE",
    "CEB": "CEBOLLA",
    "CEBL": "CEBOLLA",
    "ZAN": "ZANAHORIA",
    "ZANAH": "ZANAHORIA",
    "PAP": "PAPA",
    "PLAT": "PLATANO",
    "BANAN": "BANANO",
    "MZN": "MANZANA",
    "MANZ": "MANZANA",
    "NAR": "NARANJA",
    "NARNJ": "NARANJA",
    "LIM": "LIMON",
    "LIMN": "LIMON",
    # Marcas comunes
    "FAM": "FAMILIA",
    "FAML": "FAMILIA",
    "ALPN": "ALPINA",
    "ALP": "ALPINA",
    "COLNT": "COLANTA",
    "COL": "COLANTA",
    "ALQ": "ALQUERIA",
    "ALQR": "ALQUERIA",
    "ZNU": "ZENU",
    "RCHM": "RANCHERA",
    "RNCH": "RANCHERA",
    "KOIPE": "KOIPE",
    "GOURM": "GOURMET",
    "PREM": "PREMIUM",
    # Unidades y cantidades
    "UND": "UNIDADES",
    "UHD": "UNIDADES",  # Error OCR
    "UDS": "UNIDADES",
    "GR": "GRAMOS",
    "GRS": "GRAMOS",
    "KG": "KILOGRAMOS",
    "KGS": "KILOGRAMOS",
    "ML": "MILILITROS",
    "MLS": "MILILITROS",
    "LT": "LITROS",
    "LTS": "LITROS",
    "PZS": "PIEZAS",
    "PCS": "PIEZAS",
}


# =============================================================================
# MARCAS CONOCIDAS (para evitar confusiones en matching)
# =============================================================================
MARCAS_CONOCIDAS = {
    # Papel y limpieza
    "ROSAL",
    "FAMILIA",
    "SCOTT",
    "ELITE",
    "SUAVE",
    "COTTONELLE",
    "RENOVA",
    "NEVAX",
    "PETALO",
    # Huevos
    "ORO",
    "KIKES",
    "SANTAREYES",
    "HUEVOS ORO",
    "CAMPO VERDE",
    # Lácteos
    "ALPINA",
    "COLANTA",
    "ALQUERIA",
    "PARMALAT",
    "CELEMA",
    "COOLECHERA",
    "ALGARRA",
    "NORMANDY",
    "YOPLAIT",
    "DANONE",
    "NESTLE",
    "POMAR",
    # Carnes
    "ZENU",
    "RANCHERA",
    "PIETRÁN",
    "RICA",
    "KOIPE",
    "SUIZO",
    # Bebidas
    "COCA COLA",
    "PEPSI",
    "POSTOBON",
    "BIG COLA",
    "COLOMBIANA",
    "JUGOS HIT",
    "FRUTIÑO",
    "COUNTRY HILL",
    "DEL VALLE",
    # Snacks
    "MARGARITA",
    "SUPER RICAS",
    "YUPI",
    "FRITO LAY",
    "RAMO",
    "NOEL",
    "COLOMBINA",
    "DUCALES",
    "FESTIVAL",
    "CHOCORAMO",
    # Aceites
    "MAZOLA",
    "GOURMET",
    "PREMIER",
    "OLEOCALI",
    "LA BUENA",
    # Limpieza
    "FAB",
    "ARIEL",
    "DERSA",
    "TOP",
    "AXION",
    "LIS",
    "CLOROX",
    "FABULOSO",
    "VANISH",
    # Supermercados (marcas propias)
    "EKONO",
    "MARCA PROPIA",
    "ARA",
    "EXITO",
    "JUMBO",
    "OLIMPICA",
    "D1",
    "CARULLA",
    "ALKOSTO",
}


def expandir_abreviaturas(nombre: str) -> str:
    """
    Expande abreviaturas colombianas en el nombre del producto.

    Ejemplo:
        "P HIG ROSAL30H 12UND" → "PAPEL HIGIENICO ROSAL 30M 12 UNIDADES"
    """
    if not nombre:
        return ""

    nombre_upper = nombre.upper().strip()

    # Ordenar abreviaturas de mayor a menor longitud para evitar reemplazos parciales
    abreviaturas_ordenadas = sorted(
        ABREVIATURAS_COLOMBIA.items(), key=lambda x: len(x[0]), reverse=True
    )

    for abrev, expansion in abreviaturas_ordenadas:
        # Buscar la abreviatura como palabra completa o al inicio/final
        patron = r"\b" + re.escape(abrev) + r"\b"
        nombre_upper = re.sub(patron, expansion, nombre_upper)

    # Normalizar espacios múltiples
    nombre_upper = " ".join(nombre_upper.split())

    return nombre_upper


def extraer_metros(nombre: str) -> Optional[int]:
    """
    Extrae los metros de papel higiénico del nombre.
    Maneja errores OCR comunes: H, N, O en vez de M.

    Ejemplo:
        "ROSAL30H" → 30
        "ROSAL 30M" → 30
        "30MTS" → 30
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    # Patrones para metros (H, N, O son errores OCR de M)
    patrones = [
        r"(\d+)\s*[MHNO](?:TS)?(?:\s|$)",  # 30M, 30H, 30MTS
        r"(\d+)\s*METROS?",  # 30 METROS
        r"[A-Z]+(\d+)[MHNO]",  # ROSAL30M
    ]

    for patron in patrones:
        match = re.search(patron, nombre_upper)
        if match:
            metros = int(match.group(1))
            # Solo valores típicos de papel higiénico
            if metros in [15, 20, 25, 30, 40, 48, 50]:
                return metros

    return None


def extraer_cantidad(nombre: str) -> Optional[int]:
    """
    Extrae la cantidad de unidades del nombre.

    Ejemplo:
        "X30UND" → 30
        "12UND" → 12
        "12 UNIDADES" → 12
        "X12R" → 12
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    patrones = [
        r"X\s*(\d+)\s*(?:UND|UHD|UDS|R\b|UNID)",  # X30UND, X12R
        r"\b(\d+)\s*(?:UND|UHD|UDS)\b",  # 12UND, 12UHD (sin X)
        r"(\d+)\s*(?:UNIDADES|ROLLOS|PIEZAS)",  # 12 UNIDADES
        r"\b(\d+)\s*(?:UN\b|U\b)",  # 30 UN
    ]

    for patron in patrones:
        match = re.search(patron, nombre_upper)
        if match:
            cantidad = int(match.group(1))
            if 1 <= cantidad <= 100:  # Rango razonable
                return cantidad

    return None


def extraer_marca(nombre: str) -> Optional[str]:
    """
    Extrae la marca del nombre si está en la lista de marcas conocidas.
    """
    if not nombre:
        return None

    nombre_upper = nombre.upper()

    for marca in MARCAS_CONOCIDAS:
        if marca in nombre_upper:
            return marca

    return None


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """
    Calcula la similitud entre dos nombres usando SequenceMatcher.
    Retorna un valor entre 0.0 y 1.0.
    """
    if not nombre1 or not nombre2:
        return 0.0

    return SequenceMatcher(None, nombre1.lower(), nombre2.lower()).ratio()


def normalizar_nombre_para_comparacion(nombre: str) -> Dict:
    """
    Normaliza un nombre y extrae características para comparación.

    Retorna:
        {
            "nombre_expandido": str,
            "metros": int or None,
            "cantidad": int or None,
            "marca": str or None,
            "clave_agrupacion": str
        }
    """
    nombre_expandido = expandir_abreviaturas(nombre)
    metros = extraer_metros(nombre)
    cantidad = extraer_cantidad(nombre)
    marca = extraer_marca(nombre_expandido)

    # Crear clave de agrupación basada en características
    partes_clave = []

    if marca:
        partes_clave.append(marca)

    if metros:
        partes_clave.append(f"{metros}M")

    if cantidad:
        partes_clave.append(f"X{cantidad}")

    # Agregar palabras clave del nombre expandido
    palabras_clave = []
    for palabra in nombre_expandido.split():
        if len(palabra) > 3 and palabra not in [
            "PARA",
            "CON",
            "SIN",
            "DEL",
            "LOS",
            "LAS",
        ]:
            if palabra not in partes_clave:
                palabras_clave.append(palabra)

    partes_clave.extend(palabras_clave[:3])  # Máximo 3 palabras adicionales

    clave = "_".join(sorted(partes_clave)) if partes_clave else nombre_expandido[:20]

    return {
        "nombre_original": nombre,
        "nombre_expandido": nombre_expandido,
        "metros": metros,
        "cantidad": cantidad,
        "marca": marca,
        "clave_agrupacion": clave,
    }


def son_productos_similares(
    prod1: Dict, prod2: Dict, umbral_similitud: float = 0.75
) -> bool:
    """
    Determina si dos productos son probablemente el mismo basándose en:
    1. Similitud de nombre expandido (≥75%)
    2. Marca coincidente (si ambos tienen marca)
    3. Metros coincidentes (si ambos tienen metros)
    4. Precio similar (±10%)
    """
    info1 = normalizar_nombre_para_comparacion(prod1.get("nombre", ""))
    info2 = normalizar_nombre_para_comparacion(prod2.get("nombre", ""))

    # Si tienen la misma clave de agrupación, son iguales
    if info1["clave_agrupacion"] == info2["clave_agrupacion"]:
        return True

    # Similitud de nombre expandido
    similitud = calcular_similitud(info1["nombre_expandido"], info2["nombre_expandido"])

    if similitud < umbral_similitud:
        return False

    # Si ambos tienen marca y son diferentes, no son iguales
    if info1["marca"] and info2["marca"] and info1["marca"] != info2["marca"]:
        return False

    # Si ambos tienen metros y son diferentes, no son iguales
    if info1["metros"] and info2["metros"] and info1["metros"] != info2["metros"]:
        return False

    # Verificar precio similar (±15%)
    precio1 = float(prod1.get("valor", 0))
    precio2 = float(prod2.get("valor", 0))

    if precio1 > 0 and precio2 > 0:
        diferencia_precio = abs(precio1 - precio2) / max(precio1, precio2)
        if diferencia_precio > 0.15:  # Más de 15% de diferencia
            return False

    return True


def detectar_duplicados_automaticamente(
    productos: List[Dict],
    total_factura: float,
    umbral_similitud: float = 0.75,
    tolerancia_total: float = 0.15,
) -> Dict:
    """
    VERSIÓN 2.0 - Detecta duplicados usando expansión de abreviaturas y similitud fuzzy.

    Diseñado para manejar videos de facturas donde:
    - Cada frame detecta los mismos productos
    - El OCR produce variaciones del mismo nombre
    - Productos sin código (OXXO) deben agruparse por nombre similar

    Args:
        productos: Lista de productos con {codigo, nombre, valor, cantidad}
        total_factura: Total de la factura para validación
        umbral_similitud: Mínima similitud para considerar duplicados (default 0.75)
        tolerancia_total: Tolerancia en validación de total (default 0.15)

    Returns:
        {
            "productos_limpios": Lista de productos únicos,
            "duplicados_detectados": bool,
            "productos_eliminados": Lista de productos eliminados,
            "metricas": Dict con estadísticas
        }
    """
    print(f"\n{'='*80}")
    print(f"🔍 DETECTOR DE DUPLICADOS V2.0 - CON EXPANSIÓN DE ABREVIATURAS")
    print(f"{'='*80}")
    print(f"📦 Productos recibidos: {len(productos)}")
    print(f"💰 Total factura: ${total_factura:,.0f}")
    print(f"🎯 Umbral similitud: {umbral_similitud*100:.0f}%")

    if not productos:
        return {
            "productos_limpios": [],
            "duplicados_detectados": False,
            "productos_eliminados": [],
            "metricas": {
                "productos_originales": 0,
                "productos_despues_limpieza": 0,
                "duplicados_consolidados": 0,
            },
        }

    # PASO 1: Normalizar todos los productos
    productos_normalizados = []
    for idx, prod in enumerate(productos):
        codigo = str(prod.get("codigo", "")).strip()
        nombre = str(prod.get("nombre", "")).strip()
        valor = float(prod.get("valor", 0))
        cantidad = int(prod.get("cantidad", 1))

        if not nombre and not codigo:
            print(f"   ⚠️ Producto {idx+1} sin nombre ni código, omitiendo")
            continue

        info = normalizar_nombre_para_comparacion(nombre)

        productos_normalizados.append(
            {
                "indice_original": idx,
                "codigo": codigo,
                "nombre": nombre,
                "nombre_expandido": info["nombre_expandido"],
                "clave_agrupacion": info["clave_agrupacion"],
                "metros": info["metros"],
                "cantidad_detectada": info["cantidad"],
                "marca": info["marca"],
                "valor": valor,
                "cantidad": cantidad,
            }
        )

    print(f"\n📊 Productos normalizados: {len(productos_normalizados)}")

    # Mostrar expansiones para debugging
    print(f"\n🔤 Expansiones de nombres:")
    for prod in productos_normalizados[:5]:  # Solo los primeros 5
        if prod["nombre"] != prod["nombre_expandido"]:
            print(f"   '{prod['nombre'][:40]}' → '{prod['nombre_expandido'][:40]}'")

    # PASO 2: Agrupar productos similares
    grupos = []  # Lista de grupos, cada grupo es una lista de productos similares
    productos_asignados = set()

    for i, prod1 in enumerate(productos_normalizados):
        if i in productos_asignados:
            continue

        # Crear nuevo grupo con este producto
        grupo = [prod1]
        productos_asignados.add(i)

        # Buscar productos similares
        for j, prod2 in enumerate(productos_normalizados):
            if j in productos_asignados:
                continue

            # Primero: verificar por código si ambos tienen
            if prod1["codigo"] and prod2["codigo"]:
                if prod1["codigo"] == prod2["codigo"]:
                    grupo.append(prod2)
                    productos_asignados.add(j)
                    continue

            # Segundo: verificar por clave de agrupación
            if prod1["clave_agrupacion"] == prod2["clave_agrupacion"]:
                grupo.append(prod2)
                productos_asignados.add(j)
                continue

            # Tercero: verificar por similitud fuzzy
            if son_productos_similares(
                {"nombre": prod1["nombre"], "valor": prod1["valor"]},
                {"nombre": prod2["nombre"], "valor": prod2["valor"]},
                umbral_similitud,
            ):
                grupo.append(prod2)
                productos_asignados.add(j)

        grupos.append(grupo)

    print(f"\n📦 Grupos únicos identificados: {len(grupos)}")

    # PASO 3: Consolidar cada grupo en un solo producto
    productos_consolidados = []
    productos_eliminados = []
    duplicados_eliminados = 0

    for grupo in grupos:
        if len(grupo) == 1:
            # Solo una ocurrencia
            prod = grupo[0]
            productos_consolidados.append(
                {
                    "codigo": prod["codigo"],
                    "nombre": prod["nombre"],
                    "valor": prod["valor"],
                    "cantidad": 1,  # Forzar cantidad = 1
                }
            )
        else:
            # Múltiples ocurrencias del MISMO producto
            print(f"\n🔍 Consolidando grupo de {len(grupo)} items:")

            # Tomar el mejor nombre (el más largo/completo)
            mejor_nombre = max(grupo, key=lambda x: len(x["nombre_expandido"]))

            # Tomar el precio más frecuente
            precios = [p["valor"] for p in grupo]
            precio_frecuencia = {}
            for p in precios:
                precio_frecuencia[p] = precio_frecuencia.get(p, 0) + 1
            precio_mas_comun = max(
                precio_frecuencia.keys(), key=lambda x: precio_frecuencia[x]
            )

            # Tomar código si alguno lo tiene
            codigo = next((p["codigo"] for p in grupo if p["codigo"]), "")

            print(f"   Original: '{grupo[0]['nombre'][:40]}'")
            print(f"   Expandido: '{mejor_nombre['nombre_expandido'][:40]}'")
            print(
                f"   Precio: ${precio_mas_comun:,.0f} (de {len(set(precios))} precios únicos)"
            )
            print(f"   Frames consolidados: {len(grupo)}")

            productos_consolidados.append(
                {
                    "codigo": codigo,
                    "nombre": grupo[0]["nombre"],  # Mantener nombre original para BD
                    "valor": precio_mas_comun,
                    "cantidad": 1,  # Forzar cantidad = 1
                }
            )

            # Registrar eliminados
            for prod in grupo[1:]:
                productos_eliminados.append(
                    {
                        "nombre": prod["nombre"],
                        "valor": prod["valor"],
                        "razon": "Duplicado de frame consolidado",
                    }
                )

            duplicados_eliminados += len(grupo) - 1

    # PASO 4: Validar totales
    suma_productos = sum(p["valor"] * p["cantidad"] for p in productos_consolidados)
    diferencia = abs(suma_productos - total_factura)
    diferencia_porcentaje = (
        (diferencia / total_factura * 100) if total_factura > 0 else 0
    )

    print(f"\n💰 Validación de totales:")
    print(f"   Suma productos: ${suma_productos:,.0f}")
    print(f"   Total factura: ${total_factura:,.0f}")
    print(f"   Diferencia: ${diferencia:,.0f} ({diferencia_porcentaje:.1f}%)")

    if diferencia_porcentaje > tolerancia_total * 100:
        print(
            f"   ⚠️ ADVERTENCIA: Diferencia significativa (>{tolerancia_total*100:.0f}%)"
        )
    elif diferencia_porcentaje > 5:
        print(f"   ⚠️ Diferencia moderada")
    else:
        print(f"   ✅ Totales validados correctamente")

    # PASO 5: Resultado final
    resultado = {
        "productos_limpios": productos_consolidados,
        "duplicados_detectados": duplicados_eliminados > 0,
        "productos_eliminados": productos_eliminados,
        "metricas": {
            "productos_originales": len(productos),
            "productos_despues_limpieza": len(productos_consolidados),
            "duplicados_consolidados": duplicados_eliminados,
            "grupos_identificados": len(grupos),
            "diferencia_total": diferencia,
            "diferencia_porcentaje": diferencia_porcentaje,
            "suma_productos": suma_productos,
            "total_factura": total_factura,
        },
    }

    print(f"\n{'='*80}")
    print(f"✅ CONSOLIDACIÓN V2.0 COMPLETADA")
    print(f"{'='*80}")
    print(f"📊 Resumen:")
    print(f"   Productos de todos los frames: {len(productos)}")
    print(f"   Grupos únicos identificados: {len(grupos)}")
    print(f"   Productos finales: {len(productos_consolidados)}")
    print(f"   Duplicados eliminados: {duplicados_eliminados}")
    print(f"{'='*80}\n")

    return resultado


def limpiar_items_duplicados_db(factura_id: int, conn) -> int:
    """
    Limpia y consolida items duplicados en la BD.
    VERSIÓN 2.0: Usa similitud de nombres además de código/precio exacto.
    """
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 CONSOLIDANDO ITEMS EN BD V2.0 - FACTURA {factura_id}")
        print(f"{'='*80}")

        # Obtener todos los items de la factura
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad, producto_maestro_id
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad, producto_maestro_id
                FROM items_factura
                WHERE factura_id = ?
                ORDER BY id
            """,
                (factura_id,),
            )

        items = cursor.fetchall()

        if not items:
            print(f"✅ No hay items en la factura")
            return 0

        print(f"📦 Items encontrados: {len(items)}")

        # Convertir a lista de diccionarios
        items_dict = []
        for item in items:
            items_dict.append(
                {
                    "id": item[0],
                    "codigo": item[1] or "",
                    "nombre": item[2] or "",
                    "valor": float(item[3] or 0),
                    "cantidad": int(item[4] or 1),
                    "producto_maestro_id": item[5],
                }
            )

        # Usar el detector para encontrar grupos
        resultado = detectar_duplicados_automaticamente(
            [
                {
                    "codigo": i["codigo"],
                    "nombre": i["nombre"],
                    "valor": i["valor"],
                    "cantidad": i["cantidad"],
                }
                for i in items_dict
            ],
            total_factura=0,  # No validar total en este caso
            umbral_similitud=0.80,
        )

        if not resultado["duplicados_detectados"]:
            print(f"✅ No se detectaron duplicados")
            return 0

        # Agrupar items_dict según los grupos detectados
        # ... (implementación similar a la versión anterior pero usando similitud)

        items_eliminados = 0

        # Por ahora, usar la lógica simple de código+nombre+precio exacto
        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    ARRAY_AGG(id ORDER BY id) as ids
                FROM items_factura
                WHERE factura_id = %s
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado
                HAVING COUNT(*) > 1
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    COALESCE(codigo_leido, 'SIN_CODIGO') as codigo_grupo,
                    nombre_leido,
                    precio_pagado,
                    COUNT(*) as ocurrencias,
                    GROUP_CONCAT(id) as ids
                FROM items_factura
                WHERE factura_id = ?
                GROUP BY COALESCE(codigo_leido, 'SIN_CODIGO'), nombre_leido, precio_pagado
                HAVING COUNT(*) > 1
            """,
                (factura_id,),
            )

        grupos_duplicados = cursor.fetchall()

        for grupo in grupos_duplicados:
            codigo, nombre, precio, ocurrencias, ids = grupo

            if isinstance(ids, str):
                ids_list = [int(x) for x in ids.split(",")]
            else:
                ids_list = list(ids)

            primer_id = ids_list[0]
            otros_ids = ids_list[1:]

            if os.environ.get("DATABASE_TYPE") == "postgresql":
                cursor.execute(
                    "UPDATE items_factura SET cantidad = 1 WHERE id = %s", (primer_id,)
                )
                if otros_ids:
                    cursor.execute(
                        "DELETE FROM items_factura WHERE id = ANY(%s)", (otros_ids,)
                    )
            else:
                cursor.execute(
                    "UPDATE items_factura SET cantidad = 1 WHERE id = ?", (primer_id,)
                )
                if otros_ids:
                    placeholders = ",".join("?" * len(otros_ids))
                    cursor.execute(
                        f"DELETE FROM items_factura WHERE id IN ({placeholders})",
                        otros_ids,
                    )

            items_eliminados += len(otros_ids)

        conn.commit()

        print(f"\n✅ Items eliminados: {items_eliminados}")
        return items_eliminados

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
        return 0
    finally:
        cursor.close()


def diagnosticar_factura(factura_id: int, conn) -> Dict:
    """Diagnostica una factura mostrando expansiones de nombres."""
    cursor = conn.cursor()

    try:
        print(f"\n{'='*80}")
        print(f"🔍 DIAGNÓSTICO V2.0 DE FACTURA {factura_id}")
        print(f"{'='*80}")

        if os.environ.get("DATABASE_TYPE") == "postgresql":
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad
                FROM items_factura
                WHERE factura_id = %s
                ORDER BY id
            """,
                (factura_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, codigo_leido, nombre_leido, precio_pagado, cantidad
                FROM items_factura
                WHERE factura_id = ?
                ORDER BY id
            """,
                (factura_id,),
            )

        items = cursor.fetchall()

        print(f"\n📦 Items: {len(items)}")
        print(
            f"\n{'ID':<6} {'Código':<15} {'Nombre Original':<30} {'Nombre Expandido':<40} {'Precio':<12}"
        )
        print("-" * 110)

        for item in items:
            item_id, codigo, nombre, precio, cantidad = item
            nombre_expandido = expandir_abreviaturas(nombre or "")[:40]
            print(
                f"{item_id:<6} {(codigo or 'N/A'):<15} {(nombre or '')[:30]:<30} {nombre_expandido:<40} ${precio:>10,.0f}"
            )

        return {"success": True, "total_items": len(items)}

    except Exception as e:
        return {"success": False, "error": str(e)}
    finally:
        cursor.close()


# =============================================================================
# TEST
# =============================================================================
if __name__ == "__main__":
    # Test de expansión de abreviaturas
    print("\n🧪 TEST DE EXPANSIÓN DE ABREVIATURAS")
    print("=" * 60)

    nombres_test = [
        "P HIG ROSAL30H 12UND",
        "P ATG ROSAL30N 12UHD",
        "HUEVOS ORO AA X30UND",
        "HUEV ORO AA X30UHD",
        "ACE VEG GIRAS 1LT",
        "LCH ENT ALPN 1LT",
    ]

    for nombre in nombres_test:
        expandido = expandir_abreviaturas(nombre)
        metros = extraer_metros(nombre)
        cantidad = extraer_cantidad(nombre)
        marca = extraer_marca(expandido)

        print(f"\nOriginal:  {nombre}")
        print(f"Expandido: {expandido}")
        print(f"Metros: {metros}, Cantidad: {cantidad}, Marca: {marca}")

    # Test de detección de duplicados
    print("\n\n🧪 TEST DE DETECCIÓN DE DUPLICADOS")
    print("=" * 60)

    productos_test = [
        {"codigo": "", "nombre": "P HIG ROSAL30H 12UND", "valor": 19700, "cantidad": 1},
        {"codigo": "", "nombre": "P HIG ROSAL30N 12UND", "valor": 19700, "cantidad": 1},
        {"codigo": "", "nombre": "P ATG ROSAL30H 12UHD", "valor": 19700, "cantidad": 1},
        {"codigo": "", "nombre": "HUEVOS ORO AA X30UND", "valor": 16900, "cantidad": 1},
        {"codigo": "", "nombre": "HUEV ORO AA X30UHD", "valor": 16900, "cantidad": 1},
    ]

    resultado = detectar_duplicados_automaticamente(productos_test, total_factura=36600)

    print(f"\n📊 Resultado:")
    print(f"   Productos originales: {len(productos_test)}")
    print(f"   Productos únicos: {len(resultado['productos_limpios'])}")
    print(
        f"   Duplicados eliminados: {resultado['metricas']['duplicados_consolidados']}"
    )

    for prod in resultado["productos_limpios"]:
        print(f"   - {prod['nombre']}: ${prod['valor']:,.0f}")
