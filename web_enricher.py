"""
============================================================================
WEB ENRICHER - SISTEMA DE ENRIQUECIMIENTO DE PRODUCTOS VÍA WEB
============================================================================
Versión: 1.5
Fecha: 2025-11-28

🔧 CAMBIOS V1.5:
- 🆕 BÚSQUEDA INTELIGENTE POR PALABRAS CLAVE + VALIDACIÓN DE PLU
- Cuando no encuentra por PLU exacto, extrae palabras clave del nombre OCR
- Busca por palabras clave (marca, categoría) y valida que el PLU coincida
- Ejemplo: OCR="VERDURA CONGELADA MC CAIN" → busca "MC CAIN" → filtra por PLU
- Resuelve casos donde el nombre web es muy diferente al OCR

🔧 CAMBIOS V1.3 (heredados):
- VALIDAR NOMBRE OCR vs NOMBRE WEB en búsqueda por PLU
- Detecta cuando el OCR lee mal el PLU

🔧 CAMBIOS V1.2 (heredados):
- VALIDACIÓN DE PRECIO - rechaza si precio web es >5x o <0.2x del OCR

SUPERMERCADOS SOPORTADOS (VTEX):
- Carulla, Éxito, Jumbo, Olímpica, Alkosto, Makro, Colsubsidio
============================================================================
"""

import os
import re
import time
import traceback
from typing import Optional, Dict, Any, Tuple, List
from datetime import datetime, timedelta
from dataclasses import dataclass


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

DIAS_CACHE_PRECIO = 7

SUPERMERCADOS_VTEX = {
    "CARULLA": "carulla",
    "EXITO": "exito",
    "ÉXITO": "exito",
    "JUMBO": "jumbo",
    "METRO": "jumbo",
    "OLIMPICA": "olimpica",
    "OLÍMPICA": "olimpica",
    "ALKOSTO": "alkosto",
    "MAKRO": "makro",
    "COLSUBSIDIO": "mercadocolsubsidio",
    "MERCADO COLSUBSIDIO": "mercadocolsubsidio",
    "SUPERMERCADO COLSUBSIDIO": "mercadocolsubsidio",
}

VTEX_CONFIG = {
    "carulla": "https://www.carulla.com",
    "exito": "https://www.exito.com",
    "jumbo": "https://www.tiendasjumbo.co",
    "olimpica": "https://www.olimpica.com",
    "alkosto": "https://www.alkosto.com",
    "makro": "https://www.makro.com.co",
    "mercadocolsubsidio": "https://www.mercadocolsubsidio.com",
}

# 🆕 V1.5: Marcas conocidas para extracción de palabras clave
MARCAS_CONOCIDAS = {
    "MC CAIN",
    "MCCAIN",
    "ALPINA",
    "COLANTA",
    "ALQUERIA",
    "NESTLE",
    "KELLOGGS",
    "FAMILIA",
    "COLOMBINA",
    "CORONA",
    "DIANA",
    "ROA",
    "ARROZ ROA",
    "MARGARITA",
    "FRITO LAY",
    "POSTOBON",
    "COCA COLA",
    "PEPSI",
    "BIMBO",
    "RAMO",
    "NOEL",
    "ZENÚ",
    "RICA",
    "PIETRÁN",
    "KOKORIKO",
    "TOSH",
    "DORIA",
    "LA FINA",
    "NIVEA",
    "DOVE",
    "COLGATE",
    "PALMOLIVE",
    "PROTEX",
    "HEAD SHOULDERS",
    "PANTENE",
    "SEDAL",
    "EGGO",
    "AUNT JEMIMA",
    "QUAKER",
    "KELLOGG",
    "GREAT VALUE",
    "HACENDADO",
    "KIRKLAND",
    "OLIMPICA",
    "EXITO",
    "CARULLA",
    "DEL MONTE",
    "HEINZ",
    "MAGGI",
    "KNORR",
    "FRUCO",
    "SAN JORGE",
}

# Palabras a ignorar en búsquedas
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


def normalizar_supermercado(nombre: str) -> str:
    if not nombre:
        return None
    nombre_upper = nombre.upper().strip()
    for key, value in SUPERMERCADOS_VTEX.items():
        if key in nombre_upper or nombre_upper in key:
            return value
    return None


def es_supermercado_vtex(establecimiento: str) -> bool:
    """Verifica si el establecimiento tiene API VTEX disponible"""
    return normalizar_supermercado(establecimiento) is not None


def es_tienda_vtex(establecimiento: str) -> bool:
    """Alias para compatibilidad"""
    return es_supermercado_vtex(establecimiento)


def obtener_url_vtex(establecimiento: str) -> str:
    key = normalizar_supermercado(establecimiento)
    if key:
        return VTEX_CONFIG.get(key)
    return None


# ============================================================================
# DATACLASS PARA RESULTADO
# ============================================================================


@dataclass
class ProductoEnriquecido:
    """Datos enriquecidos de un producto"""

    codigo_plu: str = ""
    codigo_ean: str = ""
    nombre_web: str = ""
    marca: str = ""
    presentacion: str = ""
    categoria: str = ""
    precio_web: int = 0
    supermercado: str = ""
    url_producto: str = ""
    fuente: str = ""
    encontrado: bool = False
    verificado: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "codigo_plu": self.codigo_plu,
            "codigo_ean": self.codigo_ean,
            "nombre_web": self.nombre_web,
            "marca": self.marca,
            "presentacion": self.presentacion,
            "categoria": self.categoria,
            "precio_web": self.precio_web,
            "supermercado": self.supermercado,
            "url_producto": self.url_producto,
            "fuente": self.fuente,
            "encontrado": self.encontrado,
            "verificado": self.verificado,
        }


# ============================================================================
# CLASE PRINCIPAL: WebEnricher
# ============================================================================


class WebEnricher:
    """
    Sistema de enriquecimiento de productos vía web scraping.
    V1.5: Incluye búsqueda inteligente por palabras clave.
    """

    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn

    # ========================================================================
    # MÉTODO PRINCIPAL
    # ========================================================================

    def enriquecer(
        self,
        codigo: str,
        nombre_ocr: str,
        establecimiento: str,
        precio_ocr: int = 0,
    ) -> ProductoEnriquecido:
        """
        Enriquece un producto buscando en cache y/o web.
        V1.5: Incluye búsqueda por palabras clave cuando falla PLU directo.
        """
        resultado = ProductoEnriquecido()
        resultado.supermercado = establecimiento

        supermercado_key = normalizar_supermercado(establecimiento)
        if not supermercado_key:
            print(f"   ℹ️ {establecimiento} no tiene API web - usando datos OCR")
            return resultado

        tipo_codigo = self._clasificar_codigo(codigo)

        print(f"\n   🌐 ENRIQUECIMIENTO WEB V1.5:")
        print(f"      Código: {codigo} ({tipo_codigo})")
        print(f"      Nombre OCR: {nombre_ocr[:40] if nombre_ocr else 'N/A'}")
        print(f"      Supermercado: {supermercado_key}")

        # ====================================================================
        # PASO 1: Buscar en cache PLU
        # ====================================================================
        cache_plu = self._buscar_en_cache_plu(codigo, supermercado_key)

        if cache_plu:
            print(f"      ✅ ENCONTRADO EN CACHE PLU")
            self._incrementar_uso_cache(cache_plu["id"])

            resultado.encontrado = True
            resultado.fuente = "cache_plu"
            resultado.codigo_plu = cache_plu.get("codigo_plu", codigo)
            resultado.codigo_ean = cache_plu.get("codigo_ean", "")
            resultado.nombre_web = cache_plu.get("nombre_web", "")
            resultado.marca = cache_plu.get("marca", "")
            resultado.presentacion = cache_plu.get("presentacion", "")
            resultado.precio_web = cache_plu.get("precio_web", 0)
            resultado.url_producto = cache_plu.get("url_producto", "")

            return resultado

        # ====================================================================
        # PASO 2: Buscar en cache web enriched
        # ====================================================================
        cache_web = self._buscar_en_cache_web(codigo, supermercado_key, tipo_codigo)

        if cache_web:
            fecha_cache = cache_web.get("fecha_actualizacion")
            if fecha_cache and self._cache_es_valido(fecha_cache):
                print(f"      ✅ ENCONTRADO EN CACHE WEB")

                resultado.encontrado = True
                resultado.fuente = "cache_web"
                resultado.codigo_plu = cache_web.get("codigo_plu", codigo)
                resultado.codigo_ean = cache_web.get("codigo_ean", "")
                resultado.nombre_web = cache_web.get("nombre_completo", "")
                resultado.marca = cache_web.get("marca", "")
                resultado.presentacion = cache_web.get("presentacion", "")
                resultado.categoria = cache_web.get("categoria", "")
                resultado.precio_web = cache_web.get("precio_web", 0)
                resultado.url_producto = cache_web.get("url_producto", "")

                self._guardar_en_cache_plu(resultado)
                return resultado

        # ====================================================================
        # PASO 3: Consultar API VTEX (búsqueda directa por PLU/EAN)
        # ====================================================================
        print(f"      🔍 Consultando API VTEX...")

        datos_vtex = self._consultar_vtex(
            codigo=codigo,
            nombre_ocr=nombre_ocr,
            supermercado_key=supermercado_key,
            tipo_codigo=tipo_codigo,
            precio_ocr=precio_ocr,
        )

        if datos_vtex:
            return self._procesar_resultado_vtex(
                datos_vtex, codigo, nombre_ocr, precio_ocr, supermercado_key, resultado
            )

        # ====================================================================
        # 🆕 PASO 3.5: BÚSQUEDA INTELIGENTE POR PALABRAS CLAVE + VALIDACIÓN PLU
        # ====================================================================
        if tipo_codigo == "PLU" and nombre_ocr and len(nombre_ocr) >= 5:
            print(f"\n      🔍 PASO 3.5: Búsqueda inteligente por palabras clave...")

            datos_vtex_inteligente = self._busqueda_inteligente_por_palabras_clave(
                plu_buscado=codigo,
                nombre_ocr=nombre_ocr,
                supermercado_key=supermercado_key,
                precio_ocr=precio_ocr,
            )

            if datos_vtex_inteligente:
                print(f"      ✅ ENCONTRADO por búsqueda inteligente!")
                return self._procesar_resultado_vtex(
                    datos_vtex_inteligente,
                    codigo,
                    nombre_ocr,
                    precio_ocr,
                    supermercado_key,
                    resultado,
                )

        # No encontrado
        print(f"      ❌ NO ENCONTRADO EN VTEX")

        self._registrar_consulta(
            tipo="plu" if tipo_codigo == "PLU" else "ean",
            termino=codigo,
            supermercado=supermercado_key,
            encontrado=False,
        )

        return resultado

    # ========================================================================
    # 🆕 V1.5: BÚSQUEDA INTELIGENTE POR PALABRAS CLAVE
    # ========================================================================

    def _busqueda_inteligente_por_palabras_clave(
        self,
        plu_buscado: str,
        nombre_ocr: str,
        supermercado_key: str,
        precio_ocr: int = 0,
    ) -> Optional[Dict]:
        """
        🆕 V1.5: Búsqueda inteligente cuando el PLU directo no funciona.

        Flujo:
        1. Extrae palabras clave del nombre OCR (marcas, categorías)
        2. Busca en VTEX por cada palabra clave
        3. Filtra resultados que tengan el MISMO PLU
        4. Valida precio si está disponible

        Ejemplo:
        - OCR: "VERDURA CONGELADA MC CAIN" con PLU 632456
        - Extrae: ["MC CAIN", "CONGELAD", "VERDURA"]
        - Busca "MC CAIN" en VTEX → obtiene lista de productos MC CAIN
        - Filtra: ¿cuál tiene PLU 632456? → "VEGETALES MIXTOS MC CAIN 500"
        """
        import requests
        import urllib.parse

        base_url = VTEX_CONFIG.get(supermercado_key)
        if not base_url:
            return None

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "es-CO,es;q=0.9",
        }

        # 1. Extraer palabras clave del nombre OCR
        palabras_clave = self._extraer_palabras_clave(nombre_ocr)

        if not palabras_clave:
            print(
                f"         ℹ️ No se pudieron extraer palabras clave de: '{nombre_ocr}'"
            )
            return None

        print(f"         📝 Palabras clave extraídas: {palabras_clave}")

        # 2. Buscar por cada palabra clave
        for palabra in palabras_clave:
            print(f"         🔍 Buscando: '{palabra}'...")

            try:
                url = f"{base_url}/api/catalog_system/pub/products/search/{urllib.parse.quote(palabra)}"
                resp = requests.get(url, headers=headers, timeout=10)

                if resp.status_code not in [200, 206]:
                    continue

                data = resp.json()

                if not data or len(data) == 0:
                    continue

                print(f"         📦 {len(data)} resultados para '{palabra}'")

                # 3. Filtrar por PLU
                for item in data[:20]:  # Revisar hasta 20 resultados
                    producto = self._parsear_producto_vtex(item, base_url)

                    if not producto:
                        continue

                    plu_producto = producto.get("plu", "")

                    # Comparar PLUs (pueden tener diferencias menores)
                    if self._plus_coinciden(plu_buscado, plu_producto):
                        nombre_web = producto.get("nombre", "")
                        precio_web = producto.get("precio", 0)

                        print(f"         ✅ ¡MATCH por PLU!")
                        print(f"            PLU buscado: {plu_buscado}")
                        print(f"            PLU encontrado: {plu_producto}")
                        print(f"            Nombre web: {nombre_web[:50]}")

                        # 4. Validar precio si está disponible
                        if precio_ocr > 0 and precio_web > 0:
                            ratio = precio_web / precio_ocr
                            if ratio > 5.0 or ratio < 0.2:
                                print(
                                    f"         ⚠️ Precio muy diferente: OCR=${precio_ocr:,} vs Web=${precio_web:,}"
                                )
                                continue

                        return producto

            except Exception as e:
                print(f"         ⚠️ Error buscando '{palabra}': {str(e)[:50]}")
                continue

        print(
            f"         ❌ No se encontró producto con PLU {plu_buscado} en ninguna búsqueda"
        )
        return None

    def _extraer_palabras_clave(self, nombre_ocr: str) -> List[str]:
        """
        🆕 V1.5: Extrae palabras clave relevantes del nombre OCR.

        Prioridad:
        1. Marcas conocidas (MC CAIN, ALPINA, etc.)
        2. Palabras largas significativas (>= 5 caracteres)
        3. Categorías de producto
        """
        if not nombre_ocr:
            return []

        nombre_upper = nombre_ocr.upper().strip()
        palabras_clave = []

        # 1. Buscar marcas conocidas (tienen prioridad)
        for marca in MARCAS_CONOCIDAS:
            if marca in nombre_upper:
                palabras_clave.append(marca)
                # Quitar la marca del nombre para no duplicar
                nombre_upper = nombre_upper.replace(marca, " ")

        # 2. Extraer palabras significativas
        palabras = re.findall(r"\b[A-Z]{4,}\b", nombre_upper)

        for palabra in palabras:
            if palabra not in PALABRAS_IGNORAR and len(palabra) >= 4:
                # Evitar duplicados
                if palabra not in palabras_clave and not any(
                    palabra in pc for pc in palabras_clave
                ):
                    palabras_clave.append(palabra)

        # 3. Si hay muy pocas palabras, usar el nombre limpio completo
        if len(palabras_clave) < 2 and len(nombre_ocr) >= 8:
            nombre_limpio = self._limpiar_nombre_busqueda(nombre_ocr)
            if nombre_limpio and len(nombre_limpio) >= 6:
                palabras_clave.append(nombre_limpio)

        # Limitar a las 4 mejores palabras clave
        return palabras_clave[:4]

    def _plus_coinciden(self, plu1: str, plu2: str) -> bool:
        """
        🆕 V1.5: Verifica si dos PLUs coinciden (exacto o con diferencia mínima).
        """
        if not plu1 or not plu2:
            return False

        # Limpiar PLUs (solo dígitos)
        plu1_limpio = "".join(filter(str.isdigit, str(plu1)))
        plu2_limpio = "".join(filter(str.isdigit, str(plu2)))

        # Coincidencia exacta
        if plu1_limpio == plu2_limpio:
            return True

        # Permitir diferencia de 1 dígito (errores de OCR)
        if len(plu1_limpio) == len(plu2_limpio):
            diferencias = sum(1 for a, b in zip(plu1_limpio, plu2_limpio) if a != b)
            if diferencias <= 1:
                return True

        # Un PLU puede ser subcadena del otro (variantes)
        if plu1_limpio in plu2_limpio or plu2_limpio in plu1_limpio:
            if abs(len(plu1_limpio) - len(plu2_limpio)) <= 2:
                return True

        return False

    def _procesar_resultado_vtex(
        self,
        datos_vtex: Dict,
        codigo: str,
        nombre_ocr: str,
        precio_ocr: int,
        supermercado_key: str,
        resultado: ProductoEnriquecido,
    ) -> ProductoEnriquecido:
        """Procesa y valida resultado de VTEX"""
        precio_web = datos_vtex.get("precio", 0)
        nombre_web = datos_vtex.get("nombre", "")

        # Validación de precio
        if precio_ocr > 0 and precio_web > 0:
            ratio = precio_web / precio_ocr

            if ratio > 5.0:
                print(
                    f"      ⚠️ RECHAZADO: Precio web (${precio_web:,}) es {ratio:.1f}x mayor que OCR (${precio_ocr:,})"
                )
                self._registrar_consulta(
                    tipo="plu",
                    termino=codigo,
                    supermercado=supermercado_key,
                    encontrado=False,
                )
                return resultado

            if ratio < 0.2:
                print(
                    f"      ⚠️ RECHAZADO: Precio web (${precio_web:,}) es {ratio:.1f}x menor que OCR (${precio_ocr:,})"
                )
                self._registrar_consulta(
                    tipo="plu",
                    termino=codigo,
                    supermercado=supermercado_key,
                    encontrado=False,
                )
                return resultado

        print(f"      ✅ ENCONTRADO EN VTEX")
        print(f"         Nombre: {nombre_web[:50]}")

        resultado.encontrado = True
        resultado.fuente = "api_vtex"
        resultado.codigo_plu = datos_vtex.get("plu", codigo)
        resultado.codigo_ean = datos_vtex.get("ean", "")
        resultado.nombre_web = nombre_web
        resultado.marca = datos_vtex.get("marca", "")
        resultado.presentacion = datos_vtex.get("presentacion", "")
        resultado.categoria = datos_vtex.get("categoria", "")
        resultado.precio_web = precio_web
        resultado.url_producto = datos_vtex.get("url", "")

        # Guardar en caches
        self._guardar_en_cache_web(resultado)
        self._guardar_en_cache_plu(resultado)

        self._registrar_consulta(
            tipo="plu", termino=codigo, supermercado=supermercado_key, encontrado=True
        )

        return resultado

    # ========================================================================
    # MÉTODOS DE CACHE
    # ========================================================================

    def _buscar_en_cache_plu(self, codigo: str, supermercado: str) -> Optional[Dict]:
        """Busca en la tabla plu_supermercado_mapping"""
        try:
            self.cursor.execute(
                """
                SELECT id, codigo_plu, codigo_ean, nombre_web, marca,
                       presentacion, precio_web, url_producto, veces_usado
                FROM plu_supermercado_mapping
                WHERE codigo_plu = %s AND LOWER(supermercado) = LOWER(%s)
                LIMIT 1
            """,
                (codigo, supermercado),
            )

            row = self.cursor.fetchone()

            if row:
                return {
                    "id": row[0],
                    "codigo_plu": row[1],
                    "codigo_ean": row[2],
                    "nombre_web": row[3],
                    "marca": row[4],
                    "presentacion": row[5],
                    "precio_web": row[6],
                    "url_producto": row[7],
                    "veces_usado": row[8],
                }

            return None

        except Exception as e:
            print(f"      ⚠️ Error buscando en cache PLU: {e}")
            return None

    def _buscar_en_cache_web(
        self, codigo: str, supermercado: str, tipo_codigo: str
    ) -> Optional[Dict]:
        """Busca en la tabla productos_web_enriched"""
        try:
            if tipo_codigo == "EAN":
                self.cursor.execute(
                    """
                    SELECT id, codigo_ean, codigo_plu, nombre_completo, marca,
                           categoria, presentacion, precio_web, url_producto,
                           fecha_actualizacion
                    FROM productos_web_enriched
                    WHERE codigo_ean = %s AND LOWER(supermercado) = LOWER(%s)
                    AND activo = TRUE
                    LIMIT 1
                """,
                    (codigo, supermercado),
                )
            else:
                self.cursor.execute(
                    """
                    SELECT id, codigo_ean, codigo_plu, nombre_completo, marca,
                           categoria, presentacion, precio_web, url_producto,
                           fecha_actualizacion
                    FROM productos_web_enriched
                    WHERE codigo_plu = %s AND LOWER(supermercado) = LOWER(%s)
                    AND activo = TRUE
                    LIMIT 1
                """,
                    (codigo, supermercado),
                )

            row = self.cursor.fetchone()

            if row:
                return {
                    "id": row[0],
                    "codigo_ean": row[1],
                    "codigo_plu": row[2],
                    "nombre_completo": row[3],
                    "marca": row[4],
                    "categoria": row[5],
                    "presentacion": row[6],
                    "precio_web": row[7],
                    "url_producto": row[8],
                    "fecha_actualizacion": row[9],
                }

            return None

        except Exception as e:
            print(f"      ⚠️ Error buscando en cache web: {e}")
            return None

    def _guardar_en_cache_plu(self, producto: ProductoEnriquecido) -> bool:
        """Guarda en plu_supermercado_mapping"""
        try:
            supermercado_key = normalizar_supermercado(producto.supermercado)

            self.cursor.execute(
                """
                INSERT INTO plu_supermercado_mapping (
                    codigo_plu, codigo_ean, supermercado, nombre_web,
                    marca, presentacion, precio_web, url_producto, veces_usado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT (codigo_plu, supermercado)
                DO UPDATE SET
                    codigo_ean = COALESCE(EXCLUDED.codigo_ean, plu_supermercado_mapping.codigo_ean),
                    nombre_web = EXCLUDED.nombre_web,
                    marca = COALESCE(EXCLUDED.marca, plu_supermercado_mapping.marca),
                    presentacion = COALESCE(EXCLUDED.presentacion, plu_supermercado_mapping.presentacion),
                    precio_web = EXCLUDED.precio_web,
                    url_producto = EXCLUDED.url_producto,
                    veces_usado = plu_supermercado_mapping.veces_usado + 1,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """,
                (
                    producto.codigo_plu,
                    producto.codigo_ean if producto.codigo_ean else None,
                    supermercado_key,
                    producto.nombre_web,
                    producto.marca if producto.marca else None,
                    producto.presentacion if producto.presentacion else None,
                    producto.precio_web if producto.precio_web else None,
                    producto.url_producto if producto.url_producto else None,
                ),
            )

            self.conn.commit()
            return True

        except Exception as e:
            print(f"      ⚠️ Error guardando en cache PLU: {e}")
            self.conn.rollback()
            return False

    def _guardar_en_cache_web(self, producto: ProductoEnriquecido) -> bool:
        """Guarda en productos_web_enriched"""
        try:
            supermercado_key = normalizar_supermercado(producto.supermercado)

            self.cursor.execute(
                """
                INSERT INTO productos_web_enriched (
                    codigo_ean, codigo_plu, supermercado, nombre_completo,
                    marca, categoria, presentacion, precio_web, url_producto,
                    fuente, fecha_scraping
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'vtex', CURRENT_TIMESTAMP)
                ON CONFLICT (codigo_ean, codigo_plu, supermercado)
                DO UPDATE SET
                    nombre_completo = EXCLUDED.nombre_completo,
                    marca = COALESCE(EXCLUDED.marca, productos_web_enriched.marca),
                    categoria = COALESCE(EXCLUDED.categoria, productos_web_enriched.categoria),
                    presentacion = COALESCE(EXCLUDED.presentacion, productos_web_enriched.presentacion),
                    precio_web = EXCLUDED.precio_web,
                    url_producto = EXCLUDED.url_producto,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """,
                (
                    producto.codigo_ean if producto.codigo_ean else None,
                    producto.codigo_plu if producto.codigo_plu else None,
                    supermercado_key,
                    producto.nombre_web,
                    producto.marca if producto.marca else None,
                    producto.categoria if producto.categoria else None,
                    producto.presentacion if producto.presentacion else None,
                    producto.precio_web if producto.precio_web else None,
                    producto.url_producto if producto.url_producto else None,
                ),
            )

            self.conn.commit()
            return True

        except Exception as e:
            print(f"      ⚠️ Error guardando en cache web: {e}")
            self.conn.rollback()
            return False

    def _incrementar_uso_cache(self, mapping_id: int):
        """Incrementa el contador de uso del cache"""
        try:
            self.cursor.execute(
                """
                UPDATE plu_supermercado_mapping
                SET veces_usado = veces_usado + 1,
                    fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (mapping_id,),
            )
            self.conn.commit()
        except Exception as e:
            print(f"      ⚠️ Error incrementando uso: {e}")

    # ========================================================================
    # CONSULTA VTEX
    # ========================================================================

    def _consultar_vtex(
        self,
        codigo: str,
        nombre_ocr: str,
        supermercado_key: str,
        tipo_codigo: str,
        precio_ocr: int = 0,
    ) -> Optional[Dict]:
        """Consulta la API VTEX para obtener datos del producto."""
        import requests
        import urllib.parse

        base_url = VTEX_CONFIG.get(supermercado_key)
        if not base_url:
            print(f"      ⚠️ Supermercado {supermercado_key} no configurado")
            return None

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "es-CO,es;q=0.9",
        }

        producto = None

        try:
            # 1. Buscar por PLU
            if tipo_codigo == "PLU" and codigo and len(codigo) >= 3:
                url_plu = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_RefId:{codigo}"
                print(f"      🏷️ Buscando PLU directo...")

                try:
                    resp = requests.get(url_plu, headers=headers, timeout=10)
                    if resp.status_code in [200, 206]:
                        data = resp.json()
                        if data and len(data) > 0:
                            producto_candidato = self._parsear_producto_vtex(
                                data[0], base_url
                            )
                            if producto_candidato:
                                nombre_web = producto_candidato.get(
                                    "nombre", ""
                                ).upper()
                                nombre_ocr_upper = (
                                    nombre_ocr.upper() if nombre_ocr else ""
                                )

                                similitud_nombre = self._calcular_similitud_simple(
                                    nombre_ocr_upper, nombre_web
                                )
                                palabras_comunes = self._contar_palabras_comunes(
                                    nombre_ocr_upper, nombre_web
                                )

                                print(
                                    f"      🔍 Validando: OCR='{nombre_ocr_upper[:25]}' vs Web='{nombre_web[:25]}'"
                                )
                                print(
                                    f"         Similitud: {similitud_nombre:.2f}, Palabras comunes: {palabras_comunes}"
                                )

                                if palabras_comunes == 0 and similitud_nombre < 0.15:
                                    print(
                                        f"      ⚠️ RECHAZADO: Nombre OCR no coincide con producto web"
                                    )
                                    producto = None
                                else:
                                    producto = producto_candidato
                                    print(f"      ✅ Encontrado por PLU directo!")
                except Exception as e:
                    print(f"      ⚠️ Error buscando PLU: {str(e)[:50]}")

            # 2. Buscar por EAN
            if not producto and tipo_codigo == "EAN" and codigo and len(codigo) >= 8:
                url_ean = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{codigo}"
                print(f"      📊 Buscando EAN...")

                try:
                    resp = requests.get(url_ean, headers=headers, timeout=10)
                    if resp.status_code in [200, 206]:
                        data = resp.json()
                        if data and len(data) > 0:
                            producto = self._parsear_producto_vtex(data[0], base_url)
                            if producto:
                                print(f"      ✅ Encontrado por EAN!")
                except Exception as e:
                    print(f"      ⚠️ Error buscando EAN: {str(e)[:50]}")

            # 3. Buscar por NOMBRE (solo si tiene suficiente info)
            if not producto and nombre_ocr:
                nombre_limpio = self._limpiar_nombre_busqueda(nombre_ocr)

                if len(nombre_limpio) < 6:
                    print(f"      ℹ️ Nombre muy corto para buscar: '{nombre_limpio}'")
                else:
                    url_nombre = f"{base_url}/api/catalog_system/pub/products/search/{urllib.parse.quote(nombre_limpio)}"
                    print(f"      📝 Buscando nombre: '{nombre_limpio}'")

                    try:
                        resp = requests.get(url_nombre, headers=headers, timeout=10)
                        if resp.status_code in [200, 206]:
                            data = resp.json()
                            if data and len(data) > 0:
                                mejor_match = None
                                mejor_score = 0

                                for item in data[:5]:
                                    prod = self._parsear_producto_vtex(item, base_url)
                                    if prod:
                                        score = self._calcular_similitud_simple(
                                            nombre_limpio.upper(),
                                            prod["nombre"].upper(),
                                        )
                                        if score > mejor_score:
                                            mejor_score = score
                                            mejor_match = prod

                                if mejor_match and mejor_score >= 0.6:
                                    producto = mejor_match
                                    print(
                                        f"      ✅ Encontrado por nombre (score={mejor_score:.2f})"
                                    )
                                elif mejor_match:
                                    print(
                                        f"      ⚠️ Match rechazado por baja similitud (score={mejor_score:.2f} < 0.6)"
                                    )
                    except Exception as e:
                        print(f"      ⚠️ Error buscando nombre: {str(e)[:50]}")

            return producto

        except Exception as e:
            print(f"      ⚠️ Error consultando VTEX: {e}")
            traceback.print_exc()
            return None

    def _parsear_producto_vtex(self, item: Dict, base_url: str) -> Optional[Dict]:
        """Parsea un producto del JSON de VTEX"""
        try:
            nombre = item.get("productName", "")
            if not nombre:
                return None

            link = item.get("link", "")
            plu = None
            ean = None
            precio = None

            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                ean = sku.get("ean", "")

                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict) and ref.get("Value"):
                            plu = ref["Value"]
                            break

                if not plu:
                    plu = item.get("productReference", "") or item.get("productId", "")

                sellers = sku.get("sellers", [])
                if sellers:
                    oferta = sellers[0].get("commertialOffer", {})
                    precio = int(oferta.get("Price", 0)) or None

            if link and not link.startswith("http"):
                link = f"{base_url}{link}"

            return {
                "plu": str(plu) if plu else "",
                "ean": str(ean) if ean else "",
                "nombre": nombre,
                "marca": "",
                "presentacion": "",
                "categoria": "",
                "precio": precio or 0,
                "url": link,
            }
        except Exception as e:
            print(f"      ⚠️ Error parseando producto: {e}")
            return None

    def _limpiar_nombre_busqueda(self, nombre: str) -> str:
        """Limpia el nombre OCR para búsqueda"""
        abreviaciones = {
            "QSO": "queso",
            "OSO": "queso",
            "LCH": "leche",
            "DESLAC": "deslactosada",
            "DESCREM": "descremada",
            "UND": "unidad",
            "GR": "gramos",
            "KG": "kilo",
            "LT": "litro",
            "ML": "mililitros",
            "MARG": "margarina",
            "YOG": "yogurt",
            "JAM": "jamon",
            "ARR": "arroz",
            "AZUC": "azucar",
            "ACEIT": "aceite",
        }

        palabras = nombre.upper().split()
        resultado = []

        for palabra in palabras:
            limpia = re.sub(r"[^A-Z0-9]", "", palabra)
            if limpia in abreviaciones:
                resultado.append(abreviaciones[limpia])
            elif len(limpia) >= 2:
                resultado.append(limpia.lower())

        return " ".join(resultado)

    def _calcular_similitud_simple(self, s1: str, s2: str) -> float:
        """Calcula similitud simple entre dos strings"""
        palabras1 = set(s1.split())
        palabras2 = set(s2.split())

        if not palabras1 or not palabras2:
            return 0.0

        interseccion = len(palabras1 & palabras2)
        union = len(palabras1 | palabras2)

        return interseccion / union if union > 0 else 0.0

    def _contar_palabras_comunes(self, s1: str, s2: str) -> int:
        """Cuenta palabras significativas en común"""
        palabras1 = set(
            p
            for p in re.findall(r"\b[A-Z]{3,}\b", s1.upper())
            if p not in PALABRAS_IGNORAR
        )
        palabras2 = set(
            p
            for p in re.findall(r"\b[A-Z]{3,}\b", s2.upper())
            if p not in PALABRAS_IGNORAR
        )
        return len(palabras1 & palabras2)

    # ========================================================================
    # UTILIDADES
    # ========================================================================

    def _clasificar_codigo(self, codigo: str) -> str:
        """Clasifica el tipo de código"""
        if not codigo:
            return "DESCONOCIDO"

        codigo_limpio = "".join(filter(str.isdigit, str(codigo)))
        longitud = len(codigo_limpio)

        if longitud >= 8:
            return "EAN"
        elif 3 <= longitud <= 7:
            return "PLU"

        return "DESCONOCIDO"

    def _cache_es_valido(self, fecha_cache: datetime) -> bool:
        """Verifica si el cache sigue siendo válido"""
        if not fecha_cache:
            return False

        limite = datetime.now() - timedelta(days=DIAS_CACHE_PRECIO)
        return fecha_cache > limite

    def _registrar_consulta(
        self,
        tipo: str,
        termino: str,
        supermercado: str,
        encontrado: bool,
        tiempo_ms: int = 0,
        error: str = None,
    ):
        """Registra la consulta en el log"""
        try:
            self.cursor.execute(
                """
                INSERT INTO web_scraping_log (
                    tipo_busqueda, termino_busqueda, supermercado,
                    encontrado, tiempo_respuesta_ms, error_mensaje
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (tipo, termino, supermercado, encontrado, tiempo_ms, error),
            )
            self.conn.commit()
        except Exception:
            pass


# ============================================================================
# FUNCIÓN DE CONVENIENCIA
# ============================================================================


def enriquecer_producto(
    codigo: str,
    nombre_ocr: str,
    establecimiento: str,
    precio_ocr: int,
    cursor,
    conn,
) -> ProductoEnriquecido:
    """Función de conveniencia para enriquecer un producto."""
    enricher = WebEnricher(cursor, conn)
    return enricher.enriquecer(
        codigo=codigo,
        nombre_ocr=nombre_ocr,
        establecimiento=establecimiento,
        precio_ocr=precio_ocr,
    )


# ============================================================================
# MENSAJE DE CARGA
# ============================================================================

print("=" * 80)
print("🌐 WEB ENRICHER V1.5 - BÚSQUEDA INTELIGENTE POR PALABRAS CLAVE")
print("=" * 80)
print("🆕 NUEVO: Búsqueda por palabras clave cuando PLU directo falla")
print("   → Extrae marcas (MC CAIN, ALPINA, etc.) del nombre OCR")
print("   → Busca por marca y filtra resultados por PLU")
print("   → Resuelve casos donde nombre web ≠ nombre OCR")
print("=" * 80)
print("✅ Cache de 3 niveles: plu_mapping → web_enriched → API VTEX")
print("✅ Supermercados: Carulla, Éxito, Jumbo, Olímpica, Alkosto, Makro")
print("=" * 80)
