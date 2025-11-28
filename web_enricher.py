"""
============================================================================
WEB ENRICHER - Enriquecimiento de productos desde VTEX
============================================================================
Versión: 2.0
Fecha: 2025-11-28

Proporciona la clase WebEnricher para buscar productos en APIs VTEX
y enriquecer datos de productos con información web.
============================================================================
"""

import requests
import urllib.parse
from dataclasses import dataclass
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURACIÓN DE SUPERMERCADOS VTEX
# ============================================================================

SUPERMERCADOS_VTEX = {
    # Olímpica
    "OLIMPICA": "OLIMPICA",
    "OLÍMPICA": "OLIMPICA",
    "SAO": "OLIMPICA",
    # Éxito
    "EXITO": "EXITO",
    "ÉXITO": "EXITO",
    # Carulla
    "CARULLA": "CARULLA",
    # Jumbo/Metro
    "JUMBO": "JUMBO",
    "METRO": "JUMBO",
    "CENCOSUD": "JUMBO",
    # Alkosto
    "ALKOSTO": "ALKOSTO",
    "KTRONIX": "ALKOSTO",
    # Makro
    "MAKRO": "MAKRO",
    # Colsubsidio
    "COLSUBSIDIO": "COLSUBSIDIO",
    "MERCADO COLSUBSIDIO": "COLSUBSIDIO",
}

VTEX_URLS = {
    "OLIMPICA": "https://www.olimpica.com",
    "EXITO": "https://www.exito.com",
    "CARULLA": "https://www.carulla.com",
    "JUMBO": "https://www.tiendasjumbo.co",
    "ALKOSTO": "https://www.alkosto.com",
    "MAKRO": "https://www.makro.com.co",
    "COLSUBSIDIO": "https://www.mercadocolsubsidio.com",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-CO,es;q=0.9,en;q=0.8",
}


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================


def es_tienda_vtex(nombre: str) -> bool:
    """Verifica si un establecimiento tiene API VTEX disponible."""
    if not nombre:
        return False
    nombre_upper = nombre.upper().strip()
    return nombre_upper in SUPERMERCADOS_VTEX


def normalizar_establecimiento(nombre: str) -> Optional[str]:
    """Normaliza el nombre del establecimiento al formato VTEX."""
    if not nombre:
        return None
    nombre_upper = nombre.upper().strip()
    return SUPERMERCADOS_VTEX.get(nombre_upper)


def obtener_url_vtex(establecimiento: str) -> Optional[str]:
    """Obtiene la URL base de VTEX para un establecimiento."""
    normalizado = normalizar_establecimiento(establecimiento)
    if normalizado:
        return VTEX_URLS.get(normalizado)
    return None


# ============================================================================
# DATACLASS PARA RESULTADOS
# ============================================================================


@dataclass
class ResultadoEnriquecimiento:
    """Resultado del enriquecimiento de un producto."""

    encontrado: bool = False
    fuente: str = ""
    nombre_web: str = ""
    codigo_ean: str = ""
    codigo_plu: str = ""
    marca: str = ""
    precio_web: int = 0
    presentacion: str = ""
    categoria: str = ""
    url_producto: str = ""
    imagen_url: str = ""
    confianza: float = 0.0
    mensaje: str = ""


# ============================================================================
# CLASE PRINCIPAL: WebEnricher
# ============================================================================


class WebEnricher:
    """
    Enriquece productos buscando en APIs VTEX de supermercados colombianos.

    Uso:
        enricher = WebEnricher(cursor, conn)
        resultado = enricher.enriquecer(
            codigo="632967",
            nombre_ocr="CHOCOLATE CORONA",
            establecimiento="OLIMPICA",
            precio_ocr=13500
        )

        if resultado.encontrado:
            print(f"Nombre oficial: {resultado.nombre_web}")
            print(f"EAN: {resultado.codigo_ean}")
    """

    def __init__(self, cursor=None, conn=None):
        """
        Inicializa el enricher.

        Args:
            cursor: Cursor de base de datos (opcional, para cache)
            conn: Conexión de base de datos (opcional, para cache)
        """
        self.cursor = cursor
        self.conn = conn
        self.timeout = 10

    def enriquecer(
        self, codigo: str, nombre_ocr: str, establecimiento: str, precio_ocr: int = 0
    ) -> ResultadoEnriquecimiento:
        """
        Busca un producto en VTEX y retorna información enriquecida.

        Args:
            codigo: Código PLU o EAN del producto
            nombre_ocr: Nombre leído por OCR
            establecimiento: Nombre del supermercado
            precio_ocr: Precio leído por OCR (para validación)

        Returns:
            ResultadoEnriquecimiento con los datos encontrados
        """
        resultado = ResultadoEnriquecimiento()

        # Validar establecimiento
        if not es_tienda_vtex(establecimiento):
            resultado.mensaje = f"Establecimiento {establecimiento} no soporta VTEX"
            return resultado

        base_url = obtener_url_vtex(establecimiento)
        if not base_url:
            resultado.mensaje = "No se pudo obtener URL de VTEX"
            return resultado

        establecimiento_norm = normalizar_establecimiento(establecimiento)
        codigo_limpio = codigo.strip() if codigo else ""

        # ========================================
        # PASO 1: Buscar en cache local (si hay BD)
        # ========================================
        if self.cursor and codigo_limpio:
            cache_result = self._buscar_en_cache(codigo_limpio, establecimiento_norm)
            if cache_result:
                return cache_result

        # ========================================
        # PASO 2: Buscar por PLU en VTEX
        # ========================================
        if codigo_limpio and len(codigo_limpio) >= 3:
            vtex_result = self._buscar_por_plu_vtex(
                codigo_limpio, base_url, establecimiento_norm
            )
            if vtex_result.encontrado:
                # Validar precio si tenemos referencia
                if precio_ocr > 0 and vtex_result.precio_web > 0:
                    ratio = vtex_result.precio_web / precio_ocr
                    if ratio > 5 or ratio < 0.2:
                        # Precio muy diferente, posible producto incorrecto
                        vtex_result.confianza = 0.3
                        vtex_result.mensaje = (
                            "Precio web difiere significativamente del OCR"
                        )

                # Guardar en cache
                if self.cursor:
                    self._guardar_en_cache(vtex_result, establecimiento_norm)

                return vtex_result

        # ========================================
        # PASO 3: Buscar por EAN en VTEX
        # ========================================
        if codigo_limpio and len(codigo_limpio) >= 8:
            ean_result = self._buscar_por_ean_vtex(
                codigo_limpio, base_url, establecimiento_norm
            )
            if ean_result.encontrado:
                if self.cursor:
                    self._guardar_en_cache(ean_result, establecimiento_norm)
                return ean_result

        # ========================================
        # PASO 4: Búsqueda inteligente por palabras clave
        # ========================================
        if nombre_ocr and len(nombre_ocr) >= 3:
            keyword_result = self._busqueda_inteligente(
                codigo_limpio, nombre_ocr, base_url, establecimiento_norm, precio_ocr
            )
            if keyword_result.encontrado:
                if self.cursor:
                    self._guardar_en_cache(keyword_result, establecimiento_norm)
                return keyword_result

        # No encontrado
        resultado.mensaje = "Producto no encontrado en catálogo web"
        return resultado

    def _buscar_en_cache(
        self, codigo: str, establecimiento: str
    ) -> Optional[ResultadoEnriquecimiento]:
        """Busca en el cache local de productos VTEX."""
        try:
            self.cursor.execute(
                """
                SELECT nombre, ean, plu, marca, precio, categoria,
                       presentacion, url_producto, imagen_url
                FROM productos_vtex_cache
                WHERE (plu = %s OR ean = %s)
                  AND establecimiento = %s
                ORDER BY veces_usado DESC
                LIMIT 1
            """,
                (codigo, codigo, establecimiento),
            )

            row = self.cursor.fetchone()
            if row:
                return ResultadoEnriquecimiento(
                    encontrado=True,
                    fuente="cache_vtex",
                    nombre_web=row[0] or "",
                    codigo_ean=row[1] or "",
                    codigo_plu=row[2] or "",
                    marca=row[3] or "",
                    precio_web=int(row[4]) if row[4] else 0,
                    categoria=row[5] or "",
                    presentacion=row[6] or "",
                    url_producto=row[7] or "",
                    imagen_url=row[8] or "",
                    confianza=0.95,
                    mensaje="Encontrado en cache",
                )
        except Exception as e:
            logger.warning(f"Error buscando en cache: {e}")

        return None

    def _buscar_por_plu_vtex(
        self, plu: str, base_url: str, establecimiento: str
    ) -> ResultadoEnriquecimiento:
        """Busca un producto por PLU en VTEX."""
        resultado = ResultadoEnriquecimiento()

        url = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_RefId:{plu}"

        try:
            resp = requests.get(url, headers=HEADERS, timeout=self.timeout)

            if resp.status_code in [200, 206]:
                data = resp.json()
                if data and len(data) > 0:
                    return self._parsear_producto_vtex(
                        data[0], base_url, establecimiento, "api_vtex_plu"
                    )
        except Exception as e:
            logger.warning(f"Error buscando PLU en VTEX: {e}")

        return resultado

    def _buscar_por_ean_vtex(
        self, ean: str, base_url: str, establecimiento: str
    ) -> ResultadoEnriquecimiento:
        """Busca un producto por EAN en VTEX."""
        resultado = ResultadoEnriquecimiento()

        url = f"{base_url}/api/catalog_system/pub/products/search?fq=alternateIds_Ean:{ean}"

        try:
            resp = requests.get(url, headers=HEADERS, timeout=self.timeout)

            if resp.status_code in [200, 206]:
                data = resp.json()
                if data and len(data) > 0:
                    return self._parsear_producto_vtex(
                        data[0], base_url, establecimiento, "api_vtex_ean"
                    )
        except Exception as e:
            logger.warning(f"Error buscando EAN en VTEX: {e}")

        return resultado

    def _busqueda_inteligente(
        self,
        codigo: str,
        nombre_ocr: str,
        base_url: str,
        establecimiento: str,
        precio_ocr: int,
    ) -> ResultadoEnriquecimiento:
        """
        Búsqueda inteligente por palabras clave.
        Útil cuando el nombre OCR difiere del nombre web.
        """
        resultado = ResultadoEnriquecimiento()

        # Extraer palabras clave del nombre
        palabras = self._extraer_palabras_clave(nombre_ocr)

        if not palabras:
            return resultado

        # Buscar por cada palabra clave
        for palabra in palabras[:3]:  # Máximo 3 intentos
            url = f"{base_url}/api/catalog_system/pub/products/search?ft={urllib.parse.quote(palabra)}&_from=0&_to=20"

            try:
                resp = requests.get(url, headers=HEADERS, timeout=self.timeout)

                if resp.status_code in [200, 206]:
                    data = resp.json()

                    # Filtrar por PLU si lo tenemos
                    if codigo and len(codigo) >= 4:
                        for item in data:
                            plu_item = self._extraer_plu_de_item(item)
                            if plu_item and self._plus_coinciden(codigo, plu_item):
                                result = self._parsear_producto_vtex(
                                    item,
                                    base_url,
                                    establecimiento,
                                    "api_vtex_busqueda_inteligente",
                                )
                                if result.encontrado:
                                    # Validar precio
                                    if precio_ocr > 0 and result.precio_web > 0:
                                        ratio = result.precio_web / precio_ocr
                                        if 0.2 <= ratio <= 5:
                                            return result
                                    else:
                                        return result

                    # Si no hay PLU, buscar por coincidencia de nombre
                    else:
                        otras_palabras = [p.lower() for p in palabras if p != palabra]
                        for item in data:
                            nombre_item = item.get("productName", "").lower()
                            if all(p in nombre_item for p in otras_palabras):
                                return self._parsear_producto_vtex(
                                    item,
                                    base_url,
                                    establecimiento,
                                    "api_vtex_busqueda_inteligente",
                                )

            except Exception as e:
                logger.warning(f"Error en búsqueda inteligente: {e}")
                continue

        return resultado

    def _extraer_palabras_clave(self, nombre: str) -> List[str]:
        """Extrae palabras clave significativas del nombre."""
        if not nombre:
            return []

        # Marcas conocidas (prioridad alta)
        MARCAS = [
            "MC CAIN",
            "MCCAIN",
            "ALPINA",
            "COLANTA",
            "ALQUERIA",
            "NESTLE",
            "KELLOGGS",
            "KELLOGG",
            "FAMILIA",
            "COLOMBINA",
            "CORONA",
            "DIANA",
            "ROA",
            "MARGARITA",
            "FRITO LAY",
            "POSTOBON",
            "COCA COLA",
            "PEPSI",
            "BIMBO",
            "RAMO",
            "NOEL",
            "ZENÚ",
            "ZENU",
            "RICA",
            "PIETRÁN",
            "PIETRAN",
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
            "QUAKER",
            "GREAT VALUE",
            "DEL MONTE",
            "HEINZ",
            "MAGGI",
            "KNORR",
            "FRUCO",
        ]

        # Palabras a ignorar
        IGNORAR = {
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

        nombre_upper = nombre.upper()
        palabras_clave = []

        # Primero buscar marcas
        for marca in MARCAS:
            if marca in nombre_upper:
                palabras_clave.append(marca)
                break

        # Luego palabras significativas
        palabras = nombre_upper.split()
        for palabra in palabras:
            palabra_limpia = "".join(c for c in palabra if c.isalnum())
            if (
                len(palabra_limpia) >= 4
                and palabra_limpia not in IGNORAR
                and palabra_limpia not in palabras_clave
            ):
                palabras_clave.append(palabra_limpia)

        return palabras_clave[:5]

    def _plus_coinciden(self, plu1: str, plu2: str) -> bool:
        """Verifica si dos PLUs son el mismo (con tolerancia a errores OCR)."""
        if not plu1 or not plu2:
            return False

        plu1_limpio = "".join(c for c in plu1 if c.isdigit())
        plu2_limpio = "".join(c for c in plu2 if c.isdigit())

        # Coincidencia exacta
        if plu1_limpio == plu2_limpio:
            return True

        # Diferencia de 1 dígito (error OCR común)
        if len(plu1_limpio) == len(plu2_limpio):
            diferencias = sum(1 for a, b in zip(plu1_limpio, plu2_limpio) if a != b)
            if diferencias <= 1:
                return True

        # Subcadena (variantes de producto)
        if plu1_limpio in plu2_limpio or plu2_limpio in plu1_limpio:
            if abs(len(plu1_limpio) - len(plu2_limpio)) <= 2:
                return True

        return False

    def _extraer_plu_de_item(self, item: dict) -> Optional[str]:
        """Extrae el PLU de un item de VTEX."""
        try:
            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]
                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict) and ref.get("Value"):
                            return ref["Value"]

            return item.get("productReference", "") or item.get("productId", "")
        except:
            return None

    def _parsear_producto_vtex(
        self, item: dict, base_url: str, establecimiento: str, fuente: str
    ) -> ResultadoEnriquecimiento:
        """Parsea un producto de VTEX a ResultadoEnriquecimiento."""
        resultado = ResultadoEnriquecimiento()

        try:
            nombre = item.get("productName", "")
            if not nombre:
                return resultado

            resultado.encontrado = True
            resultado.fuente = fuente
            resultado.nombre_web = nombre
            resultado.marca = item.get("brand", "")

            # Extraer categoría
            categorias = item.get("categories", [])
            if categorias:
                resultado.categoria = categorias[0].replace("/", " > ").strip()

            # Link
            link = item.get("link", "")
            if link and not link.startswith("http"):
                link = f"{base_url}{link}"
            resultado.url_producto = link

            # Datos del SKU
            if item.get("items") and len(item["items"]) > 0:
                sku = item["items"][0]

                # EAN
                resultado.codigo_ean = sku.get("ean", "") or ""

                # PLU
                ref_ids = sku.get("referenceId", [])
                if ref_ids and isinstance(ref_ids, list):
                    for ref in ref_ids:
                        if isinstance(ref, dict) and ref.get("Value"):
                            resultado.codigo_plu = ref["Value"]
                            break

                if not resultado.codigo_plu:
                    resultado.codigo_plu = item.get("productReference", "") or item.get(
                        "productId", ""
                    )

                # Precio
                sellers = sku.get("sellers", [])
                if sellers:
                    oferta = sellers[0].get("commertialOffer", {})
                    resultado.precio_web = int(oferta.get("Price", 0) or 0)

                # Imagen
                images = sku.get("images", [])
                if images and len(images) > 0:
                    resultado.imagen_url = images[0].get("imageUrl", "")

                # Presentación (del nombre del SKU)
                resultado.presentacion = sku.get("name", "")

            resultado.confianza = 0.9
            resultado.mensaje = f"Encontrado en {establecimiento}"

        except Exception as e:
            logger.warning(f"Error parseando producto VTEX: {e}")
            resultado.encontrado = False

        return resultado

    def _guardar_en_cache(
        self, resultado: ResultadoEnriquecimiento, establecimiento: str
    ):
        """Guarda el resultado en el cache de productos VTEX."""
        if not self.cursor or not self.conn:
            return

        try:
            # Verificar si ya existe
            self.cursor.execute(
                """
                SELECT id FROM productos_vtex_cache
                WHERE establecimiento = %s AND (plu = %s OR ean = %s)
            """,
                (establecimiento, resultado.codigo_plu, resultado.codigo_ean),
            )

            existe = self.cursor.fetchone()

            if existe:
                # Actualizar
                self.cursor.execute(
                    """
                    UPDATE productos_vtex_cache SET
                        nombre = %s,
                        marca = %s,
                        precio = %s,
                        categoria = %s,
                        presentacion = %s,
                        url_producto = %s,
                        imagen_url = %s,
                        fecha_actualizacion = CURRENT_TIMESTAMP,
                        veces_usado = veces_usado + 1
                    WHERE id = %s
                """,
                    (
                        resultado.nombre_web,
                        resultado.marca,
                        resultado.precio_web,
                        resultado.categoria,
                        resultado.presentacion,
                        resultado.url_producto,
                        resultado.imagen_url,
                        existe[0],
                    ),
                )
            else:
                # Insertar
                self.cursor.execute(
                    """
                    INSERT INTO productos_vtex_cache (
                        establecimiento, plu, ean, nombre, marca, precio,
                        categoria, presentacion, url_producto, imagen_url,
                        veces_usado
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
                """,
                    (
                        establecimiento,
                        resultado.codigo_plu,
                        resultado.codigo_ean,
                        resultado.nombre_web,
                        resultado.marca,
                        resultado.precio_web,
                        resultado.categoria,
                        resultado.presentacion,
                        resultado.url_producto,
                        resultado.imagen_url,
                    ),
                )

            self.conn.commit()

        except Exception as e:
            logger.warning(f"Error guardando en cache: {e}")
            try:
                self.conn.rollback()
            except:
                pass


# ============================================================================
# FUNCIÓN DE BÚSQUEDA DIRECTA (sin clase)
# ============================================================================


def buscar_en_vtex(
    codigo: str, establecimiento: str, nombre_ocr: str = "", precio_ocr: int = 0
) -> Dict[str, Any]:
    """
    Función simple para buscar en VTEX sin necesidad de instanciar la clase.

    Args:
        codigo: PLU o EAN del producto
        establecimiento: Nombre del supermercado
        nombre_ocr: Nombre leído por OCR (opcional)
        precio_ocr: Precio leído por OCR (opcional)

    Returns:
        Dict con los datos del producto o error
    """
    enricher = WebEnricher()
    resultado = enricher.enriquecer(
        codigo=codigo,
        nombre_ocr=nombre_ocr,
        establecimiento=establecimiento,
        precio_ocr=precio_ocr,
    )

    if resultado.encontrado:
        return {
            "success": True,
            "encontrado": True,
            "fuente": resultado.fuente,
            "producto": {
                "nombre": resultado.nombre_web,
                "ean": resultado.codigo_ean,
                "plu": resultado.codigo_plu,
                "marca": resultado.marca,
                "precio": resultado.precio_web,
                "categoria": resultado.categoria,
                "presentacion": resultado.presentacion,
                "url": resultado.url_producto,
                "imagen": resultado.imagen_url,
            },
            "confianza": resultado.confianza,
        }
    else:
        return {
            "success": True,
            "encontrado": False,
            "mensaje": resultado.mensaje,
        }


# ============================================================================
# TEST
# ============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("TEST: WebEnricher")
    print("=" * 60)

    # Test 1: Verificar establecimiento
    print("\n1. Verificando establecimientos VTEX:")
    for est in ["OLIMPICA", "CARULLA", "D1", "ARA"]:
        print(
            f"   {est}: {'✅ Soportado' if es_tienda_vtex(est) else '❌ No soportado'}"
        )

    # Test 2: Búsqueda simple
    print("\n2. Búsqueda en VTEX:")
    resultado = buscar_en_vtex(
        codigo="632967", establecimiento="OLIMPICA", nombre_ocr="CHOCOLATE CORONA"
    )

    if resultado.get("encontrado"):
        prod = resultado["producto"]
        print(f"   ✅ Encontrado: {prod['nombre']}")
        print(f"      PLU: {prod['plu']}")
        print(f"      EAN: {prod['ean']}")
        print(f"      Precio: ${prod['precio']:,}")
    else:
        print(f"   ❌ No encontrado: {resultado.get('mensaje', 'Sin mensaje')}")
