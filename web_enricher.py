"""
============================================================================
WEB ENRICHER - SISTEMA DE ENRIQUECIMIENTO DE PRODUCTOS VÍA WEB
============================================================================
Versión: 1.0
Fecha: 2025-11-26

PROPÓSITO:
- Enriquecer datos de productos usando APIs web (VTEX)
- Cache de 3 niveles para minimizar consultas externas
- Obtener nombres correctos, EAN, marcas desde webs de supermercados

FLUJO:
1. Buscar en plu_supermercado_mapping (cache rápido) → < 1ms
2. Buscar en productos_web_enriched (cache completo) → < 5ms
3. Consultar API VTEX (solo si no hay cache) → ~2-5 segundos
4. Guardar resultado en cache para futuras consultas

SUPERMERCADOS SOPORTADOS:
- Carulla (VTEX)
- Éxito (VTEX)
- Jumbo (VTEX)
- Olímpica (VTEX)
============================================================================
"""

import os
import time
import asyncio
import traceback
from typing import Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

# Días antes de refrescar el cache
DIAS_CACHE_PRECIO = 7

# Supermercados VTEX soportados
SUPERMERCADOS_VTEX = {
    "CARULLA": "carulla",
    "EXITO": "exito",
    "ÉXITO": "exito",
    "JUMBO": "jumbo",
    "OLIMPICA": "olimpica",
    "OLÍMPICA": "olimpica",
}


# Mapeo de nombres de establecimiento a clave VTEX
def normalizar_supermercado(establecimiento: str) -> Optional[str]:
    """Normaliza el nombre del establecimiento a clave VTEX"""
    if not establecimiento:
        return None

    establecimiento_upper = establecimiento.upper().strip()

    for key, value in SUPERMERCADOS_VTEX.items():
        if key in establecimiento_upper:
            return value

    return None


def es_supermercado_vtex(establecimiento: str) -> bool:
    """Verifica si el establecimiento es soportado por VTEX"""
    return normalizar_supermercado(establecimiento) is not None


# ============================================================================
# DATACLASS PARA RESULTADO
# ============================================================================


@dataclass
class ProductoEnriquecido:
    """Datos enriquecidos de un producto"""

    # Identificadores
    codigo_plu: str = ""
    codigo_ean: str = ""

    # Datos del producto
    nombre_web: str = ""
    marca: str = ""
    presentacion: str = ""
    categoria: str = ""

    # Precio (solo referencia)
    precio_web: int = 0

    # Metadata
    supermercado: str = ""
    url_producto: str = ""
    fuente: str = ""  # 'cache_plu', 'cache_web', 'api_vtex'

    # Estado
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

    Uso:
        enricher = WebEnricher(cursor, conn)
        resultado = enricher.enriquecer(
            codigo="632967",
            nombre_ocr="ZNAHRIA X K",
            establecimiento="OLIMPICA"
        )

        if resultado.encontrado:
            nombre_correcto = resultado.nombre_web
            ean = resultado.codigo_ean
    """

    def __init__(self, cursor, conn):
        self.cursor = cursor
        self.conn = conn
        self._vtex_scraper_class = None
        self._scraper_loaded = False

    def _load_scraper(self):
        """Carga el módulo vtex_scraper de forma lazy"""
        if self._scraper_loaded:
            return

        try:
            from vtex_scraper import VTEXScraper

            self._vtex_scraper_class = VTEXScraper
            self._scraper_loaded = True
            print("   ✅ VTEXScraper cargado")
        except ImportError as e:
            print(f"   ⚠️ VTEXScraper no disponible: {e}")
            self._vtex_scraper_class = None
            self._scraper_loaded = True  # Marcar como intentado

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

        Args:
            codigo: PLU o EAN del producto
            nombre_ocr: Nombre detectado por OCR (puede tener errores)
            establecimiento: Nombre del supermercado
            precio_ocr: Precio de la factura (solo para referencia)

        Returns:
            ProductoEnriquecido con los datos encontrados
        """
        resultado = ProductoEnriquecido()
        resultado.supermercado = establecimiento

        # Verificar si es supermercado VTEX
        supermercado_key = normalizar_supermercado(establecimiento)
        if not supermercado_key:
            print(f"   ℹ️ {establecimiento} no es supermercado VTEX soportado")
            return resultado

        # Clasificar código
        tipo_codigo = self._clasificar_codigo(codigo)

        print(f"\n   🌐 ENRIQUECIMIENTO WEB:")
        print(f"      Código: {codigo} ({tipo_codigo})")
        print(f"      Supermercado: {supermercado_key}")

        # ====================================================================
        # PASO 1: Buscar en cache PLU (más rápido)
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
            # Verificar si el cache está fresco
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

                # Guardar también en cache PLU para acceso más rápido
                self._guardar_en_cache_plu(resultado)

                return resultado

        # ====================================================================
        # PASO 3: Consultar API VTEX
        # ====================================================================
        print(f"      🔍 Consultando API VTEX...")

        datos_vtex = self._consultar_vtex(
            codigo=codigo,
            nombre_ocr=nombre_ocr,
            supermercado_key=supermercado_key,
            tipo_codigo=tipo_codigo,
        )

        if datos_vtex:
            print(f"      ✅ ENCONTRADO EN VTEX")
            print(f"         Nombre: {datos_vtex.get('nombre', '')[:50]}")

            resultado.encontrado = True
            resultado.fuente = "api_vtex"
            resultado.codigo_plu = datos_vtex.get("plu", codigo)
            resultado.codigo_ean = datos_vtex.get("ean", "")
            resultado.nombre_web = datos_vtex.get("nombre", "")
            resultado.marca = datos_vtex.get("marca", "")
            resultado.presentacion = datos_vtex.get("presentacion", "")
            resultado.categoria = datos_vtex.get("categoria", "")
            resultado.precio_web = datos_vtex.get("precio", 0)
            resultado.url_producto = datos_vtex.get("url", "")

            # Guardar en ambos caches
            self._guardar_en_cache_web(resultado)
            self._guardar_en_cache_plu(resultado)

            # Registrar en log
            self._registrar_consulta(
                tipo="plu" if tipo_codigo == "PLU" else "ean",
                termino=codigo,
                supermercado=supermercado_key,
                encontrado=True,
            )

            return resultado

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
    ) -> Optional[Dict]:
        """Consulta la API VTEX para obtener datos del producto"""

        self._load_scraper()

        if not hasattr(self, "_vtex_scraper_class") or not self._vtex_scraper_class:
            print(f"      ⚠️ Scraper no disponible")
            return None

        try:
            # Crear instancia del scraper para el supermercado específico
            scraper = self._vtex_scraper_class(supermercado=supermercado_key)

            # Ejecutar búsqueda async de forma sync
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Usar el método enriquecer() que tiene toda la lógica
                resultado = loop.run_until_complete(
                    scraper.enriquecer(
                        nombre_ocr=nombre_ocr,
                        plu_ocr=codigo if tipo_codigo == "PLU" else None,
                        ean_ocr=codigo if tipo_codigo == "EAN" else None,
                    )
                )
            finally:
                loop.close()

            # Verificar si encontró algo
            if (
                resultado
                and resultado.get("encontrado")
                or resultado.get("enriquecido")
            ):
                return {
                    "plu": resultado.get("plu_web", codigo),
                    "ean": resultado.get("ean_web", ""),
                    "nombre": resultado.get("nombre_completo", ""),
                    "marca": "",  # VTEX no retorna marca directamente
                    "presentacion": "",
                    "categoria": "",
                    "precio": resultado.get("precio_web", 0),
                    "url": resultado.get("url", ""),
                }

            return None

        except Exception as e:
            print(f"      ⚠️ Error consultando VTEX: {e}")
            traceback.print_exc()
            return None

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
        except Exception as e:
            # No fallar si el log falla
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
    """
    Función de conveniencia para enriquecer un producto.

    Uso:
        resultado = enriquecer_producto(
            codigo="632967",
            nombre_ocr="ZNAHRIA X K",
            establecimiento="OLIMPICA",
            precio_ocr=3500,
            cursor=cursor,
            conn=conn,
        )

        if resultado.encontrado:
            nombre_correcto = resultado.nombre_web
    """
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
print("🌐 WEB ENRICHER V1.0 - SISTEMA DE ENRIQUECIMIENTO VÍA WEB")
print("=" * 80)
print("✅ Cache de 3 niveles: plu_mapping → web_enriched → API VTEX")
print("✅ Supermercados: Carulla, Éxito, Jumbo, Olímpica")
print("✅ Datos: EAN, PLU, nombre correcto, marca, presentación")
print("=" * 80)
