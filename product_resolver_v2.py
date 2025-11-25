"""
Servicio de Resolución de Productos v2 - LecFac
================================================

Sistema inteligente que resuelve productos del OCR:
1. PRIMERO busca en plu_supermercado_mapping (mapeo local PLU→EAN)
2. SEGUNDO busca en productos_web_enriched (cache de precios)
3. TERCERO consulta la API VTEX (Carulla, Éxito, Jumbo, Olímpica)
4. Guarda el resultado y actualiza productos_maestros_v2

Beneficios:
- 90%+ de productos se resuelven desde BD local (< 1ms)
- Solo la primera vez de cada PLU consulta la web
- EANs universales permiten comparar precios entre supermercados
- Nombres limpios mejoran la experiencia del usuario

Uso:
    from product_resolver_v2 import ProductResolver

    resolver = ProductResolver()

    # Resolver un producto
    resultado = await resolver.resolver(
        nombre_ocr="AREPA EXTRADELGA SARY",
        codigo_ocr="237373",
        precio_ocr=7800,
        supermercado="Carulla"
    )

    # Resolver factura completa
    resultados = await resolver.resolver_factura(items, supermercado="Carulla")
"""

import asyncio
import os
import re
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

# Cargar variables de entorno desde .env si existe
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # dotenv no instalado, usar variables de entorno del sistema

# Importar psycopg
try:
    import psycopg

    PSYCOPG_VERSION = 3
except ImportError:
    try:
        import psycopg2 as psycopg

        PSYCOPG_VERSION = 2
    except ImportError:
        psycopg = None
        PSYCOPG_VERSION = 0


# ============================================================================
# CONFIGURACIÓN
# ============================================================================

DIAS_CACHE_PRECIO = 7  # Días antes de actualizar precio

MAPEO_SUPERMERCADOS = {
    "carulla": "Carulla",
    "éxito": "Éxito",
    "exito": "Éxito",
    "almacenes exito": "Éxito",
    "jumbo": "Jumbo",
    "metro": "Jumbo",
    "tiendas jumbo": "Jumbo",
    "olimpica": "Olímpica",
    "olímpica": "Olímpica",
    "sao": "Olímpica",
    "supertiendas olimpica": "Olímpica",
}

SUPERMERCADOS_VTEX = ["Carulla", "Éxito", "Jumbo", "Olímpica"]


# ============================================================================
# DATA CLASSES
# ============================================================================


@dataclass
class ProductoResuelto:
    """Resultado de la resolución de un producto"""

    # Datos del OCR
    nombre_ocr: str
    codigo_ocr: str
    precio_ocr: int
    supermercado: str

    # Datos resueltos
    nombre_resuelto: str = None
    ean: str = None
    plu: str = None
    marca: str = None
    presentacion: str = None

    # Precios
    precio_web: int = None
    url: str = None

    # Metadata
    producto_maestro_id: int = None
    resuelto: bool = False
    verificado: bool = False
    fuente: str = None  # "plu_mapping", "cache_enriched", "api_web", "no_encontrado"

    def to_dict(self) -> Dict:
        return {
            "nombre_ocr": self.nombre_ocr,
            "codigo_ocr": self.codigo_ocr,
            "precio_ocr": self.precio_ocr,
            "supermercado": self.supermercado,
            "nombre_resuelto": self.nombre_resuelto or self.nombre_ocr,
            "ean": self.ean,
            "plu": self.plu,
            "marca": self.marca,
            "presentacion": self.presentacion,
            "precio_web": self.precio_web,
            "url": self.url,
            "producto_maestro_id": self.producto_maestro_id,
            "resuelto": self.resuelto,
            "verificado": self.verificado,
            "fuente": self.fuente,
        }


# ============================================================================
# CLASE PRINCIPAL: ProductResolver
# ============================================================================


class ProductResolver:
    """
    Resuelve productos del OCR usando estrategia de cache inteligente.
    """

    def __init__(self, database_url: str = None):
        self.database_url = (
            database_url
            or os.environ.get("DATABASE_URL")
            or os.environ.get("DATABASE_PUBLIC_URL")
        )
        self._conn = None

        # Debug de conexión
        if self.database_url:
            # Mostrar solo host para no exponer credenciales
            try:
                host_part = (
                    self.database_url.split("@")[1].split("/")[0]
                    if "@" in self.database_url
                    else "configurado"
                )
                print(f"📡 Database configurada: {host_part}")
            except:
                print(f"📡 Database URL configurada")
        else:
            print("⚠️ No se encontró DATABASE_URL ni DATABASE_PUBLIC_URL")

        # Estadísticas
        self.stats = {
            "total_consultas": 0,
            "desde_plu_mapping": 0,
            "desde_cache": 0,
            "desde_web": 0,
            "no_encontrados": 0,
        }

    def _get_connection(self):
        """Obtiene conexión a PostgreSQL"""
        if not self.database_url or not psycopg:
            print("   ⚠️ No hay DATABASE_URL o psycopg no disponible")
            return None
        try:
            if PSYCOPG_VERSION == 3:
                return psycopg.connect(self.database_url, autocommit=False)
            else:
                return psycopg.connect(self.database_url)
        except Exception as e:
            print(f"❌ Error conectando a BD: {e}")
            return None
            return None

    @staticmethod
    def normalizar_supermercado(nombre: str) -> Optional[str]:
        """Normaliza nombre del supermercado"""
        if not nombre:
            return None
        nombre_lower = nombre.lower().strip()
        for key, value in MAPEO_SUPERMERCADOS.items():
            if key in nombre_lower:
                return value
        return nombre.title()

    @staticmethod
    def es_ean(codigo: str) -> bool:
        """Determina si un código es EAN (8+ dígitos numéricos)"""
        if not codigo:
            return False
        codigo_limpio = str(codigo).strip()
        return codigo_limpio.isdigit() and len(codigo_limpio) >= 8

    @staticmethod
    def es_plu(codigo: str) -> bool:
        """Determina si un código es PLU (3-7 dígitos)"""
        if not codigo:
            return False
        codigo_limpio = str(codigo).strip()
        return codigo_limpio.isdigit() and 3 <= len(codigo_limpio) <= 7

    # ========================================================================
    # PASO 1: Buscar en plu_supermercado_mapping
    # ========================================================================

    def buscar_en_plu_mapping(self, plu: str, supermercado: str) -> Optional[Dict]:
        """
        Busca un PLU en la tabla de mapeo.
        Esta es la búsqueda más rápida y preferida.
        """
        conn = self._get_connection()
        if not conn:
            print(f"   ⚠️ No hay conexión para buscar en plu_mapping")
            return None

        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    m.id, m.plu, m.ean, m.nombre_web, m.marca, m.presentacion,
                    m.producto_maestro_id, m.url_producto, m.verificado, m.confianza,
                    pm.nombre_consolidado
                FROM plu_supermercado_mapping m
                LEFT JOIN productos_maestros_v2 pm ON m.producto_maestro_id = pm.id
                WHERE m.plu = %s AND m.supermercado = %s
            """,
                (plu, supermercado),
            )

            row = cursor.fetchone()
            if row:
                print(
                    f"   ✅ ENCONTRADO en plu_mapping: {row[3][:40] if row[3] else 'N/A'}..."
                )
                return {
                    "id": row[0],
                    "plu": row[1],
                    "ean": row[2],
                    "nombre_web": row[3],
                    "marca": row[4],
                    "presentacion": row[5],
                    "producto_maestro_id": row[6],
                    "url": row[7],
                    "verificado": row[8],
                    "confianza": float(row[9]) if row[9] else 0.5,
                    "nombre_maestro": row[10],
                    "fuente": "plu_mapping",
                }
            return None

        except Exception as e:
            print(f"   ❌ Error buscando en plu_mapping: {e}")
            return None
        finally:
            conn.close()

    # ========================================================================
    # PASO 2: Buscar en productos_web_enriched (cache)
    # ========================================================================

    def buscar_en_cache(
        self, codigo: str, supermercado: str, es_ean: bool = False
    ) -> Optional[Dict]:
        """
        Busca en el cache de productos enriquecidos.
        """
        conn = self._get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()

            if es_ean:
                cursor.execute(
                    """
                    SELECT id, ean, plu, nombre_completo, marca, presentacion,
                           supermercado, precio_web, url_producto, verificado,
                           fecha_actualizacion
                    FROM productos_web_enriched
                    WHERE ean = %s AND supermercado = %s
                """,
                    (codigo, supermercado),
                )
            else:
                cursor.execute(
                    """
                    SELECT id, ean, plu, nombre_completo, marca, presentacion,
                           supermercado, precio_web, url_producto, verificado,
                           fecha_actualizacion
                    FROM productos_web_enriched
                    WHERE plu = %s AND supermercado = %s
                """,
                    (codigo, supermercado),
                )

            row = cursor.fetchone()
            if row:
                return {
                    "id": row[0],
                    "ean": row[1],
                    "plu": row[2],
                    "nombre_completo": row[3],
                    "marca": row[4],
                    "presentacion": row[5],
                    "supermercado": row[6],
                    "precio_web": row[7],
                    "url": row[8],
                    "verificado": row[9],
                    "fecha_actualizacion": row[10],
                    "fuente": "cache_enriched",
                }
            return None

        except Exception as e:
            print(f"❌ Error buscando en cache: {e}")
            return None
        finally:
            conn.close()

    # ========================================================================
    # PASO 3: Consultar API VTEX
    # ========================================================================

    async def consultar_api_web(
        self, nombre_ocr: str, codigo: str, supermercado: str, es_ean: bool = False
    ) -> Optional[Dict]:
        """
        Consulta la API VTEX del supermercado.
        Solo se usa cuando no hay datos en BD local.
        """
        try:
            from vtex_scraper import enriquecer_producto, SUPERMERCADOS

            # Mapear nombre de supermercado a key del scraper
            scraper_key = None
            for key, config in SUPERMERCADOS.items():
                if config["nombre"] == supermercado:
                    scraper_key = key
                    break

            if not scraper_key:
                return None

            resultado = await enriquecer_producto(
                nombre_ocr=nombre_ocr,
                plu_ocr=codigo if not es_ean else None,
                ean_ocr=codigo if es_ean else None,
                supermercado=scraper_key,
            )

            if resultado.get("enriquecido"):
                return {
                    "ean": resultado.get("ean_web"),
                    "plu": resultado.get("plu_web"),
                    "nombre_completo": resultado.get("nombre_completo"),
                    "marca": resultado.get("marca"),
                    "presentacion": resultado.get("presentacion"),
                    "precio_web": resultado.get("precio_web"),
                    "url": resultado.get("url"),
                    "verificado": resultado.get("verificado", False),
                    "fuente": "api_web",
                }
            return None

        except ImportError:
            print("   ⚠️ Módulo vtex_scraper no disponible")
            return None
        except Exception as e:
            print(f"   ❌ Error consultando API: {e}")
            return None

    # ========================================================================
    # GUARDAR RESULTADOS
    # ========================================================================

    def guardar_en_plu_mapping(
        self,
        plu: str,
        supermercado: str,
        ean: str,
        nombre_web: str,
        nombre_ocr: str,
        marca: str = None,
        presentacion: str = None,
        url: str = None,
        verificado: bool = False,
        producto_maestro_id: int = None,
    ) -> bool:
        """Guarda o actualiza el mapeo PLU→EAN"""
        conn = self._get_connection()
        if not conn:
            print(f"   ⚠️ No se pudo conectar para guardar PLU mapping")
            return False

        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO plu_supermercado_mapping
                (plu, supermercado, ean, nombre_web, nombre_ocr_original,
                 marca, presentacion, url_producto, verificado, producto_maestro_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (plu, supermercado) DO UPDATE SET
                    ean = COALESCE(EXCLUDED.ean, plu_supermercado_mapping.ean),
                    nombre_web = COALESCE(EXCLUDED.nombre_web, plu_supermercado_mapping.nombre_web),
                    marca = COALESCE(EXCLUDED.marca, plu_supermercado_mapping.marca),
                    presentacion = COALESCE(EXCLUDED.presentacion, plu_supermercado_mapping.presentacion),
                    url_producto = COALESCE(EXCLUDED.url_producto, plu_supermercado_mapping.url_producto),
                    verificado = EXCLUDED.verificado OR plu_supermercado_mapping.verificado,
                    veces_visto = plu_supermercado_mapping.veces_visto + 1,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """,
                (
                    plu,
                    supermercado,
                    ean,
                    nombre_web,
                    nombre_ocr,
                    marca,
                    presentacion,
                    url,
                    verificado,
                    producto_maestro_id,
                ),
            )

            conn.commit()
            print(f"   💾 Guardado en plu_mapping: PLU={plu} → EAN={ean}")
            return True

        except Exception as e:
            print(f"   ❌ Error guardando en plu_mapping: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            conn.close()

    def guardar_en_cache(self, datos: Dict, supermercado: str) -> bool:
        """Guarda producto en cache de precios"""
        conn = self._get_connection()
        if not conn:
            print(f"   ⚠️ No se pudo conectar para guardar en cache")
            return False

        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO productos_web_enriched
                (ean, plu, nombre_completo, marca, presentacion,
                 supermercado, precio_web, url_producto, verificado)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (plu, supermercado) DO UPDATE SET
                    ean = COALESCE(EXCLUDED.ean, productos_web_enriched.ean),
                    nombre_completo = EXCLUDED.nombre_completo,
                    precio_web = EXCLUDED.precio_web,
                    url_producto = EXCLUDED.url_producto,
                    fecha_actualizacion = CURRENT_TIMESTAMP
            """,
                (
                    datos.get("ean"),
                    datos.get("plu"),
                    datos.get("nombre_completo"),
                    datos.get("marca"),
                    datos.get("presentacion"),
                    supermercado,
                    datos.get("precio_web"),
                    datos.get("url"),
                    datos.get("verificado", False),
                ),
            )

            conn.commit()
            print(
                f"   💾 Guardado en cache: {datos.get('nombre_completo', '')[:40]}..."
            )
            return True

        except Exception as e:
            print(f"❌ Error guardando en cache: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def actualizar_producto_maestro(
        self, ean: str, nombre: str, marca: str = None, presentacion: str = None
    ) -> Optional[int]:
        """
        Busca o crea producto en productos_maestros_v2.
        Retorna el ID del producto.
        """
        if not ean:
            return None

        conn = self._get_connection()
        if not conn:
            return None

        try:
            cursor = conn.cursor()

            # Buscar existente por EAN
            cursor.execute(
                """
                SELECT id, nombre_consolidado FROM productos_maestros_v2
                WHERE codigo_ean = %s
            """,
                (ean,),
            )

            row = cursor.fetchone()

            if row:
                # Actualizar nombre si el nuevo es mejor (más largo)
                if nombre and len(nombre) > len(row[1] or ""):
                    cursor.execute(
                        """
                        UPDATE productos_maestros_v2 SET
                            nombre_consolidado = %s,
                            marca = COALESCE(%s, marca),
                            fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """,
                        (nombre, marca, row[0]),
                    )
                    conn.commit()
                return row[0]

            # Crear nuevo
            # Extraer peso y unidad del nombre o presentacion
            peso_neto = None
            unidad_medida = None

            texto_buscar = presentacion or nombre or ""
            match = re.search(
                r"(\d+(?:\.\d+)?)\s*(gr|g|ml|l|kg|lt|und|un)\b",
                texto_buscar,
                re.IGNORECASE,
            )
            if match:
                peso_neto = float(match.group(1))
                unidad_medida = match.group(2).lower()
                # Normalizar unidades
                if unidad_medida in ["g", "gr"]:
                    unidad_medida = "gr"
                elif unidad_medida in ["l", "lt"]:
                    unidad_medida = "lt"
                elif unidad_medida in ["un", "und"]:
                    unidad_medida = "und"

            cursor.execute(
                """
                INSERT INTO productos_maestros_v2
                (codigo_ean, nombre_consolidado, marca, peso_neto, unidad_medida,
                 confianza_datos, veces_visto, estado, fecha_primera_vez,
                 fecha_ultima_actualizacion)
                VALUES (%s, %s, %s, %s, %s, 0.8, 1, 'activo', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                RETURNING id
            """,
                (ean, nombre, marca, peso_neto, unidad_medida),
            )

            new_id = cursor.fetchone()[0]
            conn.commit()
            print(f"   ➕ Nuevo producto maestro: {nombre[:40]}... (ID: {new_id})")
            return new_id

        except Exception as e:
            print(f"❌ Error actualizando producto maestro: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    # ========================================================================
    # FUNCIÓN PRINCIPAL: RESOLVER
    # ========================================================================

    async def resolver(
        self,
        nombre_ocr: str,
        codigo_ocr: str = None,
        precio_ocr: int = None,
        supermercado: str = "Carulla",
        forzar_web: bool = False,
    ) -> ProductoResuelto:
        """
        🎯 FUNCIÓN PRINCIPAL: Resuelve un producto del OCR

        Estrategia:
        1. Si hay PLU, buscar en plu_supermercado_mapping (más rápido)
        2. Si no está, buscar en productos_web_enriched
        3. Si no está, consultar API VTEX
        4. Guardar resultado en todas las tablas correspondientes
        """
        self.stats["total_consultas"] += 1

        supermercado_norm = self.normalizar_supermercado(supermercado)
        codigo_limpio = str(codigo_ocr).strip() if codigo_ocr else None

        resultado = ProductoResuelto(
            nombre_ocr=nombre_ocr,
            codigo_ocr=codigo_limpio,
            precio_ocr=precio_ocr,
            supermercado=supermercado_norm,
        )

        es_ean = self.es_ean(codigo_limpio)
        es_plu = self.es_plu(codigo_limpio)

        print(
            f"\n🔍 Resolviendo: '{nombre_ocr[:35]}...' | {'EAN' if es_ean else 'PLU' if es_plu else 'Sin código'}: {codigo_limpio or 'N/A'}"
        )

        # ====================================================================
        # PASO 1: Buscar en plu_supermercado_mapping
        # ====================================================================
        if es_plu and not forzar_web:
            print(f"   📊 PASO 1: Buscando PLU {codigo_limpio} en mapping...")
            datos_mapping = self.buscar_en_plu_mapping(codigo_limpio, supermercado_norm)

            if datos_mapping:
                print(
                    f"   ✅ ENCONTRADO en plu_mapping: {datos_mapping['nombre_web'][:40]}..."
                )
                self.stats["desde_plu_mapping"] += 1

                resultado.nombre_resuelto = datos_mapping.get(
                    "nombre_maestro"
                ) or datos_mapping.get("nombre_web")
                resultado.ean = datos_mapping.get("ean")
                resultado.plu = datos_mapping.get("plu")
                resultado.marca = datos_mapping.get("marca")
                resultado.presentacion = datos_mapping.get("presentacion")
                resultado.url = datos_mapping.get("url")
                resultado.producto_maestro_id = datos_mapping.get("producto_maestro_id")
                resultado.verificado = datos_mapping.get("verificado", False)
                resultado.resuelto = True
                resultado.fuente = "plu_mapping"

                # Buscar precio actualizado en cache
                if resultado.ean:
                    cache = self.buscar_en_cache(
                        resultado.ean, supermercado_norm, es_ean=True
                    )
                    if cache:
                        resultado.precio_web = cache.get("precio_web")

                return resultado

        # ====================================================================
        # PASO 2: Buscar en productos_web_enriched (cache)
        # ====================================================================
        if codigo_limpio and not forzar_web:
            print(f"   📊 PASO 2: Buscando en cache enriched...")
            datos_cache = self.buscar_en_cache(
                codigo_limpio, supermercado_norm, es_ean=es_ean
            )

            if datos_cache:
                print(
                    f"   ✅ ENCONTRADO en cache: {datos_cache['nombre_completo'][:40]}..."
                )
                self.stats["desde_cache"] += 1

                resultado.nombre_resuelto = datos_cache.get("nombre_completo")
                resultado.ean = datos_cache.get("ean")
                resultado.plu = datos_cache.get("plu") or codigo_limpio
                resultado.marca = datos_cache.get("marca")
                resultado.presentacion = datos_cache.get("presentacion")
                resultado.precio_web = datos_cache.get("precio_web")
                resultado.url = datos_cache.get("url")
                resultado.verificado = datos_cache.get("verificado", False)
                resultado.resuelto = True
                resultado.fuente = "cache_enriched"

                # Guardar en plu_mapping para futuras consultas
                if es_plu and resultado.ean:
                    self.guardar_en_plu_mapping(
                        plu=codigo_limpio,
                        supermercado=supermercado_norm,
                        ean=resultado.ean,
                        nombre_web=resultado.nombre_resuelto,
                        nombre_ocr=nombre_ocr,
                        marca=resultado.marca,
                        presentacion=resultado.presentacion,
                        url=resultado.url,
                        verificado=resultado.verificado,
                    )

                return resultado

        # ====================================================================
        # PASO 3: Consultar API VTEX
        # ====================================================================
        if supermercado_norm in SUPERMERCADOS_VTEX:
            print(f"   🌐 PASO 3: Consultando API {supermercado_norm}...")
            datos_web = await self.consultar_api_web(
                nombre_ocr=nombre_ocr,
                codigo=codigo_limpio,
                supermercado=supermercado_norm,
                es_ean=es_ean,
            )

            if datos_web:
                print(
                    f"   ✅ ENCONTRADO en web: {datos_web['nombre_completo'][:40]}..."
                )
                self.stats["desde_web"] += 1

                resultado.nombre_resuelto = datos_web.get("nombre_completo")
                resultado.ean = datos_web.get("ean")
                resultado.plu = datos_web.get("plu") or codigo_limpio
                resultado.marca = datos_web.get("marca")
                resultado.presentacion = datos_web.get("presentacion")
                resultado.precio_web = datos_web.get("precio_web")
                resultado.url = datos_web.get("url")
                resultado.verificado = datos_web.get("verificado", False)
                resultado.resuelto = True
                resultado.fuente = "api_web"

                # Guardar en cache
                print(f"   💾 Guardando en cache y mapping...")
                self.guardar_en_cache(datos_web, supermercado_norm)

                # Actualizar/crear producto maestro
                if resultado.ean:
                    print(
                        f"   📝 Actualizando producto maestro (EAN: {resultado.ean})..."
                    )
                    producto_id = self.actualizar_producto_maestro(
                        ean=resultado.ean,
                        nombre=resultado.nombre_resuelto,
                        marca=resultado.marca,
                        presentacion=resultado.presentacion,
                    )
                    resultado.producto_maestro_id = producto_id

                    # Guardar mapeo PLU→EAN
                    if es_plu:
                        print(
                            f"   📝 Guardando mapeo PLU→EAN: {codigo_limpio} → {resultado.ean}"
                        )
                        self.guardar_en_plu_mapping(
                            plu=codigo_limpio,
                            supermercado=supermercado_norm,
                            ean=resultado.ean,
                            nombre_web=resultado.nombre_resuelto,
                            nombre_ocr=nombre_ocr,
                            marca=resultado.marca,
                            presentacion=resultado.presentacion,
                            url=resultado.url,
                            verificado=resultado.verificado,
                            producto_maestro_id=producto_id,
                        )
                    else:
                        print(
                            f"   ⚠️ No es PLU, no se guarda en mapping (código: {codigo_limpio}, es_plu: {es_plu})"
                        )
                else:
                    print(f"   ⚠️ No hay EAN, no se puede crear mapping")

                return resultado

        # ====================================================================
        # NO ENCONTRADO
        # ====================================================================
        print(f"   ❌ No encontrado")
        self.stats["no_encontrados"] += 1
        resultado.fuente = "no_encontrado"
        return resultado

    # ========================================================================
    # RESOLVER FACTURA COMPLETA
    # ========================================================================

    async def resolver_factura(
        self, items: List[Dict], supermercado: str, max_concurrent: int = 3
    ) -> List[ProductoResuelto]:
        """
        Resuelve todos los productos de una factura.

        Args:
            items: Lista de dicts con 'nombre', 'codigo', 'precio'
            supermercado: Nombre del supermercado
            max_concurrent: Consultas concurrentes máximas
        """
        if not items:
            return []

        print(f"\n{'='*70}")
        print(f"📋 RESOLVIENDO FACTURA: {len(items)} productos de {supermercado}")
        print("=" * 70)

        semaphore = asyncio.Semaphore(max_concurrent)

        async def resolver_item(item: Dict) -> ProductoResuelto:
            async with semaphore:
                await asyncio.sleep(0.2)  # Rate limiting
                return await self.resolver(
                    nombre_ocr=item.get("nombre", ""),
                    codigo_ocr=item.get("codigo"),
                    precio_ocr=item.get("precio"),
                    supermercado=supermercado,
                )

        tareas = [resolver_item(item) for item in items]
        resultados = await asyncio.gather(*tareas, return_exceptions=True)

        # Convertir excepciones a ProductoResuelto vacío
        for i, r in enumerate(resultados):
            if isinstance(r, Exception):
                resultados[i] = ProductoResuelto(
                    nombre_ocr=items[i].get("nombre", ""),
                    codigo_ocr=items[i].get("codigo"),
                    precio_ocr=items[i].get("precio"),
                    supermercado=supermercado,
                    fuente="error",
                )

        # Resumen
        resueltos = sum(1 for r in resultados if r.resuelto)
        desde_mapping = sum(1 for r in resultados if r.fuente == "plu_mapping")
        desde_cache = sum(1 for r in resultados if r.fuente == "cache_enriched")
        desde_web = sum(1 for r in resultados if r.fuente == "api_web")

        print(f"\n{'='*70}")
        print(f"📊 RESUMEN FACTURA:")
        print(f"   Total productos: {len(items)}")
        print(f"   ✅ Resueltos: {resueltos}")
        print(f"      - Desde plu_mapping: {desde_mapping} ⚡⚡ (más rápido)")
        print(f"      - Desde cache: {desde_cache} ⚡")
        print(f"      - Desde web: {desde_web} 🌐 (nuevos)")
        print(f"   ❌ No resueltos: {len(items) - resueltos}")
        print("=" * 70)

        return resultados

    # ========================================================================
    # ESTADÍSTICAS
    # ========================================================================

    def obtener_estadisticas(self) -> Dict:
        """Obtiene estadísticas del resolver y de la BD"""
        conn = self._get_connection()
        if not conn:
            return self.stats

        try:
            cursor = conn.cursor()

            # Estadísticas de plu_mapping
            cursor.execute(
                """
                SELECT
                    supermercado,
                    COUNT(*) as total,
                    COUNT(DISTINCT ean) as eans,
                    COUNT(*) FILTER (WHERE verificado) as verificados
                FROM plu_supermercado_mapping
                GROUP BY supermercado
            """
            )

            mapping_stats = {
                row[0]: {"total": row[1], "eans": row[2], "verificados": row[3]}
                for row in cursor.fetchall()
            }

            # Estadísticas de cache
            cursor.execute(
                """
                SELECT
                    supermercado,
                    COUNT(*) as total,
                    AVG(precio_web) as precio_promedio
                FROM productos_web_enriched
                GROUP BY supermercado
            """
            )

            cache_stats = {
                row[0]: {"total": row[1], "precio_promedio": int(row[2] or 0)}
                for row in cursor.fetchall()
            }

            return {
                "sesion": self.stats,
                "plu_mapping": mapping_stats,
                "cache_precios": cache_stats,
            }

        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return self.stats
        finally:
            conn.close()


# ============================================================================
# FUNCIONES DE CONVENIENCIA (para usar sin instanciar clase)
# ============================================================================

_resolver_instance = None


def get_resolver() -> ProductResolver:
    """Obtiene instancia singleton del resolver"""
    global _resolver_instance
    if _resolver_instance is None:
        _resolver_instance = ProductResolver()
    return _resolver_instance


async def resolver_producto(
    nombre_ocr: str,
    codigo_ocr: str = None,
    precio_ocr: int = None,
    supermercado: str = "Carulla",
) -> Dict:
    """Resuelve un producto (función de conveniencia)"""
    resolver = get_resolver()
    resultado = await resolver.resolver(
        nombre_ocr, codigo_ocr, precio_ocr, supermercado
    )
    return resultado.to_dict()


async def resolver_factura(items: List[Dict], supermercado: str) -> List[Dict]:
    """Resuelve una factura completa (función de conveniencia)"""
    resolver = get_resolver()
    resultados = await resolver.resolver_factura(items, supermercado)
    return [r.to_dict() for r in resultados]


# ============================================================================
# TEST
# ============================================================================


async def test():
    print("=" * 70)
    print("🧪 TEST: Product Resolver v2")
    print("   Estrategia: plu_mapping → cache → API web")
    print("=" * 70)

    resolver = ProductResolver()

    # Test 1: Primera vez (irá a web)
    print("\n" + "=" * 70)
    print("📋 TEST 1: Primera consulta (debería ir a API web)")
    print("=" * 70)

    r1 = await resolver.resolver(
        nombre_ocr="AREPA EXTRADELGA SARY",
        codigo_ocr="237373",
        precio_ocr=7800,
        supermercado="Carulla",
    )

    print(f"\n📊 Resultado:")
    print(f"   Nombre: {r1.nombre_resuelto}")
    print(f"   EAN: {r1.ean}")
    print(f"   Fuente: {r1.fuente}")

    # Test 2: Segunda vez (debería venir de mapping)
    print("\n" + "=" * 70)
    print("📋 TEST 2: Segunda consulta (debería venir de plu_mapping)")
    print("=" * 70)

    r2 = await resolver.resolver(
        nombre_ocr="AREPA EXTRADELGA SARY",
        codigo_ocr="237373",
        precio_ocr=7800,
        supermercado="Carulla",
    )

    print(f"\n📊 Resultado:")
    print(f"   Nombre: {r2.nombre_resuelto}")
    print(
        f"   Fuente: {r2.fuente} {'⚡⚡ RÁPIDO!' if r2.fuente == 'plu_mapping' else ''}"
    )

    # Test 3: Factura completa
    print("\n" + "=" * 70)
    print("📋 TEST 3: Resolver factura")
    print("=" * 70)

    items = [
        {"nombre": "AREPA EXTRADELGA SARY", "codigo": "237373", "precio": 7800},
        {"nombre": "LECHE ALQUERIA DESLACT", "codigo": "81933", "precio": 7100},
        {"nombre": "ARROZ DIANA 500G", "codigo": "456789", "precio": 3500},
    ]

    resultados = await resolver.resolver_factura(items, "Carulla")

    # Estadísticas
    print("\n" + "=" * 70)
    print("📊 ESTADÍSTICAS:")
    print("=" * 70)

    stats = resolver.obtener_estadisticas()
    print(f"   Sesión:")
    for k, v in stats.get("sesion", {}).items():
        print(f"      {k}: {v}")

    print("\n" + "=" * 70)


if __name__ == "__main__":
    asyncio.run(test())
