"""
product_matcher.py - VERSIÓN 9.5 - ACTUALIZACIÓN DE PRODUCTOS CON DATOS WEB
========================================================================

🎯 CAMBIOS V9.5:
- ✅ NUEVO: Actualiza productos EXISTENTES con datos del web
- ✅ Cuando encuentra producto en PASO 1/2/3 → Consulta web → Actualiza nombre/EAN
- ✅ Prioriza nombre del web sobre nombre OCR (más preciso)
- ✅ Mantiene historial de precios intacto

🎯 CAMBIOS V9.4 (heredados):
- ✅ PASO 3.5 - Enriquecimiento Web antes de crear producto
- ✅ Busca en cache local (plu_supermercado_mapping) primero
- ✅ Si no hay cache → Consulta API VTEX (Carulla, Éxito, Jumbo, Olímpica)
- ✅ Usa nombre WEB (correcto) en lugar del OCR (con errores)
- ✅ Obtiene EAN del PLU automáticamente
- ✅ Guarda en cache para futuras consultas

🎯 CAMBIOS V9.3 (heredados):
- ✅ BUSCA PAPAS PRIMERO antes de crear productos
- ✅ Si existe PAPA → Usa sus datos (nombre, marca, categoría)
- ✅ CREA registro en productos_por_establecimiento si no existe
- ✅ ACTUALIZA precios SIEMPRE (min, max, actual)
"""

import re
from unidecode import unidecode
from typing import Optional, Dict, Any, Tuple
import traceback


# ============================================================================
# IMPORTAR MÓDULOS
# ============================================================================

CORRECCIONES_OCR_AVAILABLE = False

try:
    from perplexity_validator import validar_con_perplexity

    PERPLEXITY_AVAILABLE = True
except ImportError:
    PERPLEXITY_AVAILABLE = False
    print("⚠️  perplexity_validator.py no disponible")

try:
    from aprendizaje_manager import AprendizajeManager

    APRENDIZAJE_AVAILABLE = True
except ImportError:
    APRENDIZAJE_AVAILABLE = False
    print("⚠️  aprendizaje_manager.py no disponible")

try:
    from plu_consolidator import aplicar_consolidacion_plu, ENABLE_PLU_CONSOLIDATION

    PLU_CONSOLIDATOR_AVAILABLE = True
except ImportError:
    PLU_CONSOLIDATOR_AVAILABLE = False
    ENABLE_PLU_CONSOLIDATION = False
    print("⚠️  plu_consolidator.py no disponible")

# 🆕 V9.4: Importar Web Enricher para enriquecimiento vía scraping VTEX
try:
    from web_enricher import WebEnricher, es_supermercado_vtex

    WEB_ENRICHER_AVAILABLE = True
except ImportError:
    WEB_ENRICHER_AVAILABLE = False
    print("⚠️  web_enricher.py no disponible")


# ============================================================================
# FUNCIONES AUXILIARES
# ============================================================================


def normalizar_nombre_producto(
    nombre: str, aplicar_correcciones_ocr: bool = True
) -> str:
    """Normaliza nombre del producto para búsquedas"""
    if not nombre:
        return ""

    nombre = nombre.upper()
    nombre = unidecode(nombre)
    nombre = re.sub(r"[^\w\s]", " ", nombre)
    nombre = re.sub(r"\s+", " ", nombre)

    return nombre.strip()[:100]


def calcular_similitud(nombre1: str, nombre2: str) -> float:
    """Calcula similitud entre dos nombres de productos"""
    n1 = normalizar_nombre_producto(nombre1, False)
    n2 = normalizar_nombre_producto(nombre2, False)

    if n1 == n2:
        return 1.0

    if n1 in n2 or n2 in n1:
        return 0.8 + (0.2 * min(len(n1), len(n2)) / max(len(n1), len(n2)))

    palabras1 = set(n1.split())
    palabras2 = set(n2.split())

    if not palabras1.union(palabras2):
        return 0.0

    return len(palabras1.intersection(palabras2)) / len(palabras1.union(palabras2))


def clasificar_codigo_tipo(codigo: str) -> str:
    """Clasifica el tipo de código del producto"""
    if not codigo:
        return "DESCONOCIDO"

    codigo_limpio = "".join(filter(str.isdigit, str(codigo)))
    longitud = len(codigo_limpio)

    if longitud >= 8:
        return "EAN"
    elif 3 <= longitud <= 7:
        return "PLU"

    return "DESCONOCIDO"


def detectar_cadena(establecimiento: str) -> str:
    """Detecta la cadena principal del establecimiento"""
    if not establecimiento:
        return "DESCONOCIDO"

    establecimiento_upper = establecimiento.upper()

    cadenas = {
        "JUMBO": "JUMBO",
        "EXITO": "EXITO",
        "CARULLA": "CARULLA",
        "OLIMPICA": "OLIMPICA",
        "D1": "D1",
        "ARA": "ARA",
        "CRUZ VERDE": "CRUZ VERDE",
        "FARMATODO": "FARMATODO",
    }

    for cadena_key, cadena_value in cadenas.items():
        if cadena_key in establecimiento_upper:
            return cadena_value

    return establecimiento_upper.split()[0] if establecimiento_upper else "DESCONOCIDO"


def buscar_en_productos_referencia(codigo_ean: str, cursor) -> Optional[Dict[str, Any]]:
    """Busca producto en la tabla de referencia oficial"""
    import os

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    if not codigo_ean or len(codigo_ean) < 8:
        return None

    try:
        cursor.execute(
            f"""
            SELECT codigo_ean, nombre, marca, categoria, presentacion, unidad_medida
            FROM productos_referencia
            WHERE codigo_ean = {param}
            LIMIT 1
        """,
            (codigo_ean,),
        )

        resultado = cursor.fetchone()

        if not resultado:
            return None

        ean = resultado[0]
        nombre = resultado[1] or ""
        marca = resultado[2] or ""
        categoria = resultado[3] or ""
        presentacion = resultado[4] or ""
        unidad_medida = resultado[5] or ""

        partes = []
        if marca:
            partes.append(marca.upper().strip())
        if nombre:
            partes.append(nombre.upper().strip())
        if presentacion:
            partes.append(presentacion.upper().strip())
        if unidad_medida and unidad_medida.upper() not in ["UNIDAD", "UND", "U"]:
            partes.append(unidad_medida.upper().strip())

        nombre_oficial = " ".join(partes)

        return {
            "codigo_ean": ean,
            "nombre_oficial": nombre_oficial,
            "marca": marca,
            "nombre": nombre,
            "presentacion": presentacion,
            "categoria": categoria,
            "unidad_medida": unidad_medida,
            "fuente": "productos_referencia",
        }

    except Exception as e:
        print(f"   ⚠️ Error buscando en productos_referencia: {e}")
        return None


def buscar_nombre_por_plu_historial(
    codigo_plu: str, establecimiento_id: int, cursor
) -> Optional[Dict[str, Any]]:
    """Busca el nombre más común para un PLU en el historial de compras - USA V2"""
    if not codigo_plu or not establecimiento_id:
        return None

    try:
        cursor.execute(
            """
            SELECT pm.nombre_consolidado, COUNT(*) as frecuencia, MAX(if2.fecha_creacion) as ultima_vez
            FROM items_factura if2
            JOIN productos_maestros_v2 pm ON if2.producto_maestro_id = pm.id
            JOIN facturas f ON if2.factura_id = f.id
            WHERE if2.codigo_leido = %s AND f.establecimiento_id = %s
            GROUP BY pm.nombre_consolidado
            ORDER BY frecuencia DESC, ultima_vez DESC
            LIMIT 1
        """,
            (codigo_plu, establecimiento_id),
        )

        resultado = cursor.fetchone()

        if resultado and len(resultado) >= 3:
            frecuencia = resultado[1] or 1
            return {
                "nombre": resultado[0],
                "frecuencia": frecuencia,
                "ultima_vez": resultado[2],
                "fuente": "historial_plu",
                "confianza": min(0.85, 0.65 + (frecuencia * 0.05)),
            }

        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando PLU en historial: {e}")
        return None


def marcar_para_revision_admin(
    cursor,
    conn,
    producto_maestro_id: int,
    nombre_ocr: str,
    nombre_sugerido: str,
    codigo: str,
    establecimiento: str,
    razon: str,
) -> bool:
    """Marca un producto para revisión por administrador"""
    try:
        cursor.execute(
            """
            INSERT INTO productos_revision_admin (
                producto_maestro_id, nombre_ocr_original, nombre_sugerido, codigo_producto,
                establecimiento, motivo_revision, razon_revision, estado, fecha_creacion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'pendiente', CURRENT_TIMESTAMP)
            ON CONFLICT (producto_maestro_id)
            DO UPDATE SET
                nombre_ocr_original = EXCLUDED.nombre_ocr_original,
                nombre_sugerido = EXCLUDED.nombre_sugerido,
                motivo_revision = EXCLUDED.motivo_revision,
                razon_revision = EXCLUDED.razon_revision,
                fecha_creacion = CURRENT_TIMESTAMP,
                estado = 'pendiente'
        """,
            (
                producto_maestro_id,
                nombre_ocr[:200] if nombre_ocr else "",
                nombre_sugerido[:200] if nombre_sugerido else "",
                codigo[:50] if codigo else "",
                establecimiento[:100] if establecimiento else "",
                razon[:500] if razon else "Sin especificar",
                razon[:500] if razon else "Sin especificar",
            ),
        )
        conn.commit()
        print(f"      📋 Marcado para revisión: {nombre_ocr[:40]}")
        return True
    except Exception as e:
        print(f"      ⚠️ Error marcando para revisión: {e}")
        return False


def validar_nombre_con_sistema_completo(
    nombre_ocr_original: str,
    nombre_corregido: str,
    precio: int,
    establecimiento: str,
    codigo: str = "",
    aprendizaje_mgr=None,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None,
    cursor=None,
    establecimiento_id: int = None,
    datos_web: Dict[str, Any] = None,  # 🆕 V9.4: Datos del web enricher
) -> dict:
    """V9.4: Sistema con jerarquía de validación + datos web"""

    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)
    marcar_revision = False
    razon_revision = ""

    # 🆕 V9.4: SI TENEMOS DATOS WEB → Usar directamente (95% confianza)
    if datos_web and datos_web.get("encontrado"):
        nombre_web = datos_web.get("nombre_web", "")
        if nombre_web:
            print(f"   ✅ USANDO DATOS WEB (enriquecimiento)")
            print(f"   📝 Nombre web: {nombre_web}")
            print(f"   🎯 Confianza: 95% (fuente web)")

            # Guardar en aprendizaje si está disponible
            if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
                try:
                    aprendizaje_mgr.guardar_correccion_aprendida(
                        ocr_original=nombre_ocr_original,
                        ocr_normalizado=nombre_corregido,
                        nombre_validado=nombre_web,
                        establecimiento=cadena,
                        confianza_inicial=0.95,
                        codigo_ean=datos_web.get("codigo_ean", codigo),
                    )
                except Exception as e:
                    print(f"      ⚠️ Error guardando aprendizaje: {e}")

            return {
                "nombre_final": nombre_web,
                "fue_validado": True,
                "confianza": 0.95,
                "categoria_confianza": "muy_alta",
                "fuente": f"web_{datos_web.get('fuente', 'vtex')}",
                "detalles": f"Enriquecido desde {datos_web.get('supermercado', 'web')}",
                "necesita_revision": False,
                "razon_revision": "",
                "marca": datos_web.get("marca"),
                "codigo_ean_web": datos_web.get("codigo_ean"),
            }

    # PASO 1: BUSCAR EN PRODUCTOS_REFERENCIA (99%)
    if tipo_codigo == "EAN" and codigo and cursor:
        producto_oficial = buscar_en_productos_referencia(codigo, cursor)

        if producto_oficial:
            print(f"   ✅ ENCONTRADO EN PRODUCTOS REFERENCIA")
            print(f"   📝 Nombre oficial: {producto_oficial['nombre_oficial']}")
            print(f"   🎯 Confianza: 99% (fuente oficial)")

            if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
                try:
                    aprendizaje_mgr.guardar_correccion_aprendida(
                        ocr_original=nombre_ocr_original,
                        ocr_normalizado=nombre_corregido,
                        nombre_validado=producto_oficial["nombre_oficial"],
                        establecimiento=cadena,
                        confianza_inicial=0.99,
                        codigo_ean=codigo,
                    )
                except Exception as e:
                    print(f"      ⚠️ Error guardando aprendizaje: {e}")

            return {
                "nombre_final": producto_oficial["nombre_oficial"],
                "fue_validado": True,
                "confianza": 0.99,
                "categoria_confianza": "muy_alta",
                "fuente": "productos_referencia",
                "detalles": f"Código EAN oficial: {codigo}",
                "necesita_revision": False,
                "razon_revision": "",
                "marca": producto_oficial.get("marca"),
            }
        else:
            marcar_revision = True
            razon_revision = f"EAN {codigo} no está en productos_referencia"

    # PASO 2: BUSCAR PLU EN HISTORIAL (80%)
    if tipo_codigo == "PLU" and codigo and cursor and establecimiento_id:
        resultado_plu = buscar_nombre_por_plu_historial(
            codigo, establecimiento_id, cursor
        )

        if resultado_plu and resultado_plu["frecuencia"] >= 2:
            print(f"   ✅ PLU ENCONTRADO EN HISTORIAL")
            print(f"   📝 Nombre histórico: {resultado_plu['nombre']}")
            print(f"   📊 Frecuencia: {resultado_plu['frecuencia']} veces")
            print(f"   🎯 Confianza: {resultado_plu['confianza']:.0%}")

            similitud = calcular_similitud(nombre_corregido, resultado_plu["nombre"])
            if similitud < 0.70:
                marcar_revision = True
                razon_revision = (
                    f"Discrepancia OCR vs Historial (similitud {similitud:.0%})"
                )

            return {
                "nombre_final": resultado_plu["nombre"],
                "fue_validado": True,
                "confianza": resultado_plu["confianza"],
                "categoria_confianza": (
                    "alta" if resultado_plu["confianza"] >= 0.80 else "media"
                ),
                "fuente": "historial_plu",
                "detalles": f"PLU {codigo} visto {resultado_plu['frecuencia']} veces",
                "necesita_revision": marcar_revision,
                "razon_revision": razon_revision,
                "marca": None,
            }
        elif tipo_codigo == "PLU":
            marcar_revision = True
            razon_revision = f"PLU {codigo} es nuevo (menos de 2 apariciones)"

    # PASO 3: BUSCAR EN APRENDIZAJE (70%+)
    if APRENDIZAJE_AVAILABLE and aprendizaje_mgr:
        try:
            correccion = aprendizaje_mgr.buscar_correccion_aprendida(
                ocr_normalizado=nombre_corregido,
                establecimiento=cadena,
                codigo_ean=codigo if tipo_codigo == "EAN" else None,
            )

            if correccion and correccion["confianza"] >= 0.80:
                confianza = correccion["confianza"]
                aprendizaje_mgr.incrementar_confianza(correccion["id"], True)

                print(f"   ✅ ENCONTRADO EN APRENDIZAJE")
                print(f"   📝 Nombre validado: {correccion['nombre_validado']}")
                print(f"   🎯 Confianza: {confianza:.0%}")

                return {
                    "nombre_final": correccion["nombre_validado"],
                    "fue_validado": True,
                    "confianza": confianza,
                    "categoria_confianza": "alta" if confianza >= 0.85 else "media",
                    "fuente": "aprendizaje",
                    "detalles": f"Validado {correccion['veces_confirmado']} veces",
                    "aprendizaje_id": correccion["id"],
                    "necesita_revision": False,
                    "razon_revision": "",
                    "marca": None,
                }
        except Exception as e:
            print(f"   ⚠️ Error consultando aprendizaje: {e}")

    # PASO 4: USAR NOMBRE OCR (60%)
    print(f"   📝 USANDO NOMBRE OCR CORREGIDO (sin validación externa)")

    tiene_ean = tipo_codigo == "EAN"
    confianza = 0.65 if tiene_ean else 0.60
    categoria = "media" if confianza >= 0.65 else "baja"

    if not marcar_revision:
        marcar_revision = True
        razon_revision = "Producto nuevo sin validación externa"

    print(f"   ⚠️ NO se guarda en aprendizaje (requiere validación)")

    return {
        "nombre_final": nombre_corregido,
        "fue_validado": False,
        "confianza": confianza,
        "categoria_confianza": categoria,
        "fuente": "ocr_corregido",
        "detalles": "Sin validación externa",
        "necesita_revision": marcar_revision,
        "razon_revision": razon_revision,
        "marca": None,
    }


def crear_producto_en_v2(cursor, conn, nombre_normalizado, codigo_ean=None, marca=None):
    """
    ⭐ VERSIÓN 9.1: Crea producto en productos_maestros_v2 (tabla nueva)
    """
    try:
        if not nombre_normalizado or not nombre_normalizado.strip():
            print(f"   ❌ ERROR: nombre_normalizado vacío")
            return None

        nombre_final = nombre_normalizado.strip().upper()

        codigo_ean_safe = codigo_ean if codigo_ean and codigo_ean.strip() else None
        marca_safe = marca if marca and marca.strip() else None

        print(f"   📝 Creando producto en V2: {nombre_final}")

        cursor.execute(
            """
            INSERT INTO productos_maestros_v2 (
                codigo_ean, nombre_consolidado, marca, categoria_id,
                confianza_datos, veces_visto, estado,
                fecha_primera_vez, fecha_ultima_actualizacion
            ) VALUES (%s, %s, %s, NULL, 0.5, 1, 'pendiente', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING id
        """,
            (codigo_ean_safe, nombre_final, marca_safe),
        )

        resultado = cursor.fetchone()

        if not resultado or len(resultado) == 0:
            print(f"   ❌ ERROR: INSERT no retornó ID")
            conn.rollback()
            return None

        producto_id = resultado[0]

        if not producto_id or producto_id <= 0:
            print(f"   ❌ ERROR: ID inválido: {producto_id}")
            conn.rollback()
            return None

        conn.commit()
        print(f"   ✅ Producto creado en V2: ID {producto_id}")
        return producto_id

    except Exception as e:
        print(f"   ❌ Error en crear_producto_en_v2: {e}")
        traceback.print_exc()
        conn.rollback()
        return None


def guardar_plu_en_establecimiento(
    cursor,
    conn,
    producto_id: int,
    establecimiento_id: int,
    codigo_plu: str,
    precio: int,
) -> bool:
    """
    ⭐ V9.3: Guarda el PLU en productos_por_establecimiento con ACTUALIZACIÓN DE PRECIOS
    """
    if not producto_id or not establecimiento_id or not codigo_plu:
        return False

    try:
        cursor.execute(
            """
            INSERT INTO productos_por_establecimiento (
                producto_maestro_id, establecimiento_id, codigo_plu,
                precio_actual, precio_unitario, precio_minimo, precio_maximo,
                total_reportes, fecha_creacion, fecha_actualizacion, ultima_actualizacion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (producto_maestro_id, establecimiento_id)
            DO UPDATE SET
                codigo_plu = EXCLUDED.codigo_plu,
                precio_actual = EXCLUDED.precio_actual,
                precio_unitario = EXCLUDED.precio_unitario,
                precio_minimo = LEAST(COALESCE(productos_por_establecimiento.precio_minimo, EXCLUDED.precio_minimo), EXCLUDED.precio_minimo),
                precio_maximo = GREATEST(COALESCE(productos_por_establecimiento.precio_maximo, EXCLUDED.precio_maximo), EXCLUDED.precio_maximo),
                total_reportes = productos_por_establecimiento.total_reportes + 1,
                fecha_actualizacion = CURRENT_TIMESTAMP,
                ultima_actualizacion = CURRENT_TIMESTAMP
        """,
            (
                producto_id,
                establecimiento_id,
                codigo_plu,
                precio,
                precio,
                precio,
                precio,
            ),
        )
        conn.commit()
        print(
            f"   💾 PLU {codigo_plu} guardado/actualizado en productos_por_establecimiento"
        )
        return True
    except Exception as e:
        print(f"   ⚠️ Error guardando PLU en establecimiento: {e}")
        traceback.print_exc()
        return False


# ═══════════════════════════════════════════════════════════════
# 🆕 V9.5: ACTUALIZAR PRODUCTO EXISTENTE CON DATOS WEB
# ═══════════════════════════════════════════════════════════════


def actualizar_producto_con_datos_web(
    producto_id: int,
    codigo: str,
    nombre_ocr: str,
    establecimiento: str,
    precio: int,
    cursor,
    conn,
    establecimiento_id: int = None,
) -> bool:
    """
    🆕 V9.5: Actualiza un producto EXISTENTE con datos del web enricher.

    - Consulta el web enricher
    - Si encuentra datos mejores → Actualiza nombre, EAN, marca
    - Actualiza el cache plu_supermercado_mapping con producto_maestro_id

    Returns:
        True si se actualizó, False si no
    """
    if not WEB_ENRICHER_AVAILABLE:
        return False

    if not es_supermercado_vtex(establecimiento):
        return False

    try:
        print(
            f"   🔄 V9.5: Verificando datos web para producto existente ID={producto_id}"
        )

        enricher = WebEnricher(cursor, conn)
        resultado_web = enricher.enriquecer(
            codigo=codigo,
            nombre_ocr=nombre_ocr,
            establecimiento=establecimiento,
            precio_ocr=precio,
        )

        if not resultado_web.encontrado:
            print(f"      ℹ️ No se encontró en web, manteniendo datos actuales")
            return False

        datos_web = resultado_web.to_dict()
        nombre_web = datos_web.get("nombre_web", "")
        ean_web = datos_web.get("codigo_ean", "")
        marca_web = datos_web.get("marca", "")

        if not nombre_web:
            return False

        # Normalizar nombre para guardar
        nombre_web_normalizado = nombre_web.upper().strip()

        print(f"      ✅ Actualizando producto con datos web:")
        print(f"         Nombre: {nombre_web_normalizado[:50]}")
        print(f"         EAN: {ean_web or 'N/A'}")

        # Actualizar productos_maestros_v2
        if ean_web:
            cursor.execute(
                """
                UPDATE productos_maestros_v2
                SET nombre_consolidado = %s,
                    codigo_ean = COALESCE(codigo_ean, %s),
                    marca = COALESCE(marca, %s),
                    confianza_datos = GREATEST(confianza_datos, 0.95),
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (nombre_web_normalizado, ean_web, marca_web or None, producto_id),
            )
        else:
            cursor.execute(
                """
                UPDATE productos_maestros_v2
                SET nombre_consolidado = %s,
                    marca = COALESCE(marca, %s),
                    confianza_datos = GREATEST(confianza_datos, 0.95),
                    fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (nombre_web_normalizado, marca_web or None, producto_id),
            )

        # Actualizar plu_supermercado_mapping con el producto_maestro_id
        supermercado_key = establecimiento.upper()
        for key in ["OLIMPICA", "CARULLA", "EXITO", "JUMBO"]:
            if key in supermercado_key:
                supermercado_key = key.lower()
                break

        cursor.execute(
            """
            UPDATE plu_supermercado_mapping
            SET producto_maestro_id = %s
            WHERE codigo_plu = %s AND LOWER(supermercado) = %s
        """,
            (producto_id, codigo, supermercado_key),
        )

        conn.commit()
        print(f"      ✅ Producto ID={producto_id} actualizado con datos web")
        return True

    except Exception as e:
        print(f"      ⚠️ Error actualizando con datos web: {e}")
        traceback.print_exc()
        return False


# ═══════════════════════════════════════════════════════════════
# 🎯 FASE 1.1: FUNCIÓN MEJORADA - BUSCAR PAPA PRIMERO
# ═══════════════════════════════════════════════════════════════


def buscar_papa_primero(
    codigo: str,
    establecimiento_id: int,
    cursor,
    conn,
    precio: int = None,
) -> Optional[Dict[str, Any]]:
    """
    🎯 FASE 1.1 V9.3: Busca si existe un PAPA para este código
    ✅ MEJORADO: Crea registro en productos_por_establecimiento si no existe
    ✅ MEJORADO: Actualiza TODOS los precios (min, max, actual)

    Retorna:
    - None si no existe PAPA
    - Dict con datos del PAPA si existe
    """
    if not codigo or not establecimiento_id:
        return None

    tipo_codigo = clasificar_codigo_tipo(codigo)

    try:
        # ESTRATEGIA 1: Buscar por EAN en PAPAS (JUMBO, ARA, D1)
        if tipo_codigo == "EAN":
            cursor.execute(
                """
                SELECT pm.id, pm.codigo_ean, pm.nombre_consolidado, pm.marca,
                       pm.categoria_id, pm.veces_visto
                FROM productos_maestros_v2 pm
                WHERE pm.es_producto_papa = TRUE
                  AND pm.codigo_ean = %s
                LIMIT 1
            """,
                (codigo,),
            )

            resultado = cursor.fetchone()

            if resultado:
                papa_id = resultado[0]
                print(f"   👑 PAPA ENCONTRADO por EAN: ID={papa_id}")
                print(f"      📝 Nombre PAPA: {resultado[2]}")
                print(f"      🏷️  Marca: {resultado[3] or 'N/A'}")
                print(f"      📊 Visto {resultado[5]} veces")

                # Actualizar estadísticas del PAPA
                cursor.execute(
                    """
                    UPDATE productos_maestros_v2
                    SET veces_visto = veces_visto + 1,
                        fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (papa_id,),
                )

                # ✅ V9.3: CREAR O ACTUALIZAR en productos_por_establecimiento
                if precio:
                    cursor.execute(
                        """
                        INSERT INTO productos_por_establecimiento (
                            producto_maestro_id, establecimiento_id, codigo_plu,
                            precio_actual, precio_unitario, precio_minimo, precio_maximo,
                            total_reportes, fecha_creacion, fecha_actualizacion, ultima_actualizacion
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (producto_maestro_id, establecimiento_id)
                        DO UPDATE SET
                            precio_actual = EXCLUDED.precio_actual,
                            precio_unitario = EXCLUDED.precio_unitario,
                            precio_minimo = LEAST(COALESCE(productos_por_establecimiento.precio_minimo, EXCLUDED.precio_minimo), EXCLUDED.precio_minimo),
                            precio_maximo = GREATEST(COALESCE(productos_por_establecimiento.precio_maximo, EXCLUDED.precio_maximo), EXCLUDED.precio_maximo),
                            total_reportes = productos_por_establecimiento.total_reportes + 1,
                            ultima_actualizacion = CURRENT_TIMESTAMP,
                            fecha_actualizacion = CURRENT_TIMESTAMP
                    """,
                        (
                            papa_id,
                            establecimiento_id,
                            codigo,  # EAN también se guarda como PLU
                            precio,
                            precio,
                            precio,
                            precio,
                        ),
                    )
                    print(
                        f"      💾 Precios actualizados en productos_por_establecimiento"
                    )

                conn.commit()

                return {
                    "papa_id": papa_id,
                    "codigo_ean": resultado[1],
                    "nombre_consolidado": resultado[2],
                    "marca": resultado[3],
                    "categoria_id": resultado[4],
                    "veces_visto": resultado[5] + 1,
                    "fuente": "papa_ean",
                }

        # ESTRATEGIA 2: Buscar por PLU en productos_por_establecimiento (OLÍMPICA, CARULLA, ÉXITO)
        elif tipo_codigo == "PLU":
            cursor.execute(
                """
                SELECT pm.id, pm.nombre_consolidado, pm.marca, pm.categoria_id,
                       pm.veces_visto, ppe.codigo_plu
                FROM productos_maestros_v2 pm
                JOIN productos_por_establecimiento ppe ON pm.id = ppe.producto_maestro_id
                WHERE pm.es_producto_papa = TRUE
                  AND ppe.codigo_plu = %s
                  AND ppe.establecimiento_id = %s
                LIMIT 1
            """,
                (codigo, establecimiento_id),
            )

            resultado = cursor.fetchone()

            if resultado:
                papa_id = resultado[0]
                print(f"   👑 PAPA ENCONTRADO por PLU: ID={papa_id}")
                print(f"      📝 Nombre PAPA: {resultado[1]}")
                print(f"      🏷️  Marca: {resultado[2] or 'N/A'}")
                print(f"      📌 PLU: {resultado[5]}")
                print(f"      📊 Visto {resultado[4]} veces")

                # Actualizar estadísticas del PAPA
                cursor.execute(
                    """
                    UPDATE productos_maestros_v2
                    SET veces_visto = veces_visto + 1,
                        fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (papa_id,),
                )

                # ✅ V9.3: ACTUALIZAR precios en productos_por_establecimiento
                if precio:
                    cursor.execute(
                        """
                        UPDATE productos_por_establecimiento
                        SET precio_actual = %s,
                            precio_unitario = %s,
                            precio_minimo = LEAST(COALESCE(precio_minimo, %s), %s),
                            precio_maximo = GREATEST(COALESCE(precio_maximo, %s), %s),
                            total_reportes = total_reportes + 1,
                            ultima_actualizacion = CURRENT_TIMESTAMP,
                            fecha_actualizacion = CURRENT_TIMESTAMP
                        WHERE producto_maestro_id = %s AND establecimiento_id = %s
                    """,
                        (
                            precio,
                            precio,
                            precio,
                            precio,
                            precio,
                            precio,
                            papa_id,
                            establecimiento_id,
                        ),
                    )
                    print(
                        f"      💾 Precios actualizados en productos_por_establecimiento"
                    )

                conn.commit()

                return {
                    "papa_id": papa_id,
                    "codigo_plu": resultado[5],
                    "nombre_consolidado": resultado[1],
                    "marca": resultado[2],
                    "categoria_id": resultado[3],
                    "veces_visto": resultado[4] + 1,
                    "fuente": "papa_plu",
                }

        # No se encontró PAPA
        return None

    except Exception as e:
        print(f"   ⚠️ Error buscando PAPA: {e}")
        traceback.print_exc()
        return None


# ═══════════════════════════════════════════════════════════════
# FUNCIÓN PRINCIPAL MODIFICADA - V9.4 CON ENRIQUECIMIENTO WEB
# ═══════════════════════════════════════════════════════════════


def buscar_o_crear_producto_inteligente(
    codigo: str,
    nombre: str,
    precio: int,
    establecimiento: str,
    cursor,
    conn,
    factura_id: int = None,
    usuario_id: int = None,
    item_factura_id: int = None,
    establecimiento_id: int = None,
) -> Optional[int]:
    """
    ⭐ VERSIÓN 9.5: Función principal con ACTUALIZACIÓN DE PRODUCTOS EXISTENTES

    FLUJO:
    ✅ PASO 0: Buscar PAPA (si existe)
    ✅ PASO 1: Buscar PLU exacto → Si encuentra, ACTUALIZA con datos web
    ✅ PASO 2: Buscar EAN → Si encuentra, ACTUALIZA con datos web
    ✅ PASO 3: Buscar por nombre similar → Si encuentra, ACTUALIZA con datos web
    🆕 PASO 3.5: ENRIQUECIMIENTO WEB (si es supermercado VTEX)
    ✅ PASO 4: Crear nuevo producto (con datos web si existen)
    """
    import os

    print(f"\n🔍 BUSCAR O CREAR PRODUCTO V9.5 (ACTUALIZA EXISTENTES CON WEB):")
    print(f"   Código: {codigo or 'Sin código'}")
    print(f"   Nombre OCR: {nombre[:50]}")
    print(f"   Precio: ${precio:,}")
    print(f"   Establecimiento: {establecimiento}")
    print(f"   Establecimiento ID: {establecimiento_id}")

    # Definir variables ANTES de usarlas
    nombre_normalizado = normalizar_nombre_producto(nombre, True)
    tipo_codigo = clasificar_codigo_tipo(codigo)
    cadena = detectar_cadena(establecimiento)

    is_postgresql = os.environ.get("DATABASE_TYPE") == "postgresql"
    param = "%s" if is_postgresql else "?"

    # ═══════════════════════════════════════════════════════════════
    # 🎯 PASO 0: BUSCAR PAPA PRIMERO
    # ═══════════════════════════════════════════════════════════════
    print(f"\n   🔍 PASO 0: Buscando PAPA...")
    papa_encontrado = buscar_papa_primero(
        codigo=codigo,
        establecimiento_id=establecimiento_id,
        cursor=cursor,
        conn=conn,
        precio=precio,
    )

    if papa_encontrado:
        print(f"   ✅ USANDO DATOS DEL PAPA")
        return papa_encontrado["papa_id"]

    print(f"   ℹ️  No existe PAPA → Continuar búsqueda normal")

    # ═══════════════════════════════════════════════════════════════
    # PASO 1: BUSCAR POR PLU EXACTO EN productos_por_establecimiento
    # ═══════════════════════════════════════════════════════════════
    if tipo_codigo == "PLU" and codigo and establecimiento_id:
        print(f"\n   🔍 PASO 1: Buscando PLU exacto...")
        try:
            cursor.execute(
                """
                SELECT producto_maestro_id
                FROM productos_por_establecimiento
                WHERE codigo_plu = %s AND establecimiento_id = %s
                LIMIT 1
            """,
                (codigo, establecimiento_id),
            )
            resultado = cursor.fetchone()

            if resultado:
                producto_id = resultado[0]
                print(
                    f"   ✅ Encontrado por PLU en productos_por_establecimiento: ID={producto_id}"
                )

                # Actualizar veces_visto y precio
                cursor.execute(
                    """
                    UPDATE productos_maestros_v2
                    SET veces_visto = veces_visto + 1,
                        fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (producto_id,),
                )

                # Actualizar precio en productos_por_establecimiento
                cursor.execute(
                    """
                    UPDATE productos_por_establecimiento
                    SET precio_actual = %s,
                        precio_unitario = %s,
                        precio_minimo = LEAST(COALESCE(precio_minimo, %s), %s),
                        precio_maximo = GREATEST(COALESCE(precio_maximo, %s), %s),
                        total_reportes = total_reportes + 1,
                        ultima_actualizacion = CURRENT_TIMESTAMP,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE producto_maestro_id = %s AND establecimiento_id = %s
                """,
                    (
                        precio,
                        precio,
                        precio,
                        precio,
                        precio,
                        precio,
                        producto_id,
                        establecimiento_id,
                    ),
                )

                conn.commit()

                # 🆕 V9.5: Actualizar con datos web si es supermercado VTEX
                actualizar_producto_con_datos_web(
                    producto_id=producto_id,
                    codigo=codigo,
                    nombre_ocr=nombre,
                    establecimiento=establecimiento,
                    precio=precio,
                    cursor=cursor,
                    conn=conn,
                    establecimiento_id=establecimiento_id,
                )

                return producto_id
        except Exception as e:
            print(f"   ⚠️ Error buscando PLU en productos_por_establecimiento: {e}")

    # ═══════════════════════════════════════════════════════════════
    # PASO 2: BUSCAR POR EAN EXISTENTE EN V2
    # ═══════════════════════════════════════════════════════════════
    if tipo_codigo == "EAN" and codigo:
        print(f"\n   🔍 PASO 2: Buscando EAN en V2...")
        try:
            cursor.execute(
                f"SELECT id, nombre_consolidado FROM productos_maestros_v2 WHERE codigo_ean = {param}",
                (codigo,),
            )
            resultado = cursor.fetchone()

            if resultado and len(resultado) >= 1:
                producto_id = resultado[0]
                print(f"   ✅ Encontrado por EAN en V2: ID={producto_id}")

                # Actualizar veces_visto
                cursor.execute(
                    """
                    UPDATE productos_maestros_v2
                    SET veces_visto = veces_visto + 1,
                        fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = %s
                """,
                    (producto_id,),
                )
                conn.commit()

                # También guardar en productos_por_establecimiento si tiene establecimiento_id
                if establecimiento_id:
                    guardar_plu_en_establecimiento(
                        cursor, conn, producto_id, establecimiento_id, codigo, precio
                    )

                # 🆕 V9.5: Actualizar con datos web si es supermercado VTEX
                actualizar_producto_con_datos_web(
                    producto_id=producto_id,
                    codigo=codigo,
                    nombre_ocr=nombre,
                    establecimiento=establecimiento,
                    precio=precio,
                    cursor=cursor,
                    conn=conn,
                    establecimiento_id=establecimiento_id,
                )

                return producto_id
        except Exception as e:
            print(f"   ⚠️ Error buscando por EAN: {e}")

    try:
        # ═══════════════════════════════════════════════════════════════
        # PASO 3: BUSCAR POR NOMBRE SIMILAR EN V2
        # ═══════════════════════════════════════════════════════════════
        if not codigo or tipo_codigo == "DESCONOCIDO":
            print(f"\n   🔍 PASO 3: Buscando por nombre similar...")
            try:
                search_pattern = f"%{nombre_normalizado[:50]}%"
                cursor.execute(
                    f"""
                    SELECT id, nombre_consolidado, codigo_ean
                    FROM productos_maestros_v2
                    WHERE nombre_consolidado {('ILIKE' if is_postgresql else 'LIKE')} {param}
                    LIMIT 10
                """,
                    (search_pattern,),
                )

                candidatos = cursor.fetchall()

                for candidato in candidatos:
                    if not candidato or len(candidato) < 3:
                        continue

                    cand_id = candidato[0]
                    cand_nombre = candidato[1]

                    if not cand_id or not cand_nombre:
                        continue

                    similitud = calcular_similitud(nombre_normalizado, cand_nombre)

                    if similitud >= 0.90:
                        producto_id = cand_id
                        print(
                            f"   ✅ Encontrado por similitud en V2: ID={producto_id} (sim={similitud:.2f})"
                        )

                        # Actualizar veces_visto
                        cursor.execute(
                            """
                            UPDATE productos_maestros_v2
                            SET veces_visto = veces_visto + 1,
                                fecha_ultima_actualizacion = CURRENT_TIMESTAMP
                            WHERE id = %s
                        """,
                            (producto_id,),
                        )
                        conn.commit()

                        # Guardar PLU si existe
                        if codigo and establecimiento_id:
                            guardar_plu_en_establecimiento(
                                cursor,
                                conn,
                                producto_id,
                                establecimiento_id,
                                codigo,
                                precio,
                            )

                        # 🆕 V9.5: Actualizar con datos web si es supermercado VTEX
                        actualizar_producto_con_datos_web(
                            producto_id=producto_id,
                            codigo=codigo,
                            nombre_ocr=nombre,
                            establecimiento=establecimiento,
                            precio=precio,
                            cursor=cursor,
                            conn=conn,
                            establecimiento_id=establecimiento_id,
                        )

                        return producto_id

            except Exception as e:
                print(f"   ⚠️ Error buscando por similitud: {e}")
                traceback.print_exc()

        # ═══════════════════════════════════════════════════════════════
        # 🆕 PASO 3.5: ENRIQUECIMIENTO WEB (VTEX)
        # ═══════════════════════════════════════════════════════════════
        datos_web = None

        if WEB_ENRICHER_AVAILABLE and codigo:
            print(f"\n   🌐 PASO 3.5: Enriquecimiento Web...")

            # Verificar si es supermercado VTEX
            if es_supermercado_vtex(establecimiento):
                try:
                    enricher = WebEnricher(cursor, conn)
                    resultado_web = enricher.enriquecer(
                        codigo=codigo,
                        nombre_ocr=nombre,
                        establecimiento=establecimiento,
                        precio_ocr=precio,
                    )

                    if resultado_web.encontrado:
                        datos_web = resultado_web.to_dict()
                        print(f"      ✅ Datos web obtenidos:")
                        print(f"         Nombre: {datos_web['nombre_web'][:50]}")
                        print(f"         EAN: {datos_web['codigo_ean'] or 'N/A'}")
                        print(f"         Marca: {datos_web['marca'] or 'N/A'}")
                        print(f"         Fuente: {datos_web['fuente']}")

                        # 🆕 Si obtuvimos EAN del web y no teníamos, buscar si ya existe
                        if datos_web.get("codigo_ean") and tipo_codigo == "PLU":
                            ean_web = datos_web["codigo_ean"]
                            cursor.execute(
                                f"SELECT id FROM productos_maestros_v2 WHERE codigo_ean = {param}",
                                (ean_web,),
                            )
                            existe = cursor.fetchone()

                            if existe:
                                producto_id = existe[0]
                                print(
                                    f"      ✅ Producto encontrado por EAN web: ID={producto_id}"
                                )

                                # Guardar PLU en productos_por_establecimiento
                                if establecimiento_id:
                                    guardar_plu_en_establecimiento(
                                        cursor,
                                        conn,
                                        producto_id,
                                        establecimiento_id,
                                        codigo,
                                        precio,
                                    )

                                return producto_id
                    else:
                        print(f"      ℹ️ No se encontró en web")

                except Exception as e:
                    print(f"      ⚠️ Error en enriquecimiento web: {e}")
            else:
                print(f"      ℹ️ {establecimiento} no es supermercado VTEX")

        # ═══════════════════════════════════════════════════════════════
        # PASO 4: NO ENCONTRADO → VALIDAR Y CREAR EN V2
        # ═══════════════════════════════════════════════════════════════
        print(f"\n   📝 PASO 4: Creando producto nuevo...")

        aprendizaje_mgr = None

        if APRENDIZAJE_AVAILABLE:
            try:
                aprendizaje_mgr = AprendizajeManager(cursor, conn)
            except Exception as e:
                print(f"   ⚠️ Error AprendizajeManager: {e}")

        # 🆕 V9.4: Pasar datos_web a la validación
        resultado_validacion = validar_nombre_con_sistema_completo(
            nombre_ocr_original=nombre,
            nombre_corregido=nombre_normalizado,
            precio=precio,
            establecimiento=cadena,
            codigo=codigo,
            aprendizaje_mgr=aprendizaje_mgr,
            factura_id=factura_id,
            usuario_id=usuario_id,
            item_factura_id=item_factura_id,
            cursor=cursor,
            establecimiento_id=establecimiento_id,
            datos_web=datos_web,  # 🆕 Pasar datos del web enricher
        )

        nombre_final = resultado_validacion["nombre_final"]
        marca_final = resultado_validacion.get("marca")

        # 🆕 Si obtuvimos EAN del web, usarlo
        codigo_ean_final = None
        if tipo_codigo == "EAN":
            codigo_ean_final = codigo
        elif datos_web and datos_web.get("codigo_ean"):
            codigo_ean_final = datos_web["codigo_ean"]
            print(f"   🔗 Usando EAN del web: {codigo_ean_final}")

        print(f"   📊 Fuente: {resultado_validacion['fuente']}")
        print(f"   🎯 Confianza: {resultado_validacion['confianza']:.0%}")

        # CREAR EN productos_maestros_v2
        producto_id = crear_producto_en_v2(
            cursor=cursor,
            conn=conn,
            nombre_normalizado=nombre_final,
            codigo_ean=codigo_ean_final,
            marca=marca_final,
        )

        if not producto_id:
            print(f"   ❌ SKIP: No se pudo crear '{nombre_final}'")
            return None

        # GUARDAR PLU EN productos_por_establecimiento
        if codigo and establecimiento_id:
            guardar_plu_en_establecimiento(
                cursor, conn, producto_id, establecimiento_id, codigo, precio
            )

        if resultado_validacion.get("necesita_revision", False) and producto_id:
            try:
                marcar_para_revision_admin(
                    cursor=cursor,
                    conn=conn,
                    producto_maestro_id=producto_id,
                    nombre_ocr=nombre,
                    nombre_sugerido=nombre_final,
                    codigo=codigo or "",
                    establecimiento=cadena,
                    razon=resultado_validacion.get("razon_revision", "Sin especificar"),
                )
            except Exception as e:
                print(f"      ⚠️ No se pudo marcar para revisión: {e}")

        print(f"   ✅ Producto nuevo creado en V2: ID={producto_id}")
        return producto_id

    except Exception as e:
        print(f"   ❌ ERROR CRÍTICO en buscar_o_crear_producto_inteligente: {e}")
        traceback.print_exc()
        return None


# ═══════════════════════════════════════════════════════════════
# MENSAJE DE CARGA
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("✅ product_matcher.py V9.5 - ACTUALIZACIÓN DE PRODUCTOS CON DATOS WEB")
print("=" * 80)
print("🎯 CAMBIOS V9.5:")
print("   🆕 Actualiza productos EXISTENTES con datos del web")
print("   🆕 Cuando encuentra producto → Consulta web → Actualiza nombre/EAN")
print("   🆕 Prioriza nombre del web sobre nombre OCR")
print("=" * 80)
print("🎯 CAMBIOS V9.4 (heredados):")
print("   ✅ PASO 3.5: Enriquecimiento Web antes de crear producto")
print("   ✅ Busca en cache local (plu_supermercado_mapping) primero")
print("   ✅ Si no hay cache → Consulta API VTEX")
print("   ✅ Usa nombre WEB (correcto) en lugar del OCR (con errores)")
print("   ✅ Obtiene EAN del PLU automáticamente")
print("   ✅ Guarda en cache para futuras consultas")
print("=" * 80)
print("🎯 CAMBIOS V9.3 (heredados):")
print("   ✅ Busca PAPAS PRIMERO (antes de crear productos)")
print("   ✅ CREA registro en productos_por_establecimiento si no existe")
print("   ✅ ACTUALIZA precios SIEMPRE (min, max, actual)")
print("=" * 80)
print(f"{'✅' if APRENDIZAJE_AVAILABLE else '⚠️ '} Aprendizaje Automático")
print(f"{'✅' if WEB_ENRICHER_AVAILABLE else '⚠️ '} Web Enricher (VTEX)")
print("=" * 80)
