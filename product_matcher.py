"""
============================================================================
PRODUCT MATCHER V10.0 - CON VALIDACIÓN DE AUDITORÍA
============================================================================
Versión: 10.0
Fecha: 2025-11-29

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
UMBRAL_SIMILITUD_AUDITORIA = 0.80  # 80% para match con auditoría (más permisivo)

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
    Útil cuando el OCR no tiene EAN pero el nombre coincide.
    """
    if not nombre_ocr or len(nombre_ocr) < 3:
        return None

    try:
        nombre_limpio = limpiar_nombre(nombre_ocr)

        # Buscar candidatos
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
            ORDER BY validaciones DESC
            LIMIT 100
        """
        )

        mejor_match = None
        mejor_similitud = 0

        for row in cursor.fetchall():
            similitud = calcular_similitud(nombre_limpio, row[2])
            if similitud > mejor_similitud and similitud >= umbral:
                mejor_similitud = similitud
                mejor_match = {
                    "referencia_id": row[0],
                    "codigo_ean": row[1],
                    "nombre": row[2],
                    "marca": row[3],
                    "presentacion": row[4],
                    "categoria": row[5],
                    "validaciones": row[6],
                    "similitud": similitud,
                    "fuente": "AUDITORIA_NOMBRE",
                    "confianza": 0.90 * similitud,
                }

        if mejor_match:
            print(
                f"   📱 [PASO 2] Auditoría por nombre ({mejor_similitud:.0%}): {mejor_match['nombre']}"
            )

        return mejor_match

    except Exception as e:
        print(f"   ⚠️ Error buscando en auditoría por nombre: {e}")

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

    # Buscar por nombre similar
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
print("✅ PRODUCT MATCHER V10.0 CARGADO")
print("   Flujo de validación:")
print("   1. PAPA (100%)")
print("   2. AUDITORÍA - EAN escaneado (95%)")
print("   3. WEB + Validación auditoría (80-95%)")
print("   4. CACHE VTEX (70%)")
print("   5. OCR (50%)")
print("=" * 60)
